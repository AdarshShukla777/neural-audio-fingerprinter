import asyncio
import uuid
import numpy as np
import logging
from pathlib import Path
from typing import Optional
from fastapi import APIRouter, Depends, UploadFile, File, HTTPException, Request
from fastapi.responses import FileResponse

from api.dependencies import get_milvus_col, get_processor, cleaner, sanitize_filename, truncate_song_name
from api.config import MAX_FILE_SIZE_MB, TEMP_DIR, REQUEST_TIMEOUT, CSV_LOG_FILE
from core.mbid_utils import extract_mbid
from core.database import fetch_song_metadata
from core.tasks import track_ingestion_safe
from data_pipeline.api_client import send_song_to_bulk_api

logger = logging.getLogger("AudioSystem")
router = APIRouter(prefix="/fastapi-milvus")

@router.post("/upload")
async def upload_song(request: Request, file: UploadFile = File(...), song_name: Optional[str] = None, col = Depends(get_milvus_col), processor = Depends(get_processor)):
    if not song_name:
        song_name = Path(file.filename).stem
    
    file.file.seek(0, 2)
    file_size = file.file.tell()
    file.file.seek(0)
    
    if file_size > MAX_FILE_SIZE_MB * 1024 * 1024:
        raise HTTPException(413, f"File too large (max {MAX_FILE_SIZE_MB}MB)")
    
    song_name = truncate_song_name(song_name)
    safe_name = sanitize_filename(file.filename)
    temp_path = TEMP_DIR / f"upload_{uuid.uuid4().hex[:6]}_{safe_name}"
    
    try:
        await cleaner.register(temp_path)
        
        async with asyncio.timeout(REQUEST_TIMEOUT):
            with open(temp_path, "wb") as buffer:
                content = await file.read()
                buffer.write(content)
            
            mbid, mbid_time = extract_mbid(str(temp_path))
            if not mbid:
                track_ingestion_safe(song_name, f"Upload: {safe_name}", f"Failed: No MBID found")
                return {"status": "failed", "detail": "No MBID found", "mbid_lookup_time": mbid_time}
            
            try:
                if col.query(expr=f'mbid == "{mbid}"', output_fields=["mbid"], limit=1):
                    track_ingestion_safe(song_name, f"Upload: {safe_name}", f"Skipped: MBID {mbid} already exists")
                    return {"status": "skipped", "message": f"Song with MBID {mbid} already exists", "mbid": mbid, "song_name": song_name}
            except Exception as e:
                logger.warning(f"Could not verify MBID uniqueness: {e}")
            
            metadata = await fetch_song_metadata(mbid)
            if not metadata:
                track_ingestion_safe(song_name, f"Upload: {safe_name}", f"Failed: No metadata in DB for MBID {mbid}")
                return {"status": "failed", "detail": f"No metadata found for MBID: {mbid}", "mbid": mbid, "mbid_lookup_time": mbid_time}
            
            # Triton Call
            try:
                with open(temp_path, "rb") as f:
                    fingerprints = processor.process_audio(f.read())
            except Exception as e:
                logger.error(f"Error during audio processing: {e}")
                fingerprints = []
            
            if not fingerprints:
                track_ingestion_safe(song_name, f"Upload: {safe_name}", "Failed: Audio too short or corrupted")
                return {"status": "failed", "detail": "Audio too short", "mbid": mbid, "metadata": metadata}
            
            vectors = [np.array(f["vector"]).tolist() for f in fingerprints]
            mbids = [mbid] * len(fingerprints)
            offsets = [float(f["offset"]) for f in fingerprints]
            flags = [True] * len(fingerprints)
            
            try:
                col.insert([vectors, mbids, offsets, flags])
                col.flush()
            except Exception as milvus_error:
                track_ingestion_safe(song_name, f"Upload: {safe_name}", f"Failed: Milvus insert error - {milvus_error}")
                return {"status": "failed", "detail": f"Milvus insert failed: {milvus_error}", "mbid": mbid, "metadata": metadata}
            
            genres = [g.strip() for g in str(metadata.get("genre", "")).split(",") if g.strip()] if metadata.get("genre") else []
            
            song_payload = {
                "title": metadata["title"], "artist": metadata["artist"], "album": metadata["album"],
                "duration": metadata["duration"], "milvus_embedding_id": mbid, "genres": genres, "is_mbid_present": True
            }
            
            api_sent = await send_song_to_bulk_api(song_payload)
            if not api_sent:
                track_ingestion_safe(song_name, f"Upload: {safe_name}", f"Partial: In Milvus but API failed (mbid={mbid})")
                return {"status": "partial_success", "detail": "Stored in Milvus but failed to send to bulk API", "song_name": song_name, "vectors": len(fingerprints), "mbid": mbid, "milvus_embedding_id": mbid, "metadata": metadata, "api_sent": False}
            
            track_ingestion_safe(song_name, f"Upload: {safe_name}", f"Success (mbid={mbid}, api=sent)")
            
            return {
                "status": "success", "song_name": song_name, "vectors": len(fingerprints), "mbid": mbid,
                "mbid_lookup_time": mbid_time, "milvus_embedding_id": mbid, "metadata_found": True, "api_sent": True, "metadata": metadata
            }
            
    except asyncio.TimeoutError:
        logger.error(f"Upload timeout for {song_name}")
        raise HTTPException(504, "Processing timeout")
    except Exception as e:
        logger.exception(f"Upload error: {e}")
        track_ingestion_safe(song_name, f"Upload: {safe_name}", f"Error: {str(e)}")
        raise HTTPException(500, f"Processing failed: {str(e)}")
    finally:
        await cleaner.cleanup_file(temp_path)

@router.get("/download_logs")
def download_csv_logs():
    import os
    if not os.path.exists(CSV_LOG_FILE):
        return {"error": "No log file found. Run a batch job first."}
    return FileResponse(path=CSV_LOG_FILE, filename="uploaded_songs.csv", media_type='text/csv')
