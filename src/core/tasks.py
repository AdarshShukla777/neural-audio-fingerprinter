import os
import csv
import logging
import uuid
import asyncio
import boto3
import numpy as np
from pathlib import Path
from tqdm import tqdm
from botocore.exceptions import ClientError
from celery.utils.log import get_task_logger

from core.celery_app import app
from inference.engine import FingerprintGenerator
from core.mbid_utils import extract_mbid
from core.database import fetch_song_metadata
from core.milvus_client import get_milvus_collection
from data_pipeline.api_client import send_batch_to_bulk_api_sync
from api.config import TEMP_DIR, MAX_FILE_SIZE_MB, BATCH_INSERT_SIZE, CSV_LOG_FILE, USE_ACOUSTID
from api.dependencies import truncate_song_name, sanitize_filename

logger = get_task_logger(__name__)

# Initialize Triton client per worker process
processor = FingerprintGenerator()

def track_ingestion_safe(song_name: str, source: str, status: str):
    song_name = truncate_song_name(song_name)
    file_exists = os.path.isfile(CSV_LOG_FILE)
    try:
        with open(CSV_LOG_FILE, mode='a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(["Song Name", "Source", "Status"])
            writer.writerow([song_name, source, status])
    except Exception as e:
        logger.exception(f"Failed to log {song_name}: {e}")

@app.task(bind=True, name="core.tasks.process_audio_task")
def process_audio_task(self, file_path: str):
    try:
        with open(file_path, "rb") as f:
            return processor.process_audio(f.read())
    except Exception as e:
        logger.exception(f"Process audio error: {e}")
        return None

@app.task(bind=True, name="core.tasks.process_local_batch")
def process_local_batch(self, folder_path: str, limit: int = None):
    folder = Path(folder_path)
    files = [f for f in folder.iterdir() if f.suffix.lower() in {'.mp3', '.wav', '.flac', '.m4a'}]
    if limit:
        files = files[:limit]
    
    logger.info(f"🚀 Celery: Starting local batch job for {len(files)} files")
    
    db_col = get_milvus_collection()
    
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    success, failed, skipped = 0, 0, 0
    insert_batch = {"vectors": [], "mbids": [], "offsets": [], "is_mbid_present": []}
    api_batch = []
    
    for file_path in tqdm(files, desc="Local Batch", ncols=100):
        raw_song_name = file_path.stem
        song_name = truncate_song_name(raw_song_name)
        
        try:
            mbid, _ = extract_mbid(str(file_path))
            if not mbid:
                track_ingestion_safe(song_name, str(file_path), "Failed: No MBID")
                failed += 1
                continue
            
            try:
                expr = f'mbid == "{mbid}"'
                if db_col.query(expr=expr, output_fields=["mbid"], limit=1):
                    track_ingestion_safe(song_name, str(file_path), "Skipped: MBID exists")
                    skipped += 1
                    continue
            except: pass
            
            metadata = None
            try:
                metadata = loop.run_until_complete(fetch_song_metadata(mbid))
            except: pass
            if not metadata:
                metadata = {"title": song_name, "artist": "Unknown Artist", "album": "Unknown Album", "genre": None, "duration": 0}
            
            with open(file_path, "rb") as f:
                fingerprints = processor.process_audio(f.read())
            
            if not fingerprints:
                track_ingestion_safe(song_name, str(file_path), "Failed: No fingerprints")
                failed += 1
                continue
            
            vectors = [np.array(f["vector"]).tolist() for f in fingerprints]
            mbids = [mbid] * len(fingerprints)
            offsets = [float(f["offset"]) for f in fingerprints]
            flags = [True] * len(fingerprints)
            
            insert_batch["vectors"].extend(vectors)
            insert_batch["mbids"].extend(mbids)
            insert_batch["offsets"].extend(offsets)
            insert_batch["is_mbid_present"].extend(flags)
            
            genres = [g.strip() for g in str(metadata.get("genre", "")).split(",") if g.strip()] if metadata.get("genre") else []
            api_batch.append({
                "title": metadata["title"], "artist": metadata["artist"], "album": metadata["album"],
                "duration": metadata["duration"], "milvus_embedding_id": mbid, "genres": genres, "is_mbid_present": True
            })
            
            if len(insert_batch["vectors"]) >= BATCH_INSERT_SIZE:
                db_col.insert([insert_batch["vectors"], insert_batch["mbids"], insert_batch["offsets"], insert_batch["is_mbid_present"]])
                db_col.flush()
                insert_batch = {"vectors": [], "mbids": [], "offsets": [], "is_mbid_present": []}
            if len(api_batch) >= 10:
                send_batch_to_bulk_api_sync(api_batch)
                api_batch = []
            
            track_ingestion_safe(song_name, str(file_path), f"Success (mbid={mbid})")
            success += 1
        except Exception as e:
            track_ingestion_safe(song_name, str(file_path), f"Error: {e}")
            failed += 1
    
    if insert_batch["vectors"]:
        try:
            db_col.insert([insert_batch["vectors"], insert_batch["mbids"], insert_batch["offsets"], insert_batch["is_mbid_present"]])
            db_col.flush()
        except: pass
    if api_batch:
        send_batch_to_bulk_api_sync(api_batch)
    
    return {"status": "success", "processed": success, "failed": failed, "skipped": skipped}

@app.task(bind=True, name="core.tasks.worker_s3_job")
def worker_s3_job(self, bucket: str, prefix: str, limit: int = None):
    logger.info(f"🚀 Celery: Starting S3 job: s3://{bucket}/{prefix}")
    
    s3 = boto3.client('s3')
    db_col = get_milvus_collection()
    
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    try:
        paginator = s3.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)
    except ClientError as e:
        return {"status": "error", "message": "S3 access denied"}
    
    all_keys = []
    for page in page_iterator:
        for obj in page.get("Contents", []):
            if obj["Key"].lower().endswith(('.mp3', '.wav', '.flac', '.m4a')):
                all_keys.append(obj["Key"])
    
    success_count, failed_count, skipped_count = 0, 0, 0
    insert_batch = {"vectors": [], "mbids": [], "offsets": [], "is_mbid_present": []}
    api_batch = []

    for key in all_keys:
        if limit and success_count >= limit: break
        song_name = truncate_song_name(Path(key).stem)
        local_path = TEMP_DIR / f"s3worker_{uuid.uuid4().hex}_{sanitize_filename(key)}"
        
        mbid = None
        if not USE_ACOUSTID:
            from api.workers import extract_mbid_from_filename
            mbid = extract_mbid_from_filename(key)
            if mbid:
                try:
                    if db_col.query(expr=f'mbid == "{mbid}"', output_fields=["mbid"], limit=1):
                        track_ingestion_safe(song_name, key, "Skipped: MBID exists")
                        skipped_count += 1
                        continue
                except: pass
        
        try:
            s3.download_file(bucket, key, str(local_path))
            if local_path.stat().st_size / (1024 * 1024) > MAX_FILE_SIZE_MB:
                track_ingestion_safe(song_name, key, "Failed: Too Large")
                failed_count += 1
                continue
            
            if USE_ACOUSTID and not mbid:
                mbid, _ = extract_mbid(str(local_path))
            
            if not mbid:
                track_ingestion_safe(song_name, key, "Failed: No MBID")
                failed_count += 1
                continue
            
            metadata = None
            try: metadata = loop.run_until_complete(fetch_song_metadata(mbid))
            except: pass
            if not metadata:
                metadata = {"title": song_name, "artist": "Unknown Artist", "album": "Unknown Album", "genre": None, "duration": 0}
            
            with open(local_path, "rb") as f:
                fingerprints = processor.process_audio(f.read())
            
            if not fingerprints:
                track_ingestion_safe(song_name, key, "Failed: No Fingerprints")
                failed_count += 1
                continue
            
            vectors = [np.array(f["vector"]).tolist() for f in fingerprints]
            mbids = [mbid] * len(fingerprints)
            offsets = [float(f["offset"]) for f in fingerprints]
            flags = [True] * len(fingerprints)

            insert_batch["vectors"].extend(vectors)
            insert_batch["mbids"].extend(mbids)
            insert_batch["offsets"].extend(offsets)
            insert_batch["is_mbid_present"].extend(flags)
            
            genres = [g.strip() for g in str(metadata.get("genre", "")).split(",") if g.strip()] if metadata.get("genre") else []
            api_batch.append({
                "title": metadata["title"], "artist": metadata["artist"], "album": metadata["album"],
                "duration": metadata["duration"], "milvus_embedding_id": mbid, "genres": genres, "is_mbid_present": True
            })
            
            if len(insert_batch["vectors"]) >= BATCH_INSERT_SIZE:
                db_col.insert([insert_batch["vectors"], insert_batch["mbids"], insert_batch["offsets"], insert_batch["is_mbid_present"]])
                db_col.flush()
                insert_batch = {"vectors": [], "mbids": [], "offsets": [], "is_mbid_present": []}
            
            if len(api_batch) >= 10:
                send_batch_to_bulk_api_sync(api_batch)
                api_batch = []
            
            track_ingestion_safe(song_name, key, f"Success (mbid={mbid})")
            success_count += 1
        
        except Exception as e:
            track_ingestion_safe(song_name, key, f"Error: {e}")
            failed_count += 1
        finally:
            if local_path.exists(): local_path.unlink()
            
    if insert_batch["vectors"]:
        try:
            db_col.insert([insert_batch["vectors"], insert_batch["mbids"], insert_batch["offsets"], insert_batch["is_mbid_present"]])
            db_col.flush()
        except: pass
    if api_batch:
        send_batch_to_bulk_api_sync(api_batch)
    
    return {"status": "success", "processed": success_count, "failed": failed_count, "skipped": skipped_count}
