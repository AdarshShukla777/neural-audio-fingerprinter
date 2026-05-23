import asyncio
import time
import uuid
import numpy as np
import logging
from typing import Optional
from fastapi import APIRouter, Depends, UploadFile, File, HTTPException, Request

from api.dependencies import get_milvus_col, get_processor, cleaner, sanitize_filename
from api.config import MAX_FILE_SIZE_MB, TEMP_DIR, REQUEST_TIMEOUT

logger = logging.getLogger("AudioSystem")
router = APIRouter(prefix="/fastapi-milvus")

@router.post("/search")
async def search_song(audio: UploadFile = File(...), col = Depends(get_milvus_col), processor = Depends(get_processor)):
    file = audio
    
    file.file.seek(0, 2)
    file_size = file.file.tell()
    file.file.seek(0)
    
    if file_size > MAX_FILE_SIZE_MB * 1024 * 1024:
        raise HTTPException(413, "Query file too large")
    
    safe_name = sanitize_filename(file.filename)
    temp_path = TEMP_DIR / f"q_{uuid.uuid4().hex}_{safe_name}"
    
    try:
        await cleaner.register(temp_path)
        
        async with asyncio.timeout(REQUEST_TIMEOUT):
            with open(temp_path, "wb") as buffer:
                content = await file.read()
                buffer.write(content)
            
            t0 = time.perf_counter()
            with open(temp_path, "rb") as f:
                query_data = processor.process_audio(f.read())
            logger.info(f"🎧 Fingerprinting time: {time.perf_counter() - t0:.3f}s")
            
            if not query_data:
                return {
                    "status": "success", "message": "No audio fingerprints could be generated",
                    "exact_match_song_id": None, "similar_songs_ids": []
                }
            
            query_vectors = [np.array(q["vector"]).tolist() for q in query_data]
            total_segments = len(query_vectors)
            
            search_params = {"metric_type": "COSINE", "params": {"nprobe": 10}}
            results = col.search(
                data=query_vectors, anns_field="embedding", param=search_params, limit=3, output_fields=["mbid"]
            )
            
            song_stats = {}
            VECTOR_SIMILARITY_THRESHOLD = 0.60
            
            for hits in results:
                if not hits: continue
                top_hit = hits[0]
                if top_hit.distance > VECTOR_SIMILARITY_THRESHOLD:
                    mbid = top_hit.entity.get("mbid")
                    if mbid not in song_stats:
                        song_stats[mbid] = {"count": 0, "total_distance": 0.0}
                    song_stats[mbid]["count"] += 1
                    song_stats[mbid]["total_distance"] += top_hit.distance
            
            ranked_songs = []
            for mbid, stats in song_stats.items():
                match_percentage = (stats["count"] / total_segments) * 100
                ranked_songs.append({
                    "mbid": mbid, "match_percentage": round(match_percentage, 2),
                    "match_count": stats["count"], "total_segments": total_segments
                })
            
            ranked_songs.sort(key=lambda x: x["match_percentage"], reverse=True)
            
            EXACT_MATCH_CONFIDENCE = 40.0
            TOP_K = 5
            
            response = {"status": "success", "message": "", "exact_match_song_id": None, "similar_songs_ids": []}
            
            if ranked_songs:
                top_song = ranked_songs[0]
                is_exact = top_song["match_percentage"] >= EXACT_MATCH_CONFIDENCE
                if is_exact:
                    response["exact_match_song_id"] = top_song["mbid"]
                    response["similar_songs_ids"] = [song["mbid"] for song in ranked_songs[1:TOP_K + 1]]
                    response["message"] = f"Exact match found with {top_song['match_percentage']}% confidence"
                else:
                    response["exact_match_song_id"] = None
                    response["similar_songs_ids"] = [song["mbid"] for song in ranked_songs[:TOP_K]]
                    response["message"] = f"No exact match. Top result has {top_song['match_percentage']}% match"
            else:
                response["message"] = "No matches found"
            
            return response
            
    except asyncio.TimeoutError:
        raise HTTPException(504, "Search timeout")
    except Exception as e:
        logger.exception(f"Search error: {e}")
        raise HTTPException(500, "Search failed")
    finally:
        await cleaner.cleanup_file(temp_path)

@router.get("/stats")
async def get_milvus_stats(mbid: Optional[str] = None, col = Depends(get_milvus_col)):
    try:
        if mbid:
            results = col.query(
                expr=f'mbid == "{mbid}"', output_fields=["mbid", "offsets", "is_mbid_present"], limit=10000
            )
            if not results:
                raise HTTPException(404, f"MBID '{mbid}' not found")
            offsets = sorted([float(r.get("offsets", 0)) for r in results])
            return {
                "mbid": mbid, "vector_count": len(results), "is_mbid_present": results[0].get("is_mbid_present", False),
                "offsets": {"min": offsets[0] if offsets else 0, "max": offsets[-1] if offsets else 0, "sample": offsets[:10]}
            }
        
        total_vectors = col.num_entities
        all_mbids = set()
        offset = 0
        while True:
            results = col.query(expr="mbid != ''", output_fields=["mbid"], limit=1000, offset=offset)
            if not results: break
            all_mbids.update(r.get("mbid") for r in results if r.get("mbid"))
            if len(results) < 1000: break
            offset += 1000
        
        unique_songs = len(all_mbids)
        return {
            "total_unique_songs": unique_songs, "total_vectors": total_vectors,
            "avg_vectors_per_song": round(total_vectors / unique_songs, 2) if unique_songs > 0 else 0
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"❌ Stats error: {e}")
        raise HTTPException(500, str(e))
