# api_client.py
import os
import httpx
import logging
from typing import List, Dict

logger = logging.getLogger(__name__)

# API Configuration
BULK_UPLOAD_API_URL = os.getenv("BULK_UPLOAD_API_URL")


async def send_song_to_bulk_api(song_data: Dict) -> bool:
    """
    Send single song metadata to bulk upload API.
    
    Expected song_data format:
    {
        "title": str,
        "artist": str,
        "album": str,
        "duration": int,
        "milvus_embedding_id": str,
        "genres": List[str],
        "is_mbid_present": bool
    }
    
    Returns True if successful, False otherwise.
    """
    try:
        # Wrap single song in array as API expects bulk format
        payload = [song_data]
        
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                BULK_UPLOAD_API_URL,
                json=payload,
                headers={"Content-Type": "application/json"}
            )
            
            if response.status_code in [200, 201]:
                logger.info(f"✅ Sent song '{song_data['title']}' to bulk API")
                return True
            else:
                logger.error(
                    f"❌ Bulk API error {response.status_code}: {response.text}"
                )
                return False
                
    except Exception as e:
        logger.exception(f"Failed to send song to bulk API: {e}")
        return False


def send_batch_to_bulk_api_sync(songs_data: List[Dict]) -> bool:
    """
    Send multiple songs metadata to bulk upload API (synchronous version for worker processes).
    
    Expected songs_data format: List of dicts with:
    {
        "title": str,
        "artist": str,
        "album": str,
        "duration": int,
        "milvus_embedding_id": str,
        "genres": List[str],
        "is_mbid_present": bool
    }
    
    Returns True if successful, False otherwise.
    """
    if not songs_data:
        return True
    
    try:
        
        
        with httpx.Client(timeout=60.0) as client:
            response = client.post(
                BULK_UPLOAD_API_URL,
                json=songs_data,
                headers={"Content-Type": "application/json"}
            )
            
            if response.status_code in [200, 201]:
                logger.info(f"✅ Sent {len(songs_data)} songs to bulk API")
                return True
            else:
                logger.error(
                    f"❌ Bulk API error {response.status_code}: {response.text}"
                )
                return False
                
    except Exception as e:
        logger.exception(f"Failed to send batch to bulk API: {e}")
        return False
