import os
import logging
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import JSONResponse

from api.dependencies import get_milvus_col, cleaner, get_processor
from core.database import test_connection
from core.milvus_client import reset_collection
from api.config import CSV_LOG_FILE

logger = logging.getLogger("AudioSystem")
router = APIRouter(prefix="/fastapi-milvus")

@router.get("/health")
async def health_check(col = Depends(get_milvus_col)):
    try:
        track_count = col.num_entities 
        temp_count = len(cleaner.files)
        return {
            "status": "online",
            "entities": track_count,
            "temp_files_tracked": temp_count
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return JSONResponse(
            status_code=503,
            content={
                "status": "offline", 
                "entities": 0, 
                "temp_files_tracked": 0,
                "error": str(e)
            }
        )

@router.get("/healthcheck")
def health(col = Depends(get_milvus_col), processor = Depends(get_processor)):
    triton_healthy = processor is not None and processor.triton_client is not None
    if triton_healthy:
        triton_healthy = processor.triton_client.is_server_live()
        
    return {
        "status": "healthy" if col and triton_healthy else "degraded",
        "collection": col is not None,
        "triton_inference_server": triton_healthy
    }

@router.get("/test_db_connection")
async def test_db_conn():
    success = await test_connection()
    if success:
        return {"status": "success", "message": "SSH tunnel and database connection working"}
    else:
        raise HTTPException(503, "Database connection failed")

@router.delete("/reset_db")
async def reset_db(request: dict = None):
    # This requires access to app.state.csv_lock 
    from core.milvus_client import reset_collection
    # update global in app.state via dependency is tricky, but reset_collection connects to milvus
    col = reset_collection()
    
    # We shouldn't clear lock, just acquire it if we can
    # For now, just delete file
    if os.path.exists(CSV_LOG_FILE):
        try:
            os.remove(CSV_LOG_FILE)
        except:
            pass
    
    logger.warning("⚠️ DB Reset - In-progress S3 jobs may fail")
    return {
        "status": "success",
        "message": "DB reset. Active workers may encounter errors.",
        "warning": "Consider restarting service for clean state"
    }
