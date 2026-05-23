import os
import boto3
import logging
from pathlib import Path
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, status
from pydantic import BaseModel
from botocore.exceptions import ClientError
from typing import Optional

from api.dependencies import get_milvus_col
from core.tasks import process_local_batch, worker_s3_job
from data_pipeline.bulk_manager import BulkRequestUploadS3, upload_bulk_songs_to_s3_job, reconcile_s3_db_job

logger = logging.getLogger("AudioSystem")
router = APIRouter(prefix="/fastapi-milvus")

class S3IngestRequest(BaseModel):
    bucket_name: str
    prefix: str = ""
    limit: Optional[int] = 50

class LocalIngestRequest(BaseModel):
    folder_path: str
    limit: Optional[int] = 50

class ReconcileRequest(BaseModel):
    s3_bucket_name: str
    s3_folder_name: str

@router.post("/batch_ingest_local")
async def batch_ingest_local(request: LocalIngestRequest):
    folder = Path(request.folder_path)
    if not folder.exists():
        raise HTTPException(400, "Folder not found")
    
    task = process_local_batch.delay(str(folder), request.limit)
    
    return {
        "status": "processing", "message": f"Local batch job started for {folder}", "task_id": task.id, "note": "Check Celery logs for progress"
    }

@router.post("/batch_ingest_s3")
async def batch_ingest_s3(request: S3IngestRequest):
    s3 = boto3.client('s3', aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'), aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'), region_name=os.getenv('AWS_DEFAULT_REGION'))
    try:
        s3.head_bucket(Bucket=request.bucket_name)
    except ClientError:
        raise HTTPException(400, "Invalid bucket or no permissions")
    
    task = worker_s3_job.delay(request.bucket_name, request.prefix, request.limit)
    
    return {
        "status": "queued", "message": f"S3 job queued for {request.bucket_name}/{request.prefix}", "task_id": task.id, "note": "Check Celery logs for progress"
    }

@router.post("/upload-songs-to-s3/")
async def ingest_music(request: BulkRequestUploadS3, background_tasks: BackgroundTasks):
    try:
        if request.song_count <= 0:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Song count must be greater than 0.")
        if request.song_count > 10000:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Please limit batches to 10000 songs.")
        if not request.s3_bucket_name.strip() or not request.s3_folder_name.strip():
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="S3 bucket and folder names cannot be empty.")
        
        logger.info(f"Queuing bulk job for {request.song_count} songs to {request.s3_bucket_name}")
        background_tasks.add_task(upload_bulk_songs_to_s3_job, request)
        
        return {
            "message": "Bulk ingestion started", "target_count": request.song_count, "s3_bucket": request.s3_bucket_name,
            "s3_folder": request.s3_folder_name, "note": "Check database 'wazzdat.s3_downloads_log' for progress."
        }
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"CRITICAL: Failed to queue bulk job: {str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to initialize background job.")

@router.post("/reconcile-s3-db")
async def reconcile_data(req: ReconcileRequest):
    result = await reconcile_s3_db_job(req.s3_bucket_name, req.s3_folder_name)
    return {"message": "Reconciliation completed", "details": result}
