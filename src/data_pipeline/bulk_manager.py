# import logging
# import os
# import glob  # <--- ADDED THIS
# from typing import List
# from dotenv import load_dotenv 
# from sqlalchemy import create_engine, text
# from pydantic import BaseModel
# from sqlalchemy.orm import sessionmaker
# from concurrent.futures import ThreadPoolExecutor, as_completed
# from fastapi.concurrency import run_in_threadpool

# # Import your modules
# import database
# from db_models import S3DownloadsLogORM, DownloadStatus
# from song_downloader import SmartDownloader, DownloadOptions
# from s3_uploader import S3Uploader

# load_dotenv()
# logger = logging.getLogger("BulkManager")

# class BulkProcessor:
#     def __init__(self, s3_bucket: str, s3_folder: str, session_factory, max_workers: int = 5):
#         self.downloader = SmartDownloader(download_dir="temp_processing")
#         self.s3_uploader = S3Uploader(bucket_name=s3_bucket)
#         self.s3_folder = s3_folder
#         self.max_workers = max_workers
#         self.SessionLocal = session_factory

#     def get_fresh_candidates(self, session, limit: int):
#         """
#         Fetches songs from the Materialized View (recording_external_links).
#         """
#         sql = text("""
#             SELECT 
#                 recording_mbid as mbid,
#                 song_title as title,
#                 artist_name as artist,
#                 url_list
#             FROM recording_external_links
#             WHERE NOT EXISTS (
#                 SELECT 1 FROM wazzdat.s3_downloads_log log 
#                 WHERE log.mbid = recording_external_links.recording_mbid
#             )
#             ORDER BY recording_mbid
#             LIMIT :limit
#         """)
#         return session.execute(sql, {"limit": limit}).fetchall()

#     def process_single_track(self, mbid, url_list, title, artist): 
#         file_path = None
#         db = self.SessionLocal()
#         try:
#             # --- Defensive List Cleaning ---
#             if isinstance(url_list, str):
#                 url_list = url_list.strip("{}").split(",")
#             if not url_list:
#                 url_list = []
            
#             opts = DownloadOptions(
#                 urls=url_list,
#                 mbid=str(mbid),
#                 title=title,    
#                 artist=artist,  
#                 audio_format="wav" 
#             )
            
#             # 1. Download
#             result = self.downloader.process(opts)
#             file_path = result['file_path']
#             used_source = result['source']

#             # 2. Upload to S3
#             s3_key = self.s3_uploader.upload_file(file_path, self.s3_folder)

#             # 3. Log Success
#             log_entry = S3DownloadsLogORM(
#                 mbid=mbid,
#                 title=title,            
#                 url=used_source,        
#                 s3_key=s3_key,
#                 download_status=DownloadStatus.SUCCESS
#             )
#             db.add(log_entry)
#             db.commit()
            
#             return True

#         except Exception as e:
#             logger.error(f"Failed {mbid} ({title}): {str(e)[:100]}")
            
#             log_entry = S3DownloadsLogORM(
#                 mbid=mbid,
#                 title=title,
#                 url="ALL_FAILED",
#                 s3_key=None,
#                 download_status=DownloadStatus.FAILED
#             )
#             db.add(log_entry)
#             db.commit()
#             return False
            
#         finally:
#             # --- ROBUST CLEANUP FIX ---
#             # 1. Try deleting the specific file if we know it
#             if file_path and os.path.exists(file_path):
#                 try: 
#                     os.remove(file_path)
#                     logger.info(f"Deleted {file_path}")
#                 except: pass
            
#             # 2. SAFETY NET: Look for ANY file starting with this MBID in the temp dir
#             # This catches cases where file_path was None (download crash) but a file exists
#             try:
#                 temp_dir = self.downloader.download_dir
#                 # Pattern matches {mbid}.wav, {mbid}.mp3, {mbid}.part, etc.
#                 pattern = os.path.join(temp_dir, f"{mbid}.*")
                
#                 for f in glob.glob(pattern):
#                     try: 
#                         os.remove(f)
#                         logger.info(f"Cleanup Safety: Deleted artifact {f}")
#                     except Exception as e:
#                         logger.warning(f"Failed to delete artifact {f}: {e}")
#             except Exception as glob_error:
#                 logger.error(f"Cleanup glob error: {glob_error}")

#             db.close()

#     def run_job(self, target_count: int):
#         total_success = 0
        
#         while total_success < target_count:
#             needed = target_count - total_success
#             logger.info(f"--- Fetching {needed} candidates ---")
            
#             with self.SessionLocal() as session:
#                 candidates = self.get_fresh_candidates(session, needed)
            
#             if not candidates:
#                 logger.warning("No more candidates available in View.")
#                 break

#             with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
#                 futures = {}
#                 for row in candidates:
#                     futures[executor.submit(
#                         self.process_single_track, 
#                         row.mbid, 
#                         row.url_list, 
#                         row.title, 
#                         row.artist
#                     )] = row
                
#                 for future in as_completed(futures):
#                     if future.result():
#                         total_success += 1
                        
#             logger.info(f"Batch Complete: {total_success}/{target_count}")
#         return total_success

# # --- REQUEST MODEL & JOB ENTRY POINT ---

# class BulkRequestUploadS3(BaseModel):
#     song_count: int
#     s3_bucket_name: str
#     s3_folder_name: str 

# async def upload_bulk_songs_to_s3_job(req: BulkRequestUploadS3):
#     logger.info("Initializing Bulk Upload Job...")

#     if database.USE_SSH_TUNNEL:
#         await database.init_ssh_tunnel()
    
#     local_db_url = database.get_sqlalchemy_url()

#     engine = create_engine(local_db_url)
#     ScopedSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

#     processor = BulkProcessor(
#         s3_bucket=req.s3_bucket_name,
#         s3_folder=req.s3_folder_name,
#         session_factory=ScopedSessionLocal
#     )

#     logger.info(f"Starting bulk processing using DB URL: {local_db_url}")
#     try:
#         await run_in_threadpool(processor.run_job, req.song_count)
#     finally:
#         engine.dispose()

import logging
import os
import glob
import uuid
from typing import List
from dotenv import load_dotenv 
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError, SQLAlchemyError
from pydantic import BaseModel
from sqlalchemy.orm import sessionmaker
from concurrent.futures import ThreadPoolExecutor, as_completed
from fastapi.concurrency import run_in_threadpool

# Import your modules
from core import database
from core.models import S3DownloadsLogORM, DownloadStatus
from data_pipeline.song_downloader import SmartDownloader, DownloadOptions
from data_pipeline.s3_uploader import S3Uploader

load_dotenv()
logger = logging.getLogger("BulkManager")

# --- Custom Exception for SSH/DB Failure ---
class CriticalConnectionError(Exception):
    pass

class BulkProcessor:
    def __init__(self, s3_bucket: str, s3_folder: str, session_factory, max_workers: int = 5):
        self.downloader = SmartDownloader(download_dir="temp_processing")
        self.s3_uploader = S3Uploader(bucket_name=s3_bucket)
        self.s3_folder = s3_folder
        self.max_workers = max_workers
        self.SessionLocal = session_factory

    def check_db_connection(self):
        """
        Lightweight ping to ensure SSH tunnel/DB is alive.
        """
        try:
            with self.SessionLocal() as session:
                session.execute(text("SELECT 1"))
        except Exception as e:
            # This catches OperationalError (SSH drop) and others
            logger.critical(f"🔥 DATABASE CONNECTION LOST: {e}")
            raise CriticalConnectionError("Database unreachable. Stopping process.")

    def get_fresh_candidates(self, session, limit: int):
        # ... (Keep existing code) ...
        sql = text("""
            SELECT 
                recording_mbid as mbid,
                song_title as title,
                artist_name as artist,
                url_list
            FROM recording_external_links
            WHERE NOT EXISTS (
                SELECT 1 FROM wazzdat.s3_downloads_log log 
                WHERE log.mbid = recording_external_links.recording_mbid
            )
            ORDER BY recording_mbid
            LIMIT :limit
        """)
        return session.execute(sql, {"limit": limit}).fetchall()

    def process_single_track(self, mbid, url_list, title, artist): 
        # 1. CRITICAL: Check Connection BEFORE doing work
        self.check_db_connection()

        file_path = None
        db = self.SessionLocal()
        
        try:
            # ... (List Cleaning Code) ...
            if isinstance(url_list, str):
                url_list = url_list.strip("{}").split(",")
            if not url_list:
                url_list = []
            
            opts = DownloadOptions(
                urls=url_list,
                mbid=str(mbid),
                title=title,    
                artist=artist,  
                audio_format="wav" 
            )
            
            # Download
            result = self.downloader.process(opts)
            file_path = result['file_path']
            used_source = result['source']

            # Upload to S3
            s3_key = self.s3_uploader.upload_file(file_path, self.s3_folder)

            # Log Success
            log_entry = S3DownloadsLogORM(
                mbid=mbid,
                title=title,            
                url=used_source,        
                s3_key=s3_key,
                download_status=DownloadStatus.SUCCESS
            )
            db.add(log_entry)
            db.commit()
            return True

        except CriticalConnectionError:
            # Re-raise specifically to stop the main loop
            raise 

        # --- FIX START: Handle Missing Files Separately ---
        except FileNotFoundError as e:
            logger.error(f"File missing for {mbid} ({title}): {e}")
            # Treat this as a standard FAILURE, not a system crash
            try:
                log_entry = S3DownloadsLogORM(
                    mbid=mbid,
                    title=title,
                    url="FILE_MISSING",
                    s3_key=None,
                    download_status=DownloadStatus.FAILED
                )
                db.add(log_entry)
                db.commit()
            except Exception:
                # If we can't write to DB, THEN it's a critical connection error
                raise CriticalConnectionError("DB dead during failure logging")
            return False
        # --- FIX END ---

        except (OperationalError, OSError) as e:
            # Catch DB disconnection during commit or SSH failures
            # Since we handled FileNotFoundError above, this usually means Socket/Network issues
            logger.critical(f"🔥 Connection died during processing {mbid}: {e}")
            raise CriticalConnectionError(f"Connection died: {e}")

        except Exception as e:
            # Standard download failures (404, DRM, etc)
            logger.error(f"Failed {mbid} ({title}): {str(e)[:100]}")
            try:
                log_entry = S3DownloadsLogORM(
                    mbid=mbid,
                    title=title,
                    url="ALL_FAILED",
                    s3_key=None,
                    download_status=DownloadStatus.FAILED
                )
                db.add(log_entry)
                db.commit()
            except Exception:
                logger.critical("Cannot log FAILURE status. DB presumed dead.")
                raise CriticalConnectionError("DB dead during failure logging")
            
            return False
            
        finally:
            # Cleanup logic
            if file_path and os.path.exists(file_path):
                try: os.remove(file_path)
                except: pass
            
            try:
                temp_dir = self.downloader.download_dir
                pattern = os.path.join(temp_dir, f"{mbid}.*")
                for f in glob.glob(pattern):
                    try: os.remove(f)
                    except: pass
            except: pass

            db.close()

    def run_job(self, target_count: int):
        total_success = 0
        
        # Wrap the whole loop to catch the Critical Stop signal
        try:
            while total_success < target_count:
                # Check connection before fetching candidates
                self.check_db_connection()

                needed = target_count - total_success
                logger.info(f"--- Fetching {needed} candidates ---")
                
                with self.SessionLocal() as session:
                    candidates = self.get_fresh_candidates(session, needed)
                
                if not candidates:
                    logger.warning("No more candidates available.")
                    break

                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    futures = {}
                    for row in candidates:
                        futures[executor.submit(
                            self.process_single_track, 
                            row.mbid, 
                            row.url_list, 
                            row.title, 
                            row.artist
                        )] = row
                    
                    for future in as_completed(futures):
                        try:
                            if future.result():
                                total_success += 1
                        except CriticalConnectionError:
                            logger.critical("🛑 STOPPING JOB: SSH/DB Connection Lost.")
                            executor.shutdown(wait=False, cancel_futures=True)
                            raise # Break the outer loop
                        except Exception as e:
                            logger.error(f"Thread error: {e}")

                logger.info(f"Batch Complete: {total_success}/{target_count}")

        except CriticalConnectionError:
            logger.critical("Job terminated due to connection failure.")
        
        return total_success

    # --- NEW RECONCILIATION LOGIC ---
    def reconcile_s3_to_db(self):
        """
        Scans S3 for files, checks DB for missing entries, and backfills them.
        """
        logger.info("Starting S3 <-> DB Reconciliation...")
        
        # 1. Get all files from S3
        s3_files = self.s3_uploader.list_all_files(prefix=self.s3_folder)
        
        # Extract MBIDs from S3 keys (assuming format: folder/uuid.wav)
        s3_mbids = set()
        key_map = {} # Map MBID -> S3 Key
        
        for key in s3_files:
            filename = os.path.basename(key)
            # Remove extension (.wav)
            mbid_str = os.path.splitext(filename)[0]
            try:
                # Validate UUID
                u_obj = uuid.UUID(mbid_str)
                s3_mbids.add(str(u_obj))
                key_map[str(u_obj)] = key
            except ValueError:
                continue # Skip non-mbid files
        
        logger.info(f"Found {len(s3_mbids)} valid MBID files in S3.")

        # 2. Get all successful MBIDs from DB
        db_mbids = set()
        with self.SessionLocal() as session:
            rows = session.query(S3DownloadsLogORM.mbid).filter(
                S3DownloadsLogORM.download_status == DownloadStatus.SUCCESS
            ).all()
            for r in rows:
                db_mbids.add(str(r.mbid))
        
        # 3. Find missing
        missing_mbids = s3_mbids - db_mbids
        logger.info(f"Found {len(missing_mbids)} songs present in S3 but missing in DB.")

        if not missing_mbids:
            return {"status": "synced", "restored": 0}

        # 4. Backfill
        restored_count = 0
        with self.SessionLocal() as session:
            for m_mbid in missing_mbids:
                try:
                    # Fetch Metadata (Title)
                    # We query recording table directly or via view to get title
                    sql = text("SELECT name FROM recording WHERE gid = :gid")
                    res = session.execute(sql, {"gid": m_mbid}).fetchone()
                    title = res[0] if res else "Unknown (Restored)"
                    
                    log_entry = S3DownloadsLogORM(
                        mbid=m_mbid,
                        title=title,
                        url="RESTORED_FROM_S3",
                        s3_key=key_map[m_mbid],
                        download_status=DownloadStatus.SUCCESS
                    )
                    session.add(log_entry)
                    restored_count += 1
                    
                    if restored_count % 100 == 0:
                        session.commit()
                        logger.info(f"Restored {restored_count} records...")
                        
                except Exception as e:
                    logger.error(f"Failed to restore {m_mbid}: {e}")
            
            session.commit()
            
        logger.info(f"✅ Reconciliation Complete. Restored {restored_count} DB records.")
        return {"status": "reconciled", "restored": restored_count}

# --- JOB ENTRY POINTS ---

class BulkRequestUploadS3(BaseModel):
    song_count: int
    s3_bucket_name: str
    s3_folder_name: str 

async def upload_bulk_songs_to_s3_job(req: BulkRequestUploadS3):
    logger.info("Initializing Bulk Upload Job...")

    if database.USE_SSH_TUNNEL:
        await database.init_ssh_tunnel()
    
    local_db_url = database.get_sqlalchemy_url()

    engine = create_engine(local_db_url)
    ScopedSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

    processor = BulkProcessor(
        s3_bucket=req.s3_bucket_name,
        s3_folder=req.s3_folder_name,
        session_factory=ScopedSessionLocal
    )

    logger.info(f"Starting bulk processing using DB URL: {local_db_url}")
    try:
        await run_in_threadpool(processor.run_job, req.song_count)
    finally:
        engine.dispose()

# --- NEW ASYNC JOB FOR API ---
async def reconcile_s3_db_job(bucket: str, folder: str):
    if database.USE_SSH_TUNNEL:
        await database.init_ssh_tunnel()
    
    local_db_url = database.get_sqlalchemy_url()
    engine = create_engine(local_db_url)
    ScopedSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

    processor = BulkProcessor(
        s3_bucket=bucket,
        s3_folder=folder,
        session_factory=ScopedSessionLocal
    )
    
    try:
        # Run synchronous reconciliation in thread
        result = await run_in_threadpool(processor.reconcile_s3_to_db)
        return result
    finally:
        engine.dispose()