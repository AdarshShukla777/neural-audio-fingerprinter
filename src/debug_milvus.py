import os
import sys
import time
import logging
import concurrent.futures
from pymilvus import connections, Collection, utility

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("DebugMilvus")

# Mock settings
MILVUS_HOST = "localhost"
MILVUS_PORT = 19530
COLLECTION_NAME = "audio_fingerprints_32bit_float"

def connect_milvus():
    logger.info(f"Connecting to Milvus at {MILVUS_HOST}:{MILVUS_PORT}...")
    connections.connect("default", host=MILVUS_HOST, port=MILVUS_PORT)
    logger.info("Connected!")

def load_collection_blocking(name):
    logger.info(f"[Thread] Loading collection '{name}'...")
    col = Collection(name)
    col.load()
    logger.info(f"[Thread] Collection '{name}' loaded!")
    return True

def main():
    try:
        connect_milvus()
        
        if not utility.has_collection(COLLECTION_NAME):
            logger.error(f"Collection {COLLECTION_NAME} does not exist!")
            return

        logger.info(f"Collection exists. Starting ASYNC load test...")
        
        timeout = 20
        start_time = time.time()
        
        # Async load
        col = Collection(COLLECTION_NAME)
        col.load(_async=True)
        
        while True:
            try:
                progress = utility.loading_progress(COLLECTION_NAME)
                # Parse progress which might be {'loading_progress': '100%'}
                raw_val = progress.get('loading_progress', '0')
                if isinstance(raw_val, str):
                    raw_val = raw_val.replace('%', '').strip()
                p_val = float(raw_val)
                
                logger.info(f"Progress: {p_val}%")
                
                if p_val >= 100:
                    logger.info("Success! Collection loaded.")
                    break
            except Exception as e:
                logger.warning(f"Error checking progress: {e}")
            
            if time.time() - start_time > timeout:
                logger.error("TIMEOUT in async load!")
                break
                
            time.sleep(1)

    except Exception as e:
        logger.exception(f"Top level error: {e}")

if __name__ == "__main__":
    main()
