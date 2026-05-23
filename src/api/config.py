import os
import multiprocessing
from pathlib import Path

MAX_FILE_SIZE_MB = 500  
MAX_SONG_NAME_LENGTH = 120
BATCH_INSERT_SIZE = 100  
REQUEST_TIMEOUT = 300

TEMP_DIR = Path(os.getenv("TEMP_DIR", "temp_processing"))
TEMP_DIR.mkdir(exist_ok=True)
MODEL_PATH = os.getenv("MODEL_PATH", "inference/exported_model")
CSV_LOG_FILE = os.getenv("CSV_LOG_FILE", "ingested_songs.csv")

SEARCH_SIMILARITY_THRESHOLD = 0.65  
SEARCH_MIN_CONFIDENCE = 0.15
SEARCH_LIMIT = 2

USE_ACOUSTID = os.getenv("USE_ACOUSTID", "true").lower() == "true"

mp_context = multiprocessing.get_context('spawn')
