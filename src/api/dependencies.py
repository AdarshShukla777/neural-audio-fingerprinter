import asyncio
import re
from pathlib import Path
from typing import Set
import logging
from fastapi import Request, HTTPException

from core.milvus_client import get_milvus_collection, connect_milvus

logger = logging.getLogger("AudioSystem")

class CleanupManager:
    """Manages temporary files with proper exception handling."""
    def __init__(self):
        self.files: Set[Path] = set()
        self._lock = asyncio.Lock()
    
    async def register(self, path: Path):
        async with self._lock:
            self.files.add(path)
    
    async def cleanup_file(self, path: Path):
        async with self._lock:
            try:
                if path.exists():
                    path.unlink()
            except Exception as e:
                logger.error(f"Cleanup error for {path}: {e}")
            finally:
                self.files.discard(path)
    
    async def cleanup_all(self):
        for path in list(self.files):
            await self.cleanup_file(path)

cleaner = CleanupManager()

def sanitize_filename(filename: str) -> str:
    path = Path(filename)
    clean_stem = re.sub(r'[^\w\-\. ]', '', path.stem)
    clean_stem = clean_stem.replace(' ', '_')
    return f"{clean_stem or 'audio'}{path.suffix.lower()}"

def truncate_song_name(name: str, max_len: int = 120) -> str:
    if not isinstance(name, str):
        name = str(name)
    if len(name) <= max_len:
        return name
    return name[:max_len]

def get_milvus_col(request: Request):
    """Dependency to ensure Milvus is connected and return collection."""
    if not hasattr(request.app.state, "collection") or request.app.state.collection is None:
        logger.warning("⚠️ Collection ref is None. Reconnecting...")
        try:
            connect_milvus()
            request.app.state.collection = get_milvus_collection()
            logger.info("✅ Reconnected!")
        except Exception as e:
            logger.exception(f"❌ DB Connect Failed: {e}")
            raise HTTPException(503, "Database unavailable")
    return request.app.state.collection

def get_processor(request: Request):
    """Dependency to get the Triton inference client."""
    if not hasattr(request.app.state, "processor") or request.app.state.processor is None:
        from inference.engine import FingerprintGenerator
        request.app.state.processor = FingerprintGenerator()
    return request.app.state.processor

