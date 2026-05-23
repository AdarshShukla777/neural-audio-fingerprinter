import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

from api.config import mp_context, MODEL_PATH
from api.dependencies import cleaner
from core.database import init_pg_pool, close_pg_pool
from core.milvus_client import connect_milvus, get_milvus_collection
from inference.engine import FingerprintGenerator

from api.routes import health, upload, search, batch

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("AudioSystem")

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("🟢 SYSTEM STARTUP")
    
    manager = mp_context.Manager()
    app.state.csv_lock = manager.Lock()
    app.state.manager = manager
    
    try:
        connect_milvus()
        app.state.collection = get_milvus_collection()
        logger.info("✅ Main Process Ready")
    except Exception as e:
        logger.exception(f"❌ Main Init Failed: {e}")
        app.state.collection = None
        
    try:
        from inference.engine import FingerprintGenerator
        app.state.processor = FingerprintGenerator()
    except Exception as e:
        logger.exception(f"❌ Triton Client Init Failed: {e}")
        app.state.processor = None
    
    try:
        await init_pg_pool()
    except Exception as e:
        logger.exception(f"❌ PostgreSQL pool init failed: {e}")
        
    yield
    
    logger.info("🔴 SHUTDOWN INITIATED")
    if hasattr(app.state, "manager") and app.state.manager:
        app.state.manager.shutdown()
        
    await close_pg_pool()
    await cleaner.cleanup_all()
    logger.info("✅ Cleanup complete")

app = FastAPI(title="Neural Audio Search", lifespan=lifespan)

# Note: make sure the static directory is created
app.mount("/static", StaticFiles(directory="static"), name="static")

app.include_router(health.router)
app.include_router(upload.router)
app.include_router(search.router)
app.include_router(batch.router)

@app.get("/fastapi-milvus/")
def read_root():
    return FileResponse('static/index.html')

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, workers=1)
