import os
import sys
import uvicorn
from pathlib import Path

# Add the 'src' directory to the Python path
sys.path.append(str(Path(__file__).parent / "src"))

if __name__ == "__main__":
    # Ensure environment variables are loaded from src/.env if it exists
    env_path = Path(__file__).parent / "src" / ".env"
    if env_path.exists():
        from dotenv import load_dotenv
        load_dotenv(dotenv_path=env_path)

    # Change current working directory to 'src' so relative paths (like 'static/' or 'exported_model/') work correctly
    src_dir = Path(__file__).parent / "src"
    os.chdir(src_dir)

    print("🚀 Starting FastAPI Server (Local Mode)...")
    print(f"📁 Working directory set to: {src_dir}")
    print("🌍 API will be available at: http://localhost:8000/docs")
    
    # Run the application with hot-reloading enabled for local development
    uvicorn.run(
        "api.main:app", 
        host="0.0.0.0", 
        port=8080, 
        reload=True,
    )
