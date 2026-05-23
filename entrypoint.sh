#!/bin/bash
set -e

echo "--- Initializing Neural Audio Fingerprinter ---"

# Note: The database connection is handled by SQLAlchemy within Alembic.
# If we are starting the main API server, we run migrations first.
if [ "$1" = "api" ]; then
    echo "Running Database Migrations..."
    # Ensure you are in the correct directory for Alembic
    cd src || echo "Already in src directory or src not found"
    alembic upgrade head
    
    echo "Starting FastAPI Gateway..."
    exec uvicorn api.main:app --host 0.0.0.0 --port 8000
    
elif [ "$1" = "worker" ]; then
    echo "Starting Celery Background Worker..."
    cd src || echo "Already in src directory or src not found"
    exec celery -A core.celery_app worker --loglevel=info

else
    echo "Executing Custom Command..."
    exec "$@"
fi