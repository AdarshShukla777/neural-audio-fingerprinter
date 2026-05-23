# 1. Base Image
FROM python:3.12-slim

# 2. Environment Variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# 3. Set Build Directory
WORKDIR /app

# 4. System Dependencies (Critical for Audio)
# libsndfile1 & ffmpeg are required for librosa/soundfile to read mp3/wav
RUN apt-get update && apt-get install -y --no-install-recommends \
    libsndfile1 \
    ffmpeg \
    libchromaprint-tools \
    && rm -rf /var/lib/apt/lists/*

# 5. Install Python Packages
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# 6. Copy Application Code
COPY . .

# 7. Set Runtime Directory
# Switching to where main.py resides so imports work correctly
WORKDIR /app/neural_network_fingerprinter/src

# 8. Expose & Run
EXPOSE 7000
CMD ["sh", "-c", "alembic upgrade head && uvicorn api.main:app --host 0.0.0.0 --port 7000"]