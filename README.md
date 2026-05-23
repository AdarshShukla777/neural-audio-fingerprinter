# Neural Audio Fingerprinter

An enterprise-grade, event-driven Machine Learning system designed to instantly identify audio and music by generating dense neural fingerprints and performing sub-second vector similarity searches across massive audio databases.

---

## 1. Project Introduction & Model Architecture

This project replaces legacy algorithmic audio fingerprinting (like Shazam's peak-picking hashes) with a **Deep Learning approach**. 

### The AI Architecture
1. **Feature Extraction**: Raw audio is parsed using `librosa` into Log-Mel Spectrograms, breaking the audio down into 34-frame time windows.
2. **Neural Inference**: A TensorFlow-based Neural Network compresses these spectrogram windows into highly discriminative **128-dimensional embedding vectors**.
3. **Vector Database**: Instead of standard SQL lookups, embeddings are stored in **Milvus**, a specialized Vector Database. By calculating the Euclidean distance/Cosine similarity between the uploaded audio vector and the database vectors, the system can instantly recognize songs even if they are heavily distorted, compressed, or overlaid with background noise.

### The Backend Architecture
The backend has been entirely decoupled into a distributed microservice architecture to maximize throughput:
- **API Gateway (FastAPI)**: A lightweight, non-blocking asynchronous Python server handling HTTP requests and client routing.
- **Inference Server (NVIDIA Triton)**: The TensorFlow model is hosted on a dedicated Triton C++ Server. It uses **Dynamic Batching** to group concurrent requests together, maximizing GPU utilization.
- **Message Broker (RabbitMQ)**: Safely queues massive batch ingestion tasks (e.g., synchronizing thousands of songs from an AWS S3 bucket).
- **Task Orchestration (Celery)**: Background worker nodes consume jobs from RabbitMQ, handle the heavy I/O of downloading files and querying PostgreSQL metadata, and stream vectors to Milvus without blocking the main API.

---

## 2. Folder Structure & How to Run

### Folder Structure
```text
neural audio fingerprinter/
├── docker-compose.yaml        # Orchestrates the entire microservice stack
├── model_repository/          # Strict folder structure required by Triton Server
│   └── fingerprinter/
│       ├── config.pbtxt       # Triton configuration (input shapes, batching limits)
│       └── 1/model.savedmodel/# The actual TensorFlow model weights
├── requirements.txt           # Python dependencies
├── run.py                     # Local execution entrypoint
└── src/
    ├── api/                   # FastAPI gateway (routers, configs, dependencies)
    ├── core/                  # Celery background tasks, Milvus/DB clients
    ├── data_pipeline/         # External API synchronization scripts
    └── inference/             # The gRPC client that communicates with Triton
```

### How to Run the Project

**Prerequisites:** 
- Docker and Docker Compose installed.
- (Optional but Recommended) NVIDIA Container Toolkit for GPU acceleration.

**Step 1: Start the Infrastructure**
This single command will pull the required images, build the FastAPI and Celery containers, and start the entire stack (Milvus, RabbitMQ, Triton, FastAPI, Celery).
```bash
docker-compose up --build -d
```

**Step 2: Access the Application**
Once the containers are healthy, the API gateway is mapped to host port **8005**.
- **Interactive API Docs (Swagger):** [http://localhost:8005/docs](http://localhost:8005/docs)
- *(Note: Port 8000 is occupied by the Triton Inference Server's HTTP endpoint)*

**Step 3: Local Development (Hot Reloading)**
If you are developing new API routes and don't want to rebuild the Docker image every time, you can run the API locally on your host machine while still relying on the Dockerized backend (Triton, RabbitMQ, Milvus):
1. Ensure your local virtual environment is active and dependencies are installed (`uv pip install -r requirements.txt`).
2. Open `run.py` and change the `port` from `8000` to `8080` (to avoid conflicts with Triton).
3. Run the server:
   ```bash
   python run.py
   ```

---

## 3. MLOps, Scalability & Load Management

As the system ingests millions of tracks and traffic increases, the architecture is designed to scale horizontally. Here is our MLOps strategy for handling extreme load:

### Handling Traffic Spikes
- **Message Queuing**: By using **RabbitMQ**, if 10,000 files are uploaded at once, the API will not crash. It will instantly return a `task_id` and place the jobs in the queue.
- **Dynamic Batching**: The **Triton Server** is configured with `max_batch_size: 256` and `max_queue_delay_microseconds: 50000`. If 50 different Celery workers request fingerprints at the same millisecond, Triton waits 50ms, groups all 50 requests into a single tensor, and processes them in one GPU cycle.

### Future CI/CD & Deployment Strategy
When migrating from a single-machine Docker Compose setup to a true Enterprise Cloud deployment, we will utilize the following stack:

1. **Continuous Integration (GitHub Actions)**
   - Every commit triggers a pipeline that runs unit tests, runs code linters (`flake8`, `black`), and builds the new Docker images.
2. **Infrastructure as Code (Terraform)**
   - We will use Terraform to automatically provision cloud infrastructure, ensuring our environments (Staging vs. Production) are completely identical and reproducible.
3. **Orchestration (Kubernetes)**
   - Instead of Docker Compose, the services will be deployed to **AWS EKS** or **Google GKE**. 
   - **Horizontal Pod Autoscaling (HPA)** will be enabled. If RabbitMQ reports a backlog of 5,000 jobs, Kubernetes will automatically spin up 10 more Celery Worker pods to process the queue.
   - We will deploy the Triton Server on isolated, dedicated GPU Node Pools (e.g., NVIDIA T4s or A10g) using **KServe**, allowing the GPU nodes to scale-to-zero when there is no traffic to save cloud costs. 
4. **Observability**
   - We will integrate **Prometheus and Grafana** to visualize system health, tracking metrics like Triton GPU memory usage, RabbitMQ queue lengths, and Milvus search latency.
