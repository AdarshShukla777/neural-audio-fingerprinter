import os
from celery import Celery

# Use environment variable for RabbitMQ URL or fallback to default
broker_url = os.getenv("CELERY_BROKER_URL", "amqp://user:password@localhost:5672//")
backend_url = os.getenv("CELERY_RESULT_BACKEND", "rpc://")

app = Celery(
    'nnfp_tasks',
    broker=broker_url,
    backend=backend_url,
    include=['core.tasks']
)

# Optional configuration, see the application user guide.
app.conf.update(
    result_expires=3600,
    task_serializer='json',
    accept_content=['json'],  
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
    # Acknowledge task only after completion (prevents data loss if worker crashes)
    task_acks_late=True,
    worker_prefetch_multiplier=1,
)

if __name__ == '__main__':
    app.start()
