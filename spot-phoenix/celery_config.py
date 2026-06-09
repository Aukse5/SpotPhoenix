import os
from celery import Celery
from dotenv import load_dotenv

# Load infrastructure configurations and broker access tokens
load_dotenv()

# Network routing variables for RabbitMQ and Redis caching layers
BROKER_URL = os.getenv("CELERY_BROKER_URL", "amqp://guest:guest@localhost:5672//")
BACKEND_URL = os.getenv("CELERY_RESULT_BACKEND", "redis://localhost:6379/0")

# Initialize the main Celery application context and register background task modules
app = Celery(
    'spot_phoenix',
    broker=BROKER_URL,
    backend=BACKEND_URL,
    include=['tasks']
)

# Enterprise production configuration tuning for reliability and consistency
app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,

    # Critical Reliability Tuning: Enforces acknowledgment after task completion.
    # If a worker container dies mid-eviction, the message safely goes back to RabbitMQ.
    task_acks_late=True,

    # Enforces strict 1-task-at-a-time processing to ensure execution predictability under stress
    worker_prefetch_multiplier=1,

    # Ensures the orchestrator automatically recovers connections if RabbitMQ restarts
    broker_connection_retry_on_startup=True
)
