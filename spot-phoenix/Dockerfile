FROM python:3.11-slim

# Set the operational directory inside the container isolated file system
WORKDIR /app

# Copy dependency manifests first to leverage Docker's layer caching mechanism
COPY requirements.txt .

# Install dependencies without local pip caching to minimize image size overhead
RUN pip install --no-cache-dir -r requirements.txt

# Copy the remaining core application source code into the container context
COPY . .

# Launch the Celery worker daemon, targeting the 'tasks.py' orchestration core
CMD ["celery", "-A", "celery_config", "worker", "--loglevel=info"]
