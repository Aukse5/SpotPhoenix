FROM python:3.11-slim

# Set an isolated operational directory inside the container file system
WORKDIR /app

# Copy dependency manifests first to maximize Docker layer caching efficiency
COPY requirements.txt .

# Install necessary libraries cleanly without leaving behind transient package caches
RUN pip install --no-cache-dir -r requirements.txt

# Explicitly copy only the listener script and configuration to maintain a minimal surface area
COPY spot-phoenix-listener.py .
COPY .env .

# Execute python with unbuffered logging (-u) to guarantee immediate log delivery
# to the container stdout, preventing delayed visibility during eviction events.
CMD ["python", "-u", "spot-phoenix-listener.py"]
