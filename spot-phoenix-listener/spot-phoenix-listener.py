import time
import requests
import os
import sys
import signal
from datetime import datetime, timezone
from celery import Celery
from dotenv import load_dotenv

# Load infrastructure configurations and secrets from the local environment file
load_dotenv()

# Azure Instance Metadata Service (IMDS) Scheduled Events endpoint
IMDS_URL = "http://169.254.169.254/metadata/scheduledevents?api-version=2020-07-01"
HEADERS = {"Metadata": "true"}

# Celery Initialization for high-priority eviction dispatching
BROKER_URL = os.getenv("CELERY_BROKER_URL", "amqp://guest:guest@localhost:5672//")
app = Celery('spot_phoenix', broker=BROKER_URL)

def handle_sigterm(signum, frame):
    """
    Captures system termination signals to trigger contingency protocols 
    before the container runtime hard-kills the application.
    """
    print("\nSystem shutdown signal received")
    eviction(reason="SIGTERM")

def check_eviction():
    """
    Polls the non-routable Azure IMDS link to check for active preemptive events.
    Returns True if an explicit Spot eviction notice is identified.
    """
    try:
        session = requests.Session()
        session.trust_env = False # Bypasses local system proxies inside the container

        response = session.get(IMDS_URL, headers=HEADERS, timeout=1)
        if response.status_code == 200:
            data = response.json()
            events = data.get("Events", [])

            for event in events:
                if event.get("EventType") in ["Preempt", "Terminate"]:
                    print(f"Eviction notice received via IMDS: {event.get('EventType')}")
                    return True
    except Exception as e:
        print(f"Error while checking IMDS: {e}")
    return False

def eviction(reason="IMDS"):
    """
    Executes local defensive actions and offloads the global recovery workflow 
    to the central Celery management backend.
    """
    print("\nEviction procedure starting")
    print("Flushing system buffers (os.sync)")
    os.sync() # Commit unwritten file system caches to disk to minimize corruption risks

    print("Sending eviction payload to Central Celery server")

    payload = {
        "vm_name": "spot-phoenix-test",
        "resource_group": "Spot-Phoenix-rg",
        "subscription_id": os.getenv("AZURE_SUBSCRIPTION_ID", "29fe3b0f-ab0e-421e-afab-78059bbe2981"),
        "event_type": "Preempt",
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    }

    try:
        # Asynchronously dispatch to the out-of-instance central framework
        app.send_task('tasks.spot_eviction_workflow', args=[payload])
        print("Notification successfully delivered to RabbitMQ")
    except Exception as e:
        print(f"Failed to send signal to Celery: {e}")

    print("Evacuation preparation complete. Exiting gracefully")
    # Sleep keeps the container alive to absorb remaining host operations
    time.sleep(1800)
    sys.exit(0)

def main():
    # Setup OS signal handlers for graceful failover handling
    signal.signal(signal.SIGTERM, handle_sigterm)
    signal.signal(signal.SIGINT, handle_sigterm)

    print("Spot Phoenix Listener started, awaiting Azure IMDS signals")

    # Strict 1-second polling frequency to optimize the narrow 30s preemption window
    while True:
        if check_eviction():
            eviction(reason="IMDS_Signal")
        time.sleep(1)

if __name__ == "__main__":
    main()
