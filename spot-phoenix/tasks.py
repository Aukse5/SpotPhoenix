import os
import time
import redis
from datetime import datetime, timedelta, timezone
from threading import Thread
from celery import Celery
from azure.core.rest import HttpRequest
from celery_config import app

# Azure SDK Components
from azure.identity import DefaultAzureCredential
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.trafficmanager import TrafficManagerManagementClient
from azure.mgmt.alertsmanagement import AlertsManagementClient
import azure.mgmt.alertsmanagement.models as alerts_models

# Shared Redis instance connection designated for distributed state management
redis_client = redis.Redis(host='redis', port=6379, db=1)


# Module A: Mitigation Functions
def disable_traffic_manager_endpoint(network_client, rg, profile_name, endpoint_name):
    """
    Module A - Thread 1: Isolates the evicted node from the global routing profile.
    Prevents black-holing client requests by dropping traffic before host eviction hits.
    """
    try:
        print(f"[Module A] [Thread 1] Fetching Traffic Manager endpoint: {endpoint_name}...")
        endpoint = network_client.endpoints.get(rg, profile_name, "externalEndpoints", endpoint_name)
        endpoint.endpoint_status = "Disabled"
        network_client.endpoints.create_or_update(rg, profile_name, "externalEndpoints", endpoint_name, endpoint)
        print(f"[Module A] [Thread 1] Traffic Manager endpoint '{endpoint_name}' successfully DISABLED.")
    except Exception as e:
        print(f"[Module A] [Thread 1] Error disabling Traffic Manager: {e}")


def silence_monitor_alerts(subscription_id, rg, vm_name):
    """
    Module A - Thread 2: Suppresses automated alert notifications from Azure Monitor.
    Bypasses outdated/broken Python SDK client limitations by executing a raw, high-performance 
    REST API PUT call directly into the Azure Resource Manager (ARM) backend.
    """
    try:
        # Authentication is managed via SDK, but transport layer is used for raw REST call
        credential = DefaultAzureCredential()
        client = AlertsManagementClient(credential, subscription_id)

        # Generate a unique rule name using a timestamp to prevent override conflicts
        rule_name = f"Suppress-{vm_name}-{int(datetime.now(timezone.utc).timestamp())}"
        
        # Target ARM endpoint for Action Rules (Suppression) management
        url = (
            f"https://management.azure.com/subscriptions/{subscription_id}/"
            f"resourceGroups/{rg}/providers/Microsoft.AlertsManagement/"
            f"actionRules/{rule_name}?api-version=2019-05-05-preview"
        )
        
        # Define rule scope strictly to the target Virtual Machine
        target_vm_scope = f"/subscriptions/{subscription_id}/resourceGroups/{rg}/providers/Microsoft.Compute/virtualMachines/{vm_name}"
        start_time = datetime.now(timezone.utc)
        end_time = start_time + timedelta(minutes=30)

        # JSON schema tailored for the 2019-05-05-preview specification.
        payload = {
            "location": "Global",
            "properties": {
                "scope": {
                    "scopeType": "Resource",
                    "values": [target_vm_scope]
                },
                "type": "Suppression",
                "suppressionConfig": {
                    "recurrenceType": "Once",
                    "schedule": {
                        "startDate": start_time.strftime("%m/%d/%Y"),
                        "endDate": end_time.strftime("%m/%d/%Y"),
                        "startTime": start_time.strftime("%H:%M:%S"),
                        "endTime": end_time.strftime("%H:%M:%S")
                    }
                },
                "description": f"Phoenix automated alert suppression for {vm_name}"
            }
        }
        print(f"[Module A] [Thread 2] Sending direct REST request to Azure Monitor API for rule: '{rule_name}'...")
        
        request = HttpRequest("PUT", url, json=payload)
        response = client._client.send_request(request)
        if response.status_code in [200, 201]:
            print(f"[Module A] [Thread 2] Alerts successfully SUPPRESSED for VM '{vm_name}' until {end_time.strftime('%H:%M:%S')} UTC.")
        else:
            print(f"[Module A] [Thread 2] Azure API returned error status {response.status_code}: {response.text()}")
            
    except Exception as e:
        print(f"[Module A] [Thread 2] Error suppressing alerts: {e}")

@app.task(name="tasks.spot_eviction_workflow")
def spot_eviction_workflow(payload):
    """
    Central Entrypoint Pipeline: Handles payload sanitation, executes idempotent locks,
    and coordinates parallel mitigation tasks with long-running background restoration.
    """
    # Defensive payload validation to guarantee cross-compatibility with CLI text args
    if isinstance(payload, dict):
        vm_name = payload.get("vm_name", "spot-phoenix-test")
        subscription_id = payload.get("subscription_id", os.getenv("AZURE_SUBSCRIPTION_ID"))
        rg = payload.get("resource_group", "Spot-Phoenix-rg")
    else:
        vm_name = payload
        subscription_id = os.getenv("AZURE_SUBSCRIPTION_ID")
        rg = "Spot-Phoenix-rg"

    lock_key = f"lock:eviction:{vm_name}"

    # Distributed Locking: Prevents multiple redundant workflow 
    # instantiations if the listener container fires duplicate HTTP retries.
    if not redis_client.set(lock_key, "true", ex=45, nx=True):
        print(f"[CENTRAL] Duplicate eviction notice detected for VM '{vm_name}'. Skipping execution to prevent conflicts.")
        return "Duplicate ignored"

    print(f"[CENTRAL] Eviction workflow initiated for VM: {vm_name}")

    # Unified Infrastructure Naming Declarations
    tm_profile = "Phoenix-Traffic-Profiles"
    endpoint_name = "Phoenix-Endpoint-A"

    credential = DefaultAzureCredential()
    network_client = TrafficManagerManagementClient(credential, subscription_id)

    context = {
        "vm_name": vm_name,
        "subscription_id": subscription_id,
        "resource_group": rg
    }

    # Asynchronously dispatch Module B to a separate background lifecycle runner.
    # This prevents blocking the primary queue channel with long-lived retry loops.
    module_b_resurrector.delay(context)

    t1 = Thread(target=disable_traffic_manager_endpoint, args=(network_client, rg, tm_profile, endpoint_name))
    t2 = Thread(target=silence_monitor_alerts, args=(subscription_id, rg, vm_name))

    t1.start()
    t2.start()

    # Block orchestrator thread context until both mitigation tasks safely finish
    t1.join()
    t2.join()

    print("[CENTRAL] Module A execution finished successfully.")


# Module B: Resurrector
@app.task(name="tasks.module_b_resurrector")
def module_b_resurrector(payload):
    """
    Module B - Resurrector Loop: Waits for full host deallocation, then initiates 
    an aggressive wait-and-retry provisioning loop to reclaim Azure Spot capacity.
    """
    cooldown = int(os.getenv("COOLDOWN_PERIOD", 600))

    if isinstance(payload, dict):
        vm_name = payload.get("vm_name", "spot-phoenix-test")
        subscription_id = payload.get("subscription_id", os.getenv("AZURE_SUBSCRIPTION_ID"))
        rg = payload.get("resource_group", "Spot-Phoenix-rg")
    else:
        vm_name = payload
        subscription_id = os.getenv("AZURE_SUBSCRIPTION_ID")
        rg = "Spot-Phoenix-rg"

    print(f"[Module B] Initiating {cooldown}s cooldown period for Azure to complete VM deallocation...")
    time.sleep(cooldown)

    credential = DefaultAzureCredential()
    compute_client = ComputeManagementClient(credential, subscription_id)

    print(f"[Module B] Checking if VM '{vm_name}' is fully Deallocated before starting...")
    
    # State Synchronization: Enforces a hard check on Azure Fabric status.
    # Initiating a start call while Azure is midway through preemption triggers state conflicts.
    while True:
        try:
            vm_instance = compute_client.virtual_machines.get(rg, vm_name, expand="instanceView")
            statuses = [s.code for s in vm_instance.instance_view.statuses]

            if "PowerState/deallocated" in statuses:
                print(f"[Module B] Confirmation received: VM '{vm_name}' is Deallocated.")
                break
            else:
                print(f"[Module B] VM status is currently: {statuses}. Waiting 30s for Azure to finish deallocation...")
                time.sleep(30)
        except Exception as e:
            print(f"[Module B] Warning while fetching VM status: {e}. Retrying status check in 30s...")
            time.sleep(30)

    print(f"[Module B] Entering Resurrection loop for VM: {vm_name}")

    # Back-off Capacity Loop: Repeatedly fires VM start requests until regional compute capacity frees up
    while True:
        try:
            async_start = compute_client.virtual_machines.begin_start(rg, vm_name)
            async_start.wait() # Block context execution synchronously until VM is Fully Operational

            print(f"[Module B] Resurrection SUCCESSFUL. VM '{vm_name}' is now running and stable.")
            break

        except Exception as e:
            error_msg = str(e).lower()
            # Intercept explicit Azure compute limitation patterns and enforce an algorithmic back-off
            if "overconstrained" in error_msg or "capacity" in error_msg or "allocationfailed" in error_msg:
                print(f"[Module B] CAPACITY ERROR: Azure has no available Spot capacity right now. Retrying in 10 minutes... (Details: {e})")
                time.sleep(cooldown)
            else:
                print(f"[Module B] Unexpected API Error during start: {e}. Retrying in 60 seconds...")
                time.sleep(60)

    # Restoration Phase: Wire the newly deployed compute resource back into active service
    try:
        network_client = TrafficManagerManagementClient(credential, subscription_id)
        endpoint = network_client.endpoints.get(rg, "Phoenix-Traffic-Profiles", "externalEndpoints", "Phoenix-Endpoint-A")
        endpoint.endpoint_status = "Enabled"
        network_client.endpoints.create_or_update(rg, "Phoenix-Traffic-Profiles", "externalEndpoints", "Phoenix-Endpoint-A", endpoint)
        print("[Module B] Traffic Manager endpoint restored to 'Enabled'. System fully recovered.")
    except Exception as e:
        print(f"[Module B] Critical: Failed to restore Traffic Manager endpoint: {e}")
