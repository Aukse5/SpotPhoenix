# Spot Phoenix application

## Architecture overview

### 1.Spot Phoenix Listener (Docker Container On-Instance)
* Python script deployed as a Docker Container on the Spot VM.
* Polls the Azure Instance Metadata Service (IMDS) via non-routable IP '169.254.169.254'.
* 1-second polling interval to maximize the 30 second eviction notice window.
* On eviction notice:
  * Flushes system buffers to disk (via os.sync()) to prevent data corruption
  * Signals local services for graceful shutdown (SIGTERM)
  * Publishes the eviction event to Celery

### 2. Spot Phoenix (Docker Container)
This application runs in a separate environment, acts as a Celery worker and triggers two concurrent workflows:

#### Module A : Worker
* Uses threads for tasks for time efficiency.
* Upon receiving an eviction message from Celery:
  * Updates Azure Traffic Manager to set the endpoint status to <i>Disabled</i> (using azure-mgmt-trafficmanager library).
  * Silences Azure Monitor alerts for the specific VM (using azure.mgmt.alertsmanagement library).

#### Module B : Resurrector
* Extracts <i>vm_name</i>, <i>resource_group</i> and <i>subscription_id</i> from the Celery task payload.
* Ensures the VM state is <i>Deallocated </i> (via azure-mgmt-compute) before initiating the restart loop to avoid API conflicts during the eviction process.
* Uses the ComputeManagementClient provided by the Azure SDK to abstract and execute the underlying REST API operations.
* Wait-and-Retry loop
  * Initiates a 10-minute cooldown period after eviction
  * After the cooldown, enters a loop calling the VM Start REST API (POST)
  * If capacity is unavailable, the module waits 10 minutes before retrying the start command
  * Repeats until capacity is available
  * Upon successful start, automatically restores the Azure Traffic Manager endpoint back to <i>Enabled</i> status.

#### Sequence diagram

```mermaid
sequenceDiagram
    participant IMDS as Azure IMDS
    participant L as Spot Phoenix Listener (Docker on VM)

    participant C as Celery Broker
    participant W as Worker Module
    participant R as Resurrector Module

    participant AZ as Azure API

    L->>IMDS: Poll 169.254.169.254 (1s interval)
    IMDS-->>L: Eviction Notice
    L->>L: os.sync() / SIGTERM

    Note over L,C: Listener sends task out of the dying VM
    L->>C: Dispatch Eviction Task

    C->>R: Trigger Resurrector immediately in background (.delay)
    R->>R: Start 10m Cooldown Period

    C->>W: Start Worker Module (Main Thread)
    par Concurrent Workflows
        W->>AZ: Disable Traffic Manager
        W->>AZ: Silence Monitor Alerts
    end

    loop Status Checking
        R->>AZ: Get instanceView (Check if PowerState/deallocated)
        AZ-->>R: Return status codes
    end

    loop Start Retry Loop
       R->>AZ: vm.start()
       alt Capacity Error
          AZ-->>R: Capacity Error (Wait 10m)
          R->>R: Sleep 10m
       else Success
         AZ-->>R: VM Running
       end
    end
    R->>AZ: Restore Traffic Manager to 'Enabled'
    AZ-->>L: VM Back Online & Traffic Restored
```

#### Infrastructure diagram

```mermaid
graph TD
    subgraph "Spot VM (On-Instance)"
        A[Spot VM] -->|Termination notice| L1[Spot Phoenix Listener]
        L1 -->|Local Action| MEM[os.sync / Log Flush]
    end

    L1 -->|Network Call: Dispatch Task| CELERY

    subgraph "Spot Phoenix Core Infrastructure"
        CELERY{Celery Broker} -->|Triggers Threaded Tasks| W[Worker Module]
        CELERY -->|Triggers Background Task| R[Resurrector Module]

        W -->|API Call| ATM[Azure Traffic Manager: Disable]
        W -->|API Call| MON[Azure Monitor: Suppress Alerts]
        
        R --> C[10 min Cooldown]
        C --> S{Is Deallocated?}
        S -- No --> W1[Wait & Poll]
        W1 --> S

        S -- Yes --> AZ[Azure Compute API: Start]
        AZ -- Capacity Error --> W2[Wait 10 min]
        W2 --> AZ

        AZ -.->|Success| ATM_EN[Azure Traffic Manager: Enable]
        ATM_EN -.->|System Recovered| A
    end
```

### Message Schema (Celery Task Payload)

To ensure the application can identify the resource and execute the correct contingency measures, the Listener will publish a JSON payload using the following structure:

```json
{
  "vm_name": "spot-vm-01",
  "resource_group": "production-rg",
  "subscription_id": "00000000-0000-0000-0000-000000000000",
  "event_type": "Preempt",
  "timestamp": "2026-04-18T14:55:02Z"
}
```

### Environment configuration (.env)

To run the Spot Phoenix application, the following environment variables need to be configured. These will be used by DefaultAzureCredential to authenticate with the Azure API and to establish a connection with the Celery message broker.

#### Azure Authentication
AZURE_SUBSCRIPTION_ID=subscription-id-uuid  
AZURE_TENANT_ID=tenant-id-uuid  
AZURE_CLIENT_ID=service-principal-app-id  
AZURE_CLIENT_SECRET=service-principal-password

#### RabbitMQ Configuration (Broker)
RABBITMQ_DEFAULT_USER=your_rabbitmq_user  
RABBITMQ_DEFAULT_PASS=your_secure_password

#### Celery Configuration
CELERY_BROKER_URL=amqp://your_rabbitmq_user:your_secure_password@rabbitmq-server-address:5672//  
CELERY_RESULT_BACKEND=redis://redis-server-address:6379/0

#### Application Settings
COOLDOWN_PERIOD=600


