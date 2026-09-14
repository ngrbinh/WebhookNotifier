# Webhook Notifier

Local proof of concept for durable, fair webhook delivery.

## Run

```powershell
docker compose up --build
```

The local services are available at:

- Receiver API: `http://localhost:8080`
- Simulator dashboard: `http://localhost:8084`
- RabbitMQ management: `http://localhost:15672` (`webhook` / `webhook`)
- PostgreSQL: `localhost:5432` (`webhook` / `webhook`, database `webhooknotifier`)

## Design

Read [document/system-design.md](document/system-design.md) for the high-level flow, fairness and reliability goals, scaling assumptions, and event contract. This README focuses on running and developing the local POC; the design document explains why the system is shaped this way.

## Components

The executable services are in `cmd/`:

- `receiver` validates events and durably inserts them into PostgreSQL before responding.
- `dispatcher` finds pending and due-retry events, rotates accounts with `fairness.AccountRoundRobin`, and publishes bounded batches to RabbitMQ.
- `worker` delivers webhook payloads, records success/retry/dead-letter state, and acknowledges only after persistence.
- `simulator` generates traffic for balanced and noisy-neighbor scenarios.

The main implementation packages are:

- `internal/model`: event contracts and validation.
- `internal/storage`: migrations, idempotency, leases, event state, and delivery attempts.
- `internal/queue`: durable RabbitMQ publishing and consumption.
- `internal/fairness`: account round-robin ordering and high/low watermark hysteresis.
- `internal/delivery`: partner response classification and retry backoff.

When changing the pipeline, preserve these boundaries. In particular, retry scheduling belongs in PostgreSQL so workers do not block, and a published message may be duplicated if a process fails between RabbitMQ publication and the database update.

## Simulator

Open `http://localhost:8084` after starting Compose. The dashboard has two screens:

### Registration

- **Features:**
  - Create accounts that own webhook destinations.
  - Register a webhook for an account and select the event types it accepts.
  - Configure the expected response distribution across `2xx`, `429`, permanent `404`, and `5xx` outcomes. The percentages must total 100.
  - Configure an optional response delay from 0 to 30,000 milliseconds.
  - Review registered destinations and delete webhooks when they are no longer needed.
- **Usage:**
  1. Enter an account ID and select **Create account**.
  2. Select the account in **Register webhook**.
  3. Select at least one event type.
  4. Set the response percentages and optional response delay.
  5. Select **Register webhook**.

### Simulation and deliveries

- **Features:**
  - Add one or more account/event/count entries to generate traffic.
  - Set the shared publication rate in events per second.
  - Track each submitted simulation run independently in the browser, including runs started while another run is in progress.
  - View captured deliveries with the account, event type, receipt time, and returned status.
  - Choose **Show newest deliveries** to control how many recent captures are displayed.
  - Clear all captured deliveries from the dashboard.
- **Usage:**
  1. Select **Add event entry** and configure the account, event type, and event count for each entry.
  2. Set **Events per second**.
  3. Select **Start simulation**.
  4. Monitor the result in **Simulation runs** and inspect delivery outcomes in **Captured deliveries**.

The simulation run list is intentionally cleared when the page is reloaded. Configure accounts and webhooks on the Registration screen before starting a simulation.

## Receiver example

```powershell
Invoke-RestMethod http://localhost:8080/api/v1/events -Method Post -ContentType application/json -Body (@{
  account_id = 'acc_tenant_01'
  destination_url = 'http://host.docker.internal:8090/webhook'
  idempotency_key = 'evt_unique_uuid_9876'
  payload = @{
    event_name = 'subscriber.created'
    event_time = '2026-09-12T10:00:00Z'
    webhook_id = 'wh_sub_12345'
    subscriber = @{ id = 'sub_1'; status = 'active'; email = 'user@example.com' }
  } | ConvertTo-Json -Depth 8
} | ConvertTo-Json -Depth 8)
```
