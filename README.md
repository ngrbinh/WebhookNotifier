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

### Dashboard

Open `http://localhost:8084` after starting Compose. Create accounts and register matching webhooks first. Each webhook has a response distribution across `2xx`, `429`, permanent `404`, and `5xx` outcomes; the percentages must total 100.

In **Simulation and deliveries**, add one or more account/event/count entries, set the shared publication rate, and choose **Start simulation**. Every submitted run is tracked independently in the browser, so additional runs can start while another is in progress. The run list is intentionally cleared on page reload. Set **Newest deliveries** to control how many recent captures are shown; each row displays the account, event type, receipt time, and returned status.

Run the dashboard with:

```powershell
go run ./cmd/simulator
```

The only simulator flag is `-receiver`, which overrides the receiver ingestion URL. Configure accounts, webhook outcomes, event entries, and publication rate in the dashboard.

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
