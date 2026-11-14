# High-Throughput Real-Time Notification Engine

[![CI](https://github.com/nishchal1210/async-notification-worker/actions/workflows/ci.yml/badge.svg)](https://github.com/nishchal1210/async-notification-worker/actions/workflows/ci.yml)

A production-oriented asynchronous notification backend built with **FastAPI, PostgreSQL, Redis, RabbitMQ, Prometheus, Grafana, Docker Compose, and Locust**.

The system accepts notification requests through a FastAPI API, persists them in PostgreSQL, publishes delivery jobs to RabbitMQ, and processes them asynchronously through horizontally scalable workers. It includes idempotency, rate limiting, priority queues, retry/backoff, dead-letter handling, templating, structured logging, health checks, graceful shutdown, and observability.

> **Project status:** Fully runnable locally with Docker Compose. Cloud deployment is not included yet.

---

## Why this project exists

Sending an email directly inside an API request is simple, but it does not scale well. Slow provider calls increase API latency, transient failures can lose work, retries become difficult to coordinate, and traffic spikes can overwhelm downstream providers.

This project evolves that naive approach into an asynchronous notification pipeline with queue-based delivery, reliability controls, observability, and horizontal worker scaling.

---

## Architecture

```mermaid
flowchart LR
    C[Client] --> API[FastAPI API]

    API -->|Persist notification| PG[(PostgreSQL)]
    API -->|Rate limit| REDIS[(Redis)]
    API -->|Publish job| RMQ[(RabbitMQ)]

    RMQ --> W1[Worker 1]
    RMQ --> W2[Worker 2]
    RMQ --> W3[Worker 3]

    W1 --> P[Provider Layer]
    W2 --> P
    W3 --> P

    P --> EMAIL[SMTP Email]
    P --> SMS[Development SMS Provider]
    P --> PUSH[Firebase Push]

    W1 --> PG
    W2 --> PG
    W3 --> PG

    API --> METRICS[Prometheus]
    W1 --> METRICS
    W2 --> METRICS
    W3 --> METRICS
    METRICS --> GRAFANA[Grafana]
```

### Request flow

1. Client sends `POST /notifications` with an `Idempotency-Key`.
2. API validates the user and request payload.
3. Existing idempotency keys return the previously created notification.
4. Redis applies a per-user fixed-window rate limit.
5. PostgreSQL stores the notification with `PENDING` status.
6. A dedicated RabbitMQ publisher thread publishes the notification ID using publisher confirms.
7. API returns `202 Accepted` only after publication succeeds.
8. A worker consumes the job, creates a delivery attempt, and resolves the provider for the channel.
9. Successful delivery updates the notification to `SENT` and ACKs the RabbitMQ message.
10. Temporary provider failures are retried using RabbitMQ TTL + dead-letter routing with exponential backoff.
11. Notifications that exhaust the retry budget are moved to a dead-letter queue.

---

## Key features

### Asynchronous delivery

RabbitMQ decouples request ingestion from provider delivery. The API does not wait for SMTP, Firebase, or other provider operations to complete.

### Multi-channel provider abstraction

Supported channels:

- **Email** — SMTP provider
- **Push** — Firebase Cloud Messaging
- **SMS** — development/logging provider that can be replaced with a real SMS gateway
- **Load-test provider** — configurable fake provider for reproducible performance tests

### Idempotency

`POST /notifications` requires an `Idempotency-Key` header. Duplicate keys return the existing notification instead of creating duplicate rows.

A unique PostgreSQL constraint provides a final safeguard against concurrent duplicate requests.

### Redis rate limiting

Per-user fixed-window rate limiting is implemented atomically using a Redis Lua script with `INCR`, `EXPIRE`, and `TTL`.

Rejected requests return:

```http
429 Too Many Requests
```

with `Retry-After`, `X-RateLimit-Limit`, and `X-RateLimit-Remaining` headers.

### Priority queue

RabbitMQ uses a priority-enabled delivery queue:

- `HIGH` → priority 10
- `NORMAL` → priority 5
- `LOW` → priority 1

Priority metadata is preserved across retry and dead-letter flows.

### Retry and exponential backoff

Workers retry temporary provider failures through a dedicated retry queue.

The retry queue uses message TTL and dead-letter routing so workers do not block while waiting between attempts.

The current worker supports up to **4 delivery attempts** before dead-lettering.

### Dead-letter queue

Notifications that exhaust retries are marked `DEAD_LETTERED` and routed to a final RabbitMQ DLQ for later inspection.

### Versioned notification templates

Templates are stored in PostgreSQL and rendered using Jinja2. Template versions are retained so notifications record which template version was used.

Example template payload:

```json
{
  "name": "otp",
  "channel": "email",
  "subject_template": "Your OTP",
  "body_template": "Hello {{ name }}, your OTP is {{ otp }}."
}
```

### Structured logging and request correlation

Logs are emitted as JSON and include fields such as:

- `service`
- `request_id`
- `notification_id`
- `channel`
- `priority`
- `attempt_no`
- `error_type`

The API accepts or generates `X-Request-ID`, persists it with the notification, and propagates it through RabbitMQ messages.

Sensitive recipients are masked in logs.

### Health checks

The API exposes:

```text
GET /health/live
GET /health/ready
```

Readiness checks PostgreSQL, Redis, and the RabbitMQ publisher lifecycle.

### Graceful shutdown

The API stops accepting work during shutdown and cleanly closes the publisher thread and RabbitMQ connection.

Workers handle `SIGINT` / `SIGTERM`, stop consuming, close RabbitMQ cleanly, and preserve RabbitMQ acknowledgement semantics for in-flight jobs.

### Prometheus + Grafana observability

The API and workers expose Prometheus metrics for:

- total notification requests
- rejected requests
- notifications created
- API request latency
- RabbitMQ publish failures
- delivery attempts
- successful / failed deliveries
- retries scheduled
- dead-lettered notifications
- provider latency
- worker processing latency
- jobs in progress
- worker health
- RabbitMQ connection health
- end-to-end delivery latency
- dependency health

---

## Reliability model

The current API uses direct PostgreSQL + RabbitMQ publishing:

```text
Persist notification
        ↓
Publish to RabbitMQ
        ↓
Publisher confirmation
        ↓
Return 202
```

If RabbitMQ publication fails, the notification is marked `PUBLISH_FAILED` and the API returns `503 Service Unavailable`.

The worker ACKs a delivery message only after relevant database changes are committed.

> **Known limitation:** PostgreSQL persistence and RabbitMQ publishing are not one atomic transaction. A future production upgrade would use the Transactional Outbox pattern.

---

## Load testing

Locust is used to generate a mixed workload approximating:

- 60% email
- 30% push
- 10% SMS

For repeatable benchmarking, `LOAD_TEST_MODE=true` routes all channels through a simulated provider with configurable latency and failure rate.

### Worker-scaling experiment

| Metric | 1 Worker | 3 Workers |
|---|---:|---:|
| HTTP requests | 2,785 | 2,539 |
| HTTP failures | 0 | 0 |
| API throughput | 9.32 req/s | 8.49 req/s |
| API p50 | 10 s | 11 s |
| API p95 | 12 s | 13 s |
| Peak delivery throughput | 6.07/s | 8.73/s |
| Provider p95 | ~95 ms | ~95 ms |
| Queue drain time | 192.29 s | 13.48 s |
| Dead-lettered notifications | 0 | 0 |

Observed queue drain time decreased from **192.29 s to 13.48 s** when moving from one worker to three workers in these local test runs.

The provider p95 remained effectively unchanged, indicating that the improvement was caused by parallel worker processing rather than faster provider execution.

Detailed notes are available in [`docs/load_test_report.md`](docs/load_test_report.md).

> These are local benchmark results, not production capacity claims.

---

## Tech stack

| Area | Technology |
|---|---|
| API | FastAPI |
| Validation | Pydantic |
| Database | PostgreSQL + SQLAlchemy |
| Queue | RabbitMQ + Pika |
| Rate limiter | Redis + Lua |
| Email | SMTP |
| Push | Firebase Admin SDK |
| Templates | Jinja2 |
| Metrics | Prometheus Client |
| Dashboards | Grafana |
| Load testing | Locust |
| Containerization | Docker |
| Local orchestration | Docker Compose |
| Testing | Pytest |

---

## Repository structure

```text
.
├── main1.py                    # FastAPI application
├── worker.py                   # RabbitMQ delivery worker
├── rabbitmq_publisher.py       # Long-lived publisher thread + confirms
├── notification_schemas.py     # Pydantic request schemas
├── rate_limiter.py             # Redis Lua fixed-window rate limiter
├── redis_client.py
├── template_service.py         # Template lookup/rendering
├── logging_config.py           # JSON logging
├── log_utils.py                # PII masking helpers
├── health.py                   # Dependency health checks
├── config.py                   # Startup configuration validation
├── metrics.py                  # Prometheus metrics
│
├── providers/
│   ├── base.py
│   ├── factory.py
│   ├── email_provider.py
│   ├── sms_provider.py
│   ├── push_provider.py
│   └── load_test_provider.py
│
├── migrations/
│   ├── 013_notification_templates.sql
│   └── 014_structured_logging.sql
│
├── load_tests/
│   └── locustfile.py
│
├── docs/
│   └── load_test_report.md
│
├── test_health.py
├── test_logging.py
├── test_metrics.py
├── test_notifications.py
├── test_priority_queues.py
├── test_redis.py
├── test_template_service.py
│
├── Dockerfile
├── compose.yaml
├── prometheus.yml
├── requirements.txt
└── .env.example
```

---

## Running locally with Docker Compose

### Prerequisites

- Docker Desktop / Docker Engine with Compose
- A RabbitMQ connection URL (for example a development CloudAMQP instance)
- Provider credentials only for the real providers you plan to use

### 1. Clone the repository

```bash
git clone https://github.com/<your-username>/<your-repository>.git
cd <your-repository>
```

### 2. Create your environment file

```bash
cp .env.example .env
```

Fill in the required values.

At minimum, the application expects:

```env
DATABASE_URL=postgresql+psycopg2://notification_user:notification_password@postgres:5432/notification_db
REDIS_URL=redis://redis:6379/0
RABBITMQ_URL=amqps://...
```

For load testing, real provider credentials are not required when:

```env
LOAD_TEST_MODE=true
```

### 3. Start the stack

```bash
docker compose up --build -d
```

This starts:

- PostgreSQL
- Redis
- FastAPI
- 3 notification workers
- Prometheus
- Grafana

RabbitMQ is expected to be externally available through `RABBITMQ_URL`.

### 4. Verify services

```bash
docker compose ps
```

API readiness:

```text
http://localhost:8000/health/ready
```

FastAPI docs:

```text
http://localhost:8000/docs
```

Prometheus:

```text
http://localhost:9090
```

Grafana:

```text
http://localhost:3000
```

### 5. View logs

```bash
docker compose logs -f api
```

```bash
docker compose logs -f worker-1 worker-2 worker-3
```

### 6. Stop everything

```bash
docker compose down
```

To also delete persistent Docker volumes:

```bash
docker compose down -v
```

---

## Environment variables

Do **not** commit your real `.env` file.

| Variable | Purpose |
|---|---|
| `DATABASE_URL` | PostgreSQL connection string |
| `REDIS_URL` | Redis connection string |
| `RABBITMQ_URL` | RabbitMQ AMQP/AMQPS connection string |
| `RATE_LIMIT_REQUESTS` | Requests allowed per fixed window |
| `RATE_LIMIT_WINDOW_SECONDS` | Rate-limit window duration |
| `SMTP_HOST` | SMTP server hostname |
| `SMTP_PORT` | SMTP server port |
| `SMTP_USERNAME` | SMTP username |
| `SMTP_PASSWORD` | SMTP password |
| `FROM_EMAIL` | Sender email address |
| `FIREBASE_PROJECT_ID` | Firebase project ID |
| `FIREBASE_PRIVATE_KEY_ID` | Firebase private-key ID |
| `FIREBASE_PRIVATE_KEY` | Firebase service-account private key |
| `FIREBASE_CLIENT_EMAIL` | Firebase service-account email |
| `FIREBASE_CLIENT_ID` | Firebase service-account client ID |
| `FIREBASE_CLIENT_X509_CERT_URL` | Firebase client certificate URL |
| `LOAD_TEST_MODE` | Use simulated delivery provider |
| `LOAD_TEST_PROVIDER_MIN_MS` | Minimum simulated latency |
| `LOAD_TEST_PROVIDER_MAX_MS` | Maximum simulated latency |
| `LOAD_TEST_PROVIDER_FAILURE_RATE` | Simulated provider failure probability |
| `WORKER_METRICS_PORT` | Worker Prometheus endpoint port |
| `GRAFANA_ADMIN_PASSWORD` | Local Grafana admin password |

---

## API usage

### Create a notification

```http
POST /notifications
Idempotency-Key: 01c02c3d-...
X-Request-ID: optional-request-id
Content-Type: application/json
```

Email example:

```json
{
  "user_id": 1,
  "channel": "email",
  "recipient": "user@example.com",
  "subject": "Order update",
  "body": "Your order has shipped.",
  "priority": "NORMAL"
}
```

Successful response:

```json
{
  "notification_id": "<uuid>",
  "status": "PENDING",
  "message": "Notification accepted for processing"
}
```

### Create a template

```http
POST /templates
Content-Type: application/json
```

```json
{
  "name": "otp",
  "channel": "email",
  "subject_template": "Your OTP",
  "body_template": "Hello {{ name }}, your OTP is {{ otp }}."
}
```

### Send using a template

```json
{
  "user_id": 1,
  "channel": "email",
  "recipient": "user@example.com",
  "template_name": "otp",
  "template_data": {
    "name": "Alex",
    "otp": "123456"
  },
  "priority": "HIGH"
}
```

---

## Important HTTP responses

| Status | Meaning |
|---|---|
| `202` | Notification persisted and RabbitMQ publication confirmed |
| `400` | Invalid request / missing idempotency key / template error |
| `404` | User not found |
| `429` | Per-user rate limit exceeded |
| `503` | Required dependency or RabbitMQ publishing unavailable |

---

## Notification states

The system uses states including:

```text
PENDING
PROCESSING
SENT
PUBLISH_FAILED
FAILED
DEAD_LETTERED
```

Delivery attempts are tracked separately in `notification.notification_attempts`.

---

## RabbitMQ topology

Core queues include:

```text
notification.delivery
notification.delivery.retry
notification.delivery.dlq
```

The main delivery queue supports priorities. The retry queue uses dead-letter routing back to the main exchange after TTL expiry.

---

## Prometheus metrics

Examples:

```text
notification_requests_total
notification_requests_rejected_total
notifications_created_total
rabbitmq_publish_failures_total
notification_api_request_duration_seconds
notification_delivery_attempts_total
notification_delivery_success_total
notification_delivery_failures_total
notification_retry_scheduled_total
notification_dead_lettered_total
notification_provider_duration_seconds
notification_worker_processing_duration_seconds
notification_worker_jobs_in_progress
notification_end_to_end_delivery_duration_seconds
notification_worker_health
notification_worker_rabbitmq_connected
notification_dependency_health
```

Example p95 end-to-end latency query:

```promql
histogram_quantile(
  0.95,
  sum by (le) (
    rate(notification_end_to_end_delivery_duration_seconds_bucket[5m])
  )
)
```

---

## Running tests

Install dependencies:

```bash
pip install -r requirements.txt
pip install pytest
```

Then run:

```bash
pytest -q
```

The repository contains tests covering request validation, provider selection, health checks, structured logging, metrics, templates, Redis-related behavior, and RabbitMQ priority/retry metadata.

---

## Load testing

Example:

```bash
locust -f load_tests/locustfile.py --host http://localhost:8000
```

For deterministic provider simulation:

```env
LOAD_TEST_MODE=true
LOAD_TEST_PROVIDER_MIN_MS=20
LOAD_TEST_PROVIDER_MAX_MS=80
LOAD_TEST_PROVIDER_FAILURE_RATE=0
```

---

## Security notes

- Never commit `.env`, Firebase private keys, SMTP passwords, RabbitMQ credentials, Redis credentials, or database passwords.
- Use environment variables for all credentials.
- Keep PostgreSQL and Redis private in real deployments.
- Rotate any credential that has ever been committed to Git history.
- The development SMS provider intentionally does not call a real SMS gateway.

---

## Current limitations and future work

- Transactional outbox for atomic database-to-broker handoff
- Alembic-based schema migrations instead of startup `create_all`
- Real SMS provider integration
- Managed/cloud deployment
- CI/CD pipeline
- Authentication / authorization for API clients
- Automated Grafana dashboard provisioning in the repository
- More extensive integration and failure-injection testing

---

## Engineering lessons demonstrated

This project is intentionally broader than a CRUD API. It demonstrates:

- asynchronous job processing
- at-least-once delivery semantics
- message acknowledgement design
- retry and dead-letter flows
- idempotent request handling
- backpressure and rate limiting
- horizontal worker scaling
- health/readiness design
- graceful lifecycle management
- metrics and structured logging
- load testing and bottleneck analysis
- Docker-based multi-service orchestration

---


