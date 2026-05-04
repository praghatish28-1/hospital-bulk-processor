# Hospital Bulk Processing System

A production-grade bulk processing API that accepts CSV uploads and creates
hospital records in parallel via Celery workers, integrating with the
[Hospital Directory API](https://hospital-directory.onrender.com/docs).

**Live API:** `https://hospital-bulk-api.onrender.com/docs`  
**Flower Dashboard:** `https://hospital-bulk-flower.onrender.com`

---

## Quick Demo

```bash
# 1. Validate your CSV first (dry run, no side effects)
curl -X POST https://hospital-bulk-api.onrender.com/hospitals/bulk/validate \
  -F "file=@sample.csv"

# 2. Upload for processing → get job_id immediately
curl -X POST https://hospital-bulk-api.onrender.com/hospitals/bulk \
  -F "file=@sample.csv"

# 3. Poll progress
curl https://hospital-bulk-api.onrender.com/hospitals/bulk/{job_id}/status

# 4. Or connect WebSocket for real-time updates
wscat -c wss://hospital-bulk-api.onrender.com/hospitals/bulk/{job_id}/ws
```

Or open `/docs` and run the full flow interactively via Swagger UI.

---

## API Reference

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/hospitals/bulk` | Upload CSV → `202 Accepted` with `job_id` |
| `GET` | `/hospitals/bulk/{job_id}/status` | Poll job progress |
| `WS` | `/hospitals/bulk/{job_id}/ws` | Real-time progress via WebSocket |
| `POST` | `/hospitals/bulk/validate` | Validate CSV format, no side effects |
| `POST` | `/hospitals/bulk/{job_id}/resume` | Re-process only failed rows |
| `GET` | `/health` | Service health: API + Celery + Redis |

### CSV Format

```csv
name,address,phone
General Hospital,123 Main St,555-0101
City Medical Center,456 Oak Ave,555-0102
```

- `name` — required
- `address` — required
- `phone` — optional
- Maximum **20 hospitals** per upload

### Response Shape

```json
{
  "batch_id": "550e8400-e29b-41d4-a716-446655440000",
  "total_hospitals": 5,
  "processed_hospitals": 5,
  "failed_hospitals": 0,
  "processing_time_seconds": 2.4,
  "batch_activated": true,
  "hospitals": [
    {
      "row": 1,
      "hospital_id": 101,
      "name": "General Hospital",
      "status": "created_and_activated"
    }
  ]
}
```

---

## Architecture

```
Client
  │
  ├── POST /hospitals/bulk (CSV)
  │        └── 202 Accepted { job_id, status_url, ws_url }
  │
  ├── GET  /hospitals/bulk/{id}/status   (polling)
  └── WS   /hospitals/bulk/{id}/ws       (real-time)

FastAPI ──► Celery Orchestrator
                │
                └── chord(
                      group(create_hospital × N),  ← parallel
                      activate_batch               ← fires after all
                    )
                         │
                         ▼
              Hospital Directory API
                         │
                    Upstash Redis
              (broker + state + pub/sub)
```

### Processing Flow

1. Client uploads CSV → FastAPI validates immediately
2. FastAPI generates `job_id`, stores initial state in Redis, returns **202** instantly
3. Celery orchestrator spawns N parallel `create_hospital` tasks via `chord/group`
4. Each task POSTs one hospital to the Hospital Directory API with `batch_id`
5. Each task updates Redis state and publishes progress to pub/sub
6. Once all tasks complete, `activate_batch` callback fires automatically
7. If all succeeded → `PATCH activate` → status `complete`
8. If any failed → activation skipped → status `partial_failure`

Client receives live progress via WebSocket or by polling `/status`.

---

## Architecture Decisions

### Why 202 Accepted over synchronous response

A synchronous `POST /bulk` that blocks until all hospitals are created would
time out on slow networks and gives the client no visibility into progress.
The 202 pattern returns immediately with a `job_id` the client uses to track
progress — either by polling or WebSocket. This is the standard approach for
any operation that takes more than a few hundred milliseconds.

### Why Celery over FastAPI BackgroundTasks

`BackgroundTasks` ties task lifecycle to the HTTP process. If the process
restarts mid-batch (Render free tier spins down after inactivity), in-flight
jobs are lost with no recovery path.

Celery gives us:
- **Durable task state** — persists in Redis across process restarts
- **Per-task retry** with exponential backoff, built-in
- **Resume capability** — re-enqueue the failed subset naturally
- **Parallelism as config** — `worker_concurrency` in `celeryconfig.py`,
  no code changes to scale
- **Flower** — free observability with zero additional code

### Why chord/group over asyncio.gather inside a task

`asyncio.gather` inside a single Celery task works but has key limitations:
parallelism is a code concern (not config), each hospital creation is
invisible in Flower, and resume requires custom state machine logic.

With Celery `chord(group(...), callback)`:
- Concurrency is controlled by `worker_concurrency` in `celeryconfig.py`
- Each hospital creation is an individually observable, retryable task in Flower
- Resume is trivial — re-enqueue the failed subset, reuse the same callback
- Horizontal scaling requires no code changes — add more worker instances

### Why Upstash Redis over Render-managed Redis

Render free-tier services spin down after 15 minutes of inactivity. A Redis
broker that spins down loses all queued tasks — catastrophic for background
jobs. Upstash is serverless Redis — always-on, HTTP-based, free tier is
sufficient for this workload.

### Why both WebSocket and polling

Polling is the reliable fallback for any client. WebSocket is the real-time
bonus layer. Both read from the same Redis state — no duplication of logic.
WebSocket uses Redis pub/sub; polling reads the job state key directly.
Their coexistence means clients choose based on their capability.

### Crash Safety: Check-before-create

Before each `POST /hospitals/`, the task calls
`GET /hospitals/batch/{batch_id}` and checks if a hospital with the same
name already exists. If yes, it skips creation and returns success. This
prevents duplicate hospitals if a task is re-run after a worker crash
mid-execution — a real scenario on Render's free tier.

Combined with `task_acks_late = True`, tasks not yet completed return to
the queue automatically when a worker dies. From the client's perspective,
processing continues from where it left off.

### Strict CSV validation

All three validation layers must pass before any hospital is created. A
partial CSV (18 valid rows, 2 invalid) rejects the entire upload. The
`/validate` endpoint exists specifically so clients can check their CSV
before committing. This prevents the confusing state of a partially-created
batch from a bad upload.

---

## Scaling Considerations

**Current design handles 20 hospitals trivially.**

At 10,000 hospitals:
- Increase `worker_concurrency` in `celeryconfig.py` — no code change
- Add rate limiting on Hospital API calls (token bucket per worker)
- Move job state to PostgreSQL for durability beyond Redis TTL
- Add dead letter queue for permanently failed tasks

At 1,000,000 hospitals:
- Celery remains valid — add more worker nodes horizontally
- Stream CSV parse instead of loading entire file into memory
- Chunk processing: groups of 100 with progress aggregation
- Hospital API becomes the bottleneck — rate limit negotiation required

---

## Local Development

### Prerequisites

- Python 3.11+
- Docker + Docker Compose
- An [Upstash](https://upstash.com) Redis instance (free tier)

### Setup

```bash
# Clone
git clone https://github.com/yourusername/hospital-bulk-processor
cd hospital-bulk-processor

# Copy env file
cp .env.example .env
# Fill in REDIS_URL from Upstash console
# HOSPITAL_API_URL is already set

# Install dependencies
pip install -e ".[dev]"

# Run with Docker Compose (API + Worker + Flower)
docker-compose up
```

| Service | URL |
|---------|-----|
| API + Swagger | http://localhost:8000/docs |
| Flower | http://localhost:5555 |

### Running Without Docker

```bash
# Terminal 1 — API
uvicorn app.main:app --reload --port 8000

# Terminal 2 — Celery worker
celery -A app.worker worker \
  --concurrency=10 \
  --queues=orchestration,hospital_creation,activation \
  --loglevel=info

# Terminal 3 — Flower (optional)
celery -A app.worker flower --port=5555
```

### Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `REDIS_URL` | ✅ | — | Upstash Redis connection URL |
| `HOSPITAL_API_URL` | ✅ | — | Hospital Directory API base URL |
| `MAX_HOSPITALS_PER_CSV` | ❌ | `20` | Hard limit per upload |
| `HTTP_TIMEOUT` | ❌ | `10.0` | Timeout for Hospital API calls (seconds) |
| `MAX_RETRIES` | ❌ | `3` | Max retries per hospital on 5xx/timeout |

---

## Running Tests

```bash
# All tests
pytest tests/ -v

# Specific file
pytest tests/test_csv_validation.py -v

# With coverage
pytest tests/ --cov=app --cov-report=term-missing
```

### Test Coverage

| File | Tests | Covers |
|------|-------|--------|
| `test_csv_validation.py` | 11 | All 3 validation layers, edge cases |
| `test_bulk_upload.py` | 6 | 202 response, response shape, idempotency |
| `test_partial_failures.py` | 5 | Mixed results, activation behaviour |
| `test_progress.py` | 8 | Polling increments, WebSocket events |
| `test_resume.py` | 4 | Only failed rows re-submitted |
| `test_infrastructure.py` | 4 | Health check, concurrent jobs |

---

## Deployment

Three services deployed from one repository via `render.yaml`:

| Service | Type | Start Command |
|---------|------|---------------|
| `hospital-bulk-api` | Web | `uvicorn app.main:app --host 0.0.0.0 --port $PORT` |
| `hospital-bulk-worker` | Worker | `celery -A app.worker worker --concurrency=10` |
| `hospital-bulk-flower` | Web | `celery -A app.worker flower --port=$PORT` |

All services share the same `REDIS_URL` (Upstash) and `HOSPITAL_API_URL`
environment variables configured in Render's dashboard.

To deploy your own instance:

1. Fork this repository
2. Create an [Upstash](https://upstash.com) Redis database (free tier)
3. Connect the repo to [Render](https://render.com)
4. Render auto-detects `render.yaml` and creates all three services
5. Set `REDIS_URL` and `HOSPITAL_API_URL` in Render's environment settings

---

## Tech Stack

| Concern | Choice |
|---------|--------|
| Framework | FastAPI |
| Task queue | Celery 5.3+ |
| Broker + state | Upstash Redis |
| HTTP client | httpx (async) |
| Validation | Pydantic v2 |
| Logging | structlog (JSON) |
| Monitoring | Flower |
| Containers | Docker + docker-compose |
| Deploy | Render |
| Tests | pytest + respx + fakeredis |

---

## Project Structure

```
app/
├── main.py              # FastAPI app, lifespan, router registration
├── worker.py            # Celery app init
├── celeryconfig.py      # All concurrency and routing config
├── core/
│   ├── config.py        # Env vars via pydantic-settings
│   └── redis.py         # Sync + async Redis clients
├── routers/
│   └── bulk.py          # All endpoints (no business logic)
├── services/
│   ├── csv_validator.py # 3-layer CSV validation
│   └── bulk_service.py  # Business logic between router and tasks
├── tasks/
│   ├── orchestrator.py  # Spawns chord/group
│   ├── create_hospital.py # Creates one hospital, retry logic
│   ├── activate_batch.py  # Chord callback, activates batch
│   └── resume.py        # Re-enqueues failed rows only
└── models/
    └── schemas.py       # All Pydantic models
```