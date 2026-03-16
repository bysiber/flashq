# ⚡ FlashQ

**The task queue that works out of the box — no Redis, no RabbitMQ, just `pip install flashq` and go.**

[![CI](https://github.com/bysiber/flashq/actions/workflows/ci.yml/badge.svg)](https://github.com/bysiber/flashq/actions/workflows/ci.yml)
[![Python](https://img.shields.io/pypi/pyversions/flashq)](https://pypi.org/project/flashq/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Tests](https://img.shields.io/badge/tests-226%20passing-brightgreen)]()

---

## Why FlashQ?

Every Python developer has been there: you need background tasks, you look at Celery, and suddenly you need Redis or RabbitMQ running, a separate broker config, and 200 lines of boilerplate before your first task runs.

**FlashQ changes that.** SQLite is the default backend — zero external dependencies, zero config. Your tasks persist across restarts, and you can scale to PostgreSQL or Redis when you need to.

```python
from flashq import FlashQ

app = FlashQ()  # That's it. Uses SQLite by default.

@app.task()
def send_email(to: str, subject: str) -> None:
    print(f"Sending email to {to}: {subject}")

# Enqueue a task
send_email.delay(to="user@example.com", subject="Welcome!")
```

## Features

| Feature | FlashQ | Celery | Dramatiq | Huey | TaskIQ |
|---------|:------:|:------:|:--------:|:----:|:------:|
| Zero-config setup | ✅ | ❌ | ❌ | ⚠️ | ❌ |
| SQLite backend | ✅ | ❌ | ❌ | ✅ | ❌ |
| PostgreSQL backend | ✅ | ❌ | ❌ | ❌ | ⚠️ |
| Redis backend | ✅ | ✅ | ✅ | ✅ | ✅ |
| Async + Sync tasks | ✅ | ❌ | ❌ | ❌ | Async only |
| Type-safe `.delay()` | ✅ | ❌ | ⚠️ | ❌ | ✅ |
| Task chains/groups | ✅ | ✅ | ❌ | ❌ | ❌ |
| Middleware system | ✅ | ✅ | ✅ | ❌ | ✅ |
| Rate limiting | ✅ | ✅ | ❌ | ❌ | ❌ |
| Dead letter queue | ✅ | ❌ | ❌ | ❌ | ❌ |
| Task timeouts | ✅ | ✅ | ✅ | ❌ | ✅ |
| Periodic/cron scheduler | ✅ | ⚠️ | ❌ | ✅ | ⚠️ |
| Zero dependencies | ✅ | ❌ | ❌ | ❌ | ❌ |

## Installation

```bash
# Core (SQLite only — zero dependencies!)
pip install flashq

# With Redis
pip install "flashq[redis]"

# With PostgreSQL
pip install "flashq[postgres]"

# Development
pip install "flashq[dev]"
```

## Quick Start

### 1. Define tasks

```python
# tasks.py
from flashq import FlashQ

app = FlashQ()

@app.task()
def add(x: int, y: int) -> int:
    return x + y

@app.task(queue="emails", max_retries=5, retry_delay=30.0)
def send_email(to: str, subject: str, body: str) -> dict:
    return {"status": "sent", "to": to}

@app.task(timeout=120.0)  # Kill if takes >2 min
async def process_image(url: str) -> str:
    # Async tasks just work™
    result = await download_and_resize(url)
    return result
```

### 2. Enqueue tasks

```python
from tasks import add, send_email

# Simple dispatch
handle = add.delay(2, 3)

# With options
handle = send_email.apply(
    kwargs={"to": "user@example.com", "subject": "Hi", "body": "Hello!"},
    countdown=60,  # delay by 60 seconds
)

# Check result
result = handle.get_result()
if result and result.is_success:
    print(f"Result: {result.result}")
```

### 3. Start the worker

```bash
flashq worker tasks:app
```

```
 ⚡ FlashQ Worker
 ├─ name:        worker-12345
 ├─ backend:     SQLiteBackend
 ├─ queues:      default
 ├─ concurrency: 4
 ├─ tasks:       3
 │    └─ tasks.add
 │    └─ tasks.send_email
 │    └─ tasks.process_image
 └─ Ready! Waiting for tasks...
```

## Task Composition

Chain tasks sequentially, run them in parallel, or combine both:

```python
from flashq import chain, group, chord

# Chain: sequential — result of each passed to next
pipe = chain(
    download.s("https://example.com/data.csv"),
    parse_csv.s(),
    store_results.s(table="imports"),
)
pipe.delay(app)

# Group: parallel execution
batch = group(
    send_email.s(to="alice@test.com", subject="Hi"),
    send_email.s(to="bob@test.com", subject="Hi"),
    send_email.s(to="carol@test.com", subject="Hi"),
)
handle = batch.delay(app)
results = handle.get_results(timeout=30)

# Chord: parallel + callback when all complete
workflow = chord(
    group(fetch_price.s("AAPL"), fetch_price.s("GOOG"), fetch_price.s("MSFT")),
    aggregate_prices.s(),
)
workflow.delay(app)
```

## Middleware

Intercept task lifecycle events for logging, monitoring, or custom logic:

```python
from flashq import FlashQ, Middleware

class MetricsMiddleware(Middleware):
    def before_execute(self, message):
        self.start = time.time()
        return message

    def after_execute(self, message, result):
        duration = time.time() - self.start
        statsd.timing(f"task.{message.task_name}.duration", duration)

    def on_error(self, message, exc):
        sentry.capture_exception(exc)
        return False  # Don't suppress

    def on_dead(self, message, exc):
        alert_ops_team(f"Task {message.task_name} permanently failed: {exc}")

app = FlashQ()
app.add_middleware(MetricsMiddleware())
```

Built-in middlewares: `LoggingMiddleware`, `TimeoutMiddleware`, `RateLimiter`.

## Rate Limiting

```python
from flashq.ratelimit import RateLimiter

limiter = RateLimiter(default_rate="100/m")  # 100 tasks/minute global
limiter.configure("send_email", rate="10/m")  # 10 emails/minute
limiter.configure("api_call", rate="60/h")    # 60 API calls/hour

app.add_middleware(limiter)
```

## Dead Letter Queue

Inspect and replay permanently failed tasks:

```python
from flashq.dlq import DeadLetterQueue

dlq = DeadLetterQueue(app)
app.add_middleware(dlq.middleware())  # Auto-capture dead tasks

# Later...
for task in dlq.list():
    print(f"{task.task_name}: {task.error}")

dlq.replay(task_id="abc123")  # Re-enqueue with reset retries
dlq.replay_all()              # Replay everything
dlq.purge()                   # Clear DLQ
```

## Periodic Tasks

```python
from flashq import FlashQ, every, cron
from flashq.scheduler import Scheduler

app = FlashQ()

@app.task(name="cleanup")
def cleanup_old_data():
    delete_old_records(days=30)

@app.task(name="daily_report")
def daily_report():
    generate_and_send_report()

scheduler = Scheduler(app)
scheduler.add("cleanup", every(hours=6))
scheduler.add("daily_report", cron("0 9 * * 1-5"))  # 9 AM weekdays
scheduler.start()
```

## Retry & Error Handling

```python
@app.task(max_retries=5, retry_delay=30.0, retry_backoff=True)
def flaky_task():
    # Retries: 30s → 60s → 120s → 240s → 480s (exponential backoff)
    response = requests.get("https://unreliable-api.com")
    response.raise_for_status()
```

```python
from flashq.exceptions import TaskRetryError

@app.task(max_retries=10)
def smart_retry():
    try:
        do_something()
    except TemporaryError:
        raise TaskRetryError(countdown=5.0)  # Custom retry delay
```

## Backends

### SQLite (Default — Zero Config)

```python
app = FlashQ()  # Creates flashq.db in current dir
app = FlashQ(backend=SQLiteBackend(path="/var/lib/flashq/tasks.db"))
app = FlashQ(backend=SQLiteBackend(path=":memory:"))  # For testing
```

### PostgreSQL

Uses `LISTEN/NOTIFY` for instant task delivery + `FOR UPDATE SKIP LOCKED` for atomic dequeue.

```python
from flashq.backends.postgres import PostgresBackend
app = FlashQ(backend=PostgresBackend("postgresql://localhost/mydb"))
```

### Redis

Uses sorted sets for scheduling and Lua scripts for atomic operations.

```python
from flashq.backends.redis import RedisBackend
app = FlashQ(backend=RedisBackend("redis://localhost:6379/0"))
```

## CLI

```bash
flashq worker myapp:app                  # Start worker
flashq worker myapp:app -q emails,sms    # Specific queues
flashq worker myapp:app -c 16            # 16 concurrent threads
flashq info myapp:app                    # Queue stats
flashq purge myapp:app -f                # Purge queue
```

## Architecture

```
Your App → FlashQ → Backend (SQLite/PG/Redis) → Worker(s)
                ↕
          Middleware Stack
          Rate Limiter
          Scheduler
          Dead Letter Queue
```

FlashQ uses a clean, modular architecture:
- **Backend**: Pluggable storage (SQLite, PostgreSQL, Redis)
- **Worker**: Thread pool executor with graceful shutdown
- **Middleware**: Intercepts every stage of task lifecycle
- **Scheduler**: Interval and cron-based periodic dispatch
- **Canvas**: Task composition (chain, group, chord)

## Roadmap

- [x] Core engine with SQLite backend
- [x] PostgreSQL backend (LISTEN/NOTIFY)
- [x] Redis backend (Lua scripts)
- [x] Task timeouts (non-blocking)
- [x] Middleware system
- [x] Rate limiting (token bucket)
- [x] Dead letter queue
- [x] Task chains, groups, chords
- [x] Periodic/cron scheduler
- [x] CLI (worker, info, purge)
- [x] 226 tests, 95% core coverage
- [ ] Web dashboard
- [ ] Task result streaming
- [ ] PyPI publish

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup and guidelines.

## License

MIT License. See [LICENSE](LICENSE) for details.
