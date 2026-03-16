# ⚡ FlashQ

**The task queue that works out of the box — no Redis, no RabbitMQ, just `pip install flashq` and go.**

[![PyPI](https://img.shields.io/pypi/v/flashq)](https://pypi.org/project/flashq/)
[![Python](https://img.shields.io/pypi/pyversions/flashq)](https://pypi.org/project/flashq/)
[![License](https://img.shields.io/github/license/ozden/flashq)](LICENSE)

---

## Why FlashQ?

Every Python developer has been there: you need background tasks, you look at Celery, and suddenly you need Redis or RabbitMQ running, a separate broker config, and 200 lines of boilerplate before your first task runs.

**FlashQ changes that.** SQLite is the default backend — zero external dependencies, zero config. Your tasks persist across restarts, and you can scale to PostgreSQL or Redis when you actually need to.

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
| Async + Sync | ✅ | ❌ | ❌ | ❌ | Async only |
| Type-safe tasks | ✅ | ❌ | ⚠️ | ❌ | ✅ |
| Built-in dashboard | 🚧 | ❌* | ❌ | ❌ | ❌ |
| Built-in scheduler | ✅ | ⚠️ | ❌ | ✅ | ⚠️ |
| Zero dependencies | ✅ | ❌ | ❌ | ❌ | ❌ |

*Celery has Flower, but it's a separate project.*

## Installation

```bash
# Zero-config (SQLite only — no dependencies!)
pip install flashq

# With Redis support
pip install flashq[redis]

# With PostgreSQL support
pip install flashq[postgres]

# Everything
pip install flashq[all]
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

@app.task(queue="emails", max_retries=5)
def send_email(to: str, subject: str, body: str) -> dict:
    # Your email sending logic here
    return {"status": "sent", "to": to}

@app.task(priority=20)  # Higher = processed first
def process_payment(order_id: str, amount: float) -> bool:
    # Critical tasks get priority
    return True
```

### 2. Enqueue tasks

```python
# Anywhere in your application
from tasks import add, send_email

# Simple dispatch
handle = add.delay(2, 3)

# With options
handle = send_email.apply(
    kwargs={"to": "user@example.com", "subject": "Hi", "body": "Hello!"},
    countdown=60,  # delay by 60 seconds
)

# Check result later
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
 │    └─ tasks.process_payment
 └─ Ready! Waiting for tasks...
```

## Backends

### SQLite (Default)

Zero config. Great for development, single-server deployments, and moderate workloads.

```python
from flashq import FlashQ
from flashq.backends.sqlite import SQLiteBackend

# Default — creates flashq.db in current directory
app = FlashQ()

# Custom path
app = FlashQ(backend=SQLiteBackend(path="/var/lib/flashq/tasks.db"))

# In-memory (for testing)
app = FlashQ(backend=SQLiteBackend(path=":memory:"))
```

### PostgreSQL (Coming Soon)

For multi-server deployments. Uses `LISTEN/NOTIFY` for real-time task distribution.

```python
from flashq.backends.postgres import PostgresBackend

app = FlashQ(backend=PostgresBackend("postgresql://localhost/mydb"))
```

### Redis (Coming Soon)

For high-throughput scenarios where you need maximum speed.

```python
from flashq.backends.redis import RedisBackend

app = FlashQ(backend=RedisBackend("redis://localhost:6379/0"))
```

## CLI

```bash
# Start a worker
flashq worker myapp:app

# Multiple queues
flashq worker myapp:app -q default,emails,payments

# Concurrency
flashq worker myapp:app -c 8

# Queue info
flashq info myapp:app

# Purge queue
flashq purge myapp:app --queue default
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
        # Explicitly request retry with custom delay
        raise TaskRetryError(countdown=5.0)
```

## Roadmap

- [x] **v0.1** — Core engine, SQLite backend, CLI, worker
- [ ] **v0.2** — PostgreSQL backend with LISTEN/NOTIFY
- [ ] **v0.3** — Redis backend
- [ ] **v0.4** — Built-in web dashboard
- [ ] **v0.5** — Cron/periodic tasks
- [ ] **v0.6** — Task chains and groups
- [ ] **v1.0** — Production-ready release

## License

MIT License. See [LICENSE](LICENSE) for details.
