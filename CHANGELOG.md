# Changelog

All notable changes to FlashQ will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.1] - 2026-03-16

### Added
- **Web Dashboard**: Starlette-based real-time monitoring UI with dark theme
  - Task list with state/queue/search filtering
  - Task detail modal with result, error, traceback
  - Cancel/revoke actions
  - Auto-refresh (5s)
  - CLI: `flashq dashboard myapp:app --port 5555`
- **Canvas Primitives**: Celery-inspired task composition
  - `Signature`: task reference with args/kwargs/options
  - `Chain`: sequential execution, result piped to next
  - `Group`: parallel execution
  - `Chord`: group + callback when all complete
  - `.s()` and `.si()` (immutable) shorthand on Task
- **Rate Limiting**: Token bucket algorithm
  - Per-task and global rate limits
  - `RateLimiter` middleware
  - Rate format: `"100/m"`, `"60/h"`, `"10/s"`
- **Dead Letter Queue**: Permanent failure management
  - `DeadLetterQueue` with add, list, replay, replay_all, purge
  - `_DLQMiddleware` for automatic capture
- **Benchmark Suite**: Throughput and latency measurements
  - Enqueue throughput: ~19,000 tasks/s
  - Concurrency scaling tests
  - CPU-bound and I/O-bound workloads
- 20 production hardening edge case tests
- Examples: basic, advanced (canvas + middleware + DLQ), FastAPI integration

### Fixed
- Group.delay() no longer injects `__group_id__` into task kwargs
- Chain.delay() no longer injects `__chain_id__` into task kwargs

## [0.1.0] - 2026-03-16

### Added
- **Core Engine**: `FlashQ` app class with `@app.task()` decorator
- **Task System**: Type-safe `.delay()` and `.apply()` with PEP-612 `ParamSpec`
- **SQLite Backend**: WAL journal mode, `BEGIN IMMEDIATE` for atomic dequeue, zero dependencies
- **PostgreSQL Backend**: `LISTEN/NOTIFY` for real-time delivery, `FOR UPDATE SKIP LOCKED`
- **Redis Backend**: Lists for queues, sorted sets for scheduling, Lua scripts for atomicity
- **Worker**: Thread pool executor, async+sync task execution, graceful shutdown (SIGINT/SIGTERM)
- **Task Timeouts**: Non-blocking `ThreadPoolExecutor` with `shutdown(wait=False)`
- **Middleware System**: `before_execute`, `after_execute`, `on_error`, `on_retry`, `on_dead`
  - Built-in: `LoggingMiddleware`, `TimeoutMiddleware`
- **Periodic Scheduler**: `every(hours=6)` and `cron("0 9 * * 1-5")` syntax
- **Retry System**: Exponential backoff, configurable max retries and delay
- **CLI**: `flashq worker`, `flashq info`, `flashq purge`
- **Serializers**: JSON (default) and Pickle
- **Type Safety**: `py.typed` marker, full type annotations
- CI/CD: GitHub Actions (Python 3.10–3.13, ubuntu/macos/windows) + PyPI publish on tag
- 168 core tests at ~95% coverage
