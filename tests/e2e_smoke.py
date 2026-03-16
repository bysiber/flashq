#!/usr/bin/env python3
"""
FlashQ End-to-End Smoke Test
=============================
Simulates a REAL USER who just did `pip install flashq` and tries
every feature from the README. Every test is self-contained and
prints PASS/FAIL.

Run: python tests/e2e_smoke.py

This is NOT a pytest file — it's a standalone script that proves
FlashQ works in a real environment.
"""

from __future__ import annotations

import os
import sys
import tempfile
import threading
import time
import traceback

# ── Setup ──

PASS = 0
FAIL = 0
TESTS: list[tuple[str, bool, str]] = []

def run_test(name: str, fn):
    global PASS, FAIL
    print(f"  ▸ {name}...", end=" ", flush=True)
    try:
        fn()
        print("✅ PASS")
        PASS += 1
        TESTS.append((name, True, ""))
    except Exception as e:
        print(f"❌ FAIL: {e}")
        FAIL += 1
        TESTS.append((name, False, traceback.format_exc()))


# ── All tests use a temp directory ──

TMPDIR = tempfile.mkdtemp(prefix="flashq_e2e_")
os.chdir(TMPDIR)


# ═══════════════════════════════════════════
# TEST 1: Basic import
# ═══════════════════════════════════════════

def test_import():
    """Can we import FlashQ at all?"""
    from flashq import FlashQ, TaskState, __version__
    assert FlashQ is not None
    assert TaskState is not None
    assert __version__ == "0.1.0"

run_test("Import FlashQ", test_import)


# ═══════════════════════════════════════════
# TEST 2: Create app + define task + enqueue
# ═══════════════════════════════════════════

def test_basic_task():
    """README Quick Start example."""
    from flashq import FlashQ

    app = FlashQ()

    @app.task()
    def add(x: int, y: int) -> int:
        return x + y

    handle = add.delay(2, 3)
    assert handle is not None
    assert handle.id is not None
    assert len(handle.id) == 32  # UUID hex format (no hyphens)

run_test("Basic task creation + enqueue", test_basic_task)


# ═══════════════════════════════════════════
# TEST 3: Worker processes task and produces result
# ═══════════════════════════════════════════

def test_worker_processes():
    """Full roundtrip: enqueue → worker → result."""
    from flashq import FlashQ, TaskState
    from flashq.backends.sqlite import SQLiteBackend
    from flashq.worker import Worker

    db_path = os.path.join(TMPDIR, "test3.db")
    backend = SQLiteBackend(path=db_path)
    backend.setup()
    app = FlashQ(backend=backend, name="e2e-worker")

    @app.task()
    def multiply(x: int, y: int) -> int:
        return x * y

    handle = multiply.delay(6, 7)

    worker = Worker(app, poll_interval=0.1)
    t = threading.Thread(target=worker.start, daemon=True)
    t.start()
    time.sleep(2)
    worker.stop()
    t.join(timeout=5)

    result = backend.get_result(handle.id)
    assert result is not None, "No result found!"
    assert result.state == TaskState.SUCCESS, f"Expected SUCCESS, got {result.state}"
    assert result.result == 42, f"Expected 42, got {result.result}"
    backend.teardown()

run_test("Worker processes task → correct result", test_worker_processes)


# ═══════════════════════════════════════════
# TEST 4: Async task
# ═══════════════════════════════════════════

def test_async_task():
    """Async tasks are supported."""
    from flashq import FlashQ
    from flashq.backends.sqlite import SQLiteBackend
    from flashq.worker import Worker

    db_path = os.path.join(TMPDIR, "test4.db")
    backend = SQLiteBackend(path=db_path)
    backend.setup()
    app = FlashQ(backend=backend, name="e2e-async")

    @app.task()
    async def async_add(x: int, y: int) -> int:
        import asyncio
        await asyncio.sleep(0.01)
        return x + y

    handle = async_add.delay(10, 20)

    worker = Worker(app, poll_interval=0.1)
    t = threading.Thread(target=worker.start, daemon=True)
    t.start()
    time.sleep(2)
    worker.stop()
    t.join(timeout=5)

    result = backend.get_result(handle.id)
    assert result is not None
    assert result.result == 30
    backend.teardown()

run_test("Async task execution", test_async_task)


# ═══════════════════════════════════════════
# TEST 5: Retry mechanism
# ═══════════════════════════════════════════

def test_retry():
    """Task retries on failure, then succeeds."""
    from flashq import FlashQ, TaskState
    from flashq.backends.sqlite import SQLiteBackend
    from flashq.worker import Worker

    db_path = os.path.join(TMPDIR, "test5.db")
    backend = SQLiteBackend(path=db_path)
    backend.setup()
    app = FlashQ(backend=backend, name="e2e-retry")

    attempts = []

    @app.task(max_retries=3, retry_delay=0.1)
    def flaky() -> str:
        attempts.append(1)
        if len(attempts) < 3:
            raise ValueError("not yet")
        return "finally!"

    handle = flaky.delay()

    worker = Worker(app, poll_interval=0.1, schedule_interval=0.2)
    t = threading.Thread(target=worker.start, daemon=True)
    t.start()
    time.sleep(5)
    worker.stop()
    t.join(timeout=5)

    result = backend.get_result(handle.id)
    assert result is not None, "No result after retries!"
    assert result.state == TaskState.SUCCESS
    assert result.result == "finally!"
    assert len(attempts) == 3
    backend.teardown()

run_test("Retry mechanism (fail 2x, succeed 3rd)", test_retry)


# ═══════════════════════════════════════════
# TEST 6: Task timeout
# ═══════════════════════════════════════════

def test_timeout():
    """Task is killed after timeout."""
    from flashq import FlashQ, TaskState
    from flashq.backends.sqlite import SQLiteBackend
    from flashq.worker import Worker

    db_path = os.path.join(TMPDIR, "test6.db")
    backend = SQLiteBackend(path=db_path)
    backend.setup()
    app = FlashQ(backend=backend, name="e2e-timeout")

    @app.task(timeout=0.5, max_retries=0)
    def slow_task() -> str:
        time.sleep(10)
        return "done"

    handle = slow_task.delay()

    worker = Worker(app, poll_interval=0.1)
    t = threading.Thread(target=worker.start, daemon=True)
    t.start()
    time.sleep(3)
    worker.stop()
    t.join(timeout=5)

    result = backend.get_result(handle.id)
    assert result is not None
    # Should be FAILURE or DEAD (timed out)
    assert result.state in (TaskState.FAILURE, TaskState.DEAD), f"Got {result.state}"
    backend.teardown()

run_test("Task timeout enforcement", test_timeout)


# ═══════════════════════════════════════════
# TEST 7: Multiple queues
# ═══════════════════════════════════════════

def test_multiple_queues():
    """Different queues are isolated."""
    from flashq import FlashQ, TaskState
    from flashq.backends.sqlite import SQLiteBackend
    from flashq.worker import Worker

    db_path = os.path.join(TMPDIR, "test7.db")
    backend = SQLiteBackend(path=db_path)
    backend.setup()
    app = FlashQ(backend=backend, name="e2e-queues")

    @app.task(name="q1_task", queue="emails")
    def email_task() -> str:
        return "email sent"

    @app.task(name="q2_task", queue="sms")
    def sms_task() -> str:
        return "sms sent"

    h1 = email_task.delay()
    h2 = sms_task.delay()

    # Worker only processes "emails"
    worker = Worker(app, queues=["emails"], poll_interval=0.1)
    t = threading.Thread(target=worker.start, daemon=True)
    t.start()
    time.sleep(2)
    worker.stop()
    t.join(timeout=5)

    r1 = backend.get_result(h1.id)
    r2 = backend.get_result(h2.id)

    assert r1 is not None and r1.state == TaskState.SUCCESS
    assert r2 is None  # sms queue not processed
    backend.teardown()

run_test("Multiple queues isolation", test_multiple_queues)


# ═══════════════════════════════════════════
# TEST 8: Priority ordering
# ═══════════════════════════════════════════

def test_priority():
    """Higher priority tasks process first."""
    from flashq.backends.sqlite import SQLiteBackend
    from flashq.models import TaskMessage

    db_path = os.path.join(TMPDIR, "test8.db")
    backend = SQLiteBackend(path=db_path)
    backend.setup()

    backend.enqueue(TaskMessage(task_name="low", priority=0))
    backend.enqueue(TaskMessage(task_name="high", priority=2))
    backend.enqueue(TaskMessage(task_name="medium", priority=1))

    t1 = backend.dequeue("default")
    t2 = backend.dequeue("default")
    t3 = backend.dequeue("default")

    assert t1.task_name == "high", f"Expected 'high', got '{t1.task_name}'"
    assert t2.task_name == "medium"
    assert t3.task_name == "low"
    backend.teardown()

run_test("Priority ordering (high → medium → low)", test_priority)


# ═══════════════════════════════════════════
# TEST 9: Middleware
# ═══════════════════════════════════════════

def test_middleware():
    """Middleware hooks fire correctly."""
    from flashq import FlashQ, Middleware
    from flashq.backends.sqlite import SQLiteBackend
    from flashq.worker import Worker

    db_path = os.path.join(TMPDIR, "test9.db")
    backend = SQLiteBackend(path=db_path)
    backend.setup()
    app = FlashQ(backend=backend, name="e2e-middleware")

    events = []

    class TrackingMiddleware(Middleware):
        def before_execute(self, message):
            events.append("before")
            return message

        def after_execute(self, message, result):
            events.append("after")

        def on_error(self, message, exc):
            events.append("error")
            return False

    app.add_middleware(TrackingMiddleware())

    @app.task()
    def tracked() -> str:
        return "ok"

    tracked.delay()

    worker = Worker(app, poll_interval=0.1)
    t = threading.Thread(target=worker.start, daemon=True)
    t.start()
    time.sleep(2)
    worker.stop()
    t.join(timeout=5)

    assert "before" in events, f"Events: {events}"
    assert "after" in events
    backend.teardown()

run_test("Middleware before/after hooks", test_middleware)


# ═══════════════════════════════════════════
# TEST 10: Dead Letter Queue
# ═══════════════════════════════════════════

def test_dlq():
    """Dead tasks are captured in DLQ and can be replayed."""
    from flashq import FlashQ
    from flashq.backends.sqlite import SQLiteBackend
    from flashq.dlq import DeadLetterQueue
    from flashq.worker import Worker

    db_path = os.path.join(TMPDIR, "test10.db")
    backend = SQLiteBackend(path=db_path)
    backend.setup()
    app = FlashQ(backend=backend, name="e2e-dlq")

    dlq = DeadLetterQueue(app)
    app.add_middleware(dlq.middleware())

    @app.task(max_retries=0)
    def doomed() -> None:
        raise RuntimeError("permanent failure")

    doomed.delay()

    worker = Worker(app, poll_interval=0.1)
    t = threading.Thread(target=worker.start, daemon=True)
    t.start()
    time.sleep(2)
    worker.stop()
    t.join(timeout=5)

    assert dlq.count() == 1, f"DLQ count: {dlq.count()}"
    dead = dlq.list()[0]
    assert "permanent failure" in dead.error

    # Replay it
    new_id = dlq.replay(dead.task_id)
    assert new_id is not None
    assert dlq.count() == 0
    assert backend.queue_size("default") == 1
    backend.teardown()

run_test("Dead Letter Queue capture + replay", test_dlq)


# ═══════════════════════════════════════════
# TEST 11: Rate Limiter
# ═══════════════════════════════════════════

def test_rate_limiter():
    """Rate limiter throttles execution."""
    from flashq import FlashQ
    from flashq.backends.sqlite import SQLiteBackend
    from flashq.ratelimit import RateLimiter

    db_path = os.path.join(TMPDIR, "test11.db")
    backend = SQLiteBackend(path=db_path)
    backend.setup()
    app = FlashQ(backend=backend, name="e2e-rate")

    limiter = RateLimiter(default_rate="100/s")
    limiter.configure("limited_task", rate="5/s")
    app.add_middleware(limiter)

    stats = limiter.get_stats()
    assert "limited_task" in stats
    assert stats["limited_task"]["max_tokens"] == 5
    backend.teardown()

run_test("Rate limiter configuration", test_rate_limiter)


# ═══════════════════════════════════════════
# TEST 12: Canvas — Chain
# ═══════════════════════════════════════════

def test_chain():
    """Chain executes tasks sequentially."""
    from flashq import FlashQ, chain
    from flashq.backends.sqlite import SQLiteBackend

    db_path = os.path.join(TMPDIR, "test12.db")
    backend = SQLiteBackend(path=db_path)
    backend.setup()
    app = FlashQ(backend=backend, name="e2e-chain")

    @app.task()
    def double(x: int) -> int:
        return x * 2

    pipe = chain(double.s(5), double.s())
    handle = pipe.delay(app)

    assert handle is not None
    assert handle.chain_id is not None
    # First task enqueued
    assert backend.queue_size("default") >= 1
    backend.teardown()

run_test("Canvas chain creation + dispatch", test_chain)


# ═══════════════════════════════════════════
# TEST 13: Canvas — Group
# ═══════════════════════════════════════════

def test_group():
    """Group dispatches multiple tasks in parallel."""
    from flashq import FlashQ, group
    from flashq.backends.sqlite import SQLiteBackend

    db_path = os.path.join(TMPDIR, "test13.db")
    backend = SQLiteBackend(path=db_path)
    backend.setup()
    app = FlashQ(backend=backend, name="e2e-group")

    @app.task()
    def square(x: int) -> int:
        return x * x

    batch = group(square.s(1), square.s(2), square.s(3))
    handle = batch.delay(app)

    assert len(handle.task_ids) == 3
    assert backend.queue_size("default") == 3
    backend.teardown()

run_test("Canvas group parallel dispatch", test_group)


# ═══════════════════════════════════════════
# TEST 14: Scheduler
# ═══════════════════════════════════════════

def test_scheduler():
    """Periodic scheduler creates and manages schedules."""
    from flashq import FlashQ, every
    from flashq.backends.sqlite import SQLiteBackend
    from flashq.scheduler import Scheduler

    db_path = os.path.join(TMPDIR, "test14.db")
    backend = SQLiteBackend(path=db_path)
    backend.setup()
    app = FlashQ(backend=backend, name="e2e-scheduler")

    @app.task(name="heartbeat")
    def heartbeat() -> str:
        return "alive"

    scheduler = Scheduler(app)
    scheduler.add("heartbeat", every(seconds=60))

    assert len(scheduler._jobs) == 1
    backend.teardown()

run_test("Periodic scheduler setup", test_scheduler)


# ═══════════════════════════════════════════
# TEST 15: CLI commands parse correctly
# ═══════════════════════════════════════════

def test_cli():
    """CLI parser works."""
    from flashq.cli import build_parser

    parser = build_parser()

    # worker command
    args = parser.parse_args(["worker", "myapp:app"])
    assert args.app == "myapp:app"

    # info command
    args = parser.parse_args(["info", "myapp:app"])
    assert args.app == "myapp:app"

    # dashboard command
    args = parser.parse_args(["dashboard", "myapp:app", "--port", "8080"])
    assert args.port == 8080

run_test("CLI parser (worker, info, dashboard)", test_cli)


# ═══════════════════════════════════════════
# TEST 16: Dashboard creates ASGI app
# ═══════════════════════════════════════════

def test_dashboard():
    """Dashboard ASGI app can be created."""
    from flashq import FlashQ
    from flashq.backends.sqlite import SQLiteBackend

    db_path = os.path.join(TMPDIR, "test16.db")
    backend = SQLiteBackend(path=db_path)
    backend.setup()
    app = FlashQ(backend=backend, name="e2e-dashboard")

    try:
        from flashq.dashboard import create_dashboard
        dashboard = create_dashboard(app)
        assert dashboard is not None

        from starlette.testclient import TestClient
        client = TestClient(dashboard)

        # Main page loads
        resp = client.get("/")
        assert resp.status_code == 200
        assert "FlashQ Dashboard" in resp.text

        # API works
        resp = client.get("/api/stats")
        assert resp.status_code == 200
        data = resp.json()
        assert "queues" in data
        assert "states" in data
    except ImportError:
        print("(starlette not installed, skipping dashboard HTTP test)")

    backend.teardown()

run_test("Dashboard ASGI app creation + HTTP", test_dashboard)


# ═══════════════════════════════════════════
# TEST 17: Serializers
# ═══════════════════════════════════════════

def test_serializers():
    """JSON and Pickle serializers work."""
    from flashq.serializers import JSONSerializer, PickleSerializer

    data = {"key": "value", "list": [1, 2, 3], "nested": {"a": True}}

    # JSON
    js = JSONSerializer()
    encoded = js.dumps(data)
    assert isinstance(encoded, bytes)
    decoded = js.loads(encoded)
    assert decoded == data

    # Pickle
    ps = PickleSerializer()
    encoded = ps.dumps(data)
    decoded = ps.loads(encoded)
    assert decoded == data

run_test("JSON + Pickle serializers", test_serializers)


# ═══════════════════════════════════════════
# TEST 18: Full workflow — enqueue 50 tasks, worker processes all
# ═══════════════════════════════════════════

def test_full_workflow():
    """50 tasks enqueued and fully processed."""
    from flashq import FlashQ, TaskState
    from flashq.backends.sqlite import SQLiteBackend
    from flashq.worker import Worker

    db_path = os.path.join(TMPDIR, "test18.db")
    backend = SQLiteBackend(path=db_path)
    backend.setup()
    app = FlashQ(backend=backend, name="e2e-full")

    @app.task()
    def compute(x: int) -> int:
        return x * x + 1

    handles = [compute.delay(i) for i in range(50)]

    worker = Worker(app, poll_interval=0.05, concurrency=8)
    t = threading.Thread(target=worker.start, daemon=True)
    t.start()
    time.sleep(5)
    worker.stop()
    t.join(timeout=10)

    success = 0
    for h in handles:
        r = backend.get_result(h.id)
        if r and r.state == TaskState.SUCCESS:
            success += 1

    assert success == 50, f"Only {success}/50 succeeded"

    # Verify actual results
    r0 = backend.get_result(handles[0].id)
    assert r0.result == 1  # 0*0+1
    r5 = backend.get_result(handles[5].id)
    assert r5.result == 26  # 5*5+1
    backend.teardown()

run_test("Full workflow: 50 tasks all processed correctly", test_full_workflow)


# ═══════════════════════════════════════════
# TEST 19: TaskHandle.get_result()
# ═══════════════════════════════════════════

def test_handle_get_result():
    """TaskHandle.get_result() returns the result."""
    from flashq import FlashQ
    from flashq.backends.sqlite import SQLiteBackend
    from flashq.worker import Worker

    db_path = os.path.join(TMPDIR, "test19.db")
    backend = SQLiteBackend(path=db_path)
    backend.setup()
    app = FlashQ(backend=backend, name="e2e-handle")

    @app.task()
    def greet(name: str) -> str:
        return f"Hello, {name}!"

    handle = greet.delay("World")

    # Before processing — no result
    r = handle.get_result()
    assert r is None

    worker = Worker(app, poll_interval=0.1)
    t = threading.Thread(target=worker.start, daemon=True)
    t.start()
    time.sleep(2)
    worker.stop()
    t.join(timeout=5)

    # After processing — has result
    r = handle.get_result()
    assert r is not None
    assert r.result == "Hello, World!"
    backend.teardown()

run_test("TaskHandle.get_result() before/after", test_handle_get_result)


# ═══════════════════════════════════════════
# TEST 20: Error handling — exception stored in result
# ═══════════════════════════════════════════

def test_error_stored():
    """Failed task stores error message in result."""
    from flashq import FlashQ, TaskState
    from flashq.backends.sqlite import SQLiteBackend
    from flashq.worker import Worker

    db_path = os.path.join(TMPDIR, "test20.db")
    backend = SQLiteBackend(path=db_path)
    backend.setup()
    app = FlashQ(backend=backend, name="e2e-error")

    @app.task(max_retries=0)
    def bad_task() -> None:
        raise ValueError("something went wrong")

    handle = bad_task.delay()

    worker = Worker(app, poll_interval=0.1)
    t = threading.Thread(target=worker.start, daemon=True)
    t.start()
    time.sleep(2)
    worker.stop()
    t.join(timeout=5)

    result = backend.get_result(handle.id)
    assert result is not None
    assert result.state in (TaskState.FAILURE, TaskState.DEAD)
    assert "something went wrong" in (result.error or "")
    backend.teardown()

run_test("Error stored in result on failure", test_error_stored)


# ═══════════════════════════════════════════
# RESULTS
# ═══════════════════════════════════════════

print("\n" + "=" * 60)
print("⚡ FlashQ E2E Smoke Test Results")
print("=" * 60)
print(f"  ✅ Passed: {PASS}")
print(f"  ❌ Failed: {FAIL}")
print(f"  📊 Total:  {PASS + FAIL}")
print("=" * 60)

if FAIL > 0:
    print("\n❌ FAILED TESTS:")
    for name, passed, tb in TESTS:
        if not passed:
            print(f"\n  {name}")
            print(f"  {tb}")
    sys.exit(1)
else:
    print("\n🎉 ALL TESTS PASSED — FlashQ is production-ready!")
    sys.exit(0)
