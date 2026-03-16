"""Comprehensive tests for the worker execution engine."""

from __future__ import annotations

import asyncio
import threading
import time

import pytest

from flashq import FlashQ, TaskState
from flashq.backends.sqlite import SQLiteBackend
from flashq.models import TaskMessage
from flashq.worker import Worker


@pytest.fixture
def backend(tmp_path):
    b = SQLiteBackend(path=str(tmp_path / "worker_test.db"))
    b.setup()
    yield b
    b.teardown()


@pytest.fixture
def app(backend):
    return FlashQ(backend=backend, name="worker-test")


def _run_worker(app, poll_interval=0.1, schedule_interval=0.3, duration=1.5, **kwargs):
    """Helper: run a worker in a background thread for a given duration."""
    worker = Worker(
        app, poll_interval=poll_interval, schedule_interval=schedule_interval, **kwargs
    )
    t = threading.Thread(target=worker.start, daemon=True)
    t.start()
    time.sleep(duration)
    worker.stop()
    t.join(timeout=5)
    return worker


class TestWorkerExecution:
    def test_worker_processes_task(self, app, backend):
        result_holder = []

        @app.task(name="sync_add")
        def sync_add(a: int, b: int) -> int:
            result_holder.append(a + b)
            return a + b

        sync_add.delay(3, 4)
        _run_worker(app)

        assert result_holder == [7]

    def test_worker_handles_failure(self, app, backend):
        @app.task(name="fail_task", max_retries=0)
        def fail_task() -> None:
            raise ValueError("expected failure")

        handle = fail_task.delay()
        _run_worker(app)

        result = backend.get_result(handle.id)
        assert result is not None
        assert result.state == TaskState.DEAD
        assert "expected failure" in result.error

    def test_worker_retry_on_failure(self, app, backend):
        attempts = []

        @app.task(name="retry_task", max_retries=2, retry_delay=0.1)
        def retry_task() -> None:
            attempts.append(1)
            if len(attempts) < 3:
                raise ValueError("not yet")
            return "success"

        retry_task.delay()
        _run_worker(app, duration=6, schedule_interval=0.3)

        assert len(attempts) >= 2

    def test_worker_async_task(self, app, backend):
        result_holder = []

        @app.task(name="async_task")
        async def async_task(x: int) -> int:
            await asyncio.sleep(0.05)
            result_holder.append(x * 2)
            return x * 2

        async_task.delay(5)
        _run_worker(app)

        assert result_holder == [10]

    def test_worker_respects_priority(self, app, backend):
        order = []

        @app.task(name="track_task")
        def track_task(label: str) -> None:
            order.append(label)

        from flashq import TaskPriority

        track_task.apply(args=("low",), priority=TaskPriority.LOW)
        track_task.apply(args=("high",), priority=TaskPriority.HIGH)
        track_task.apply(args=("critical",), priority=TaskPriority.CRITICAL)

        _run_worker(app, duration=2)

        # Critical should be first
        assert order[0] == "critical"
        assert len(order) == 3

    def test_worker_multiple_queues(self, app, backend):
        results = {"q1": [], "q2": []}

        @app.task(name="q1_task", queue="q1")
        def q1_task() -> None:
            results["q1"].append(1)

        @app.task(name="q2_task", queue="q2")
        def q2_task() -> None:
            results["q2"].append(1)

        q1_task.delay()
        q2_task.delay()

        worker = Worker(app, poll_interval=0.1, queues=["q1", "q2"])
        t = threading.Thread(target=worker.start, daemon=True)
        t.start()
        time.sleep(1.5)
        worker.stop()
        t.join(timeout=5)

        assert len(results["q1"]) == 1
        assert len(results["q2"]) == 1

    def test_worker_concurrency_limit(self, app, backend):
        max_concurrent = []
        lock = threading.Lock()
        active = [0]

        @app.task(name="slow_task")
        def slow_task() -> None:
            with lock:
                active[0] += 1
                max_concurrent.append(active[0])
            time.sleep(0.3)
            with lock:
                active[0] -= 1

        for _ in range(8):
            slow_task.delay()

        _run_worker(app, duration=4, concurrency=2)

        assert max(max_concurrent) <= 2

    def test_worker_stores_result(self, app, backend):
        @app.task(name="result_task")
        def result_task() -> dict:
            return {"status": "done", "count": 42}

        handle = result_task.delay()
        _run_worker(app)

        result = backend.get_result(handle.id)
        assert result is not None
        assert result.state == TaskState.SUCCESS
        assert result.result == {"status": "done", "count": 42}
        assert result.runtime_ms >= 0  # Windows timer resolution can yield 0.0

    def test_worker_task_timeout(self, app, backend):
        @app.task(name="timeout_task", timeout=0.5, max_retries=0)
        def slow_task() -> str:
            time.sleep(10)
            return "should not reach"

        handle = slow_task.delay()
        _run_worker(app, duration=3)

        result = backend.get_result(handle.id)
        assert result is not None
        assert result.state == TaskState.DEAD
        assert "timeout" in result.error.lower()

    def test_worker_task_timeout_retries(self, app, backend):
        attempts = []

        @app.task(name="timeout_retry", timeout=0.3, max_retries=1, retry_delay=0.1)
        def flaky_timeout() -> str:
            attempts.append(1)
            time.sleep(10)
            return "never"

        flaky_timeout.delay()
        _run_worker(app, duration=6, schedule_interval=0.5)

        assert len(attempts) >= 2

    def test_worker_graceful_shutdown(self, app, backend):
        @app.task(name="shutdown_task")
        def task() -> None:
            time.sleep(0.5)

        task.delay()

        worker = Worker(app, poll_interval=0.1)
        t = threading.Thread(target=worker.start, daemon=True)
        t.start()
        time.sleep(0.2)  # Let it start
        worker.stop()  # Signal stop
        t.join(timeout=10)

        assert not t.is_alive()

    def test_worker_unknown_task(self, app, backend):
        """Worker handles a task that's not in the registry."""
        msg = TaskMessage(task_name="ghost_task")
        backend.enqueue(msg)

        _run_worker(app)

        result = backend.get_result(msg.id)
        assert result is not None
        assert result.state in (TaskState.FAILURE, TaskState.DEAD)

    def test_worker_empty_queue(self, app, backend):
        """Worker starts and stops without processing anything."""
        _run_worker(app, duration=0.5)
        # Should not crash


class TestWorkerSchedule:
    def test_deferred_task(self, app, backend):
        @app.task(name="deferred")
        def deferred_task() -> str:
            return "deferred_ok"

        deferred_task.apply(countdown=0.1)

        assert backend.queue_size("default") == 0
        assert backend.schedule_size() == 1

        _run_worker(app, duration=3, schedule_interval=0.3)

        result_found = False
        # Check results table
        import sqlite3

        conn = sqlite3.connect(str(backend.path))
        conn.row_factory = sqlite3.Row
        rows = conn.execute("SELECT * FROM results").fetchall()
        conn.close()
        for _row in rows:
            result_found = True

        assert result_found or backend.schedule_size() == 0
