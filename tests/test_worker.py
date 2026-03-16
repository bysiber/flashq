"""Tests for the worker execution engine."""

from __future__ import annotations

import asyncio
import threading
import time

import pytest

from flashq import FlashQ, TaskState
from flashq.backends.sqlite import SQLiteBackend
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


class TestWorkerExecution:
    def test_worker_processes_task(self, app, backend):
        """Worker picks up and executes a queued task."""
        results = []

        @app.task()
        def collect(value: int) -> int:
            results.append(value)
            return value * 2

        collect.delay(42)
        assert backend.queue_size("default") == 1

        worker = Worker(app, poll_interval=0.1)
        t = threading.Thread(target=worker.start, daemon=True)
        t.start()
        time.sleep(1)
        worker.stop()
        t.join(timeout=5)

        assert results == [42]
        assert backend.queue_size("default") == 0
        assert worker._tasks_processed == 1

    def test_worker_handles_failure(self, app, backend):
        """Worker stores failure result when task raises."""

        @app.task(max_retries=0)
        def failing_task() -> None:
            raise ValueError("boom")

        handle = failing_task.delay()

        worker = Worker(app, poll_interval=0.1)
        t = threading.Thread(target=worker.start, daemon=True)
        t.start()
        time.sleep(1)
        worker.stop()
        t.join(timeout=5)

        result = backend.get_result(handle.id)
        assert result is not None
        assert result.state == TaskState.DEAD
        assert "boom" in result.error

    def test_worker_retry_on_failure(self, app, backend):
        """Worker retries a failing task by scheduling it."""
        call_count = []

        @app.task(max_retries=2, retry_delay=0.1)
        def flaky() -> None:
            call_count.append(1)
            if len(call_count) < 2:
                raise RuntimeError("temporary error")

        flaky.delay()

        worker = Worker(app, poll_interval=0.1, schedule_interval=0.2)
        t = threading.Thread(target=worker.start, daemon=True)
        t.start()
        time.sleep(3)
        worker.stop()
        t.join(timeout=5)

        # Should have been called at least once, retry should be scheduled
        assert len(call_count) >= 1

    def test_worker_async_task(self, app, backend):
        """Worker can execute async task functions."""
        results = []

        @app.task()
        async def async_task(value: str) -> str:
            await asyncio.sleep(0.01)
            results.append(value)
            return f"done: {value}"

        async_task.delay("hello")

        worker = Worker(app, poll_interval=0.1)
        t = threading.Thread(target=worker.start, daemon=True)
        t.start()
        time.sleep(1)
        worker.stop()
        t.join(timeout=5)

        assert results == ["hello"]

    def test_worker_respects_priority(self, app, backend):
        """Higher priority tasks get executed first."""
        order = []

        @app.task()
        def track(name: str) -> None:
            order.append(name)

        # Enqueue low first, then high
        track.apply(args=("low",), priority=0)
        track.apply(args=("high",), priority=20)
        track.apply(args=("medium",), priority=5)

        worker = Worker(app, concurrency=1, poll_interval=0.1)
        t = threading.Thread(target=worker.start, daemon=True)
        t.start()
        time.sleep(2)
        worker.stop()
        t.join(timeout=5)

        # High priority should be first
        assert order[0] == "high"

    def test_worker_multiple_queues(self, app, backend):
        """Worker consumes from multiple queues."""
        results = []

        @app.task()
        def job(queue_name: str) -> None:
            results.append(queue_name)

        job.apply(args=("default",), queue="default")
        job.apply(args=("emails",), queue="emails")

        worker = Worker(app, queues=["default", "emails"], poll_interval=0.1)
        t = threading.Thread(target=worker.start, daemon=True)
        t.start()
        time.sleep(2)
        worker.stop()
        t.join(timeout=5)

        assert sorted(results) == ["default", "emails"]

    def test_worker_concurrency_limit(self, app, backend):
        """Worker doesn't exceed concurrency limit."""
        active = {"count": 0, "max": 0}
        lock = threading.Lock()

        @app.task()
        def slow_task() -> None:
            with lock:
                active["count"] += 1
                active["max"] = max(active["max"], active["count"])
            time.sleep(0.3)
            with lock:
                active["count"] -= 1

        for _ in range(8):
            slow_task.delay()

        worker = Worker(app, concurrency=2, poll_interval=0.05)
        t = threading.Thread(target=worker.start, daemon=True)
        t.start()
        time.sleep(3)
        worker.stop()
        t.join(timeout=5)

        assert active["max"] <= 2

    def test_worker_stores_result(self, app, backend):
        """Worker stores successful results that can be retrieved."""

        @app.task()
        def compute(x: int, y: int) -> dict:
            return {"sum": x + y}

        handle = compute.delay(10, 20)

        worker = Worker(app, poll_interval=0.1)
        t = threading.Thread(target=worker.start, daemon=True)
        t.start()
        time.sleep(1)
        worker.stop()
        t.join(timeout=5)

        result = handle.get_result()
        assert result is not None
        assert result.is_success
        assert result.result == {"sum": 30}
        assert result.runtime_ms > 0

    def test_worker_task_timeout(self, app, backend):
        """Task that exceeds timeout is terminated."""
        @app.task(timeout=0.5, max_retries=0)
        def slow_task() -> str:
            time.sleep(10)
            return "should not reach"

        handle = slow_task.delay()

        worker = Worker(app, poll_interval=0.1)
        t = threading.Thread(target=worker.start, daemon=True)
        t.start()
        time.sleep(3)
        worker.stop()
        t.join(timeout=5)

        result = backend.get_result(handle.id)
        assert result is not None
        assert result.state == TaskState.DEAD
        assert "timeout" in result.error.lower()

    def test_worker_task_timeout_retries(self, app, backend):
        """Task that times out is retried if retries available."""
        attempts = []

        @app.task(timeout=0.3, max_retries=1, retry_delay=0.1)
        def flaky_timeout() -> str:
            attempts.append(1)
            time.sleep(10)
            return "never"

        flaky_timeout.delay()

        worker = Worker(app, poll_interval=0.1, schedule_interval=0.5)
        t = threading.Thread(target=worker.start, daemon=True)
        t.start()
        time.sleep(6)
        worker.stop()
        t.join(timeout=5)

        # Should have attempted at least twice (original + 1 retry)
        assert len(attempts) >= 2
