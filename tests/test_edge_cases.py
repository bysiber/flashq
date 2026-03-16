"""Production hardening tests — edge cases, stress, and robustness."""

from __future__ import annotations

import threading
import time

import pytest

from flashq import FlashQ, TaskState
from flashq.backends.sqlite import SQLiteBackend
from flashq.models import TaskMessage
from flashq.worker import Worker


@pytest.fixture
def backend(tmp_path):
    b = SQLiteBackend(path=str(tmp_path / "edge.db"))
    b.setup()
    yield b
    b.teardown()


@pytest.fixture
def app(backend):
    return FlashQ(backend=backend, name="edge-test")


class TestEdgeCases:
    """Edge cases that could break in production."""

    def test_empty_args_kwargs(self, app, backend):
        """Task with no arguments."""

        @app.task(name="no_args")
        def no_args() -> str:
            return "ok"

        h = no_args.delay()
        worker = Worker(app, poll_interval=0.1)
        t = threading.Thread(target=worker.start, daemon=True)
        t.start()
        time.sleep(1.5)
        worker.stop()
        t.join(timeout=5)

        result = backend.get_result(h.id)
        assert result is not None
        assert result.result == "ok"

    def test_large_payload(self, app, backend):
        """Task with large arguments."""

        @app.task(name="big_data")
        def big_data(data: list) -> int:
            return len(data)

        large = list(range(10000))
        h = big_data.delay(large)
        worker = Worker(app, poll_interval=0.1)
        t = threading.Thread(target=worker.start, daemon=True)
        t.start()
        time.sleep(2)
        worker.stop()
        t.join(timeout=5)

        result = backend.get_result(h.id)
        assert result is not None
        assert result.result == 10000

    def test_unicode_args(self, app, backend):
        """Task with unicode in arguments."""

        @app.task(name="unicode")
        def unicode_task(text: str) -> str:
            return text.upper()

        h = unicode_task.delay("こんにちは世界 🌍 émojis")
        worker = Worker(app, poll_interval=0.1)
        t = threading.Thread(target=worker.start, daemon=True)
        t.start()
        time.sleep(1.5)
        worker.stop()
        t.join(timeout=5)

        result = backend.get_result(h.id)
        assert result is not None
        assert "🌍" in result.result

    def test_none_return(self, app, backend):
        """Task that returns None explicitly."""

        @app.task(name="returns_none")
        def returns_none() -> None:
            pass

        h = returns_none.delay()
        worker = Worker(app, poll_interval=0.1)
        t = threading.Thread(target=worker.start, daemon=True)
        t.start()
        time.sleep(1.5)
        worker.stop()
        t.join(timeout=5)

        result = backend.get_result(h.id)
        assert result is not None
        assert result.state == TaskState.SUCCESS
        assert result.result is None

    def test_nested_dict_kwargs(self, app, backend):
        """Task with deeply nested kwargs."""

        @app.task(name="nested")
        def nested(config: dict) -> str:
            return config["db"]["host"]

        cfg = {"db": {"host": "localhost", "port": 5432, "options": {"ssl": True}}}
        h = nested.delay(config=cfg)
        worker = Worker(app, poll_interval=0.1)
        t = threading.Thread(target=worker.start, daemon=True)
        t.start()
        time.sleep(1.5)
        worker.stop()
        t.join(timeout=5)

        result = backend.get_result(h.id)
        assert result is not None
        assert result.result == "localhost"

    def test_rapid_enqueue_dequeue(self, app, backend):
        """Rapidly enqueue and process tasks."""

        @app.task(name="quick")
        def quick(x: int) -> int:
            return x * 2

        # Enqueue 100 tasks
        handles = [quick.delay(i) for i in range(100)]
        assert len(handles) == 100

        worker = Worker(app, poll_interval=0.05, concurrency=8)
        t = threading.Thread(target=worker.start, daemon=True)
        t.start()
        time.sleep(5)
        worker.stop()
        t.join(timeout=10)

        # All should be processed
        success = 0
        for h in handles:
            r = backend.get_result(h.id)
            if r and r.state == TaskState.SUCCESS:
                success += 1
        assert success == 100

    def test_worker_restart(self, app, backend):
        """Worker can be stopped and restarted."""

        @app.task(name="resilient")
        def resilient(x: int) -> int:
            return x

        # First batch
        h1 = resilient.delay(1)

        worker1 = Worker(app, poll_interval=0.1)
        t1 = threading.Thread(target=worker1.start, daemon=True)
        t1.start()
        time.sleep(1.5)
        worker1.stop()
        t1.join(timeout=5)

        r1 = backend.get_result(h1.id)
        assert r1 is not None and r1.state == TaskState.SUCCESS

        # Second batch — new worker
        h2 = resilient.delay(2)

        worker2 = Worker(app, poll_interval=0.1)
        t2 = threading.Thread(target=worker2.start, daemon=True)
        t2.start()
        time.sleep(1.5)
        worker2.stop()
        t2.join(timeout=5)

        r2 = backend.get_result(h2.id)
        assert r2 is not None and r2.state == TaskState.SUCCESS

    def test_multiple_queues(self, app, backend):
        """Tasks in different queues are independent."""

        @app.task(name="q1_task", queue="q1")
        def q1_task() -> str:
            return "q1"

        @app.task(name="q2_task", queue="q2")
        def q2_task() -> str:
            return "q2"

        h1 = q1_task.delay()
        h2 = q2_task.delay()

        # Worker only processes q1
        worker = Worker(app, queues=["q1"], poll_interval=0.1)
        t = threading.Thread(target=worker.start, daemon=True)
        t.start()
        time.sleep(1.5)
        worker.stop()
        t.join(timeout=5)

        r1 = backend.get_result(h1.id)
        r2 = backend.get_result(h2.id)

        assert r1 is not None and r1.state == TaskState.SUCCESS
        assert r2 is None  # q2 task not processed

    def test_duplicate_task_names_raises(self, app):
        """Re-registering same task name raises DuplicateTaskError."""
        from flashq.exceptions import DuplicateTaskError

        @app.task(name="dup")
        def dup_v1() -> int:
            return 1

        with pytest.raises(DuplicateTaskError):
            @app.task(name="dup")
            def dup_v2() -> int:
                return 2

    def test_concurrent_enqueue(self, app, backend):
        """Multiple threads enqueue simultaneously."""

        @app.task(name="concurrent")
        def concurrent(x: int) -> int:
            return x

        handles = []
        lock = threading.Lock()

        def enqueue_batch(start: int) -> None:
            for i in range(50):
                h = concurrent.delay(start + i)
                with lock:
                    handles.append(h)

        threads = [threading.Thread(target=enqueue_batch, args=(i * 50,)) for i in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(handles) == 200
        assert backend.queue_size("default") == 200


class TestRetryBehavior:
    """Ensure retries behave correctly under edge conditions."""

    def test_retry_preserves_args(self, app, backend):
        """Failed task retains original arguments on retry."""
        attempt = []

        @app.task(name="retry_args", max_retries=2, retry_delay=0.1)
        def retry_args(x: int, y: int) -> int:
            attempt.append(1)
            if len(attempt) < 2:
                raise ValueError("first fail")
            return x + y

        h = retry_args.delay(10, 20)
        worker = Worker(app, poll_interval=0.1, schedule_interval=0.2)
        t = threading.Thread(target=worker.start, daemon=True)
        t.start()
        time.sleep(3)
        worker.stop()
        t.join(timeout=5)

        result = backend.get_result(h.id)
        assert result is not None
        assert result.result == 30

    def test_max_retries_exhausted(self, app, backend):
        """Task fails permanently after max retries."""

        @app.task(name="always_fail", max_retries=2, retry_delay=0.1)
        def always_fail() -> None:
            raise RuntimeError("always fails")

        h = always_fail.delay()
        worker = Worker(app, poll_interval=0.1, schedule_interval=0.2)
        t = threading.Thread(target=worker.start, daemon=True)
        t.start()
        time.sleep(5)
        worker.stop()
        t.join(timeout=5)

        result = backend.get_result(h.id)
        assert result is not None
        assert result.state == TaskState.DEAD


class TestBackendEdgeCases:
    """Backend-specific edge cases."""

    def test_get_nonexistent_task(self, backend):
        """Getting a task that doesn't exist returns None."""
        assert backend.get_task("nonexistent") is None

    def test_get_nonexistent_result(self, backend):
        """Getting a result that doesn't exist returns None."""
        assert backend.get_result("nonexistent") is None

    def test_dequeue_empty_queue(self, backend):
        """Dequeue from empty queue returns None."""
        assert backend.dequeue("default") is None

    def test_queue_size_empty(self, backend):
        """Empty queue has size 0."""
        assert backend.queue_size("default") == 0

    def test_flush_empty_queue(self, backend):
        """Flushing empty queue returns 0."""
        assert backend.flush_queue("default") == 0

    def test_enqueue_dequeue_order(self, backend):
        """Tasks are dequeued in FIFO order (same priority)."""
        for i in range(5):
            msg = TaskMessage(task_name=f"task_{i}", args=(i,))
            backend.enqueue(msg)

        for i in range(5):
            msg = backend.dequeue("default")
            assert msg is not None
            assert msg.task_name == f"task_{i}"

    def test_priority_dequeue_order(self, backend):
        """Higher priority tasks dequeue first."""
        backend.enqueue(TaskMessage(task_name="low", priority=0))
        backend.enqueue(TaskMessage(task_name="high", priority=2))
        backend.enqueue(TaskMessage(task_name="medium", priority=1))

        msg1 = backend.dequeue("default")
        msg2 = backend.dequeue("default")
        msg3 = backend.dequeue("default")

        assert msg1.task_name == "high"
        assert msg2.task_name == "medium"
        assert msg3.task_name == "low"

    def test_stats_reflects_state(self, backend):
        """get_stats aggregates correctly."""
        backend.enqueue(TaskMessage(task_name="a"))
        backend.enqueue(TaskMessage(task_name="b"))

        stats = backend.get_stats()
        assert stats["queues"]["default"] == 2
        assert stats["states"]["pending"] == 2
