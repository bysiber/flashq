"""Tests for Dead Letter Queue."""

from __future__ import annotations

import threading
import time

import pytest

from flashq import FlashQ, TaskState
from flashq.backends.sqlite import SQLiteBackend
from flashq.dlq import DeadLetterQueue, DeadTask, _DLQMiddleware
from flashq.models import TaskMessage
from flashq.worker import Worker


@pytest.fixture
def backend(tmp_path):
    b = SQLiteBackend(path=str(tmp_path / "dlq_test.db"))
    b.setup()
    yield b
    b.teardown()


@pytest.fixture
def app(backend):
    return FlashQ(backend=backend, name="dlq-test")


@pytest.fixture
def dlq(app):
    return DeadLetterQueue(app)


class TestDeadTask:
    def test_to_dict(self):
        import datetime

        dt = DeadTask(
            task_id="abc123",
            task_name="my.task",
            args=(1, 2),
            kwargs={"key": "val"},
            queue="default",
            error="ValueError: boom",
            traceback="Traceback...",
            died_at=datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc),
            retries=3,
            max_retries=3,
        )
        d = dt.to_dict()
        assert d["task_id"] == "abc123"
        assert d["task_name"] == "my.task"
        assert d["args"] == [1, 2]
        assert d["error"] == "ValueError: boom"


class TestDeadLetterQueue:
    def test_add_and_list(self, dlq):
        msg = TaskMessage(task_name="failed_task", args=(1,))
        dlq.add(msg, error="boom")

        dead = dlq.list()
        assert len(dead) == 1
        assert dead[0].task_name == "failed_task"
        assert dead[0].error == "boom"

    def test_get(self, dlq):
        msg = TaskMessage(task_name="test")
        dlq.add(msg, error="err")

        found = dlq.get(msg.id)
        assert found is not None
        assert found.task_id == msg.id

        assert dlq.get("nonexistent") is None

    def test_count(self, dlq):
        assert dlq.count() == 0

        dlq.add(TaskMessage(task_name="t1"), error="e1")
        dlq.add(TaskMessage(task_name="t2"), error="e2")
        assert dlq.count() == 2

    def test_replay(self, dlq, backend):
        msg = TaskMessage(task_name="replay_me", args=(42,))
        dlq.add(msg, error="failed")

        new_id = dlq.replay(msg.id)
        assert new_id is not None
        assert dlq.count() == 0
        assert backend.queue_size("default") == 1

    def test_replay_not_found(self, dlq):
        result = dlq.replay("nonexistent")
        assert result is None

    def test_replay_all(self, dlq, backend):
        for i in range(5):
            dlq.add(TaskMessage(task_name=f"task_{i}"), error=f"err_{i}")

        count = dlq.replay_all()
        assert count == 5
        assert dlq.count() == 0
        assert backend.queue_size("default") == 5

    def test_purge(self, dlq):
        for i in range(3):
            dlq.add(TaskMessage(task_name=f"task_{i}"), error="err")

        purged = dlq.purge()
        assert purged == 3
        assert dlq.count() == 0

    def test_remove(self, dlq):
        msg = TaskMessage(task_name="removable")
        dlq.add(msg, error="err")

        assert dlq.remove(msg.id) is True
        assert dlq.count() == 0
        assert dlq.remove("nonexistent") is False

    def test_replay_resets_retries(self, dlq, backend):
        msg = TaskMessage(task_name="retried", retries=3, max_retries=3)
        dlq.add(msg, error="max retries")

        new_id = dlq.replay(msg.id)
        assert new_id is not None

        task = backend.dequeue("default")
        assert task is not None
        assert task.retries == 0  # Reset!


class TestDLQMiddleware:
    def test_on_dead_captures(self, dlq):
        mw = dlq.middleware()
        assert isinstance(mw, _DLQMiddleware)

        msg = TaskMessage(task_name="dying")
        mw.on_dead(msg, ValueError("fatal"))

        assert dlq.count() == 1
        assert dlq.list()[0].error == "fatal"

    def test_on_error_doesnt_suppress(self, dlq):
        mw = dlq.middleware()
        msg = TaskMessage(task_name="test")
        result = mw.on_error(msg, ValueError("err"))
        assert result is False

    def test_integration_with_worker(self, app, backend):
        dlq = DeadLetterQueue(app)
        app.add_middleware(dlq.middleware())

        @app.task(name="die_task", max_retries=0)
        def die_task() -> None:
            raise ValueError("permanent failure")

        die_task.delay()

        worker = Worker(app, poll_interval=0.1)
        t = threading.Thread(target=worker.start, daemon=True)
        t.start()
        time.sleep(1.5)
        worker.stop()
        t.join(timeout=5)

        assert dlq.count() == 1
        dead = dlq.list()[0]
        assert dead.task_name == "die_task"
        assert "permanent failure" in dead.error

    def test_replay_from_dlq(self, app, backend):
        dlq = DeadLetterQueue(app)
        app.add_middleware(dlq.middleware())

        call_count = []

        @app.task(name="flaky_task", max_retries=0)
        def flaky_task() -> str:
            call_count.append(1)
            if len(call_count) < 2:
                raise ValueError("first call fails")
            return "success"

        flaky_task.delay()

        # First run — task fails, enters DLQ
        worker = Worker(app, poll_interval=0.1)
        t = threading.Thread(target=worker.start, daemon=True)
        t.start()
        time.sleep(1.5)
        worker.stop()
        t.join(timeout=5)

        assert dlq.count() == 1

        # Replay from DLQ
        new_id = dlq.replay(dlq.list()[0].task_id)
        assert new_id is not None

        # Second run — task succeeds
        worker2 = Worker(app, poll_interval=0.1)
        t2 = threading.Thread(target=worker2.start, daemon=True)
        t2.start()
        time.sleep(1.5)
        worker2.stop()
        t2.join(timeout=5)

        result = backend.get_result(new_id)
        assert result is not None
        assert result.state == TaskState.SUCCESS
        assert result.result == "success"
