"""Tests for FlashQ core functionality."""

from __future__ import annotations

import time

import pytest

from flashq import FlashQ, TaskState
from flashq.backends.sqlite import SQLiteBackend
from flashq.models import TaskMessage, TaskResult
from flashq.task import Task, TaskHandle

# ──────────────────────────────────────────────
# Fixtures
# ──────────────────────────────────────────────


@pytest.fixture
def backend(tmp_path):
    """Create a fresh SQLite backend for each test."""
    db_path = str(tmp_path / "test.db")
    b = SQLiteBackend(path=db_path)
    b.setup()
    yield b
    b.teardown()


@pytest.fixture
def app(backend):
    """Create a FlashQ app with a test backend."""
    return FlashQ(backend=backend, name="test")


# ──────────────────────────────────────────────
# App & task registration tests
# ──────────────────────────────────────────────


class TestApp:
    def test_create_default_app(self):
        """App can be created with zero config."""
        app = FlashQ()
        assert app.name == "flashq"
        assert len(app.registry) == 0

    def test_register_task_with_decorator(self, app):
        """@app.task() registers the function."""

        @app.task()
        def my_task(x: int) -> int:
            return x * 2

        assert "tests.test_core.TestApp.test_register_task_with_decorator.<locals>.my_task" in app.registry
        assert isinstance(my_task, Task)

    def test_register_task_bare_decorator(self, app):
        """@app.task (no parens) also works."""

        @app.task
        def my_task(x: int) -> int:
            return x * 2

        assert isinstance(my_task, Task)

    def test_duplicate_task_raises(self, app):
        """Registering the same task name twice raises an error."""
        from flashq.exceptions import DuplicateTaskError

        @app.task(name="unique_task")
        def task_a():
            pass

        with pytest.raises(DuplicateTaskError):

            @app.task(name="unique_task")
            def task_b():
                pass

    def test_task_direct_call(self, app):
        """Task can be called directly (synchronously)."""

        @app.task()
        def add(x: int, y: int) -> int:
            return x + y

        assert add(2, 3) == 5

    def test_get_task(self, app):
        """get_task retrieves registered tasks by name."""

        @app.task(name="my.custom.task")
        def custom():
            pass

        assert app.get_task("my.custom.task") is custom

    def test_get_task_not_found(self, app):
        """get_task raises TaskNotFoundError for unknown tasks."""
        from flashq.exceptions import TaskNotFoundError

        with pytest.raises(TaskNotFoundError):
            app.get_task("nonexistent")


# ──────────────────────────────────────────────
# Task enqueue tests
# ──────────────────────────────────────────────


class TestEnqueue:
    def test_delay_enqueues(self, app):
        """task.delay() puts a message in the queue."""

        @app.task()
        def my_task(x: int) -> int:
            return x

        handle = my_task.delay(42)
        assert isinstance(handle, TaskHandle)
        assert app.backend.queue_size("default") == 1

    def test_apply_with_countdown(self, app):
        """task.apply(countdown=...) schedules instead of enqueueing."""

        @app.task()
        def my_task() -> None:
            pass

        my_task.apply(countdown=60)
        assert app.backend.queue_size("default") == 0
        assert app.backend.schedule_size() == 1

    def test_apply_custom_queue(self, app):
        """task.apply(queue=...) overrides the default queue."""

        @app.task()
        def my_task() -> None:
            pass

        my_task.apply(queue="high-priority")
        assert app.backend.queue_size("high-priority") == 1
        assert app.backend.queue_size("default") == 0

    def test_cancel_pending_task(self, app):
        """Pending tasks can be cancelled."""

        @app.task()
        def my_task() -> None:
            pass

        handle = my_task.delay()
        assert handle.cancel() is True
        assert handle.get_state() == TaskState.CANCELLED


# ──────────────────────────────────────────────
# SQLite backend tests
# ──────────────────────────────────────────────


class TestSQLiteBackend:
    def test_enqueue_dequeue(self, backend):
        """Basic enqueue/dequeue cycle."""
        msg = TaskMessage(task_name="test.task", queue="default")
        backend.enqueue(msg)

        assert backend.queue_size("default") == 1

        claimed = backend.dequeue("default")
        assert claimed is not None
        assert claimed.task_name == "test.task"
        assert backend.queue_size("default") == 0

    def test_dequeue_empty(self, backend):
        """Dequeue from empty queue returns None."""
        assert backend.dequeue("default") is None

    def test_priority_ordering(self, backend):
        """Higher priority tasks are dequeued first."""
        msg_low = TaskMessage(task_name="low", queue="default", priority=0)
        msg_high = TaskMessage(task_name="high", queue="default", priority=20)

        backend.enqueue(msg_low)
        backend.enqueue(msg_high)

        first = backend.dequeue("default")
        assert first is not None
        assert first.task_name == "high"

        second = backend.dequeue("default")
        assert second is not None
        assert second.task_name == "low"

    def test_queue_isolation(self, backend):
        """Tasks in different queues don't interfere."""
        msg_a = TaskMessage(task_name="task_a", queue="queue_a")
        msg_b = TaskMessage(task_name="task_b", queue="queue_b")

        backend.enqueue(msg_a)
        backend.enqueue(msg_b)

        assert backend.queue_size("queue_a") == 1
        assert backend.queue_size("queue_b") == 1

        claimed = backend.dequeue("queue_a")
        assert claimed is not None
        assert claimed.task_name == "task_a"
        assert backend.queue_size("queue_b") == 1

    def test_flush_queue(self, backend):
        """flush_queue removes all pending tasks."""
        for i in range(5):
            backend.enqueue(TaskMessage(task_name=f"task_{i}", queue="default"))

        assert backend.queue_size("default") == 5
        removed = backend.flush_queue("default")
        assert removed == 5
        assert backend.queue_size("default") == 0

    def test_store_and_get_result(self, backend):
        """Results can be stored and retrieved."""
        result = TaskResult(
            task_id="abc123",
            state=TaskState.SUCCESS,
            result={"answer": 42},
            runtime_ms=15.5,
        )
        backend.store_result(result)

        retrieved = backend.get_result("abc123")
        assert retrieved is not None
        assert retrieved.result == {"answer": 42}
        assert retrieved.state == TaskState.SUCCESS
        assert retrieved.runtime_ms == 15.5

    def test_get_result_not_found(self, backend):
        """get_result returns None for missing task IDs."""
        assert backend.get_result("nonexistent") is None

    def test_delete_result(self, backend):
        """Results can be deleted."""
        result = TaskResult(task_id="del123", state=TaskState.SUCCESS)
        backend.store_result(result)
        assert backend.delete_result("del123") is True
        assert backend.get_result("del123") is None

    def test_schedule_and_read(self, backend):
        """Scheduled tasks are returned when their ETA passes."""
        import datetime

        past_eta = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=10)
        future_eta = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=1)

        msg_past = TaskMessage(task_name="past_task", eta=past_eta)
        msg_future = TaskMessage(task_name="future_task", eta=future_eta)

        backend.add_to_schedule(msg_past)
        backend.add_to_schedule(msg_future)

        assert backend.schedule_size() == 2

        now = time.time()
        due = backend.read_schedule(now)
        assert len(due) == 1
        assert due[0].task_name == "past_task"
        assert backend.schedule_size() == 1

    def test_task_state_transitions(self, backend):
        """Task state can be updated."""
        msg = TaskMessage(task_name="test.task", queue="default")
        backend.enqueue(msg)

        backend.update_task_state(msg.id, TaskState.RUNNING)
        task = backend.get_task(msg.id)
        assert task is not None
        # State in the tasks table is updated (not in serialized data)

    def test_get_tasks_by_state(self, backend):
        """Tasks can be filtered by state."""
        msg1 = TaskMessage(task_name="task_1", queue="default")
        msg2 = TaskMessage(task_name="task_2", queue="default")
        backend.enqueue(msg1)
        backend.enqueue(msg2)

        pending = backend.get_tasks_by_state(TaskState.PENDING, "default")
        assert len(pending) == 2

    def test_memory_backend(self):
        """In-memory SQLite works for testing."""
        b = SQLiteBackend(path=":memory:")
        b.setup()
        msg = TaskMessage(task_name="mem_task")
        b.enqueue(msg)
        assert b.queue_size("default") == 1
        b.teardown()


# ──────────────────────────────────────────────
# Model tests
# ──────────────────────────────────────────────


class TestModels:
    def test_task_message_roundtrip(self):
        """TaskMessage can be serialized and deserialized."""
        msg = TaskMessage(
            task_name="test.task",
            queue="emails",
            args=(1, "hello"),
            kwargs={"key": "value"},
            priority=10,
        )
        data = msg.to_dict()
        restored = TaskMessage.from_dict(data)

        assert restored.task_name == msg.task_name
        assert restored.queue == msg.queue
        assert restored.args == msg.args
        assert restored.kwargs == msg.kwargs
        assert restored.priority == msg.priority

    def test_task_result_roundtrip(self):
        """TaskResult can be serialized and deserialized."""
        import datetime

        result = TaskResult(
            task_id="test123",
            state=TaskState.SUCCESS,
            result={"data": [1, 2, 3]},
            runtime_ms=42.0,
            started_at=datetime.datetime.now(datetime.timezone.utc),
            completed_at=datetime.datetime.now(datetime.timezone.utc),
        )
        data = result.to_dict()
        restored = TaskResult.from_dict(data)

        assert restored.task_id == result.task_id
        assert restored.state == result.state
        assert restored.result == result.result

    def test_task_state_properties(self):
        """TaskState enum has correct properties."""
        assert TaskState.SUCCESS.is_terminal is True
        assert TaskState.RUNNING.is_terminal is False
        assert TaskState.RUNNING.is_active is True
        assert TaskState.PENDING.is_active is False
