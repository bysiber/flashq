"""Tests for middleware system."""

from __future__ import annotations

import threading
import time

import pytest

from flashq import FlashQ, Middleware, TaskState
from flashq.backends.sqlite import SQLiteBackend
from flashq.middleware import MiddlewareStack
from flashq.models import TaskMessage
from flashq.worker import Worker


@pytest.fixture
def backend(tmp_path):
    b = SQLiteBackend(path=str(tmp_path / "mw_test.db"))
    b.setup()
    yield b
    b.teardown()


@pytest.fixture
def app(backend):
    return FlashQ(backend=backend, name="mw-test")


class RecordingMiddleware(Middleware):
    """Middleware that records all lifecycle events."""

    def __init__(self):
        self.events: list[str] = []

    def before_execute(self, message):
        self.events.append(f"before:{message.task_name}")
        return message

    def after_execute(self, message, result):
        self.events.append(f"after:{message.task_name}:{result}")

    def on_error(self, message, exc):
        self.events.append(f"error:{message.task_name}:{exc}")
        return False

    def on_dead(self, message, exc):
        self.events.append(f"dead:{message.task_name}")

    def on_worker_start(self):
        self.events.append("worker_start")

    def on_worker_stop(self):
        self.events.append("worker_stop")


class SkipMiddleware(Middleware):
    """Middleware that skips tasks with 'skip' in name."""

    def before_execute(self, message):
        if "skip" in message.task_name:
            return None
        return message


class SuppressMiddleware(Middleware):
    """Middleware that suppresses all errors."""

    def on_error(self, message, exc):
        return True


class TestMiddlewareStack:
    def test_before_execute_chain(self):
        stack = MiddlewareStack()
        stack.add(RecordingMiddleware())
        msg = TaskMessage(task_name="test.task")
        result = stack.before_execute(msg)
        assert result is not None

    def test_before_execute_skip(self):
        stack = MiddlewareStack()
        stack.add(SkipMiddleware())
        msg = TaskMessage(task_name="skip_this")
        result = stack.before_execute(msg)
        assert result is None

    def test_middleware_ordering(self):
        stack = MiddlewareStack()
        events = []

        class First(Middleware):
            def before_execute(self, message):
                events.append("first")
                return message

        class Second(Middleware):
            def before_execute(self, message):
                events.append("second")
                return message

        stack.add(First())
        stack.add(Second())
        stack.before_execute(TaskMessage(task_name="test"))
        assert events == ["first", "second"]

    def test_remove_middleware(self):
        stack = MiddlewareStack()
        mw = RecordingMiddleware()
        stack.add(mw)
        assert len(stack.middlewares) == 1
        stack.remove(RecordingMiddleware)
        assert len(stack.middlewares) == 0


class TestMiddlewareIntegration:
    def test_middleware_called_on_success(self, app, backend):
        recorder = RecordingMiddleware()
        app.add_middleware(recorder)

        @app.task()
        def simple_task() -> str:
            return "ok"

        simple_task.delay()

        worker = Worker(app, poll_interval=0.1)
        t = threading.Thread(target=worker.start, daemon=True)
        t.start()
        time.sleep(1)
        worker.stop()
        t.join(timeout=5)

        assert "worker_start" in recorder.events
        assert any("before:" in e for e in recorder.events)
        assert any("after:" in e and "ok" in e for e in recorder.events)
        assert "worker_stop" in recorder.events

    def test_middleware_skip_task(self, app, backend):
        app.add_middleware(SkipMiddleware())

        @app.task(name="skip_me")
        def skipped_task() -> None:
            pass

        handle = skipped_task.delay()

        worker = Worker(app, poll_interval=0.1)
        t = threading.Thread(target=worker.start, daemon=True)
        t.start()
        time.sleep(1)
        worker.stop()
        t.join(timeout=5)

        state = handle.get_state()
        assert state == TaskState.CANCELLED

    def test_middleware_suppress_error(self, app, backend):
        app.add_middleware(SuppressMiddleware())

        @app.task(max_retries=0)
        def failing() -> None:
            raise ValueError("suppressed")

        handle = failing.delay()

        worker = Worker(app, poll_interval=0.1)
        t = threading.Thread(target=worker.start, daemon=True)
        t.start()
        time.sleep(1)
        worker.stop()
        t.join(timeout=5)

        result = backend.get_result(handle.id)
        assert result is not None
        assert result.state == TaskState.SUCCESS  # error was suppressed
