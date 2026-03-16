"""
FlashQ — The task queue that works out of the box.

No Redis, no RabbitMQ, just ``pip install flashq`` and go.

Usage::

    from flashq import FlashQ

    app = FlashQ()

    @app.task()
    def send_email(to: str, subject: str, body: str) -> None:
        print(f"Sending email to {to}")

    # Enqueue a task
    send_email.delay(to="user@example.com", subject="Hello", body="World")

    # Or with more control
    send_email.apply(to="user@example.com", subject="Hello", body="World", countdown=60)
"""

from __future__ import annotations

from flashq._version import __version__
from flashq.app import FlashQ
from flashq.enums import TaskPriority, TaskState
from flashq.exceptions import (
    BackendError,
    FlashQError,
    SerializationError,
    TaskNotFoundError,
    TaskTimeoutError,
    WorkerShutdownError,
)
from flashq.task import Task, TaskResult

__all__ = [
    "BackendError",
    # Core
    "FlashQ",
    # Exceptions
    "FlashQError",
    "SerializationError",
    "Task",
    "TaskNotFoundError",
    "TaskPriority",
    "TaskResult",
    # Enums
    "TaskState",
    "TaskTimeoutError",
    "WorkerShutdownError",
    # Meta
    "__version__",
]
