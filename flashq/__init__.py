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
from flashq.canvas import (
    Chain,
    ChainHandle,
    Chord,
    ChordHandle,
    Group,
    GroupHandle,
    Signature,
    chain,
    chord,
    group,
)
from flashq.dlq import DeadLetterQueue, DeadTask
from flashq.enums import TaskPriority, TaskState
from flashq.exceptions import (
    BackendError,
    DuplicateTaskError,
    FlashQError,
    SerializationError,
    TaskNotFoundError,
    TaskRetryError,
    TaskTimeoutError,
    WorkerShutdownError,
)
from flashq.middleware import Middleware
from flashq.models import TaskMessage, TaskResult
from flashq.scheduler import CronSchedule, IntervalSchedule, Scheduler, cron, every
from flashq.task import Task, TaskHandle

__all__ = [
    "BackendError",
    "Chain",
    "ChainHandle",
    "Chord",
    "ChordHandle",
    "CronSchedule",
    "DeadLetterQueue",
    "DeadTask",
    "DuplicateTaskError",
    "FlashQ",
    "FlashQError",
    "Group",
    "GroupHandle",
    "IntervalSchedule",
    "Middleware",
    "Scheduler",
    "SerializationError",
    "Signature",
    "Task",
    "TaskHandle",
    "TaskMessage",
    "TaskNotFoundError",
    "TaskPriority",
    "TaskResult",
    "TaskRetryError",
    "TaskState",
    "TaskTimeoutError",
    "WorkerShutdownError",
    "__version__",
    "chain",
    "chord",
    "cron",
    "every",
    "group",
]
