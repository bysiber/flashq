"""Dead Letter Queue (DLQ) support.

Tasks that fail permanently (exceed max retries) are moved to a dead letter
queue for later inspection, replay, or manual intervention.

Usage::

    from flashq import FlashQ
    from flashq.dlq import DeadLetterQueue

    app = FlashQ()
    dlq = DeadLetterQueue(app)

    # Get all dead tasks
    dead_tasks = dlq.list()

    # Replay a specific task
    dlq.replay(task_id="abc123")

    # Replay all dead tasks
    dlq.replay_all()

    # Purge all dead tasks
    dlq.purge()

    # DLQ as middleware — auto-captures dead tasks
    app.add_middleware(dlq.middleware())
"""

from __future__ import annotations

import datetime
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from flashq.enums import TaskState
from flashq.middleware import Middleware
from flashq.models import TaskMessage

if TYPE_CHECKING:
    from flashq.app import FlashQ

logger = logging.getLogger(__name__)


@dataclass
class DeadTask:
    """A task that has been moved to the dead letter queue."""

    task_id: str
    task_name: str
    args: tuple[Any, ...]
    kwargs: dict[str, Any]
    queue: str
    error: str
    traceback: str | None
    died_at: datetime.datetime
    retries: int
    max_retries: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "task_name": self.task_name,
            "args": list(self.args),
            "kwargs": self.kwargs,
            "queue": self.queue,
            "error": self.error,
            "traceback": self.traceback,
            "died_at": self.died_at.isoformat(),
            "retries": self.retries,
            "max_retries": self.max_retries,
        }


class DeadLetterQueue:
    """Manages dead (permanently failed) tasks.

    Dead tasks are stored in memory and can be replayed or purged.
    For persistence, the DLQ reads from the backend's result store.
    """

    def __init__(self, app: FlashQ) -> None:
        self._app = app
        self._dead_tasks: dict[str, DeadTask] = {}

    def add(self, message: TaskMessage, error: str, traceback: str | None = None) -> None:
        """Add a task to the dead letter queue."""
        dead = DeadTask(
            task_id=message.id,
            task_name=message.task_name,
            args=message.args,
            kwargs=message.kwargs,
            queue=message.queue,
            error=error,
            traceback=traceback,
            died_at=datetime.datetime.now(datetime.timezone.utc),
            retries=message.retries,
            max_retries=message.max_retries,
        )
        self._dead_tasks[message.id] = dead
        logger.info("Task %s [%s] moved to DLQ: %s", message.task_name, message.id[:8], error)

    def list(self) -> list[DeadTask]:
        """List all dead tasks."""
        return list(self._dead_tasks.values())

    def get(self, task_id: str) -> DeadTask | None:
        """Get a specific dead task by ID."""
        return self._dead_tasks.get(task_id)

    def count(self) -> int:
        """Number of tasks in the DLQ."""
        return len(self._dead_tasks)

    def replay(self, task_id: str) -> str | None:
        """Re-enqueue a dead task for retry. Returns new task ID or None."""
        dead = self._dead_tasks.pop(task_id, None)
        if dead is None:
            logger.warning("Task %s not found in DLQ", task_id[:8])
            return None

        new_msg = TaskMessage(
            task_name=dead.task_name,
            args=dead.args,
            kwargs=dead.kwargs,
            queue=dead.queue,
            max_retries=dead.max_retries,
            retries=0,  # Reset retry counter
            state=TaskState.PENDING,
        )
        self._app.backend.enqueue(new_msg)
        logger.info("Replayed task %s [%s] → new ID %s", dead.task_name, task_id[:8], new_msg.id[:8])
        return new_msg.id

    def replay_all(self) -> int:
        """Re-enqueue all dead tasks. Returns number replayed."""
        task_ids = list(self._dead_tasks.keys())
        count = 0
        for tid in task_ids:
            if self.replay(tid) is not None:
                count += 1
        return count

    def purge(self) -> int:
        """Remove all tasks from the DLQ. Returns number purged."""
        count = len(self._dead_tasks)
        self._dead_tasks.clear()
        logger.info("Purged %d tasks from DLQ", count)
        return count

    def remove(self, task_id: str) -> bool:
        """Remove a specific task from the DLQ."""
        return self._dead_tasks.pop(task_id, None) is not None

    def middleware(self) -> _DLQMiddleware:
        """Create a middleware that automatically captures dead tasks."""
        return _DLQMiddleware(self)


class _DLQMiddleware(Middleware):
    """Middleware that captures dead tasks into a DeadLetterQueue."""

    def __init__(self, dlq: DeadLetterQueue) -> None:
        self._dlq = dlq

    def on_dead(self, message: TaskMessage, exc: Exception) -> None:
        self._dlq.add(message, error=str(exc))

    def on_error(self, message: TaskMessage, exc: Exception) -> bool:
        # Don't suppress — let the normal error flow proceed
        return False
