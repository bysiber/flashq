"""Backend abstraction layer.

All backends implement the :class:`BaseBackend` interface, providing a clean
separation between task management logic and storage. This design allows
FlashQ to support SQLite, PostgreSQL, and Redis with the same public API.

Backend authors should subclass :class:`BaseBackend` and implement all
abstract methods.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Sequence

    from flashq.enums import TaskState
    from flashq.models import TaskMessage, TaskResult


class BaseBackend(ABC):
    """Abstract base class for all FlashQ storage backends.

    A backend is responsible for:
    - Storing and retrieving task messages (the queue)
    - Atomically claiming tasks for workers (dequeue)
    - Storing and retrieving task results
    - Managing scheduled/delayed tasks

    All methods have both sync and async variants. By default, async methods
    delegate to sync (via thread executor) and vice versa. Backends should
    override whichever variant is natural for their driver.
    """

    # ──────────────────────────────────────────────
    # Lifecycle
    # ──────────────────────────────────────────────

    @abstractmethod
    def setup(self) -> None:
        """Initialize backend resources (create tables, connect pools, etc.)."""

    @abstractmethod
    def teardown(self) -> None:
        """Release backend resources (close connections, etc.)."""

    async def setup_async(self) -> None:
        """Async variant of :meth:`setup`. Defaults to calling sync version."""
        self.setup()

    async def teardown_async(self) -> None:
        """Async variant of :meth:`teardown`. Defaults to calling sync version."""
        self.teardown()

    # ──────────────────────────────────────────────
    # Queue operations
    # ──────────────────────────────────────────────

    @abstractmethod
    def enqueue(self, message: TaskMessage) -> None:
        """Add a task message to the queue.

        Parameters
        ----------
        message:
            The task message to enqueue. Must have a unique ``id``.
        """

    @abstractmethod
    def dequeue(self, queue: str = "default") -> TaskMessage | None:
        """Atomically claim and return the next task from the queue.

        The task's state should be transitioned to ``RUNNING`` atomically
        to prevent other workers from claiming the same task.

        Parameters
        ----------
        queue:
            Queue name to dequeue from.

        Returns
        -------
        The claimed task message, or ``None`` if the queue is empty.
        """

    @abstractmethod
    def queue_size(self, queue: str = "default") -> int:
        """Return the number of pending tasks in the given queue."""

    @abstractmethod
    def flush_queue(self, queue: str = "default") -> int:
        """Remove all pending tasks from the queue. Return count removed."""

    # ──────────────────────────────────────────────
    # Task state
    # ──────────────────────────────────────────────

    @abstractmethod
    def get_task(self, task_id: str) -> TaskMessage | None:
        """Retrieve a task message by ID, or ``None`` if not found."""

    @abstractmethod
    def update_task_state(self, task_id: str, state: TaskState) -> None:
        """Update the state of a task."""

    @abstractmethod
    def get_tasks_by_state(
        self, state: TaskState, queue: str = "default", limit: int = 100
    ) -> Sequence[TaskMessage]:
        """Retrieve tasks filtered by state."""

    # ──────────────────────────────────────────────
    # Results
    # ──────────────────────────────────────────────

    @abstractmethod
    def store_result(self, result: TaskResult[Any]) -> None:
        """Store a task execution result."""

    @abstractmethod
    def get_result(self, task_id: str) -> TaskResult[Any] | None:
        """Retrieve a task result by task ID, or ``None`` if not available."""

    @abstractmethod
    def delete_result(self, task_id: str) -> bool:
        """Delete a stored result. Return ``True`` if a result was deleted."""

    # ──────────────────────────────────────────────
    # Scheduling
    # ──────────────────────────────────────────────

    @abstractmethod
    def add_to_schedule(self, message: TaskMessage) -> None:
        """Add a task to the schedule for future execution.

        The task's ``eta`` field determines when it should be moved to
        the active queue.
        """

    @abstractmethod
    def read_schedule(self, now: float) -> Sequence[TaskMessage]:
        """Return and remove all scheduled tasks whose ETA has passed.

        Parameters
        ----------
        now:
            Current timestamp (UTC, as float seconds since epoch).
        """

    @abstractmethod
    def schedule_size(self) -> int:
        """Return the number of tasks waiting in the schedule."""

    # ──────────────────────────────────────────────
    # Dashboard / stats helpers (concrete defaults)
    # ──────────────────────────────────────────────

    def get_queue_names(self) -> list[str]:
        """Return known queue names. Override for efficient implementation."""
        return ["default"]

    def get_stats(self) -> dict[str, Any]:
        """Aggregate stats across all queues and states.

        Returns a dict like::

            {
                "queues": {"default": 12, "emails": 3},
                "states": {"PENDING": 15, "RUNNING": 2, ...},
                "scheduled": 5,
            }
        """
        from flashq.enums import TaskState as _TS  # noqa: N814

        queues = {}
        for q in self.get_queue_names():
            queues[q] = self.queue_size(q)

        states: dict[str, int] = {}
        for s in _TS:
            total = 0
            for q in self.get_queue_names():
                total += len(self.get_tasks_by_state(s, queue=q, limit=10000))
            states[s.value] = total

        return {
            "queues": queues,
            "states": states,
            "scheduled": self.schedule_size(),
        }

    # ──────────────────────────────────────────────
    # Async variants (optional override)
    # ──────────────────────────────────────────────

    async def enqueue_async(self, message: TaskMessage) -> None:
        """Async variant of :meth:`enqueue`."""
        self.enqueue(message)

    async def dequeue_async(self, queue: str = "default") -> TaskMessage | None:
        """Async variant of :meth:`dequeue`."""
        return self.dequeue(queue)

    async def store_result_async(self, result: TaskResult[Any]) -> None:
        """Async variant of :meth:`store_result`."""
        self.store_result(result)

    async def get_result_async(self, task_id: str) -> TaskResult[Any] | None:
        """Async variant of :meth:`get_result`."""
        return self.get_result(task_id)
