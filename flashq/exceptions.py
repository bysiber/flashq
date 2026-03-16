"""Exception hierarchy for FlashQ.

All exceptions inherit from :class:`FlashQError`, making it easy to catch
any FlashQ-related error with a single ``except FlashQError`` clause.
"""

from __future__ import annotations


class FlashQError(Exception):
    """Base exception for all FlashQ errors."""


class TaskNotFoundError(FlashQError):
    """Raised when a task name cannot be resolved to a registered task."""

    def __init__(self, task_name: str) -> None:
        self.task_name = task_name
        super().__init__(f"No task registered with name {task_name!r}")


class TaskTimeoutError(FlashQError):
    """Raised when waiting for a task result exceeds the timeout."""

    def __init__(self, task_id: str, timeout: float) -> None:
        self.task_id = task_id
        self.timeout = timeout
        super().__init__(
            f"Task {task_id!r} did not complete within {timeout}s"
        )


class BackendError(FlashQError):
    """Raised when a backend operation fails (connection, query, etc.)."""


class SerializationError(FlashQError):
    """Raised when task arguments or results cannot be serialized/deserialized."""


class WorkerShutdownError(FlashQError):
    """Raised when a worker is forced to shut down while tasks are running."""


class TaskRetryError(FlashQError):
    """Raised internally to signal that a task should be retried."""

    def __init__(self, countdown: float | None = None, max_retries: int | None = None) -> None:
        self.countdown = countdown
        self.max_retries = max_retries
        super().__init__("Task requested retry")


class DuplicateTaskError(FlashQError):
    """Raised when trying to register a task with a name that already exists."""

    def __init__(self, task_name: str) -> None:
        self.task_name = task_name
        super().__init__(f"Task {task_name!r} is already registered")
