"""Enumerations used across FlashQ."""

from __future__ import annotations

import enum


class TaskState(str, enum.Enum):
    """Lifecycle states of a task.

    State transitions::

        PENDING → RUNNING → SUCCESS
                         ↘ FAILURE → RETRYING → RUNNING
                                   ↘ DEAD (max retries exceeded)
        PENDING → CANCELLED
        RUNNING → REVOKED
    """

    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILURE = "failure"
    RETRYING = "retrying"
    CANCELLED = "cancelled"
    REVOKED = "revoked"
    DEAD = "dead"  # max retries exceeded

    @property
    def is_terminal(self) -> bool:
        """Return ``True`` if no further transitions are possible."""
        return self in _TERMINAL_STATES

    @property
    def is_active(self) -> bool:
        """Return ``True`` if the task is currently being processed."""
        return self in (TaskState.RUNNING, TaskState.RETRYING)


_TERMINAL_STATES = frozenset({
    TaskState.SUCCESS,
    TaskState.CANCELLED,
    TaskState.REVOKED,
    TaskState.DEAD,
})


class TaskPriority(int, enum.Enum):
    """Built-in priority levels. Higher values = processed first.

    Custom integer priorities are also supported — these are just convenience
    constants.
    """

    LOW = 0
    NORMAL = 5
    HIGH = 10
    CRITICAL = 20
