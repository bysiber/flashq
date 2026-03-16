"""Core task and result models.

This module defines the data structures that represent tasks throughout their
lifecycle — from definition to execution to result storage.
"""

from __future__ import annotations

import datetime
import uuid
from dataclasses import dataclass, field
from typing import Any, Generic, TypeVar

from flashq.enums import TaskPriority, TaskState

R = TypeVar("R")


def _generate_task_id() -> str:
    """Generate a globally unique task ID using UUID4."""
    return uuid.uuid4().hex


@dataclass(frozen=True, slots=True)
class TaskMessage:
    """Immutable message representing a single task invocation.

    This is what gets serialized and stored in the backend. It is the
    "wire format" of a task — everything the worker needs to execute it.
    """

    id: str = field(default_factory=_generate_task_id)
    task_name: str = ""
    queue: str = "default"
    args: tuple[Any, ...] = ()
    kwargs: dict[str, Any] = field(default_factory=dict)
    priority: int = TaskPriority.NORMAL
    state: TaskState = TaskState.PENDING

    # Scheduling
    eta: datetime.datetime | None = None  # absolute time
    countdown: float | None = None  # relative seconds from now

    # Retry policy
    retries: int = 0
    max_retries: int = 3
    retry_delay: float = 60.0  # seconds between retries
    retry_backoff: bool = False  # exponential backoff

    # Metadata
    created_at: datetime.datetime = field(
        default_factory=lambda: datetime.datetime.now(datetime.timezone.utc)
    )
    updated_at: datetime.datetime | None = None

    # Result tracking
    result_ttl: float = 3600.0  # seconds to keep result (1 hour default)

    def to_dict(self) -> dict[str, Any]:
        """Convert to a JSON-serializable dictionary."""
        return {
            "id": self.id,
            "task_name": self.task_name,
            "queue": self.queue,
            "args": list(self.args),
            "kwargs": self.kwargs,
            "priority": self.priority,
            "state": self.state.value,
            "eta": self.eta.isoformat() if self.eta else None,
            "countdown": self.countdown,
            "retries": self.retries,
            "max_retries": self.max_retries,
            "retry_delay": self.retry_delay,
            "retry_backoff": self.retry_backoff,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "result_ttl": self.result_ttl,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> TaskMessage:
        """Reconstruct a TaskMessage from a dictionary."""
        data = data.copy()
        data["state"] = TaskState(data["state"])
        data["args"] = tuple(data.get("args", ()))
        if data.get("eta"):
            data["eta"] = datetime.datetime.fromisoformat(data["eta"])
        if data.get("created_at"):
            data["created_at"] = datetime.datetime.fromisoformat(data["created_at"])
        if data.get("updated_at"):
            data["updated_at"] = datetime.datetime.fromisoformat(data["updated_at"])
        return cls(**data)


@dataclass(slots=True)
class TaskResult(Generic[R]):
    """Container for a completed task's result or error.

    Workers produce these after executing a task. They are stored in the
    backend so callers can retrieve results asynchronously.
    """

    task_id: str
    state: TaskState
    result: R | None = None
    error: str | None = None
    traceback: str | None = None
    started_at: datetime.datetime | None = None
    completed_at: datetime.datetime | None = None
    runtime_ms: float | None = None

    @property
    def is_success(self) -> bool:
        return self.state == TaskState.SUCCESS

    @property
    def is_failure(self) -> bool:
        return self.state in (TaskState.FAILURE, TaskState.DEAD)

    def to_dict(self) -> dict[str, Any]:
        """Convert to a JSON-serializable dictionary."""
        return {
            "task_id": self.task_id,
            "state": self.state.value,
            "result": self.result,
            "error": self.error,
            "traceback": self.traceback,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "runtime_ms": self.runtime_ms,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> TaskResult[Any]:
        """Reconstruct a TaskResult from a dictionary."""
        data = data.copy()
        data["state"] = TaskState(data["state"])
        if data.get("started_at"):
            data["started_at"] = datetime.datetime.fromisoformat(data["started_at"])
        if data.get("completed_at"):
            data["completed_at"] = datetime.datetime.fromisoformat(data["completed_at"])
        return cls(**data)
