"""Task definition and decorator.

A :class:`Task` wraps a user-defined function and provides ``.delay()`` and
``.apply()`` methods to enqueue it for background execution. Tasks are
registered with a :class:`~flashq.app.FlashQ` instance via the
``@app.task()`` decorator.

Type safety is achieved using :pep:`612` (``ParamSpec``), so IDEs and type
checkers know the exact signature of ``.delay()`` and ``.apply()``.
"""

from __future__ import annotations

import datetime
import functools
import logging
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    TypeVar,
)

from typing_extensions import ParamSpec

from flashq.enums import TaskPriority, TaskState
from flashq.models import TaskMessage, TaskResult

if TYPE_CHECKING:
    from collections.abc import Callable

    from flashq.app import FlashQ

logger = logging.getLogger(__name__)

P = ParamSpec("P")
R = TypeVar("R")


class TaskHandle:
    """A lightweight handle returned by ``.delay()`` / ``.apply()``.

    Provides methods to check task status and retrieve results without
    needing to know the backend details.
    """

    def __init__(self, task_id: str, app: FlashQ) -> None:
        self.task_id = task_id
        self._app = app

    def __repr__(self) -> str:
        return f"TaskHandle({self.task_id!r})"

    @property
    def id(self) -> str:
        return self.task_id

    def get_result(self, timeout: float | None = None) -> TaskResult[Any] | None:
        """Retrieve the task result from the backend.

        Parameters
        ----------
        timeout:
            Maximum seconds to wait for a result. If ``None``, returns
            immediately (non-blocking). Blocking wait is not yet implemented
            in v0.1 — use polling or callbacks.
        """
        return self._app.backend.get_result(self.task_id)

    def get_state(self) -> TaskState | None:
        """Get the current state of the task."""
        msg = self._app.backend.get_task(self.task_id)
        return msg.state if msg else None

    def cancel(self) -> bool:
        """Attempt to cancel the task (only works if still pending)."""
        msg = self._app.backend.get_task(self.task_id)
        if msg and msg.state == TaskState.PENDING:
            self._app.backend.update_task_state(self.task_id, TaskState.CANCELLED)
            return True
        return False


class Task(Generic[P, R]):
    """A registered task wrapping a callable.

    Created by the ``@app.task()`` decorator. Do not instantiate directly.

    Type parameters:
    - ``P``: The parameters of the wrapped function (via ParamSpec)
    - ``R``: The return type of the wrapped function

    Example::

        @app.task(queue="emails", max_retries=5)
        def send_email(to: str, subject: str) -> bool:
            ...

        # Type-safe: IDE knows send_email.delay() expects (str, str)
        handle = send_email.delay("user@example.com", "Hello")
    """

    def __init__(
        self,
        fn: Callable[P, R],
        *,
        app: FlashQ,
        name: str | None = None,
        queue: str = "default",
        priority: int = TaskPriority.NORMAL,
        max_retries: int = 3,
        retry_delay: float = 60.0,
        retry_backoff: bool = False,
        timeout: float | None = None,
        result_ttl: float = 3600.0,
    ) -> None:
        self.fn = fn
        self.app = app
        self.name = name or f"{fn.__module__}.{fn.__qualname__}"
        self.queue = queue
        self.priority = priority
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.retry_backoff = retry_backoff
        self.timeout = timeout
        self.result_ttl = result_ttl

        # Preserve original function metadata
        functools.update_wrapper(self, fn)

    def __repr__(self) -> str:
        return f"Task({self.name!r})"

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R:
        """Execute the task function directly (synchronously).

        This is useful for testing or when you want to run the task inline
        without going through the queue.
        """
        return self.fn(*args, **kwargs)

    def delay(self, *args: P.args, **kwargs: P.kwargs) -> TaskHandle:
        """Enqueue the task for background execution with default settings.

        This is the most common way to dispatch a task. It uses the task's
        default queue, priority, and retry settings.

        Returns a :class:`TaskHandle` for tracking the task.
        """
        return self.apply(args=args, kwargs=kwargs)

    def apply(
        self,
        *,
        args: tuple[Any, ...] = (),
        kwargs: dict[str, Any] | None = None,
        queue: str | None = None,
        priority: int | None = None,
        countdown: float | None = None,
        eta: datetime.datetime | None = None,
        max_retries: int | None = None,
        retry_delay: float | None = None,
    ) -> TaskHandle:
        """Enqueue the task with full control over execution options.

        Parameters
        ----------
        args:
            Positional arguments for the task function.
        kwargs:
            Keyword arguments for the task function.
        queue:
            Override the default queue name.
        priority:
            Override the default priority level.
        countdown:
            Delay execution by this many seconds from now.
        eta:
            Execute at this specific datetime (UTC).
        max_retries:
            Override the default max retry count.
        retry_delay:
            Override the default retry delay.
        """
        resolved_kwargs = kwargs or {}
        resolved_eta = eta

        if countdown is not None:
            resolved_eta = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(
                seconds=countdown
            )

        message = TaskMessage(
            task_name=self.name,
            queue=queue or self.queue,
            args=args,
            kwargs=resolved_kwargs,
            priority=priority if priority is not None else self.priority,
            eta=resolved_eta,
            countdown=countdown,
            max_retries=max_retries if max_retries is not None else self.max_retries,
            retry_delay=retry_delay if retry_delay is not None else self.retry_delay,
            retry_backoff=self.retry_backoff,
            timeout=self.timeout,
            result_ttl=self.result_ttl,
        )

        # Route to schedule or queue
        if message.eta is not None:
            self.app.backend.add_to_schedule(message)
            logger.info("Scheduled task %s (%s) for %s", message.id, self.name, message.eta)
        else:
            self.app.backend.enqueue(message)
            logger.info("Enqueued task %s (%s)", message.id, self.name)

        return TaskHandle(message.id, self.app)
