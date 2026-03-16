"""FlashQ application — the central orchestrator.

The :class:`FlashQ` class is the main entry point for defining tasks,
configuring backends, and running workers. One ``FlashQ`` instance per
application is the recommended pattern.

Example::

    from flashq import FlashQ

    app = FlashQ()  # SQLite by default — zero config!

    @app.task()
    def add(x: int, y: int) -> int:
        return x + y

    # Enqueue
    handle = add.delay(2, 3)

    # Run the worker
    # $ flashq worker
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, TypeVar, overload

from typing_extensions import ParamSpec

from flashq.enums import TaskPriority
from flashq.exceptions import DuplicateTaskError, TaskNotFoundError
from flashq.middleware import Middleware, MiddlewareStack
from flashq.task import Task

if TYPE_CHECKING:
    from collections.abc import Callable

    from flashq.backends import BaseBackend

logger = logging.getLogger(__name__)

P = ParamSpec("P")
R = TypeVar("R")


class FlashQ:
    """The FlashQ application instance.

    Parameters
    ----------
    backend:
        Storage backend to use. If ``None`` (default), creates a
        :class:`~flashq.backends.sqlite.SQLiteBackend` with default settings
        — this is the "zero-config" experience.
    name:
        Application name, used for logging and worker identification.
    auto_setup:
        Automatically call ``backend.setup()`` on first use. Set to ``False``
        if you want to manage backend lifecycle manually.
    """

    def __init__(
        self,
        *,
        backend: BaseBackend | None = None,
        name: str = "flashq",
        auto_setup: bool = True,
    ) -> None:
        self.name = name
        self._auto_setup = auto_setup
        self._setup_done = False

        # Task registry: task_name → Task instance
        self._registry: dict[str, Task[..., Any]] = {}

        # Middleware stack
        self._middleware = MiddlewareStack()

        # Backend — lazy-init SQLite if not provided
        if backend is not None:
            self._backend = backend
        else:
            self._backend = None  # type: ignore[assignment]
            self._backend_lazy = True

    @property
    def backend(self) -> BaseBackend:
        """Access the storage backend, creating the default if needed."""
        if self._backend is None:
            from flashq.backends.sqlite import SQLiteBackend

            self._backend = SQLiteBackend()
            logger.info("Using default SQLite backend (flashq.db)")

        if self._auto_setup and not self._setup_done:
            self._backend.setup()
            self._setup_done = True

        return self._backend

    @property
    def registry(self) -> dict[str, Task[..., Any]]:
        """Read-only access to the task registry."""
        return self._registry

    @property
    def middleware(self) -> MiddlewareStack:
        """Access the middleware stack."""
        return self._middleware

    def add_middleware(self, middleware: Middleware) -> None:
        """Add a middleware to the stack."""
        self._middleware.add(middleware)

    # ──────────────────────────────────────────────
    # Task registration
    # ──────────────────────────────────────────────

    @overload
    def task(self, fn: Callable[P, R]) -> Task[P, R]: ...

    @overload
    def task(
        self,
        fn: None = None,
        *,
        name: str | None = None,
        queue: str = "default",
        priority: int = TaskPriority.NORMAL,
        max_retries: int = 3,
        retry_delay: float = 60.0,
        retry_backoff: bool = False,
        result_ttl: float = 3600.0,
    ) -> Callable[[Callable[P, R]], Task[P, R]]: ...

    def task(
        self,
        fn: Callable[P, R] | None = None,
        *,
        name: str | None = None,
        queue: str = "default",
        priority: int = TaskPriority.NORMAL,
        max_retries: int = 3,
        retry_delay: float = 60.0,
        retry_backoff: bool = False,
        result_ttl: float = 3600.0,
    ) -> Task[P, R] | Callable[[Callable[P, R]], Task[P, R]]:
        """Register a function as a background task.

        Can be used as a bare decorator or with arguments::

            @app.task
            def simple_task():
                ...

            @app.task(queue="emails", max_retries=5)
            def send_email(to: str):
                ...

        Parameters
        ----------
        fn:
            The function to wrap. If ``None``, returns a decorator.
        name:
            Explicit task name. Defaults to ``module.qualname``.
        queue:
            Default queue for this task.
        priority:
            Default priority level.
        max_retries:
            Maximum retry attempts on failure.
        retry_delay:
            Seconds between retries (base delay).
        retry_backoff:
            Use exponential backoff for retries.
        result_ttl:
            How long to keep results (seconds).
        """

        def decorator(func: Callable[P, R]) -> Task[P, R]:
            task_obj = Task(
                func,
                app=self,
                name=name,
                queue=queue,
                priority=priority,
                max_retries=max_retries,
                retry_delay=retry_delay,
                retry_backoff=retry_backoff,
                result_ttl=result_ttl,
            )

            if task_obj.name in self._registry:
                raise DuplicateTaskError(task_obj.name)

            self._registry[task_obj.name] = task_obj
            logger.debug("Registered task: %s", task_obj.name)
            return task_obj

        if fn is not None:
            # Used as @app.task (without parentheses)
            return decorator(fn)

        # Used as @app.task(...) (with arguments)
        return decorator

    def get_task(self, task_name: str) -> Task[..., Any]:
        """Look up a registered task by name.

        Raises
        ------
        TaskNotFoundError:
            If no task with the given name is registered.
        """
        try:
            return self._registry[task_name]
        except KeyError:
            raise TaskNotFoundError(task_name) from None

    # ──────────────────────────────────────────────
    # Convenience methods
    # ──────────────────────────────────────────────

    def send_task(
        self,
        task_name: str,
        *,
        args: tuple[Any, ...] = (),
        kwargs: dict[str, Any] | None = None,
        queue: str = "default",
        priority: int = TaskPriority.NORMAL,
        countdown: float | None = None,
    ) -> Any:
        """Send a task by name (useful when the task function isn't importable).

        Parameters
        ----------
        task_name:
            The registered name of the task.
        args:
            Positional arguments.
        kwargs:
            Keyword arguments.
        queue:
            Target queue.
        priority:
            Task priority.
        countdown:
            Delay in seconds.
        """
        task = self.get_task(task_name)
        return task.apply(
            args=args,
            kwargs=kwargs,
            queue=queue,
            priority=priority,
            countdown=countdown,
        )

    # ──────────────────────────────────────────────
    # Lifecycle
    # ──────────────────────────────────────────────

    def close(self) -> None:
        """Shut down the backend and release resources."""
        if self._backend is not None:
            self._backend.teardown()
            logger.info("FlashQ application shut down")

    def __repr__(self) -> str:
        backend_name = type(self._backend).__name__ if self._backend else "SQLiteBackend (lazy)"
        return f"FlashQ(name={self.name!r}, backend={backend_name}, tasks={len(self._registry)})"

    def __enter__(self) -> FlashQ:
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()
