"""Middleware system for FlashQ.

Middlewares intercept task lifecycle events (before/after execution, on error,
on retry) to add cross-cutting concerns like logging, metrics, rate limiting,
and custom error handling.

Usage::

    from flashq.middleware import Middleware

    class TimingMiddleware(Middleware):
        def before_execute(self, message):
            message._start = time.monotonic()

        def after_execute(self, message, result):
            elapsed = time.monotonic() - message._start
            print(f"Task {message.task_name} took {elapsed:.3f}s")

    app = FlashQ()
    app.add_middleware(TimingMiddleware())
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from flashq.models import TaskMessage

logger = logging.getLogger(__name__)


class Middleware:
    """Base middleware class. Override the methods you need."""

    def before_execute(self, message: TaskMessage) -> TaskMessage | None:
        """Called before a task starts executing.

        Return the message to continue, or ``None`` to skip execution.
        Modify and return the message to alter task parameters.
        """
        return message

    def after_execute(self, message: TaskMessage, result: Any) -> None:
        """Called after a task completes successfully."""

    def on_error(self, message: TaskMessage, exc: Exception) -> bool:
        """Called when a task raises an exception.

        Return ``True`` to suppress the error (task will be marked as success).
        Return ``False`` (default) to let normal error handling proceed.
        """
        return False

    def on_retry(self, message: TaskMessage, exc: Exception, countdown: float) -> None:
        """Called when a task is about to be retried."""

    def on_dead(self, message: TaskMessage, exc: Exception) -> None:
        """Called when a task exceeds max retries and is marked dead."""

    def on_worker_start(self) -> None:
        """Called when the worker process starts."""

    def on_worker_stop(self) -> None:
        """Called when the worker process shuts down."""


class MiddlewareStack:
    """Manages an ordered list of middlewares and dispatches events."""

    def __init__(self) -> None:
        self._middlewares: list[Middleware] = []

    def add(self, middleware: Middleware) -> None:
        self._middlewares.append(middleware)
        logger.debug("Added middleware: %s", type(middleware).__name__)

    def remove(self, middleware_type: type) -> None:
        self._middlewares = [m for m in self._middlewares if not isinstance(m, middleware_type)]

    @property
    def middlewares(self) -> list[Middleware]:
        return list(self._middlewares)

    def before_execute(self, message: TaskMessage) -> TaskMessage | None:
        current = message
        for mw in self._middlewares:
            result = mw.before_execute(current)
            if result is None:
                logger.debug("%s skipped task %s", type(mw).__name__, message.id)
                return None
            current = result
        return current

    def after_execute(self, message: TaskMessage, result: Any) -> None:
        for mw in self._middlewares:
            try:
                mw.after_execute(message, result)
            except Exception:
                logger.exception("Middleware %s.after_execute failed", type(mw).__name__)

    def on_error(self, message: TaskMessage, exc: Exception) -> bool:
        for mw in self._middlewares:
            try:
                if mw.on_error(message, exc):
                    return True
            except Exception:
                logger.exception("Middleware %s.on_error failed", type(mw).__name__)
        return False

    def on_retry(self, message: TaskMessage, exc: Exception, countdown: float) -> None:
        for mw in self._middlewares:
            try:
                mw.on_retry(message, exc, countdown)
            except Exception:
                logger.exception("Middleware %s.on_retry failed", type(mw).__name__)

    def on_dead(self, message: TaskMessage, exc: Exception) -> None:
        for mw in self._middlewares:
            try:
                mw.on_dead(message, exc)
            except Exception:
                logger.exception("Middleware %s.on_dead failed", type(mw).__name__)

    def on_worker_start(self) -> None:
        for mw in self._middlewares:
            try:
                mw.on_worker_start()
            except Exception:
                logger.exception("Middleware %s.on_worker_start failed", type(mw).__name__)

    def on_worker_stop(self) -> None:
        for mw in reversed(self._middlewares):
            try:
                mw.on_worker_stop()
            except Exception:
                logger.exception("Middleware %s.on_worker_stop failed", type(mw).__name__)
