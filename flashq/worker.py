"""Worker process — executes tasks from the queue.

The worker continuously polls the backend for pending tasks, executes them,
handles retries on failure, and stores results. It supports graceful shutdown
via SIGINT/SIGTERM signals.

Architecture:
- Main loop polls for tasks at configurable intervals
- Sync tasks run in a thread pool
- Async tasks run on an event loop
- Failed tasks are retried with configurable delay and backoff
- Scheduled tasks are periodically moved to the active queue
"""

from __future__ import annotations

import asyncio
import datetime
import inspect
import logging
import os
import signal
import sys
import time
import traceback as tb_module
from concurrent.futures import Future, ThreadPoolExecutor
from typing import TYPE_CHECKING, Any

from flashq.enums import TaskState
from flashq.exceptions import TaskNotFoundError, TaskRetryError, TaskTimeoutError
from flashq.models import TaskMessage, TaskResult

if TYPE_CHECKING:
    from flashq.app import FlashQ

logger = logging.getLogger(__name__)


class Worker:
    """Background task worker.

    Parameters
    ----------
    app:
        The FlashQ application instance containing task registrations.
    queues:
        List of queue names to consume from. Defaults to ``["default"]``.
    concurrency:
        Maximum number of tasks to execute in parallel (thread pool size).
    poll_interval:
        Seconds between polling the backend for new tasks when idle.
    schedule_interval:
        Seconds between checking for scheduled tasks that are due.
    name:
        Worker name (defaults to ``worker-{pid}``).
    """

    def __init__(
        self,
        app: FlashQ,
        *,
        queues: list[str] | None = None,
        concurrency: int = 4,
        poll_interval: float = 1.0,
        schedule_interval: float = 5.0,
        name: str | None = None,
    ) -> None:
        self.app = app
        self.queues = queues or ["default"]
        self.concurrency = concurrency
        self.poll_interval = poll_interval
        self.schedule_interval = schedule_interval
        self.name = name or f"worker-{os.getpid()}"

        self._running = False
        self._executor: ThreadPoolExecutor | None = None
        self._active_futures: list[Future[None]] = []
        self._tasks_processed = 0
        self._tasks_failed = 0
        self._last_schedule_check = 0.0

    # ──────────────────────────────────────────────
    # Main loop
    # ──────────────────────────────────────────────

    def start(self) -> None:
        """Start the worker. Blocks until shutdown is requested."""
        self._running = True
        self._executor = ThreadPoolExecutor(
            max_workers=self.concurrency,
            thread_name_prefix="flashq-worker",
        )
        self._install_signals()

        logger.info(
            "Worker %r started | queues=%s | concurrency=%d | poll=%.1fs",
            self.name, self.queues, self.concurrency, self.poll_interval,
        )
        self._print_banner()
        self.app.middleware.on_worker_start()

        try:
            self._main_loop()
        except KeyboardInterrupt:
            logger.info("Interrupted")
        finally:
            self.stop()

    def stop(self) -> None:
        """Gracefully shut down the worker."""
        if not self._running:
            return
        self._running = False

        # Wait for in-flight tasks
        for fut in self._active_futures:
            if not fut.done():
                fut.result(timeout=30)

        self.app.middleware.on_worker_stop()

        logger.info(
            "Worker %r stopped | processed=%d failed=%d",
            self.name, self._tasks_processed, self._tasks_failed,
        )
        if self._executor:
            self._executor.shutdown(wait=True, cancel_futures=False)
            self._executor = None

    def _main_loop(self) -> None:
        """Core polling loop with concurrent task execution."""
        while self._running:
            self._check_schedule()
            self._cleanup_futures()

            # Don't dequeue more than concurrency allows
            if len(self._active_futures) >= self.concurrency:
                self._sleep(0.1)
                continue

            processed_any = False
            for queue in self.queues:
                if len(self._active_futures) >= self.concurrency:
                    break

                message = self.app.backend.dequeue(queue)
                if message is not None:
                    future = self._executor.submit(self._execute_task, message)
                    self._active_futures.append(future)
                    processed_any = True

            if not processed_any:
                self._sleep(self.poll_interval)

    def _cleanup_futures(self) -> None:
        """Remove completed futures from the active list."""
        self._active_futures = [f for f in self._active_futures if not f.done()]

    def _sleep(self, seconds: float) -> None:
        """Interruptible sleep."""
        end = time.monotonic() + seconds
        while self._running and time.monotonic() < end:
            time.sleep(min(0.1, end - time.monotonic()))

    # ──────────────────────────────────────────────
    # Task execution
    # ──────────────────────────────────────────────

    def _execute_task(self, message: TaskMessage) -> None:
        """Execute a single task, handling both sync and async functions."""
        started_at = datetime.datetime.now(datetime.timezone.utc)
        mw = self.app.middleware

        try:
            task = self.app.get_task(message.task_name)
        except TaskNotFoundError:
            logger.error("Unknown task: %s (id=%s)", message.task_name, message.id)
            self._store_failure(
                message, error=f"TaskNotFoundError: {message.task_name}",
                traceback_str=None, started_at=started_at,
            )
            return

        # Middleware: before_execute (can skip or modify the message)
        processed = mw.before_execute(message)
        if processed is None:
            logger.info("Task %s [%s] skipped by middleware", message.task_name, message.id[:8])
            self.app.backend.update_task_state(message.id, TaskState.CANCELLED)
            return
        message = processed

        logger.info(
            "Executing %s [%s] retry=%d/%d",
            message.task_name, message.id[:8],
            message.retries, message.max_retries,
        )

        try:
            timeout = message.timeout
            if timeout is not None:
                result = self._run_with_timeout(task.fn, message, timeout)
            elif inspect.iscoroutinefunction(task.fn):
                result = self._run_async(task.fn, message)
            else:
                result = task.fn(*message.args, **message.kwargs)

            mw.after_execute(message, result)
            self._store_success(message, result=result, started_at=started_at)
            self._tasks_processed += 1

        except TaskTimeoutError:
            self._tasks_failed += 1
            tb_str = f"TaskTimeoutError: Task {message.task_name} exceeded {message.timeout}s timeout"
            logger.error("Task %s [%s] timed out after %ss",
                         message.task_name, message.id[:8], message.timeout)

            if message.retries < message.max_retries:
                self._handle_retry(message, started_at=started_at)
            else:
                mw.on_dead(message, TaskTimeoutError(message.id, message.timeout or 0))
                self._store_failure(
                    message, error=tb_str, traceback_str=tb_str,
                    started_at=started_at, state=TaskState.DEAD,
                )

        except TaskRetryError:
            self._handle_retry(message, started_at=started_at)

        except Exception as exc:
            # Middleware can suppress errors
            if mw.on_error(message, exc):
                self._store_success(message, result=None, started_at=started_at)
                self._tasks_processed += 1
                return

            self._tasks_failed += 1
            tb_str = tb_module.format_exc()
            logger.error("Task %s [%s] failed: %s", message.task_name, message.id[:8], exc)

            if message.retries < message.max_retries:
                self._handle_retry(message, exc=exc, started_at=started_at)
            else:
                mw.on_dead(message, exc)
                logger.error(
                    "Task %s [%s] dead after %d retries",
                    message.task_name, message.id[:8], message.max_retries,
                )
                self._store_failure(
                    message, error=str(exc), traceback_str=tb_str,
                    started_at=started_at, state=TaskState.DEAD,
                )

    def _run_async(self, fn, message: TaskMessage) -> Any:
        """Run an async task function in a new event loop."""
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(fn(*message.args, **message.kwargs))
        finally:
            loop.close()

    def _run_with_timeout(self, fn, message: TaskMessage, timeout: float) -> Any:
        """Run a task with a timeout using a separate thread.

        If the task exceeds the timeout, raises TaskTimeoutError.
        Note: the timed-out task thread may continue running in the background
        but its result will be ignored.
        """
        from concurrent.futures import ThreadPoolExecutor
        from concurrent.futures import TimeoutError as FuturesTimeout

        executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="flashq-timeout")
        try:
            if inspect.iscoroutinefunction(fn):
                fut = executor.submit(self._run_async, fn, message)
            else:
                fut = executor.submit(fn, *message.args, **message.kwargs)

            try:
                return fut.result(timeout=timeout)
            except FuturesTimeout:
                # Cancel won't stop a running thread, but prevents queued work
                fut.cancel()
                raise TaskTimeoutError(message.id, timeout) from None
        finally:
            # shutdown(wait=False) so we don't block on the still-running thread
            executor.shutdown(wait=False)

    def _store_success(
        self, message: TaskMessage, *, result: Any,
        started_at: datetime.datetime,
    ) -> None:
        completed_at = datetime.datetime.now(datetime.timezone.utc)
        runtime_ms = (completed_at - started_at).total_seconds() * 1000

        self.app.backend.update_task_state(message.id, TaskState.SUCCESS)
        self.app.backend.store_result(TaskResult(
            task_id=message.id, state=TaskState.SUCCESS, result=result,
            started_at=started_at, completed_at=completed_at, runtime_ms=runtime_ms,
        ))
        logger.info("Task %s [%s] done in %.0fms", message.task_name, message.id[:8], runtime_ms)

    def _store_failure(
        self, message: TaskMessage, *, error: str,
        traceback_str: str | None, started_at: datetime.datetime,
        state: TaskState = TaskState.FAILURE,
    ) -> None:
        completed_at = datetime.datetime.now(datetime.timezone.utc)
        runtime_ms = (completed_at - started_at).total_seconds() * 1000

        self.app.backend.update_task_state(message.id, state)
        self.app.backend.store_result(TaskResult(
            task_id=message.id, state=state, error=error, traceback=traceback_str,
            started_at=started_at, completed_at=completed_at, runtime_ms=runtime_ms,
        ))

    def _handle_retry(
        self, message: TaskMessage,
        exc: Exception | None = None,
        *, started_at: datetime.datetime,
    ) -> None:
        retry_count = message.retries + 1
        delay = message.retry_delay * (2 ** message.retries) if message.retry_backoff else message.retry_delay
        eta = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=delay)

        logger.warning(
            "Retrying %s [%s] in %.0fs (%d/%d)",
            message.task_name, message.id[:8], delay, retry_count, message.max_retries,
        )

        retry_message = TaskMessage(
            id=message.id,
            task_name=message.task_name,
            queue=message.queue,
            args=message.args,
            kwargs=message.kwargs,
            priority=message.priority,
            state=TaskState.PENDING,
            eta=eta,
            retries=retry_count,
            max_retries=message.max_retries,
            retry_delay=message.retry_delay,
            retry_backoff=message.retry_backoff,
            timeout=message.timeout,
            created_at=message.created_at,
            result_ttl=message.result_ttl,
        )

        self.app.backend.update_task_state(message.id, TaskState.RETRYING)
        self.app.backend.add_to_schedule(retry_message)

    # ──────────────────────────────────────────────
    # Schedule
    # ──────────────────────────────────────────────

    def _check_schedule(self) -> None:
        now = time.time()
        if now - self._last_schedule_check < self.schedule_interval:
            return
        self._last_schedule_check = now

        due_tasks = self.app.backend.read_schedule(now)
        for msg in due_tasks:
            immediate = TaskMessage(
                id=msg.id, task_name=msg.task_name, queue=msg.queue,
                args=msg.args, kwargs=msg.kwargs, priority=msg.priority,
                state=TaskState.PENDING, retries=msg.retries,
                max_retries=msg.max_retries, retry_delay=msg.retry_delay,
                retry_backoff=msg.retry_backoff, timeout=msg.timeout,
                created_at=msg.created_at, result_ttl=msg.result_ttl,
            )
            self.app.backend.enqueue(immediate)

        if due_tasks:
            logger.info("Moved %d scheduled tasks to queue", len(due_tasks))

    # ──────────────────────────────────────────────
    # Signals
    # ──────────────────────────────────────────────

    def _install_signals(self) -> None:
        # Signals can only be installed from the main thread
        try:
            if sys.platform == "win32":
                signal.signal(signal.SIGINT, self._handle_signal)
            else:
                signal.signal(signal.SIGINT, self._handle_signal)
                signal.signal(signal.SIGTERM, self._handle_signal)
        except ValueError:
            # Not in main thread (e.g. during tests) — skip signal handling
            pass

    def _handle_signal(self, signum: int, frame: Any) -> None:
        sig_name = signal.Signals(signum).name
        logger.info("Received %s, shutting down gracefully...", sig_name)
        self._running = False

    # ──────────────────────────────────────────────
    # Display
    # ──────────────────────────────────────────────

    def _print_banner(self) -> None:
        tasks = list(self.app.registry.keys())
        backend_name = type(self.app.backend).__name__
        task_lines = "\n".join(f" │    └─ {t}" for t in tasks[:10])
        if len(tasks) > 10:
            task_lines += f"\n │    ... and {len(tasks) - 10} more"

        print(f"""
 ⚡ FlashQ Worker
 ├─ name:        {self.name}
 ├─ backend:     {backend_name}
 ├─ queues:      {', '.join(self.queues)}
 ├─ concurrency: {self.concurrency}
 ├─ tasks:       {len(tasks)}
{task_lines}
 └─ Ready! Waiting for tasks...
""", flush=True)
