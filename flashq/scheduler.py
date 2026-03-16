"""Periodic task scheduler (cron-like).

Built-in scheduler that runs alongside the worker to dispatch tasks on
a recurring schedule. Supports both interval-based and cron expression
schedules.

Usage::

    from flashq import FlashQ
    from flashq.scheduler import Scheduler, every, cron

    app = FlashQ()

    @app.task()
    def cleanup_old_data():
        ...

    @app.task()
    def send_daily_report():
        ...

    scheduler = Scheduler(app)
    scheduler.add("cleanup_old_data", every(minutes=30))
    scheduler.add("send_daily_report", cron("0 9 * * *"))  # 9 AM daily
    scheduler.start()  # runs in background thread
"""

from __future__ import annotations

import datetime
import logging
import threading
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from flashq.app import FlashQ

logger = logging.getLogger(__name__)


@dataclass
class IntervalSchedule:
    """Run a task every N seconds."""
    seconds: float

    def next_run(self, last_run: float) -> float:
        return last_run + self.seconds


@dataclass
class CronSchedule:
    """Run a task on a cron schedule.

    Supports standard 5-field cron expressions: minute hour day month weekday
    Supports ``*``, specific values, ranges (``1-5``), steps (``*/5``), and lists (``1,3,5``).
    """
    expression: str
    _minute: list[int] | None = None
    _hour: list[int] | None = None
    _day: list[int] | None = None
    _month: list[int] | None = None
    _weekday: list[int] | None = None

    def __post_init__(self) -> None:
        parts = self.expression.strip().split()
        if len(parts) != 5:
            raise ValueError(f"Invalid cron expression: {self.expression!r} (need 5 fields)")
        self._minute = self._parse_field(parts[0], 0, 59)
        self._hour = self._parse_field(parts[1], 0, 23)
        self._day = self._parse_field(parts[2], 1, 31)
        self._month = self._parse_field(parts[3], 1, 12)
        self._weekday = self._parse_field(parts[4], 0, 6)

    @staticmethod
    def _parse_field(field: str, min_val: int, max_val: int) -> list[int] | None:
        if field == "*":
            return None  # matches all
        values: set[int] = set()
        for part in field.split(","):
            if "/" in part:
                base, step_str = part.split("/", 1)
                step = int(step_str)
                start = min_val if base == "*" else int(base)
                values.update(range(start, max_val + 1, step))
            elif "-" in part:
                lo, hi = part.split("-", 1)
                values.update(range(int(lo), int(hi) + 1))
            else:
                values.add(int(part))
        return sorted(v for v in values if min_val <= v <= max_val)

    def matches(self, dt: datetime.datetime) -> bool:
        if self._minute is not None and dt.minute not in self._minute:
            return False
        if self._hour is not None and dt.hour not in self._hour:
            return False
        if self._day is not None and dt.day not in self._day:
            return False
        if self._month is not None and dt.month not in self._month:
            return False
        return not (self._weekday is not None and dt.weekday() not in self._weekday)

    def next_run(self, last_run: float) -> float:
        # Start from the next minute after last_run
        dt = datetime.datetime.fromtimestamp(last_run, tz=datetime.timezone.utc)
        dt = dt.replace(second=0, microsecond=0) + datetime.timedelta(minutes=1)

        # Search up to 366 days ahead
        for _ in range(525960):  # 365.25 days in minutes
            if self.matches(dt):
                return dt.timestamp()
            dt += datetime.timedelta(minutes=1)

        raise RuntimeError(f"Could not find next run time for {self.expression!r}")


def every(
    seconds: float = 0,
    minutes: float = 0,
    hours: float = 0,
) -> IntervalSchedule:
    """Create an interval schedule."""
    total = seconds + minutes * 60 + hours * 3600
    if total <= 0:
        raise ValueError("Interval must be positive")
    return IntervalSchedule(seconds=total)


def cron(expression: str) -> CronSchedule:
    """Create a cron schedule from a 5-field expression."""
    return CronSchedule(expression=expression)


@dataclass
class _ScheduledJob:
    task_name: str
    schedule: IntervalSchedule | CronSchedule
    args: tuple = ()
    kwargs: dict | None = None
    queue: str = "default"
    last_run: float = 0.0
    next_run_at: float = 0.0


class Scheduler:
    """Periodic task scheduler.

    Runs in a background thread, checking schedules and dispatching tasks
    when they're due.
    """

    def __init__(self, app: FlashQ, *, check_interval: float = 10.0) -> None:
        self.app = app
        self.check_interval = check_interval
        self._jobs: list[_ScheduledJob] = []
        self._running = False
        self._thread: threading.Thread | None = None

    def add(
        self,
        task_name: str,
        schedule: IntervalSchedule | CronSchedule,
        *,
        args: tuple = (),
        kwargs: dict | None = None,
        queue: str = "default",
    ) -> None:
        """Register a periodic task."""
        now = time.time()
        job = _ScheduledJob(
            task_name=task_name,
            schedule=schedule,
            args=args,
            kwargs=kwargs,
            queue=queue,
            last_run=now,
            next_run_at=schedule.next_run(now),
        )
        self._jobs.append(job)
        logger.info("Scheduled %s (next: %s)", task_name,
                     datetime.datetime.fromtimestamp(job.next_run_at, tz=datetime.timezone.utc))

    def start(self) -> None:
        """Start the scheduler in a background thread."""
        self._running = True
        self._thread = threading.Thread(target=self._run, name="flashq-scheduler", daemon=True)
        self._thread.start()
        logger.info("Scheduler started with %d jobs", len(self._jobs))

    def stop(self) -> None:
        """Stop the scheduler."""
        self._running = False
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=10)
        logger.info("Scheduler stopped")

    def _run(self) -> None:
        while self._running:
            now = time.time()
            for job in self._jobs:
                if now >= job.next_run_at:
                    self._dispatch(job)
                    job.last_run = now
                    job.next_run_at = job.schedule.next_run(now)

            time.sleep(self.check_interval)

    def _dispatch(self, job: _ScheduledJob) -> None:
        try:
            task = self.app.get_task(job.task_name)
            task.apply(args=job.args, kwargs=job.kwargs, queue=job.queue)
            logger.debug("Dispatched periodic task: %s", job.task_name)
        except Exception:
            logger.exception("Failed to dispatch periodic task: %s", job.task_name)
