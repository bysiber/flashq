"""Tests for the periodic scheduler."""

from __future__ import annotations

import time

import pytest

from flashq import FlashQ
from flashq.backends.sqlite import SQLiteBackend
from flashq.scheduler import CronSchedule, IntervalSchedule, Scheduler, cron, every


@pytest.fixture
def backend(tmp_path):
    b = SQLiteBackend(path=str(tmp_path / "sched_test.db"))
    b.setup()
    yield b
    b.teardown()


@pytest.fixture
def app(backend):
    return FlashQ(backend=backend, name="sched-test")


class TestIntervalSchedule:
    def test_every_seconds(self):
        sched = every(seconds=30)
        assert sched.seconds == 30

    def test_every_minutes(self):
        sched = every(minutes=5)
        assert sched.seconds == 300

    def test_every_hours(self):
        sched = every(hours=1)
        assert sched.seconds == 3600

    def test_every_combined(self):
        sched = every(hours=1, minutes=30, seconds=15)
        assert sched.seconds == 5415

    def test_every_zero_raises(self):
        with pytest.raises(ValueError):
            every()

    def test_next_run(self):
        sched = every(seconds=60)
        now = time.time()
        next_run = sched.next_run(now)
        assert next_run == now + 60


class TestCronSchedule:
    def test_parse_basic(self):
        s = CronSchedule("*/5 * * * *")
        assert s._minute is not None
        assert 0 in s._minute
        assert 5 in s._minute
        assert 3 not in s._minute

    def test_parse_specific(self):
        s = CronSchedule("30 9 * * 1-5")
        assert s._minute == [30]
        assert s._hour == [9]
        assert s._weekday == [1, 2, 3, 4, 5]

    def test_parse_list(self):
        s = CronSchedule("0,15,30,45 * * * *")
        assert s._minute == [0, 15, 30, 45]

    def test_parse_range(self):
        s = CronSchedule("* 9-17 * * *")
        assert s._hour == list(range(9, 18))

    def test_invalid_expression(self):
        with pytest.raises(ValueError):
            CronSchedule("invalid")

    def test_invalid_too_few_fields(self):
        with pytest.raises(ValueError):
            CronSchedule("* * *")

    def test_cron_helper(self):
        s = cron("0 9 * * *")
        assert isinstance(s, CronSchedule)
        assert s._minute == [0]
        assert s._hour == [9]

    def test_matches(self):
        import datetime
        s = CronSchedule("30 9 * * *")
        dt_match = datetime.datetime(2024, 1, 15, 9, 30, tzinfo=datetime.timezone.utc)
        dt_no_match = datetime.datetime(2024, 1, 15, 10, 30, tzinfo=datetime.timezone.utc)
        assert s.matches(dt_match) is True
        assert s.matches(dt_no_match) is False

    def test_next_run_finds_time(self):
        s = CronSchedule("0 * * * *")  # every hour at :00
        now = time.time()
        next_run = s.next_run(now)
        assert next_run > now


class TestScheduler:
    def test_scheduler_dispatches_task(self, app, backend):
        @app.task(name="periodic_job")
        def periodic_job() -> None:
            pass

        scheduler = Scheduler(app, check_interval=0.2)
        scheduler.add("periodic_job", every(seconds=0.5))
        scheduler.start()

        time.sleep(2)
        scheduler.stop()

        # Should have dispatched at least one task
        assert backend.queue_size("default") >= 1

    def test_scheduler_with_cron(self, app, backend):
        @app.task(name="cron_job")
        def cron_job() -> None:
            pass

        scheduler = Scheduler(app, check_interval=0.2)
        # Far future cron — should not dispatch
        scheduler.add("cron_job", cron("0 0 31 12 *"))
        scheduler.start()
        time.sleep(1)
        scheduler.stop()

        assert backend.queue_size("default") == 0

    def test_scheduler_custom_queue(self, app, backend):
        @app.task(name="queued_job")
        def queued_job() -> None:
            pass

        scheduler = Scheduler(app, check_interval=0.2)
        scheduler.add("queued_job", every(seconds=0.5), queue="special")
        scheduler.start()
        time.sleep(2)
        scheduler.stop()

        assert backend.queue_size("special") >= 1
