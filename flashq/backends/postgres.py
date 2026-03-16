"""PostgreSQL storage backend using psycopg 3.

Uses PostgreSQL's LISTEN/NOTIFY for real-time task distribution instead of
polling. This makes it significantly more efficient than polling-based
approaches for multi-worker deployments.

Requires: pip install flashq[postgres]
"""

from __future__ import annotations

import datetime
import logging
import os
from typing import TYPE_CHECKING, Any

from flashq.backends import BaseBackend
from flashq.enums import TaskState
from flashq.models import TaskMessage, TaskResult
from flashq.serializers import Serializer, default_serializer

if TYPE_CHECKING:
    from collections.abc import Sequence

logger = logging.getLogger(__name__)

_SCHEMA = [
    """
    CREATE TABLE IF NOT EXISTS flashq_tasks (
        id          TEXT PRIMARY KEY,
        task_name   TEXT NOT NULL,
        queue       TEXT NOT NULL DEFAULT 'default',
        data        BYTEA NOT NULL,
        priority    INTEGER NOT NULL DEFAULT 5,
        state       TEXT NOT NULL DEFAULT 'pending',
        eta         DOUBLE PRECISION,
        retries     INTEGER NOT NULL DEFAULT 0,
        max_retries INTEGER NOT NULL DEFAULT 3,
        retry_delay DOUBLE PRECISION NOT NULL DEFAULT 60.0,
        created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at  TIMESTAMPTZ
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_flashq_tasks_dequeue
    ON flashq_tasks (queue, state, priority DESC, created_at ASC)
    WHERE state = 'pending'
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_flashq_tasks_state
    ON flashq_tasks (queue, state)
    """,
    """
    CREATE TABLE IF NOT EXISTS flashq_results (
        task_id      TEXT PRIMARY KEY,
        state        TEXT NOT NULL,
        result       BYTEA,
        error        TEXT,
        traceback    TEXT,
        started_at   TIMESTAMPTZ,
        completed_at TIMESTAMPTZ,
        runtime_ms   DOUBLE PRECISION,
        expires_at   TIMESTAMPTZ NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS flashq_schedules (
        id          SERIAL PRIMARY KEY,
        task_name   TEXT NOT NULL,
        queue       TEXT NOT NULL DEFAULT 'default',
        data        BYTEA NOT NULL,
        eta         DOUBLE PRECISION NOT NULL,
        created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_flashq_schedules_eta
    ON flashq_schedules (eta ASC)
    """,
]

# Atomic dequeue: SELECT FOR UPDATE SKIP LOCKED prevents double-claiming
_DEQUEUE_SQL = """
    WITH next_task AS (
        SELECT id FROM flashq_tasks
        WHERE queue = %s
          AND state = 'pending'
          AND (eta IS NULL OR eta <= EXTRACT(EPOCH FROM NOW()))
        ORDER BY priority DESC, created_at ASC
        LIMIT 1
        FOR UPDATE SKIP LOCKED
    )
    UPDATE flashq_tasks t
    SET state = 'running', updated_at = NOW()
    FROM next_task
    WHERE t.id = next_task.id
    RETURNING t.id, t.data
"""


class PostgresBackend(BaseBackend):
    """PostgreSQL backend with LISTEN/NOTIFY support.

    Parameters
    ----------
    dsn:
        PostgreSQL connection string, e.g. ``"postgresql://user:pass@localhost/mydb"``.
        Falls back to ``FLASHQ_DATABASE_URL`` env var.
    pool_min:
        Minimum connections in the pool.
    pool_max:
        Maximum connections in the pool.
    serializer:
        Serializer for task data. Defaults to JSON.
    """

    def __init__(
        self,
        dsn: str | None = None,
        *,
        pool_min: int = 2,
        pool_max: int = 10,
        serializer: Serializer | None = None,
    ) -> None:
        self.dsn = dsn or os.environ.get("FLASHQ_DATABASE_URL", "")
        if not self.dsn:
            raise ValueError(
                "PostgreSQL DSN required. Pass it directly or set FLASHQ_DATABASE_URL."
            )
        self._pool_min = pool_min
        self._pool_max = pool_max
        self._serializer = serializer or default_serializer
        self._pool = None

    def _get_pool(self):
        if self._pool is None:
            raise RuntimeError("Backend not initialized. Call setup() first.")
        return self._pool

    # -- lifecycle --

    def setup(self) -> None:
        try:
            from psycopg_pool import ConnectionPool
        except ImportError:
            raise ImportError(
                "PostgreSQL backend requires psycopg. Install with: pip install flashq[postgres]"
            ) from None

        self._pool = ConnectionPool(
            self.dsn,
            min_size=self._pool_min,
            max_size=self._pool_max,
        )
        with self._pool.connection() as conn:
            for ddl in _SCHEMA:
                conn.execute(ddl)
        logger.info("PostgreSQL backend initialized")

    def teardown(self) -> None:
        if self._pool is not None:
            self._pool.close()
            self._pool = None
            logger.debug("PostgreSQL pool closed")

    # -- queue ops --

    def enqueue(self, message: TaskMessage) -> None:
        data = self._serializer.dumps(message.to_dict())
        pool = self._get_pool()
        with pool.connection() as conn:
            conn.execute(
                """
                INSERT INTO flashq_tasks
                    (id, task_name, queue, data, priority, state, eta,
                     retries, max_retries, retry_delay, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    message.id,
                    message.task_name,
                    message.queue,
                    data,
                    message.priority,
                    message.state.value,
                    message.eta.timestamp() if message.eta else None,
                    message.retries,
                    message.max_retries,
                    message.retry_delay,
                    message.created_at,
                ),
            )
            # Notify workers listening on this queue
            conn.execute(
                "SELECT pg_notify(%s, %s)", (f"flashq_{message.queue}", message.id)
            )
        logger.debug("Enqueued %s on %r (PG)", message.id, message.queue)

    def dequeue(self, queue: str = "default") -> TaskMessage | None:
        pool = self._get_pool()
        with pool.connection() as conn:
            row = conn.execute(_DEQUEUE_SQL, (queue,)).fetchone()
            if row is None:
                return None
            data = self._serializer.loads(bytes(row[1]))
            return TaskMessage.from_dict(data)

    def queue_size(self, queue: str = "default") -> int:
        pool = self._get_pool()
        with pool.connection() as conn:
            row = conn.execute(
                "SELECT COUNT(*) FROM flashq_tasks WHERE queue = %s AND state = 'pending'",
                (queue,),
            ).fetchone()
            return row[0] if row else 0

    def flush_queue(self, queue: str = "default") -> int:
        pool = self._get_pool()
        with pool.connection() as conn:
            row = conn.execute(
                "DELETE FROM flashq_tasks WHERE queue = %s AND state = 'pending' RETURNING id",
                (queue,),
            ).fetchall()
            return len(row)

    # -- task state --

    def get_task(self, task_id: str) -> TaskMessage | None:
        pool = self._get_pool()
        with pool.connection() as conn:
            row = conn.execute(
                "SELECT data, state FROM flashq_tasks WHERE id = %s", (task_id,)
            ).fetchone()
            if row is None:
                return None
            data = self._serializer.loads(bytes(row[0]))
            data["state"] = row[1]
            return TaskMessage.from_dict(data)

    def update_task_state(self, task_id: str, state: TaskState) -> None:
        pool = self._get_pool()
        with pool.connection() as conn:
            conn.execute(
                "UPDATE flashq_tasks SET state = %s, updated_at = NOW() WHERE id = %s",
                (state.value, task_id),
            )

    def get_tasks_by_state(
        self, state: TaskState, queue: str = "default", limit: int = 100
    ) -> Sequence[TaskMessage]:
        pool = self._get_pool()
        with pool.connection() as conn:
            rows = conn.execute(
                """SELECT data FROM flashq_tasks
                   WHERE queue = %s AND state = %s
                   ORDER BY created_at DESC LIMIT %s""",
                (queue, state.value, limit),
            ).fetchall()
            return [
                TaskMessage.from_dict(self._serializer.loads(bytes(r[0])))
                for r in rows
            ]

    # -- results --

    def store_result(self, result: TaskResult[Any]) -> None:
        result_data = self._serializer.dumps(result.result) if result.result is not None else None
        expires_at = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=1)
        pool = self._get_pool()
        with pool.connection() as conn:
            conn.execute(
                """
                INSERT INTO flashq_results
                    (task_id, state, result, error, traceback,
                     started_at, completed_at, runtime_ms, expires_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (task_id) DO UPDATE SET
                    state = EXCLUDED.state,
                    result = EXCLUDED.result,
                    error = EXCLUDED.error,
                    traceback = EXCLUDED.traceback,
                    completed_at = EXCLUDED.completed_at,
                    runtime_ms = EXCLUDED.runtime_ms
                """,
                (
                    result.task_id,
                    result.state.value,
                    result_data,
                    result.error,
                    result.traceback,
                    result.started_at,
                    result.completed_at,
                    result.runtime_ms,
                    expires_at,
                ),
            )

    def get_result(self, task_id: str) -> TaskResult[Any] | None:
        pool = self._get_pool()
        with pool.connection() as conn:
            row = conn.execute(
                """SELECT task_id, state, result, error, traceback,
                          started_at, completed_at, runtime_ms
                   FROM flashq_results
                   WHERE task_id = %s AND expires_at > NOW()""",
                (task_id,),
            ).fetchone()
            if row is None:
                return None
            result_val = self._serializer.loads(bytes(row[2])) if row[2] is not None else None
            return TaskResult(
                task_id=row[0],
                state=TaskState(row[1]),
                result=result_val,
                error=row[3],
                traceback=row[4],
                started_at=row[5],
                completed_at=row[6],
                runtime_ms=row[7],
            )

    def delete_result(self, task_id: str) -> bool:
        pool = self._get_pool()
        with pool.connection() as conn:
            row = conn.execute(
                "DELETE FROM flashq_results WHERE task_id = %s RETURNING task_id",
                (task_id,),
            ).fetchone()
            return row is not None

    # -- scheduling --

    def add_to_schedule(self, message: TaskMessage) -> None:
        if message.eta is None:
            raise ValueError("Cannot schedule a task without an ETA")
        data = self._serializer.dumps(message.to_dict())
        pool = self._get_pool()
        with pool.connection() as conn:
            conn.execute(
                """INSERT INTO flashq_schedules (task_name, queue, data, eta)
                   VALUES (%s, %s, %s, %s)""",
                (message.task_name, message.queue, data, message.eta.timestamp()),
            )

    def read_schedule(self, now: float) -> Sequence[TaskMessage]:
        pool = self._get_pool()
        with pool.connection() as conn:
            rows = conn.execute(
                """DELETE FROM flashq_schedules
                   WHERE eta <= %s
                   RETURNING data""",
                (now,),
            ).fetchall()
            return [
                TaskMessage.from_dict(self._serializer.loads(bytes(r[0])))
                for r in rows
            ]

    def schedule_size(self) -> int:
        pool = self._get_pool()
        with pool.connection() as conn:
            row = conn.execute("SELECT COUNT(*) FROM flashq_schedules").fetchone()
            return row[0] if row else 0
