"""SQLite storage backend — zero-config, zero-dependency.

This is FlashQ's default backend. It uses SQLite's WAL journal mode for
high concurrency and ``BEGIN IMMEDIATE`` transactions for atomic task
claiming. No external services required — just ``pip install flashq``.

Performance characteristics:
- Single-node: handles thousands of tasks/sec easily
- Multi-process safe: WAL mode allows concurrent readers + one writer
- Atomic dequeue: ``BEGIN IMMEDIATE`` prevents double-claiming
- Persistent: tasks survive process restarts
- File-based: easy backup, easy debugging (just open the .db file)
"""

from __future__ import annotations

import contextlib
import datetime
import logging
import sqlite3
import threading
from typing import TYPE_CHECKING, Any

from flashq.backends import BaseBackend
from flashq.enums import TaskState
from flashq.models import TaskMessage, TaskResult
from flashq.serializers import Serializer, default_serializer

if TYPE_CHECKING:
    from collections.abc import Sequence

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────
# Schema
# ──────────────────────────────────────────────

_SCHEMA = [
    # Main task queue table
    """
    CREATE TABLE IF NOT EXISTS tasks (
        id          TEXT PRIMARY KEY,
        task_name   TEXT NOT NULL,
        queue       TEXT NOT NULL DEFAULT 'default',
        data        BLOB NOT NULL,
        priority    INTEGER NOT NULL DEFAULT 5,
        state       TEXT NOT NULL DEFAULT 'pending',
        eta         REAL,
        retries     INTEGER NOT NULL DEFAULT 0,
        max_retries INTEGER NOT NULL DEFAULT 3,
        retry_delay REAL NOT NULL DEFAULT 60.0,
        created_at  TEXT NOT NULL,
        updated_at  TEXT
    )
    """,
    # Index for efficient dequeue: pending tasks ordered by priority
    """
    CREATE INDEX IF NOT EXISTS idx_tasks_dequeue
    ON tasks (queue, state, priority DESC, created_at ASC)
    WHERE state = 'pending'
    """,
    # Index for scheduled task polling
    """
    CREATE INDEX IF NOT EXISTS idx_tasks_schedule
    ON tasks (state, eta)
    WHERE state = 'pending' AND eta IS NOT NULL
    """,
    # Index for task lookup by state
    """
    CREATE INDEX IF NOT EXISTS idx_tasks_state
    ON tasks (queue, state)
    """,
    # Results table — separate from tasks for clean TTL management
    """
    CREATE TABLE IF NOT EXISTS results (
        task_id      TEXT PRIMARY KEY,
        state        TEXT NOT NULL,
        result       BLOB,
        error        TEXT,
        traceback    TEXT,
        started_at   TEXT,
        completed_at TEXT,
        runtime_ms   REAL,
        expires_at   REAL NOT NULL
    )
    """,
    # Schedule table for periodic/cron tasks
    """
    CREATE TABLE IF NOT EXISTS schedules (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        task_name   TEXT NOT NULL,
        queue       TEXT NOT NULL DEFAULT 'default',
        data        BLOB NOT NULL,
        eta         REAL NOT NULL,
        created_at  TEXT NOT NULL
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_schedules_eta
    ON schedules (eta ASC)
    """,
]


class _ConnectionState(threading.local):
    """Thread-local SQLite connection state.

    Each thread gets its own connection — required by SQLite's threading model.
    """

    def __init__(self) -> None:
        super().__init__()
        self.conn: sqlite3.Connection | None = None

    @property
    def is_closed(self) -> bool:
        return self.conn is None


class SQLiteBackend(BaseBackend):
    """SQLite-based storage backend.

    Parameters
    ----------
    path:
        Path to the SQLite database file. Defaults to ``"flashq.db"``
        in the current directory. Use ``":memory:"`` for in-memory storage
        (useful for testing — data is lost when the process exits).
    cache_mb:
        SQLite page cache size in megabytes. Higher = faster for large queues.
    timeout:
        Busy-wait timeout in seconds when the database is locked by another
        connection.
    wal_mode:
        Enable WAL (Write-Ahead Logging) journal mode. Strongly recommended
        for concurrent access. Only disable for special cases.
    serializer:
        Serializer for task arguments and results. Defaults to JSON.
    """

    def __init__(
        self,
        path: str = "flashq.db",
        *,
        cache_mb: int = 8,
        timeout: float = 5.0,
        wal_mode: bool = True,
        serializer: Serializer | None = None,
    ) -> None:
        self.path = path
        self._cache_mb = cache_mb
        self._timeout = timeout
        self._wal_mode = wal_mode
        self._serializer = serializer or default_serializer
        self._state = _ConnectionState()
        self._setup_done = False

    # ──────────────────────────────────────────────
    # Connection management
    # ──────────────────────────────────────────────

    @property
    def conn(self) -> sqlite3.Connection:
        """Get or create the thread-local connection."""
        if self._state.is_closed:
            conn = sqlite3.connect(self.path, timeout=self._timeout)
            conn.isolation_level = None  # manual transaction control
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL" if self._wal_mode else "PRAGMA journal_mode=DELETE")
            conn.execute(f"PRAGMA cache_size=-{1000 * self._cache_mb}")
            conn.execute("PRAGMA synchronous=NORMAL")  # safe with WAL
            conn.execute("PRAGMA foreign_keys=ON")
            conn.execute(f"PRAGMA busy_timeout={int(self._timeout * 1000)}")
            self._state.conn = conn
        return self._state.conn  # type: ignore[return-value]

    @contextlib.contextmanager
    def _transaction(self, *, immediate: bool = False):
        """Context manager for SQLite transactions.

        Parameters
        ----------
        immediate:
            Use ``BEGIN IMMEDIATE`` to acquire a write lock immediately.
            Required for atomic dequeue operations.
        """
        conn = self.conn
        begin = "BEGIN IMMEDIATE" if immediate else "BEGIN"
        conn.execute(begin)
        try:
            yield conn
        except Exception:
            conn.execute("ROLLBACK")
            raise
        else:
            conn.execute("COMMIT")

    # ──────────────────────────────────────────────
    # Lifecycle
    # ──────────────────────────────────────────────

    def setup(self) -> None:
        """Create tables and indexes if they don't exist."""
        if self._setup_done:
            return
        conn = self.conn
        for ddl in _SCHEMA:
            conn.execute(ddl)
        self._setup_done = True
        logger.info("SQLite backend initialized: %s", self.path)

    def teardown(self) -> None:
        """Close the thread-local connection."""
        if not self._state.is_closed:
            self._state.conn.close()  # type: ignore[union-attr]
            self._state.conn = None
        logger.debug("SQLite backend connection closed")

    # ──────────────────────────────────────────────
    # Queue operations
    # ──────────────────────────────────────────────

    def enqueue(self, message: TaskMessage) -> None:
        """Insert a task into the queue."""
        data = self._serializer.dumps(message.to_dict())
        with self._transaction(immediate=True):
            self.conn.execute(
                """
                INSERT INTO tasks (id, task_name, queue, data, priority, state,
                                   eta, retries, max_retries, retry_delay,
                                   created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    message.created_at.isoformat(),
                    None,
                ),
            )
        logger.debug("Enqueued task %s (%s) on queue %r", message.id, message.task_name, message.queue)

    def dequeue(self, queue: str = "default") -> TaskMessage | None:
        """Atomically claim the highest-priority pending task.

        Uses ``BEGIN IMMEDIATE`` to ensure only one worker can claim a given
        task, even under high concurrency.
        """
        now = datetime.datetime.now(datetime.timezone.utc).timestamp()
        with self._transaction(immediate=True) as conn:
            row = conn.execute(
                """
                SELECT id, data FROM tasks
                WHERE queue = ?
                  AND state = 'pending'
                  AND (eta IS NULL OR eta <= ?)
                ORDER BY priority DESC, created_at ASC
                LIMIT 1
                """,
                (queue, now),
            ).fetchone()

            if row is None:
                return None

            task_id = row["id"]
            conn.execute(
                """
                UPDATE tasks SET state = 'running', updated_at = ?
                WHERE id = ?
                """,
                (datetime.datetime.now(datetime.timezone.utc).isoformat(), task_id),
            )

        data = self._serializer.loads(bytes(row["data"]))
        msg = TaskMessage.from_dict(data)
        logger.debug("Dequeued task %s (%s)", msg.id, msg.task_name)
        return msg

    def queue_size(self, queue: str = "default") -> int:
        """Count pending tasks in the given queue."""
        row = self.conn.execute(
            "SELECT COUNT(*) AS cnt FROM tasks WHERE queue = ? AND state = 'pending'",
            (queue,),
        ).fetchone()
        return row["cnt"] if row else 0

    def flush_queue(self, queue: str = "default") -> int:
        """Delete all pending tasks from the queue."""
        with self._transaction(immediate=True) as conn:
            cursor = conn.execute(
                "DELETE FROM tasks WHERE queue = ? AND state = 'pending'",
                (queue,),
            )
            return cursor.rowcount

    # ──────────────────────────────────────────────
    # Task state
    # ──────────────────────────────────────────────

    def get_task(self, task_id: str) -> TaskMessage | None:
        """Retrieve a task by ID.

        The authoritative state is in the ``state`` column (not the serialized
        data blob), so we overlay it after deserialization.
        """
        row = self.conn.execute(
            "SELECT data, state FROM tasks WHERE id = ?", (task_id,)
        ).fetchone()
        if row is None:
            return None
        data = self._serializer.loads(bytes(row["data"]))
        # Overlay the authoritative state from the column
        data["state"] = row["state"]
        return TaskMessage.from_dict(data)

    def update_task_state(self, task_id: str, state: TaskState) -> None:
        """Update a task's state."""
        with self._transaction(immediate=True) as conn:
            conn.execute(
                "UPDATE tasks SET state = ?, updated_at = ? WHERE id = ?",
                (
                    state.value,
                    datetime.datetime.now(datetime.timezone.utc).isoformat(),
                    task_id,
                ),
            )

    def get_tasks_by_state(
        self, state: TaskState, queue: str = "default", limit: int = 100
    ) -> Sequence[TaskMessage]:
        """List tasks filtered by state."""
        rows = self.conn.execute(
            """
            SELECT data FROM tasks WHERE queue = ? AND state = ?
            ORDER BY created_at DESC LIMIT ?
            """,
            (queue, state.value, limit),
        ).fetchall()
        return [
            TaskMessage.from_dict(self._serializer.loads(bytes(row["data"])))
            for row in rows
        ]

    # ──────────────────────────────────────────────
    # Results
    # ──────────────────────────────────────────────

    def store_result(self, result: TaskResult[Any]) -> None:
        """Store a task execution result with TTL."""
        result_data = self._serializer.dumps(result.result) if result.result is not None else None
        expires_at = (
            datetime.datetime.now(datetime.timezone.utc).timestamp() + 3600  # 1 hour default TTL
        )
        with self._transaction(immediate=True) as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO results
                    (task_id, state, result, error, traceback,
                     started_at, completed_at, runtime_ms, expires_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    result.task_id,
                    result.state.value,
                    result_data,
                    result.error,
                    result.traceback,
                    result.started_at.isoformat() if result.started_at else None,
                    result.completed_at.isoformat() if result.completed_at else None,
                    result.runtime_ms,
                    expires_at,
                ),
            )

    def get_result(self, task_id: str) -> TaskResult[Any] | None:
        """Retrieve a task result, respecting TTL."""
        now = datetime.datetime.now(datetime.timezone.utc).timestamp()
        row = self.conn.execute(
            "SELECT * FROM results WHERE task_id = ? AND expires_at > ?",
            (task_id, now),
        ).fetchone()
        if row is None:
            return None
        result_value = (
            self._serializer.loads(bytes(row["result"])) if row["result"] is not None else None
        )
        return TaskResult(
            task_id=row["task_id"],
            state=TaskState(row["state"]),
            result=result_value,
            error=row["error"],
            traceback=row["traceback"],
            started_at=(
                datetime.datetime.fromisoformat(row["started_at"])
                if row["started_at"] else None
            ),
            completed_at=(
                datetime.datetime.fromisoformat(row["completed_at"])
                if row["completed_at"] else None
            ),
            runtime_ms=row["runtime_ms"],
        )

    def delete_result(self, task_id: str) -> bool:
        """Delete a result entry."""
        with self._transaction(immediate=True) as conn:
            cursor = conn.execute("DELETE FROM results WHERE task_id = ?", (task_id,))
            return cursor.rowcount > 0

    def cleanup_expired_results(self) -> int:
        """Remove results whose TTL has expired. Returns count of deleted rows."""
        now = datetime.datetime.now(datetime.timezone.utc).timestamp()
        with self._transaction(immediate=True) as conn:
            cursor = conn.execute("DELETE FROM results WHERE expires_at <= ?", (now,))
            return cursor.rowcount

    # ──────────────────────────────────────────────
    # Scheduling
    # ──────────────────────────────────────────────

    def add_to_schedule(self, message: TaskMessage) -> None:
        """Add a task to the schedule for deferred execution."""
        if message.eta is None:
            raise ValueError("Cannot schedule a task without an ETA")
        data = self._serializer.dumps(message.to_dict())
        with self._transaction(immediate=True) as conn:
            conn.execute(
                """
                INSERT INTO schedules (task_name, queue, data, eta, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    message.task_name,
                    message.queue,
                    data,
                    message.eta.timestamp(),
                    message.created_at.isoformat(),
                ),
            )

    def read_schedule(self, now: float) -> Sequence[TaskMessage]:
        """Claim all scheduled tasks whose ETA has passed."""
        with self._transaction(immediate=True) as conn:
            rows = conn.execute(
                "SELECT id, data FROM schedules WHERE eta <= ? ORDER BY eta ASC",
                (now,),
            ).fetchall()

            if not rows:
                return []

            ids = [row["id"] for row in rows]
            placeholders = ",".join("?" * len(ids))
            conn.execute(f"DELETE FROM schedules WHERE id IN ({placeholders})", ids)

        return [
            TaskMessage.from_dict(self._serializer.loads(bytes(row["data"])))
            for row in rows
        ]

    def schedule_size(self) -> int:
        """Count scheduled tasks."""
        row = self.conn.execute("SELECT COUNT(*) AS cnt FROM schedules").fetchone()
        return row["cnt"] if row else 0
