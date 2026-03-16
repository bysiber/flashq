"""Redis storage backend for high-throughput deployments.

Uses Redis lists for queues, sorted sets for scheduling, and hashes for
task state and results. This is the fastest backend option.

Requires: pip install flashq[redis]
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

# Redis key prefixes
_PREFIX = "flashq"
_QUEUE_KEY = f"{_PREFIX}:queue:"          # list: queue:<name>
_TASK_KEY = f"{_PREFIX}:task:"            # hash: task:<id>
_RESULT_KEY = f"{_PREFIX}:result:"        # hash: result:<id>
_SCHEDULE_KEY = f"{_PREFIX}:schedule"     # sorted set (score = eta timestamp)
_STATE_KEY = f"{_PREFIX}:state:"          # set: state:<queue>:<state>

# Lua script for atomic dequeue - pops highest priority pending task
_DEQUEUE_LUA = """
local queue_key = KEYS[1]
local task_prefix = ARGV[1]
local now = tonumber(ARGV[2])

-- Pop from the priority queue (sorted set, highest score first)
local items = redis.call('ZREVRANGEBYSCORE', queue_key, '+inf', '-inf', 'LIMIT', 0, 1)
if #items == 0 then
    return nil
end

local task_id = items[1]
local task_key = task_prefix .. task_id

-- Check ETA
local eta = redis.call('HGET', task_key, 'eta')
if eta and tonumber(eta) > now then
    return nil
end

-- Remove from queue and update state
redis.call('ZREM', queue_key, task_id)
redis.call('HSET', task_key, 'state', 'running')

return {task_id, redis.call('HGET', task_key, 'data')}
"""


class RedisBackend(BaseBackend):
    """Redis-based storage backend.

    Parameters
    ----------
    url:
        Redis connection URL, e.g. ``"redis://localhost:6379/0"``.
        Falls back to ``FLASHQ_REDIS_URL`` env var.
    serializer:
        Serializer for task data. Defaults to JSON.
    """

    def __init__(
        self,
        url: str | None = None,
        *,
        serializer: Serializer | None = None,
    ) -> None:
        self.url = url or os.environ.get("FLASHQ_REDIS_URL", "redis://localhost:6379/0")
        self._serializer = serializer or default_serializer
        self._client = None
        self._dequeue_script = None

    def _redis(self):
        if self._client is None:
            raise RuntimeError("Backend not initialized. Call setup() first.")
        return self._client

    # -- lifecycle --

    def setup(self) -> None:
        try:
            import redis
        except ImportError:
            raise ImportError(
                "Redis backend requires redis-py. Install with: pip install flashq[redis]"
            ) from None

        self._client = redis.Redis.from_url(self.url, decode_responses=False)
        self._client.ping()
        self._dequeue_script = self._client.register_script(_DEQUEUE_LUA)
        logger.info("Redis backend connected: %s", self.url.split("@")[-1])

    def teardown(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None

    # -- queue ops --

    def enqueue(self, message: TaskMessage) -> None:
        data = self._serializer.dumps(message.to_dict())
        r = self._redis()
        pipe = r.pipeline()

        # Store task data as hash
        task_key = f"{_TASK_KEY}{message.id}"
        pipe.hset(task_key, mapping={
            "data": data,
            "state": message.state.value,
            "eta": str(message.eta.timestamp()) if message.eta else "",
        })

        # Add to priority sorted set (score = priority, higher = first)
        queue_key = f"{_QUEUE_KEY}{message.queue}"
        pipe.zadd(queue_key, {message.id: message.priority})

        pipe.execute()
        logger.debug("Enqueued %s on %r (Redis)", message.id, message.queue)

    def dequeue(self, queue: str = "default") -> TaskMessage | None:
        self._redis()
        queue_key = f"{_QUEUE_KEY}{queue}"
        now = datetime.datetime.now(datetime.timezone.utc).timestamp()

        result = self._dequeue_script(
            keys=[queue_key],
            args=[_TASK_KEY, now],
        )
        if result is None:
            return None

        task_id, data = result
        msg = TaskMessage.from_dict(self._serializer.loads(bytes(data)))
        logger.debug("Dequeued %s (Redis)", task_id.decode() if isinstance(task_id, bytes) else task_id)
        return msg

    def queue_size(self, queue: str = "default") -> int:
        return self._redis().zcard(f"{_QUEUE_KEY}{queue}")

    def flush_queue(self, queue: str = "default") -> int:
        r = self._redis()
        queue_key = f"{_QUEUE_KEY}{queue}"
        count = r.zcard(queue_key)
        # Get all task IDs before deleting
        task_ids = r.zrange(queue_key, 0, -1)
        pipe = r.pipeline()
        pipe.delete(queue_key)
        for tid in task_ids:
            pipe.delete(f"{_TASK_KEY}{tid.decode() if isinstance(tid, bytes) else tid}")
        pipe.execute()
        return count

    # -- task state --

    def get_task(self, task_id: str) -> TaskMessage | None:
        r = self._redis()
        task_key = f"{_TASK_KEY}{task_id}"
        raw = r.hgetall(task_key)
        if not raw:
            return None
        data = self._serializer.loads(bytes(raw[b"data"]))
        state_val = raw.get(b"state", b"pending").decode()
        data["state"] = state_val
        return TaskMessage.from_dict(data)

    def update_task_state(self, task_id: str, state: TaskState) -> None:
        self._redis().hset(f"{_TASK_KEY}{task_id}", "state", state.value)

    def get_tasks_by_state(
        self, state: TaskState, queue: str = "default", limit: int = 100
    ) -> Sequence[TaskMessage]:
        # Redis doesn't have native state indexing like SQL, so we scan
        # This is fine for dashboards but not for hot paths
        r = self._redis()
        results = []
        cursor = 0
        pattern = f"{_TASK_KEY}*"
        while len(results) < limit:
            cursor, keys = r.scan(cursor, match=pattern, count=100)
            for key in keys:
                raw = r.hgetall(key)
                if raw.get(b"state", b"").decode() == state.value:
                    data = self._serializer.loads(bytes(raw[b"data"]))
                    data["state"] = state.value
                    results.append(TaskMessage.from_dict(data))
                    if len(results) >= limit:
                        break
            if cursor == 0:
                break
        return results

    # -- results --

    def store_result(self, result: TaskResult[Any]) -> None:
        result_data = self._serializer.dumps(result.result) if result.result is not None else b""
        r = self._redis()
        key = f"{_RESULT_KEY}{result.task_id}"
        r.hset(key, mapping={
            "state": result.state.value,
            "result": result_data,
            "error": result.error or "",
            "traceback": result.traceback or "",
            "started_at": result.started_at.isoformat() if result.started_at else "",
            "completed_at": result.completed_at.isoformat() if result.completed_at else "",
            "runtime_ms": str(result.runtime_ms or 0),
        })
        r.expire(key, 3600)  # 1 hour TTL

    def get_result(self, task_id: str) -> TaskResult[Any] | None:
        r = self._redis()
        raw = r.hgetall(f"{_RESULT_KEY}{task_id}")
        if not raw:
            return None

        result_bytes = raw.get(b"result", b"")
        result_val = self._serializer.loads(result_bytes) if result_bytes else None

        started = raw.get(b"started_at", b"").decode()
        completed = raw.get(b"completed_at", b"").decode()

        return TaskResult(
            task_id=task_id,
            state=TaskState(raw[b"state"].decode()),
            result=result_val,
            error=raw.get(b"error", b"").decode() or None,
            traceback=raw.get(b"traceback", b"").decode() or None,
            started_at=datetime.datetime.fromisoformat(started) if started else None,
            completed_at=datetime.datetime.fromisoformat(completed) if completed else None,
            runtime_ms=float(raw.get(b"runtime_ms", b"0").decode()) or None,
        )

    def delete_result(self, task_id: str) -> bool:
        return self._redis().delete(f"{_RESULT_KEY}{task_id}") > 0

    # -- scheduling --

    def add_to_schedule(self, message: TaskMessage) -> None:
        if message.eta is None:
            raise ValueError("Cannot schedule a task without an ETA")
        data = self._serializer.dumps(message.to_dict())
        r = self._redis()
        # Store task data
        r.hset(f"{_TASK_KEY}{message.id}", mapping={
            "data": data,
            "state": "pending",
            "eta": str(message.eta.timestamp()),
        })
        # Add to schedule sorted set (score = eta timestamp)
        r.zadd(_SCHEDULE_KEY, {message.id: message.eta.timestamp()})

    def read_schedule(self, now: float) -> Sequence[TaskMessage]:
        r = self._redis()
        # Get all due tasks
        due_ids = r.zrangebyscore(_SCHEDULE_KEY, "-inf", now)
        if not due_ids:
            return []

        results = []
        pipe = r.pipeline()
        for tid in due_ids:
            pipe.hget(f"{_TASK_KEY}{tid.decode() if isinstance(tid, bytes) else tid}", "data")
        raw_list = pipe.execute()

        # Remove from schedule
        r.zremrangebyscore(_SCHEDULE_KEY, "-inf", now)

        for raw in raw_list:
            if raw:
                msg = TaskMessage.from_dict(self._serializer.loads(bytes(raw)))
                results.append(msg)

        return results

    def schedule_size(self) -> int:
        return self._redis().zcard(_SCHEDULE_KEY)
