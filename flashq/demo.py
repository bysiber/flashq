"""FlashQ Live Demo — run with `python -m flashq.demo`

This self-contained script demonstrates FlashQ in action:
1. Creates a task queue
2. Registers tasks
3. Enqueues work
4. Starts a worker
5. Shows results in real-time

No configuration needed. Just run it!
"""

from __future__ import annotations

import os
import sys
import threading
import time


def main() -> None:
    print()
    print("  ╔══════════════════════════════════════════════╗")
    print("  ║         ⚡ FlashQ Live Demo                  ║")
    print("  ║   The task queue that just works™            ║")
    print("  ╚══════════════════════════════════════════════╝")
    print()

    # ── Step 1: Create app ──
    print("  📦 Step 1: Create FlashQ app (SQLite, zero config)")
    print("  ─────────────────────────────────────────────────")
    print("  │  from flashq import FlashQ")
    print("  │  app = FlashQ()")
    print()

    import tempfile

    from flashq import FlashQ
    from flashq.backends.sqlite import SQLiteBackend

    tmp = tempfile.mkdtemp(prefix="flashq_demo_")
    db_path = os.path.join(tmp, "demo.db")
    backend = SQLiteBackend(path=db_path)
    backend.setup()
    app = FlashQ(backend=backend, name="demo")

    print("  ✅ App created with in-memory SQLite backend\n")

    # ── Step 2: Define tasks ──
    print("  📝 Step 2: Define tasks")
    print("  ─────────────────────────────────────────────────")
    print("  │  @app.task()")
    print("  │  def add(x: int, y: int) -> int:")
    print("  │      return x + y")
    print("  │")
    print("  │  @app.task(max_retries=2, retry_delay=0.5)")
    print("  │  def risky_task(n: int) -> str:")
    print("  │      if random.random() < 0.5:")
    print("  │          raise ValueError('Bad luck!')")
    print("  │      return f'Success #{n}'")
    print()

    import random

    @app.task()
    def add(x: int, y: int) -> int:
        return x + y

    @app.task()
    def multiply(x: int, y: int) -> int:
        return x * y

    @app.task()
    def greet(name: str) -> str:
        time.sleep(0.1)  # Simulate work
        return f"Hello, {name}! 👋"

    @app.task(max_retries=2, retry_delay=0.2)
    def risky_task(n: int) -> str:
        if random.random() < 0.3:
            raise ValueError("Bad luck!")
        return f"Success #{n}"

    print(f"  ✅ {len(app.registry)} tasks registered: {', '.join(app.registry.keys())}\n")

    # ── Step 3: Enqueue tasks ──
    print("  📤 Step 3: Enqueue 20 tasks")
    print("  ─────────────────────────────────────────────────")

    handles = []
    for i in range(5):
        h = add.delay(i, i * 10)
        handles.append(("add", h, i, i * 10))
        print(f"  │  add.delay({i}, {i*10})  →  task_id: {h.id[:8]}…")

    for i in range(5):
        h = multiply.delay(i + 1, 7)
        handles.append(("multiply", h, i + 1, 7))

    names = ["Alice", "Bob", "Charlie", "Diana", "Eve"]
    for name in names:
        h = greet.delay(name)
        handles.append(("greet", h, name, None))

    for i in range(5):
        h = risky_task.delay(i)
        handles.append(("risky_task", h, i, None))

    print("  │  ... and 15 more tasks")
    print(f"\n  ✅ {len(handles)} tasks enqueued in queue 'default'\n")

    # ── Step 4: Start worker ──
    print("  👷 Step 4: Start worker (4 concurrent threads)")
    print("  ─────────────────────────────────────────────────")

    from flashq.worker import Worker

    worker = Worker(app, poll_interval=0.05, concurrency=4, schedule_interval=0.2)
    worker_thread = threading.Thread(target=worker.start, daemon=True)
    worker_thread.start()

    print("  │  Worker started! Processing tasks...\n")

    # ── Step 5: Watch results ──
    print("  📊 Step 5: Watching results in real-time")
    print("  ─────────────────────────────────────────────────")

    from flashq import TaskState

    start_time = time.monotonic()
    completed = set()
    success_count = 0
    fail_count = 0

    for _ in range(50):  # Poll for up to 5 seconds
        for task_name, handle, arg1, arg2 in handles:
            if handle.id in completed:
                continue
            result = backend.get_result(handle.id)
            if result and result.state in (TaskState.SUCCESS, TaskState.FAILURE, TaskState.DEAD):
                completed.add(handle.id)
                if result.state == TaskState.SUCCESS:
                    success_count += 1
                    if task_name == "add":
                        print(f"  │  ✅ add({arg1}, {arg2}) = {result.result}")
                    elif task_name == "multiply":
                        print(f"  │  ✅ multiply({arg1}, {arg2}) = {result.result}")
                    elif task_name == "greet":
                        print(f"  │  ✅ greet(\"{arg1}\") = \"{result.result}\"")
                    elif task_name == "risky_task":
                        print(f"  │  ✅ risky_task({arg1}) = \"{result.result}\"")
                else:
                    fail_count += 1
                    print(f"  │  ❌ {task_name}({arg1}) failed: {result.error}")

        if len(completed) == len(handles):
            break
        time.sleep(0.1)

    elapsed = time.monotonic() - start_time
    worker.stop()
    worker_thread.join(timeout=5)

    # ── Summary ──
    print()
    print("  ═══════════════════════════════════════════════")
    print("  📈 Results")
    print("  ═══════════════════════════════════════════════")
    print(f"  │  Total tasks:     {len(handles)}")
    print(f"  │  ✅ Succeeded:     {success_count}")
    print(f"  │  ❌ Failed:        {fail_count}")
    print(f"  │  ⏱  Time:          {elapsed:.2f}s")
    print(f"  │  🚀 Throughput:    {len(completed) / elapsed:.0f} tasks/s")
    print("  ═══════════════════════════════════════════════")
    print()

    # ── Quick feature showcase ──
    print("  🔥 More features (try them yourself!):")
    print("  ─────────────────────────────────────────────────")
    print("  │")
    print("  │  # Canvas — chain tasks sequentially:")
    print("  │  from flashq import chain")
    print("  │  pipe = chain(add.s(1, 2), multiply.s(10))")
    print("  │  pipe.delay(app)")
    print("  │")
    print("  │  # Group — parallel execution:")
    print("  │  from flashq import group")
    print("  │  batch = group(greet.s('A'), greet.s('B'), greet.s('C'))")
    print("  │  batch.delay(app)")
    print("  │")
    print("  │  # Rate limiting:")
    print("  │  from flashq.ratelimit import RateLimiter")
    print("  │  limiter = RateLimiter(default_rate='100/m')")
    print("  │  app.add_middleware(limiter)")
    print("  │")
    print("  │  # Dead Letter Queue:")
    print("  │  from flashq.dlq import DeadLetterQueue")
    print("  │  dlq = DeadLetterQueue(app)")
    print("  │  app.add_middleware(dlq.middleware())")
    print("  │")
    print("  │  # Web Dashboard:")
    print("  │  flashq dashboard myapp:app --port 5555")
    print("  │")
    print("  └─────────────────────────────────────────────────")
    print()
    print("  📚 Full docs: https://github.com/bysiber/flashq")
    print("  📦 Install:   pip install flashq")
    print()

    backend.teardown()
    sys.exit(0)


if __name__ == "__main__":
    main()
