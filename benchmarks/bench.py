#!/usr/bin/env python3
"""FlashQ Benchmark Suite.

Measures throughput, latency, and resource usage for different workloads.
Run with: python benchmarks/bench.py

Results are saved to benchmarks/results.json and printed as a table.
"""

from __future__ import annotations

import json
import os
import statistics
import sys
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from flashq import FlashQ, TaskState
from flashq.backends.sqlite import SQLiteBackend
from flashq.worker import Worker


@dataclass
class BenchResult:
    """Result of a single benchmark run."""

    name: str
    tasks_total: int
    duration_s: float
    throughput_tasks_per_sec: float
    avg_latency_ms: float
    p50_latency_ms: float
    p95_latency_ms: float
    p99_latency_ms: float
    errors: int = 0
    extra: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "tasks_total": self.tasks_total,
            "duration_s": round(self.duration_s, 3),
            "throughput_tasks_per_sec": round(self.throughput_tasks_per_sec, 1),
            "avg_latency_ms": round(self.avg_latency_ms, 2),
            "p50_latency_ms": round(self.p50_latency_ms, 2),
            "p95_latency_ms": round(self.p95_latency_ms, 2),
            "p99_latency_ms": round(self.p99_latency_ms, 2),
            "errors": self.errors,
            **self.extra,
        }


def percentile(data: list[float], pct: float) -> float:
    """Calculate the given percentile of a sorted list."""
    if not data:
        return 0.0
    sorted_data = sorted(data)
    k = (len(sorted_data) - 1) * pct / 100
    f = int(k)
    c = f + 1
    if c >= len(sorted_data):
        return sorted_data[f]
    return sorted_data[f] + (k - f) * (sorted_data[c] - sorted_data[f])


def run_worker_thread(app: FlashQ, poll_interval: float = 0.05, concurrency: int = 4) -> tuple[Worker, threading.Thread]:
    """Start a worker in a background thread."""
    worker = Worker(app, poll_interval=poll_interval, concurrency=concurrency)
    t = threading.Thread(target=worker.start, daemon=True)
    t.start()
    return worker, t


def wait_for_completion(backend, task_ids: list[str], timeout: float = 60.0) -> list[float]:
    """Wait for all tasks to complete and return latencies."""
    latencies = []
    start = time.monotonic()
    remaining = set(task_ids)

    while remaining and (time.monotonic() - start) < timeout:
        for tid in list(remaining):
            result = backend.get_result(tid)
            if result and result.state in (TaskState.SUCCESS, TaskState.FAILURE):
                remaining.discard(tid)
                if result.runtime_ms is not None:
                    latencies.append(result.runtime_ms)
        if remaining:
            time.sleep(0.01)

    return latencies


# ── Benchmarks ──


def bench_enqueue_throughput(n: int = 10000) -> BenchResult:
    """Measure raw enqueue speed (no worker)."""
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        backend = SQLiteBackend(path=os.path.join(tmp, "bench.db"))
        backend.setup()
        app = FlashQ(backend=backend, name="bench-enqueue")

        @app.task(name="noop")
        def noop(x: int) -> int:
            return x

        start = time.monotonic()
        for i in range(n):
            noop.delay(i)
        elapsed = time.monotonic() - start

        backend.teardown()

    return BenchResult(
        name=f"enqueue_{n}",
        tasks_total=n,
        duration_s=elapsed,
        throughput_tasks_per_sec=n / elapsed,
        avg_latency_ms=elapsed / n * 1000,
        p50_latency_ms=elapsed / n * 1000,
        p95_latency_ms=elapsed / n * 1000,
        p99_latency_ms=elapsed / n * 1000,
    )


def bench_roundtrip(n: int = 1000, concurrency: int = 4) -> BenchResult:
    """Measure full enqueue → execute → result roundtrip."""
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        backend = SQLiteBackend(path=os.path.join(tmp, "bench.db"))
        backend.setup()
        app = FlashQ(backend=backend, name="bench-roundtrip")

        @app.task(name="add")
        def add(x: int, y: int) -> int:
            return x + y

        # Enqueue all tasks
        task_ids = []
        enqueue_start = time.monotonic()
        for i in range(n):
            handle = add.delay(i, i + 1)
            task_ids.append(handle.id)
        enqueue_time = time.monotonic() - enqueue_start

        # Start worker and wait for completion
        worker, thread = run_worker_thread(app, concurrency=concurrency)
        exec_start = time.monotonic()
        latencies = wait_for_completion(backend, task_ids, timeout=120.0)
        exec_time = time.monotonic() - exec_start
        total_time = enqueue_time + exec_time

        worker.stop()
        thread.join(timeout=10)
        backend.teardown()

    if not latencies:
        latencies = [0.0]

    return BenchResult(
        name=f"roundtrip_{n}_c{concurrency}",
        tasks_total=n,
        duration_s=total_time,
        throughput_tasks_per_sec=n / total_time,
        avg_latency_ms=statistics.mean(latencies),
        p50_latency_ms=percentile(latencies, 50),
        p95_latency_ms=percentile(latencies, 95),
        p99_latency_ms=percentile(latencies, 99),
        errors=n - len(latencies),
        extra={"enqueue_time_s": round(enqueue_time, 3), "exec_time_s": round(exec_time, 3)},
    )


def bench_cpu_bound(n: int = 500, concurrency: int = 4) -> BenchResult:
    """Benchmark CPU-intensive tasks (fibonacci)."""
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        backend = SQLiteBackend(path=os.path.join(tmp, "bench.db"))
        backend.setup()
        app = FlashQ(backend=backend, name="bench-cpu")

        @app.task(name="fib")
        def fib(x: int) -> int:
            if x <= 1:
                return x
            a, b = 0, 1
            for _ in range(x - 1):
                a, b = b, a + b
            return b

        task_ids = []
        start = time.monotonic()
        for i in range(n):
            handle = fib.delay(100 + (i % 50))
            task_ids.append(handle.id)

        worker, thread = run_worker_thread(app, concurrency=concurrency)
        latencies = wait_for_completion(backend, task_ids, timeout=120.0)
        elapsed = time.monotonic() - start

        worker.stop()
        thread.join(timeout=10)
        backend.teardown()

    if not latencies:
        latencies = [0.0]

    return BenchResult(
        name=f"cpu_bound_{n}_c{concurrency}",
        tasks_total=n,
        duration_s=elapsed,
        throughput_tasks_per_sec=n / elapsed,
        avg_latency_ms=statistics.mean(latencies),
        p50_latency_ms=percentile(latencies, 50),
        p95_latency_ms=percentile(latencies, 95),
        p99_latency_ms=percentile(latencies, 99),
        errors=n - len(latencies),
    )


def bench_io_bound(n: int = 500, concurrency: int = 8) -> BenchResult:
    """Benchmark I/O-simulating tasks (sleep)."""
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        backend = SQLiteBackend(path=os.path.join(tmp, "bench.db"))
        backend.setup()
        app = FlashQ(backend=backend, name="bench-io")

        @app.task(name="io_task")
        def io_task(ms: int) -> int:
            time.sleep(ms / 1000.0)
            return ms

        task_ids = []
        start = time.monotonic()
        for i in range(n):
            handle = io_task.delay(10)  # 10ms sleep
            task_ids.append(handle.id)

        worker, thread = run_worker_thread(app, concurrency=concurrency)
        latencies = wait_for_completion(backend, task_ids, timeout=120.0)
        elapsed = time.monotonic() - start

        worker.stop()
        thread.join(timeout=10)
        backend.teardown()

    if not latencies:
        latencies = [0.0]

    return BenchResult(
        name=f"io_bound_{n}_c{concurrency}",
        tasks_total=n,
        duration_s=elapsed,
        throughput_tasks_per_sec=n / elapsed,
        avg_latency_ms=statistics.mean(latencies),
        p50_latency_ms=percentile(latencies, 50),
        p95_latency_ms=percentile(latencies, 95),
        p99_latency_ms=percentile(latencies, 99),
        errors=n - len(latencies),
    )


def bench_concurrent_scaling() -> list[BenchResult]:
    """Test throughput at different concurrency levels."""
    results = []
    for c in [1, 2, 4, 8]:
        result = bench_roundtrip(n=500, concurrency=c)
        result.name = f"scaling_c{c}"
        results.append(result)
    return results


def bench_priority_ordering(n: int = 100) -> BenchResult:
    """Test that high-priority tasks are processed first."""
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        backend = SQLiteBackend(path=os.path.join(tmp, "bench.db"))
        backend.setup()
        app = FlashQ(backend=backend, name="bench-priority")

        execution_order = []
        lock = threading.Lock()

        @app.task(name="ordered")
        def ordered(priority: int) -> int:
            with lock:
                execution_order.append(priority)
            return priority

        # Enqueue low priority first, then high
        from flashq.models import TaskMessage

        for i in range(n):
            msg = TaskMessage(task_name="ordered", args=(i % 3,), priority=i % 3)
            backend.enqueue(msg)

        start = time.monotonic()
        worker, thread = run_worker_thread(app, concurrency=1)  # Single thread to preserve order
        time.sleep(5)
        elapsed = time.monotonic() - start

        worker.stop()
        thread.join(timeout=10)
        backend.teardown()

    return BenchResult(
        name=f"priority_{n}",
        tasks_total=n,
        duration_s=elapsed,
        throughput_tasks_per_sec=len(execution_order) / elapsed,
        avg_latency_ms=0,
        p50_latency_ms=0,
        p95_latency_ms=0,
        p99_latency_ms=0,
        extra={"processed": len(execution_order)},
    )


# ── Runner ──


def print_table(results: list[BenchResult]) -> None:
    """Print results as a formatted table."""
    header = f"{'Benchmark':<30} {'Tasks':>6} {'Time(s)':>8} {'Tasks/s':>9} {'Avg(ms)':>9} {'P50(ms)':>9} {'P95(ms)':>9} {'P99(ms)':>9}"
    print()
    print("=" * len(header))
    print("⚡ FlashQ Benchmark Results")
    print("=" * len(header))
    print(header)
    print("-" * len(header))

    for r in results:
        print(
            f"{r.name:<30} {r.tasks_total:>6} {r.duration_s:>8.3f} "
            f"{r.throughput_tasks_per_sec:>9.1f} {r.avg_latency_ms:>9.2f} "
            f"{r.p50_latency_ms:>9.2f} {r.p95_latency_ms:>9.2f} {r.p99_latency_ms:>9.2f}"
        )

    print("=" * len(header))
    print()


def main() -> None:
    print("⚡ FlashQ Benchmark Suite")
    print("=" * 40)

    results: list[BenchResult] = []

    benchmarks = [
        ("Enqueue throughput (10K)", lambda: bench_enqueue_throughput(10000)),
        ("Roundtrip 1K tasks (c=4)", lambda: bench_roundtrip(1000, 4)),
        ("CPU-bound 500 tasks (c=4)", lambda: bench_cpu_bound(500, 4)),
        ("I/O-bound 500 tasks (c=8)", lambda: bench_io_bound(500, 8)),
        ("Priority ordering (100)", lambda: bench_priority_ordering(100)),
    ]

    for name, fn in benchmarks:
        print(f"\n▸ Running: {name}...", end=" ", flush=True)
        try:
            result = fn()
            if isinstance(result, list):
                results.extend(result)
            else:
                results.append(result)
            print(f"✓ {result.throughput_tasks_per_sec:.0f} tasks/s" if not isinstance(result, list) else "✓")
        except Exception as e:
            print(f"✗ {e}")

    # Scaling test
    print("\n▸ Running: Concurrency scaling (c=1,2,4,8)...", flush=True)
    try:
        scaling = bench_concurrent_scaling()
        results.extend(scaling)
        for r in scaling:
            print(f"  c={r.name.split('c')[1]}: {r.throughput_tasks_per_sec:.0f} tasks/s")
    except Exception as e:
        print(f"  ✗ {e}")

    print_table(results)

    # Save to JSON
    output_dir = Path(__file__).parent
    output_file = output_dir / "results.json"
    with open(output_file, "w") as f:
        json.dump(
            {
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
                "python_version": sys.version,
                "results": [r.to_dict() for r in results],
            },
            f,
            indent=2,
        )
    print(f"Results saved to {output_file}")


if __name__ == "__main__":
    main()
