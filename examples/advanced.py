"""Advanced FlashQ example — middleware, rate limiting, DLQ, canvas."""

import time

from flashq import FlashQ, Middleware, chain, chord, group
from flashq.dlq import DeadLetterQueue
from flashq.middleware import LoggingMiddleware
from flashq.ratelimit import RateLimiter
from flashq.scheduler import Scheduler, every

app = FlashQ(name="advanced-example")

# --- Middleware ---


class TimingMiddleware(Middleware):
    """Track task execution time."""

    def before_execute(self, message):
        message._start = time.monotonic()
        return message

    def after_execute(self, message, result):
        elapsed = time.monotonic() - getattr(message, "_start", time.monotonic())
        print(f"  ⏱  {message.task_name} took {elapsed:.3f}s")


# --- Setup ---

app.add_middleware(LoggingMiddleware())
app.add_middleware(TimingMiddleware())

# Rate limiting: max 10 emails per minute
limiter = RateLimiter(default_rate="100/m")
limiter.configure("send_email", rate="10/m")
app.add_middleware(limiter)

# Dead letter queue
dlq = DeadLetterQueue(app)
app.add_middleware(dlq.middleware())

# --- Tasks ---


@app.task(name="add")
def add(a: int, b: int) -> int:
    return a + b


@app.task(name="multiply")
def multiply(a: int, b: int) -> int:
    return a * b


@app.task(name="aggregate")
def aggregate(results: list) -> dict:
    return {"count": len(results), "sum": sum(results)}


@app.task(name="send_email", max_retries=3, retry_delay=5.0)
def send_email(to: str, subject: str) -> dict:
    print(f"  📧 Sending to {to}: {subject}")
    return {"sent": True}


@app.task(name="cleanup", max_retries=0)
def cleanup() -> str:
    print("  🧹 Running cleanup...")
    return "cleaned"


if __name__ == "__main__":
    print("=== Canvas: Chain ===")
    pipe = chain(add.s(2, 3), multiply.s(10))
    handle = pipe.delay(app)
    print(f"Chain dispatched: {handle.chain_id[:8]}")

    print("\n=== Canvas: Group ===")
    batch = group(add.s(1, 2), add.s(3, 4), add.s(5, 6))
    ghandle = batch.delay(app)
    print(f"Group dispatched: {len(ghandle.task_ids)} tasks")

    print("\n=== Canvas: Chord ===")
    workflow = chord(
        group(add.s(10, 20), add.s(30, 40)),
        aggregate.s(),
    )
    chandle = workflow.delay(app)
    print(f"Chord dispatched: {chandle.chord_id[:8]}")

    print("\n=== Periodic Scheduler ===")
    scheduler = Scheduler(app, check_interval=5.0)
    scheduler.add("cleanup", every(minutes=30))
    print("Scheduler configured: cleanup every 30 min")

    print("\n=== Rate Limiter Stats ===")
    stats = limiter.get_stats()
    for name, s in stats.items():
        print(f"  {name}: {s['max_tokens']}/{s['period']}s")

    print("\nRun the worker:")
    print("  flashq worker examples.advanced:app")
