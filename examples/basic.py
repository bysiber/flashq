"""
FlashQ — Minimal example.

Run this to see FlashQ in action:

    # Terminal 1: Start the worker
    flashq worker examples.basic:app

    # Terminal 2: Enqueue tasks
    python examples/basic.py
"""

from flashq import FlashQ

# Create the app — zero config! Uses SQLite by default.
app = FlashQ(name="demo")


@app.task()
def add(x: int, y: int) -> int:
    """Add two numbers."""
    return x + y


@app.task(queue="emails", max_retries=5, retry_delay=30.0)
def send_email(to: str, subject: str, body: str) -> dict:
    """Simulate sending an email."""
    print(f"📧 Sending email to {to}: {subject}")
    # In a real app, you'd call an SMTP library here
    return {"status": "sent", "to": to}


@app.task(priority=20)  # CRITICAL priority
def process_payment(order_id: str, amount: float) -> bool:
    """Process a payment — high priority task."""
    print(f"💳 Processing payment for order {order_id}: ${amount:.2f}")
    return True


if __name__ == "__main__":
    # Enqueue some tasks
    print("⚡ Enqueuing tasks...\n")

    # Simple task
    handle1 = add.delay(2, 3)
    print(f"  → add(2, 3) enqueued: {handle1.id}")

    # Task with keyword arguments
    handle2 = send_email.delay(
        to="user@example.com",
        subject="Welcome!",
        body="Thanks for signing up.",
    )
    print(f"  → send_email enqueued: {handle2.id}")

    # High-priority task
    handle3 = process_payment.delay(order_id="ORD-001", amount=99.99)
    print(f"  → process_payment enqueued: {handle3.id}")

    # Delayed task (execute in 30 seconds)
    handle4 = add.apply(args=(10, 20), countdown=30)
    print(f"  → add(10, 20) scheduled for 30s later: {handle4.id}")

    # Check queue stats
    print(f"\n📊 Queue 'default': {app.backend.queue_size('default')} pending")
    print(f"📊 Queue 'emails': {app.backend.queue_size('emails')} pending")
    print(f"📊 Scheduled: {app.backend.schedule_size()} task(s)")

    print("\n✅ Done! Start the worker to process these tasks:")
    print("   flashq worker examples.basic:app")
