"""Basic FlashQ example — task definition and execution."""

from flashq import FlashQ

app = FlashQ(name="basic-example")


@app.task()
def add(x: int, y: int) -> int:
    """Add two numbers."""
    print(f"  Computing {x} + {y}")
    return x + y


@app.task(queue="emails", max_retries=3, retry_delay=10.0)
def send_email(to: str, subject: str) -> dict:
    """Simulate sending an email."""
    print(f"  Sending email to {to}: {subject}")
    return {"status": "sent", "to": to}


@app.task(timeout=30.0)
async def fetch_data(url: str) -> str:
    """Async task that fetches data."""
    import asyncio

    print(f"  Fetching {url}...")
    await asyncio.sleep(0.1)
    return f"Data from {url}"


if __name__ == "__main__":
    # Enqueue some tasks
    h1 = add.delay(2, 3)
    h2 = send_email.delay(to="alice@example.com", subject="Hello!")
    h3 = fetch_data.delay("https://api.example.com/data")

    print(f"Enqueued: add → {h1.id[:8]}")
    print(f"Enqueued: send_email → {h2.id[:8]}")
    print(f"Enqueued: fetch_data → {h3.id[:8]}")
    print()
    print("Run the worker to process these tasks:")
    print("  flashq worker examples.basic:app")
