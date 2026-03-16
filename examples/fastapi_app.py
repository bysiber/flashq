"""FastAPI integration example."""

# pip install fastapi uvicorn flashq

from flashq import FlashQ

app = FlashQ(name="fastapi-demo")


@app.task(name="process_order")
def process_order(order_id: str, items: list) -> dict:
    """Simulate order processing."""
    import time
    time.sleep(2)  # Simulate work
    return {"order_id": order_id, "status": "processed", "items": len(items)}


@app.task(name="send_notification")
def send_notification(user_id: str, message: str) -> bool:
    """Send a push notification."""
    print(f"Notification to {user_id}: {message}")
    return True


# --- FastAPI app ---

try:
    from fastapi import FastAPI

    api = FastAPI(title="FlashQ + FastAPI Demo")

    @api.post("/orders/{order_id}")
    async def create_order(order_id: str, items: list[str]):
        """Create an order and process it in the background."""
        handle = process_order.delay(order_id=order_id, items=items)
        send_notification.delay(
            user_id="admin",
            message=f"New order {order_id} received",
        )
        return {"order_id": order_id, "task_id": handle.id, "status": "queued"}

    @api.get("/tasks/{task_id}")
    async def get_task_status(task_id: str):
        """Check task status."""
        result = app.backend.get_result(task_id)
        if result is None:
            task = app.backend.get_task(task_id)
            return {"task_id": task_id, "state": task.state.value if task else "unknown"}
        return {
            "task_id": task_id,
            "state": result.state.value,
            "result": result.result,
        }

except ImportError:
    print("Install FastAPI to run this example: pip install fastapi uvicorn")
    print("Then run:")
    print("  uvicorn examples.fastapi_app:api --reload")
    print("  flashq worker examples.fastapi_app:app")
