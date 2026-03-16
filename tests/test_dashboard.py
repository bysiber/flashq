"""Tests for FlashQ Dashboard."""

from __future__ import annotations

import pytest

from flashq import FlashQ, TaskState
from flashq.backends.sqlite import SQLiteBackend
from flashq.dashboard import create_dashboard
from flashq.models import TaskMessage, TaskResult

httpx = pytest.importorskip("httpx")


@pytest.fixture
def backend(tmp_path):
    b = SQLiteBackend(path=str(tmp_path / "dash_test.db"))
    b.setup()
    yield b
    b.teardown()


@pytest.fixture
def app(backend):
    fq = FlashQ(backend=backend, name="dash-test")

    @fq.task(name="add")
    def add(x: int, y: int) -> int:
        return x + y

    @fq.task(name="send_email")
    def send_email(to: str) -> dict:
        return {"sent": True}

    return fq


@pytest.fixture
def dashboard(app):
    return create_dashboard(app)


@pytest.fixture
def client(dashboard):
    from starlette.testclient import TestClient

    return TestClient(dashboard)


class TestDashboardPage:
    def test_serves_html(self, client):
        resp = client.get("/")
        assert resp.status_code == 200
        assert "FlashQ Dashboard" in resp.text
        assert "text/html" in resp.headers["content-type"]


class TestAPIStats:
    def test_stats_empty(self, client):
        resp = client.get("/api/stats")
        assert resp.status_code == 200
        data = resp.json()
        assert "queues" in data
        assert "states" in data
        assert "scheduled" in data
        assert "registered_tasks" in data
        assert "add" in data["registered_tasks"]
        assert "send_email" in data["registered_tasks"]

    def test_stats_with_tasks(self, client, backend):
        backend.enqueue(TaskMessage(task_name="add", args=(1, 2)))
        backend.enqueue(TaskMessage(task_name="add", args=(3, 4)))

        data = client.get("/api/stats").json()
        assert data["states"]["pending"] >= 2


class TestAPIQueues:
    def test_list_queues(self, client, backend):
        backend.enqueue(TaskMessage(task_name="add", args=(), queue="default"))
        backend.enqueue(TaskMessage(task_name="add", args=(), queue="emails"))

        data = client.get("/api/queues").json()
        assert "default" in data
        assert "emails" in data


class TestAPITasks:
    def test_list_pending(self, client, backend):
        msg = TaskMessage(task_name="add", args=(1, 2))
        backend.enqueue(msg)

        resp = client.get("/api/tasks?state=PENDING")
        assert resp.status_code == 200
        tasks = resp.json()
        assert len(tasks) >= 1
        assert tasks[0]["task_name"] == "add"

    def test_invalid_state(self, client):
        resp = client.get("/api/tasks?state=BOGUS")
        assert resp.status_code == 400

    def test_task_detail(self, client, backend):
        msg = TaskMessage(task_name="send_email", args=(), kwargs={"to": "a@b.com"})
        backend.enqueue(msg)

        resp = client.get(f"/api/tasks/{msg.id}")
        assert resp.status_code == 200
        data = resp.json()
        assert data["id"] == msg.id
        assert data["task_name"] == "send_email"
        assert data["kwargs"]["to"] == "a@b.com"

    def test_task_detail_with_result(self, client, backend):
        msg = TaskMessage(task_name="add", args=(1, 2))
        backend.enqueue(msg)

        import datetime

        result = TaskResult(
            task_id=msg.id,
            state=TaskState.SUCCESS,
            result=3,
            started_at=datetime.datetime.now(datetime.timezone.utc),
            completed_at=datetime.datetime.now(datetime.timezone.utc),
            runtime_ms=5.2,
        )
        backend.store_result(result)

        data = client.get(f"/api/tasks/{msg.id}").json()
        assert data["result"]["result"] == 3
        assert data["result"]["runtime_ms"] == 5.2

    def test_task_not_found(self, client):
        resp = client.get("/api/tasks/nonexistent")
        assert resp.status_code == 404


class TestAPIActions:
    def test_cancel_task(self, client, backend):
        msg = TaskMessage(task_name="add", args=(1, 2))
        backend.enqueue(msg)

        resp = client.post(f"/api/tasks/{msg.id}/action", json={"action": "cancel"})
        assert resp.status_code == 200
        assert resp.json()["action"] == "cancelled"

        task = backend.get_task(msg.id)
        assert task.state == TaskState.CANCELLED

    def test_revoke_task(self, client, backend):
        msg = TaskMessage(task_name="add", args=(1, 2))
        backend.enqueue(msg)

        resp = client.post(f"/api/tasks/{msg.id}/action", json={"action": "revoke"})
        assert resp.json()["action"] == "revoked"

    def test_unknown_action(self, client, backend):
        msg = TaskMessage(task_name="add", args=(1, 2))
        backend.enqueue(msg)

        resp = client.post(f"/api/tasks/{msg.id}/action", json={"action": "explode"})
        assert resp.status_code == 400

    def test_purge_queue(self, client, backend):
        for i in range(5):
            backend.enqueue(TaskMessage(task_name="add", args=(i,)))

        resp = client.post("/api/purge", json={"queue": "default"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["removed"] == 5
        assert backend.queue_size("default") == 0


class TestDashboardFactory:
    def test_create_with_prefix(self, app):
        dash = create_dashboard(app, prefix="/monitor")
        from starlette.testclient import TestClient

        client = TestClient(dash)
        resp = client.get("/monitor/")
        assert resp.status_code == 200
        assert "FlashQ Dashboard" in resp.text

        resp = client.get("/monitor/api/stats")
        assert resp.status_code == 200
