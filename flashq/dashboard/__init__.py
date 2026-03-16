"""FlashQ Dashboard — built-in web monitoring UI.

A zero-dependency web dashboard (only requires ``starlette`` + ``uvicorn``
as optional dependencies) that provides:

- Real-time task statistics
- Task list with filtering by state/queue
- Task detail view with result/error info
- Actions: cancel, revoke, purge queue, retry from DLQ
- Auto-refreshing stats

Usage::

    from flashq import FlashQ
    from flashq.dashboard import create_dashboard

    app = FlashQ()
    dashboard = create_dashboard(app)

    # Run with uvicorn
    import uvicorn
    uvicorn.run(dashboard, host="0.0.0.0", port=5555)

Or via CLI::

    flashq dashboard myapp:app --port 5555
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from flashq.app import FlashQ


def create_dashboard(app: FlashQ, prefix: str = "") -> Any:
    """Create a Starlette ASGI application for the FlashQ dashboard.

    Parameters
    ----------
    app:
        The FlashQ application instance.
    prefix:
        URL prefix for all dashboard routes (e.g., ``/flashq``).

    Returns
    -------
    A Starlette ``Router`` (ASGI-compatible) instance.
    """
    try:
        from starlette.applications import Starlette
        from starlette.requests import Request  # noqa: TC002
        from starlette.responses import HTMLResponse, JSONResponse
        from starlette.routing import Route
    except ImportError as exc:
        msg = (
            "Dashboard requires starlette. "
            "Install with: pip install 'flashq[dashboard]'"
        )
        raise ImportError(msg) from exc

    from flashq.enums import TaskState

    # ── API endpoints ──

    async def api_stats(request: Request) -> JSONResponse:
        """GET /api/stats — aggregate statistics."""
        stats = app.backend.get_stats()
        stats["registered_tasks"] = list(app._registry.keys())
        return JSONResponse(stats)

    async def api_tasks(request: Request) -> JSONResponse:
        """GET /api/tasks?state=PENDING&queue=default&limit=50."""
        state_str = request.query_params.get("state", "PENDING")
        queue = request.query_params.get("queue", "default")
        limit = int(request.query_params.get("limit", "50"))

        try:
            state = TaskState(state_str.lower())
        except ValueError:
            try:
                state = TaskState[state_str.upper()]
            except KeyError:
                return JSONResponse({"error": f"Invalid state: {state_str}"}, status_code=400)

        tasks = app.backend.get_tasks_by_state(state, queue=queue, limit=limit)
        return JSONResponse([_task_to_dict(t) for t in tasks])

    async def api_task_detail(request: Request) -> JSONResponse:
        """GET /api/tasks/{task_id}."""
        task_id = request.path_params["task_id"]
        task = app.backend.get_task(task_id)
        if task is None:
            return JSONResponse({"error": "Task not found"}, status_code=404)

        data = _task_to_dict(task)
        result = app.backend.get_result(task_id)
        if result:
            data["result"] = {
                "state": result.state.value,
                "result": _safe_serialize(result.result),
                "error": result.error,
                "traceback": result.traceback,
                "runtime_ms": result.runtime_ms,
                "started_at": result.started_at.isoformat() if result.started_at else None,
                "completed_at": result.completed_at.isoformat() if result.completed_at else None,
            }
        return JSONResponse(data)

    async def api_task_action(request: Request) -> JSONResponse:
        """POST /api/tasks/{task_id}/action — cancel, revoke."""
        task_id = request.path_params["task_id"]
        body = await request.json()
        action = body.get("action")

        if action == "cancel":
            app.backend.update_task_state(task_id, TaskState.CANCELLED)
            return JSONResponse({"ok": True, "action": "cancelled"})
        elif action == "revoke":
            app.backend.update_task_state(task_id, TaskState.REVOKED)
            return JSONResponse({"ok": True, "action": "revoked"})
        else:
            return JSONResponse({"error": f"Unknown action: {action}"}, status_code=400)

    async def api_purge(request: Request) -> JSONResponse:
        """POST /api/purge — purge a queue."""
        body = await request.json()
        queue = body.get("queue", "default")
        removed = app.backend.flush_queue(queue)
        return JSONResponse({"ok": True, "removed": removed, "queue": queue})

    async def api_queues(request: Request) -> JSONResponse:
        """GET /api/queues — list queue names and sizes."""
        names = app.backend.get_queue_names()
        queues = {name: app.backend.queue_size(name) for name in names}
        return JSONResponse(queues)

    async def dashboard_page(request: Request) -> HTMLResponse:
        """GET / — serve the dashboard HTML."""
        return HTMLResponse(_DASHBOARD_HTML)

    # ── Build routes ──

    routes = [
        Route(f"{prefix}/", dashboard_page),
        Route(f"{prefix}/api/stats", api_stats),
        Route(f"{prefix}/api/tasks", api_tasks),
        Route(f"{prefix}/api/tasks/{{task_id}}", api_task_detail),
        Route(f"{prefix}/api/tasks/{{task_id}}/action", api_task_action, methods=["POST"]),
        Route(f"{prefix}/api/purge", api_purge, methods=["POST"]),
        Route(f"{prefix}/api/queues", api_queues),
    ]

    return Starlette(routes=routes, debug=False)


def _task_to_dict(task: Any) -> dict[str, Any]:
    """Convert a TaskMessage to a JSON-safe dict."""
    return {
        "id": task.id,
        "task_name": task.task_name,
        "queue": task.queue,
        "state": task.state.value if hasattr(task.state, "value") else str(task.state),
        "priority": task.priority,
        "retries": task.retries,
        "max_retries": task.max_retries,
        "created_at": task.created_at.isoformat() if task.created_at else None,
        "eta": task.eta.isoformat() if task.eta else None,
        "timeout": task.timeout,
        "args": list(task.args) if task.args else [],
        "kwargs": task.kwargs or {},
    }


def _safe_serialize(obj: Any) -> Any:
    """Safely convert an object to JSON-serializable form."""
    try:
        json.dumps(obj)
        return obj
    except (TypeError, ValueError):
        return str(obj)


# ── Embedded Dashboard HTML ──

_DASHBOARD_HTML = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>⚡ FlashQ Dashboard</title>
<style>
:root {
  --bg: #0f1117;
  --surface: #1a1d27;
  --surface2: #242838;
  --border: #2e3348;
  --text: #e1e4ed;
  --text2: #8b8fa3;
  --accent: #6366f1;
  --accent2: #818cf8;
  --green: #22c55e;
  --yellow: #eab308;
  --red: #ef4444;
  --blue: #3b82f6;
  --orange: #f97316;
  --cyan: #06b6d4;
  --radius: 10px;
}
* { margin: 0; padding: 0; box-sizing: border-box; }
body {
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
  background: var(--bg); color: var(--text); line-height: 1.5;
  min-height: 100vh;
}
a { color: var(--accent2); text-decoration: none; }
a:hover { text-decoration: underline; }

/* Header */
.header {
  background: var(--surface);
  border-bottom: 1px solid var(--border);
  padding: 16px 32px;
  display: flex; align-items: center; gap: 16px;
}
.header h1 { font-size: 20px; font-weight: 700; }
.header h1 span { color: var(--accent2); }
.header .refresh-info { margin-left: auto; color: var(--text2); font-size: 13px; }

/* Layout */
.container { max-width: 1400px; margin: 0 auto; padding: 24px 32px; }

/* Stats cards */
.stats-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
  gap: 16px; margin-bottom: 24px;
}
.stat-card {
  background: var(--surface); border: 1px solid var(--border);
  border-radius: var(--radius); padding: 20px;
  transition: border-color 0.2s;
}
.stat-card:hover { border-color: var(--accent); }
.stat-card .label { font-size: 12px; text-transform: uppercase; color: var(--text2); letter-spacing: 0.05em; }
.stat-card .value { font-size: 32px; font-weight: 700; margin-top: 4px; }
.stat-card .value.pending { color: var(--yellow); }
.stat-card .value.running { color: var(--blue); }
.stat-card .value.success { color: var(--green); }
.stat-card .value.failure { color: var(--red); }
.stat-card .value.dead { color: var(--orange); }
.stat-card .value.scheduled { color: var(--cyan); }

/* Toolbar */
.toolbar {
  display: flex; gap: 12px; align-items: center;
  margin-bottom: 16px; flex-wrap: wrap;
}
.toolbar select, .toolbar button, .toolbar input {
  background: var(--surface); border: 1px solid var(--border);
  color: var(--text); padding: 8px 14px; border-radius: 6px;
  font-size: 14px; cursor: pointer;
}
.toolbar select:hover, .toolbar button:hover { border-color: var(--accent); }
.toolbar button.danger { border-color: var(--red); color: var(--red); }
.toolbar button.danger:hover { background: var(--red); color: white; }

/* Table */
.table-wrap {
  background: var(--surface); border: 1px solid var(--border);
  border-radius: var(--radius); overflow: hidden;
}
table { width: 100%; border-collapse: collapse; }
th {
  text-align: left; padding: 12px 16px; font-size: 12px;
  text-transform: uppercase; color: var(--text2); letter-spacing: 0.05em;
  background: var(--surface2); border-bottom: 1px solid var(--border);
}
td {
  padding: 10px 16px; border-bottom: 1px solid var(--border);
  font-size: 14px; white-space: nowrap;
}
tr:hover td { background: var(--surface2); }
tr:last-child td { border-bottom: none; }

/* State badges */
.badge {
  display: inline-block; padding: 2px 10px; border-radius: 20px;
  font-size: 11px; font-weight: 600; text-transform: uppercase;
}
.badge.PENDING { background: rgba(234,179,8,0.15); color: var(--yellow); }
.badge.RUNNING { background: rgba(59,130,246,0.15); color: var(--blue); }
.badge.SUCCESS { background: rgba(34,197,94,0.15); color: var(--green); }
.badge.FAILURE { background: rgba(239,68,68,0.15); color: var(--red); }
.badge.RETRYING { background: rgba(249,115,22,0.15); color: var(--orange); }
.badge.DEAD { background: rgba(239,68,68,0.2); color: var(--red); }
.badge.CANCELLED { background: rgba(139,143,163,0.15); color: var(--text2); }
.badge.REVOKED { background: rgba(139,143,163,0.15); color: var(--text2); }

/* Detail modal */
.modal-overlay {
  display: none; position: fixed; top: 0; left: 0; right: 0; bottom: 0;
  background: rgba(0,0,0,0.6); z-index: 100;
  justify-content: center; align-items: center;
}
.modal-overlay.active { display: flex; }
.modal {
  background: var(--surface); border: 1px solid var(--border);
  border-radius: var(--radius); padding: 24px; width: 680px;
  max-height: 80vh; overflow-y: auto; position: relative;
}
.modal h2 { font-size: 18px; margin-bottom: 16px; }
.modal .close { position: absolute; top: 12px; right: 16px; cursor: pointer; color: var(--text2); font-size: 24px; }
.modal .close:hover { color: var(--text); }
.modal .field { margin-bottom: 12px; }
.modal .field-label { font-size: 12px; color: var(--text2); text-transform: uppercase; }
.modal .field-value { font-size: 14px; margin-top: 2px; word-break: break-all; white-space: pre-wrap; }
.modal .field-value.mono { font-family: 'SF Mono', 'Fira Code', monospace; font-size: 13px; background: var(--surface2); padding: 8px 12px; border-radius: 6px; }
.modal .actions { display: flex; gap: 8px; margin-top: 20px; }
.modal .actions button {
  background: var(--surface2); border: 1px solid var(--border);
  color: var(--text); padding: 8px 16px; border-radius: 6px;
  cursor: pointer; font-size: 13px;
}
.modal .actions button:hover { border-color: var(--accent); }
.modal .actions button.danger { border-color: var(--red); color: var(--red); }

/* Empty state */
.empty { text-align: center; padding: 60px 20px; color: var(--text2); }
.empty .icon { font-size: 48px; margin-bottom: 12px; }

/* Registered tasks */
.task-list { margin-top: 24px; }
.task-list h3 { font-size: 14px; color: var(--text2); margin-bottom: 8px; }
.task-chip {
  display: inline-block; background: var(--surface2); border: 1px solid var(--border);
  border-radius: 6px; padding: 4px 12px; margin: 3px; font-size: 13px;
  font-family: 'SF Mono', 'Fira Code', monospace;
}

/* Responsive */
@media (max-width: 768px) {
  .container { padding: 16px; }
  .header { padding: 12px 16px; }
  .stats-grid { grid-template-columns: repeat(2, 1fr); }
  td, th { padding: 8px 10px; font-size: 13px; }
  .modal { width: 95%; margin: 16px; }
}
</style>
</head>
<body>

<div class="header">
  <h1>⚡ <span>FlashQ</span> Dashboard</h1>
  <div class="refresh-info">Auto-refresh: <span id="countdown">5</span>s</div>
</div>

<div class="container">
  <!-- Stats -->
  <div class="stats-grid" id="stats-grid"></div>

  <!-- Toolbar -->
  <div class="toolbar">
    <select id="filter-state">
      <option value="">All States</option>
      <option value="PENDING" selected>Pending</option>
      <option value="RUNNING">Running</option>
      <option value="SUCCESS">Success</option>
      <option value="FAILURE">Failure</option>
      <option value="RETRYING">Retrying</option>
      <option value="DEAD">Dead</option>
      <option value="CANCELLED">Cancelled</option>
      <option value="REVOKED">Revoked</option>
    </select>
    <select id="filter-queue"></select>
    <input type="text" id="search" placeholder="Search task name or ID..." style="width: 240px;">
    <button onclick="loadTasks()" style="margin-left: auto;">↻ Refresh</button>
    <button class="danger" onclick="purgeQueue()">🗑 Purge Queue</button>
  </div>

  <!-- Task table -->
  <div class="table-wrap">
    <table>
      <thead>
        <tr>
          <th>ID</th>
          <th>Task</th>
          <th>State</th>
          <th>Queue</th>
          <th>Priority</th>
          <th>Retries</th>
          <th>Created</th>
        </tr>
      </thead>
      <tbody id="task-body"></tbody>
    </table>
  </div>

  <div id="empty-state" class="empty" style="display:none">
    <div class="icon">📋</div>
    <div>No tasks found</div>
  </div>

  <!-- Registered tasks -->
  <div class="task-list" id="registered-tasks"></div>
</div>

<!-- Detail modal -->
<div class="modal-overlay" id="modal-overlay" onclick="closeModal(event)">
  <div class="modal" id="modal-content"></div>
</div>

<script>
const API = '';
let refreshTimer;
let countdown = 5;

// ── Fetch helpers ──

async function fetchJSON(url) {
  const res = await fetch(API + url);
  return res.json();
}

async function postJSON(url, data) {
  const res = await fetch(API + url, {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(data),
  });
  return res.json();
}

// ── Stats ──

async function loadStats() {
  const stats = await fetchJSON('/api/stats');
  const grid = document.getElementById('stats-grid');
  const states = stats.states || {};

  grid.innerHTML = `
    <div class="stat-card">
      <div class="label">Pending</div>
      <div class="value pending">${states.pending || 0}</div>
    </div>
    <div class="stat-card">
      <div class="label">Running</div>
      <div class="value running">${states.running || 0}</div>
    </div>
    <div class="stat-card">
      <div class="label">Success</div>
      <div class="value success">${states.success || 0}</div>
    </div>
    <div class="stat-card">
      <div class="label">Failed</div>
      <div class="value failure">${states.failure || 0}</div>
    </div>
    <div class="stat-card">
      <div class="label">Retrying</div>
      <div class="value">${states.retrying || 0}</div>
    </div>
    <div class="stat-card">
      <div class="label">Dead</div>
      <div class="value dead">${states.dead || 0}</div>
    </div>
    <div class="stat-card">
      <div class="label">Scheduled</div>
      <div class="value scheduled">${stats.scheduled || 0}</div>
    </div>
  `;

  // Registered tasks
  const reg = stats.registered_tasks || [];
  if (reg.length) {
    document.getElementById('registered-tasks').innerHTML = `
      <h3>Registered Tasks (${reg.length})</h3>
      ${reg.map(t => `<span class="task-chip">${t}</span>`).join('')}
    `;
  }
}

// ── Queues ──

async function loadQueues() {
  const queues = await fetchJSON('/api/queues');
  const sel = document.getElementById('filter-queue');
  const current = sel.value;
  sel.innerHTML = '<option value="default">default</option>';
  for (const [name] of Object.entries(queues)) {
    if (name !== 'default') {
      sel.innerHTML += `<option value="${name}">${name}</option>`;
    }
  }
  if (current) sel.value = current;
}

// ── Tasks ──

async function loadTasks() {
  const state = document.getElementById('filter-state').value;
  const queue = document.getElementById('filter-queue').value || 'default';
  const search = document.getElementById('search').value.toLowerCase();

  if (!state) {
    // Load all states
    const allStates = ['pending','running','success','failure','retrying','dead','cancelled','revoked'];
    let allTasks = [];
    for (const s of allStates) {
      const tasks = await fetchJSON(`/api/tasks?state=${s}&queue=${queue}&limit=25`);
      allTasks = allTasks.concat(tasks);
    }
    renderTasks(allTasks.filter(t => filterTask(t, search)));
  } else {
    const tasks = await fetchJSON(`/api/tasks?state=${state}&queue=${queue}&limit=100`);
    renderTasks(tasks.filter(t => filterTask(t, search)));
  }
}

function filterTask(t, search) {
  if (!search) return true;
  return t.task_name.toLowerCase().includes(search)
    || t.id.toLowerCase().includes(search);
}

function renderTasks(tasks) {
  const tbody = document.getElementById('task-body');
  const empty = document.getElementById('empty-state');

  if (tasks.length === 0) {
    tbody.innerHTML = '';
    empty.style.display = 'block';
    return;
  }
  empty.style.display = 'none';

  tbody.innerHTML = tasks.map(t => `
    <tr onclick="showDetail('${t.id}')" style="cursor:pointer">
      <td><code>${t.id.substring(0, 8)}…</code></td>
      <td>${t.task_name}</td>
      <td><span class="badge ${t.state}">${t.state}</span></td>
      <td>${t.queue}</td>
      <td>${t.priority}</td>
      <td>${t.retries}/${t.max_retries}</td>
      <td>${timeAgo(t.created_at)}</td>
    </tr>
  `).join('');
}

// ── Detail modal ──

async function showDetail(taskId) {
  const data = await fetchJSON(`/api/tasks/${taskId}`);
  const modal = document.getElementById('modal-content');
  const result = data.result || {};

  modal.innerHTML = `
    <span class="close" onclick="closeModal()">&times;</span>
    <h2>${data.task_name} <span class="badge ${data.state}">${data.state}</span></h2>

    <div class="field">
      <div class="field-label">Task ID</div>
      <div class="field-value mono">${data.id}</div>
    </div>
    <div class="field">
      <div class="field-label">Queue</div>
      <div class="field-value">${data.queue}</div>
    </div>
    <div class="field">
      <div class="field-label">Priority</div>
      <div class="field-value">${data.priority}</div>
    </div>
    <div class="field">
      <div class="field-label">Retries</div>
      <div class="field-value">${data.retries} / ${data.max_retries}</div>
    </div>
    <div class="field">
      <div class="field-label">Created</div>
      <div class="field-value">${data.created_at || '—'}</div>
    </div>
    <div class="field">
      <div class="field-label">Args</div>
      <div class="field-value mono">${JSON.stringify(data.args)}</div>
    </div>
    <div class="field">
      <div class="field-label">Kwargs</div>
      <div class="field-value mono">${JSON.stringify(data.kwargs, null, 2)}</div>
    </div>
    ${result.result !== undefined ? `
    <div class="field">
      <div class="field-label">Result</div>
      <div class="field-value mono">${JSON.stringify(result.result, null, 2)}</div>
    </div>` : ''}
    ${result.error ? `
    <div class="field">
      <div class="field-label">Error</div>
      <div class="field-value mono" style="color: var(--red)">${result.error}</div>
    </div>` : ''}
    ${result.traceback ? `
    <div class="field">
      <div class="field-label">Traceback</div>
      <div class="field-value mono" style="color: var(--red); font-size: 12px;">${result.traceback}</div>
    </div>` : ''}
    ${result.runtime_ms ? `
    <div class="field">
      <div class="field-label">Runtime</div>
      <div class="field-value">${result.runtime_ms.toFixed(1)}ms</div>
    </div>` : ''}

    <div class="actions">
      ${!['SUCCESS','CANCELLED','REVOKED','DEAD'].includes(data.state) ? `
        <button class="danger" onclick="taskAction('${data.id}', 'cancel')">Cancel</button>
        <button class="danger" onclick="taskAction('${data.id}', 'revoke')">Revoke</button>
      ` : ''}
    </div>
  `;

  document.getElementById('modal-overlay').classList.add('active');
}

function closeModal(event) {
  if (!event || event.target === document.getElementById('modal-overlay')) {
    document.getElementById('modal-overlay').classList.remove('active');
  }
}

async function taskAction(taskId, action) {
  await postJSON(`/api/tasks/${taskId}/action`, {action});
  closeModal();
  loadTasks();
  loadStats();
}

async function purgeQueue() {
  const queue = document.getElementById('filter-queue').value || 'default';
  if (!confirm(`Purge all pending tasks in "${queue}"?`)) return;
  const res = await postJSON('/api/purge', {queue});
  alert(`Purged ${res.removed} tasks from ${queue}`);
  loadTasks();
  loadStats();
}

// ── Time helpers ──

function timeAgo(iso) {
  if (!iso) return '—';
  const diff = (Date.now() - new Date(iso).getTime()) / 1000;
  if (diff < 60) return `${Math.round(diff)}s ago`;
  if (diff < 3600) return `${Math.round(diff / 60)}m ago`;
  if (diff < 86400) return `${Math.round(diff / 3600)}h ago`;
  return `${Math.round(diff / 86400)}d ago`;
}

// ── Auto-refresh ──

function startRefresh() {
  countdown = 5;
  clearInterval(refreshTimer);
  refreshTimer = setInterval(() => {
    countdown--;
    document.getElementById('countdown').textContent = countdown;
    if (countdown <= 0) {
      countdown = 5;
      refresh();
    }
  }, 1000);
}

async function refresh() {
  await Promise.all([loadStats(), loadQueues(), loadTasks()]);
}

// ── Event listeners ──

document.getElementById('filter-state').addEventListener('change', loadTasks);
document.getElementById('filter-queue').addEventListener('change', loadTasks);
document.getElementById('search').addEventListener('input', loadTasks);
document.addEventListener('keydown', e => {
  if (e.key === 'Escape') closeModal();
});

// ── Init ──

refresh();
startRefresh();
</script>
</body>
</html>
""".strip()
