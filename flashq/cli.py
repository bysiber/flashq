"""FlashQ command-line interface.

Usage::

    # Start a worker
    flashq worker myapp:app

    # Start with options
    flashq worker myapp:app --queues default,emails --concurrency 8

    # Show queue info
    flashq info myapp:app

    # Purge a queue
    flashq purge myapp:app --queue default
"""

from __future__ import annotations

import argparse
import importlib
import logging
import sys
from typing import Any

from flashq._version import __version__


def _import_app(path: str) -> Any:
    """Import a FlashQ app from a dotted path like ``myapp:app`` or ``myapp.module:app``.

    Supports two formats:
    - ``module:attribute`` (recommended)
    - ``module.attribute`` (also works)
    """
    if ":" in path:
        module_path, attr_name = path.rsplit(":", 1)
    elif "." in path:
        module_path, attr_name = path.rsplit(".", 1)
    else:
        print(f"Error: Invalid app path {path!r}. Use format 'module:app'", file=sys.stderr)
        sys.exit(1)

    # Add cwd to sys.path so local modules are importable
    if "" not in sys.path:
        sys.path.insert(0, "")

    try:
        module = importlib.import_module(module_path)
    except ImportError as exc:
        print(f"Error: Could not import module {module_path!r}: {exc}", file=sys.stderr)
        sys.exit(1)

    try:
        app = getattr(module, attr_name)
    except AttributeError:
        print(
            f"Error: Module {module_path!r} has no attribute {attr_name!r}",
            file=sys.stderr,
        )
        sys.exit(1)

    return app


def _setup_logging(level: str) -> None:
    """Configure logging for the CLI."""
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def cmd_worker(args: argparse.Namespace) -> None:
    """Start a FlashQ worker."""
    from flashq.worker import Worker

    app = _import_app(args.app)
    queues = args.queues.split(",") if args.queues else ["default"]

    worker = Worker(
        app,
        queues=queues,
        concurrency=args.concurrency,
        poll_interval=args.poll_interval,
        name=args.name,
    )
    worker.start()


def cmd_info(args: argparse.Namespace) -> None:
    """Show queue and task information."""
    app = _import_app(args.app)

    print(f"\n⚡ FlashQ Info — {app.name}")
    print(f"  Backend: {type(app.backend).__name__}")
    print(f"  Registered tasks: {len(app.registry)}")
    for name, task in app.registry.items():
        print(f"    └─ {name} (queue={task.queue}, retries={task.max_retries})")

    queue = args.queue or "default"
    size = app.backend.queue_size(queue)
    sched_size = app.backend.schedule_size()
    print(f"\n  Queue {queue!r}: {size} pending task(s)")
    print(f"  Scheduled: {sched_size} task(s)")
    print()


def cmd_purge(args: argparse.Namespace) -> None:
    """Purge all pending tasks from a queue."""
    app = _import_app(args.app)
    queue = args.queue or "default"

    if not args.force:
        count = app.backend.queue_size(queue)
        confirm = input(f"Delete {count} pending task(s) from queue {queue!r}? [y/N] ")
        if confirm.lower() not in ("y", "yes"):
            print("Aborted.")
            return

    removed = app.backend.flush_queue(queue)
    print(f"Purged {removed} task(s) from queue {queue!r}")


def cmd_version(args: argparse.Namespace) -> None:
    """Print version and exit."""
    print(f"flashq {__version__}")


def build_parser() -> argparse.ArgumentParser:
    """Build the argument parser."""
    parser = argparse.ArgumentParser(
        prog="flashq",
        description="⚡ FlashQ — The task queue that works out of the box.",
    )
    parser.add_argument(
        "--version", action="version", version=f"flashq {__version__}"
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # worker command
    worker_parser = subparsers.add_parser("worker", help="Start a worker process")
    worker_parser.add_argument("app", help="App path (e.g., 'myapp:app')")
    worker_parser.add_argument(
        "-q", "--queues",
        default=None,
        help="Comma-separated list of queues (default: 'default')",
    )
    worker_parser.add_argument(
        "-c", "--concurrency",
        type=int,
        default=4,
        help="Number of concurrent task threads (default: 4)",
    )
    worker_parser.add_argument(
        "--poll-interval",
        type=float,
        default=1.0,
        help="Seconds between queue polls (default: 1.0)",
    )
    worker_parser.add_argument(
        "-n", "--name",
        default=None,
        help="Worker name (default: worker-{pid})",
    )
    worker_parser.add_argument(
        "-l", "--log-level",
        default="info",
        choices=["debug", "info", "warning", "error"],
        help="Log level (default: info)",
    )
    worker_parser.set_defaults(func=cmd_worker)

    # info command
    info_parser = subparsers.add_parser("info", help="Show queue information")
    info_parser.add_argument("app", help="App path (e.g., 'myapp:app')")
    info_parser.add_argument(
        "--queue", default=None, help="Queue name (default: 'default')"
    )
    info_parser.set_defaults(func=cmd_info)

    # purge command
    purge_parser = subparsers.add_parser("purge", help="Delete all pending tasks")
    purge_parser.add_argument("app", help="App path (e.g., 'myapp:app')")
    purge_parser.add_argument(
        "--queue", default=None, help="Queue name (default: 'default')"
    )
    purge_parser.add_argument(
        "-f", "--force",
        action="store_true",
        help="Skip confirmation prompt",
    )
    purge_parser.set_defaults(func=cmd_purge)

    return parser


def main() -> None:
    """CLI entry point."""
    parser = build_parser()
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(0)

    # Set up logging
    log_level = getattr(args, "log_level", "info")
    _setup_logging(log_level)

    # Execute the command
    args.func(args)


if __name__ == "__main__":
    main()
