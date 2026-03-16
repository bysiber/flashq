# Contributing to FlashQ

Thanks for considering contributing to FlashQ! Here's how to get started.

## Development Setup

```bash
git clone https://github.com/bysiber/flashq.git
cd flashq
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -e ".[dev]"
```

## Running Tests

```bash
# All tests
pytest tests/ -v

# With coverage
pytest tests/ --cov=flashq --cov-report=term-missing

# Specific test file
pytest tests/test_core.py -v
```

## Code Quality

```bash
# Lint
ruff check flashq/

# Format
ruff format flashq/

# Type check
mypy flashq/ --ignore-missing-imports
```

## Pull Request Process

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/my-feature`)
3. Write tests for your changes
4. Ensure all tests pass and linting is clean
5. Submit a PR with a clear description

## Architecture Overview

```
flashq/
├── app.py           # FlashQ class, task registry
├── task.py          # Task decorator, TaskHandle
├── models.py        # TaskMessage, TaskResult dataclasses
├── worker.py        # Worker process, task execution
├── middleware.py     # Middleware system
├── scheduler.py     # Periodic/cron task scheduler
├── backends/
│   ├── __init__.py  # BaseBackend ABC
│   ├── sqlite.py    # SQLite backend (default)
│   ├── postgres.py  # PostgreSQL backend
│   └── redis.py     # Redis backend
├── serializers.py   # JSON/Pickle serialization
├── cli.py           # Command-line interface
├── enums.py         # TaskState, TaskPriority
└── exceptions.py    # Exception hierarchy
```

## Adding a New Backend

1. Subclass `BaseBackend` from `flashq.backends`
2. Implement all abstract methods
3. Add tests in `tests/test_backend_yourname.py`
4. Add optional dependency in `pyproject.toml`

## Code Style

- Python 3.10+ features are welcome
- Type hints everywhere
- Docstrings for public APIs
- Keep imports sorted (ruff handles this)
