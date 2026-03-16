"""FlashQ Django integration.

Provides automatic task discovery and Django settings integration.

Usage in Django settings::

    INSTALLED_APPS = [
        ...
        "flashq.contrib.django",
    ]

    FLASHQ = {
        "backend": "sqlite",
        "database": "flashq.db",
    }

Usage in your Django app::

    from flashq.contrib.django import app

    @app.task()
    def send_welcome_email(user_id: int) -> None:
        from myapp.models import User
        user = User.objects.get(id=user_id)
        user.email_user("Welcome!", "Thanks for signing up.")
"""

from __future__ import annotations

from typing import Any


def get_flashq_app(**kwargs: Any) -> Any:
    """Create a FlashQ app from Django settings.

    Call this in your Django AppConfig.ready() or use the auto-discovery.
    """
    try:
        from django.conf import settings
    except ImportError as exc:
        msg = "Django is required to use flashq.contrib.django"
        raise ImportError(msg) from exc

    from flashq import FlashQ
    from flashq.backends.sqlite import SQLiteBackend

    config = getattr(settings, "FLASHQ", {})
    backend_type = config.get("backend", "sqlite")

    if backend_type == "sqlite":
        backend = SQLiteBackend(path=config.get("database", "flashq.db"))
    elif backend_type == "postgres":
        from flashq.backends.postgres import PostgresBackend

        backend = PostgresBackend(config["database"])
    elif backend_type == "redis":
        from flashq.backends.redis import RedisBackend

        backend = RedisBackend(config["database"])
    else:
        msg = f"Unknown backend: {backend_type}"
        raise ValueError(msg)

    return FlashQ(backend=backend, name=config.get("name", "django"), **kwargs)
