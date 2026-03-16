"""FlashQ FastAPI integration.

Provides lifespan management and dependency injection.

Usage::

    from fastapi import FastAPI
    from flashq.contrib.fastapi import FlashQExtension

    flashq_ext = FlashQExtension()

    app = FastAPI(lifespan=flashq_ext.lifespan)

    @flashq_ext.task()
    def process_order(order_id: str) -> dict:
        return {"order_id": order_id, "status": "done"}

    @app.post("/orders/{order_id}")
    async def create_order(order_id: str):
        handle = process_order.delay(order_id)
        return {"task_id": handle.id}
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator


class FlashQExtension:
    """FastAPI integration for FlashQ.

    Manages backend setup/teardown via lifespan and provides
    the FlashQ app as a dependency.
    """

    def __init__(
        self,
        backend: str = "sqlite",
        database: str = "flashq.db",
        name: str = "fastapi",
        **kwargs: Any,
    ) -> None:
        from flashq import FlashQ
        from flashq.backends.sqlite import SQLiteBackend

        if backend == "sqlite":
            self._backend = SQLiteBackend(path=database)
        elif backend == "postgres":
            from flashq.backends.postgres import PostgresBackend

            self._backend = PostgresBackend(database)
        elif backend == "redis":
            from flashq.backends.redis import RedisBackend

            self._backend = RedisBackend(database)
        else:
            msg = f"Unknown backend: {backend}"
            raise ValueError(msg)

        self.app = FlashQ(backend=self._backend, name=name, **kwargs)

    def task(self, **kwargs: Any) -> Any:
        """Decorator proxy to FlashQ.task()."""
        return self.app.task(**kwargs)

    @asynccontextmanager
    async def lifespan(self, _app: Any) -> AsyncGenerator[None, None]:
        """FastAPI lifespan — setup/teardown backend."""
        self._backend.setup()
        try:
            yield
        finally:
            self._backend.teardown()

    def create_dashboard(self, prefix: str = "/flashq") -> Any:
        """Create a Starlette dashboard app that can be mounted."""
        from flashq.dashboard import create_dashboard

        return create_dashboard(self.app, prefix=prefix)
