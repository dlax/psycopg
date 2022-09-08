"""
psycopg copy support using AnyIO
"""

# Copyright (C) 2022 The Psycopg Team


from functools import lru_cache
from types import TracebackType
from typing import Any, Optional, Type, TYPE_CHECKING

from ..copy import AsyncLibpqWriter, QUEUE_SIZE
from ..generators import copy_to

if TYPE_CHECKING:
    import anyio
    from ..cursor_async import AsyncCursor
else:
    anyio = None


@lru_cache()
def _import_anyio() -> None:
    global anyio
    try:
        import anyio
    except ImportError as e:
        raise ImportError(
            "anyio is not installed; run `pip install psycopg[anyio]`"
        ) from e


class AnyIOLibpqWriter(AsyncLibpqWriter):
    """An `AsyncWriter` using AnyIO streams."""

    __module__ = "psycopg"

    def __init__(self, cursor: "AsyncCursor[Any]"):
        _import_anyio()
        super().__init__(cursor)

        self._worker: Optional["anyio.abc.TaskGroup"] = None
        (self._send_stream, self._receive_stream,) = anyio.create_memory_object_stream(
            max_buffer_size=QUEUE_SIZE, item_type=bytes
        )

    async def worker(self) -> None:
        """Push data to the server when available from the receiving stream."""
        async with self._receive_stream:
            async for data in self._receive_stream:
                await self.connection.wait(copy_to(self._pgconn, data))

    async def write(self, data: bytes) -> None:
        if not self._worker:
            self._worker = anyio.create_task_group()
            await self._worker.__aenter__()
            self._worker.start_soon(self.worker)

        if not data:
            return

        await self._send_stream.send(data)

    async def finish(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        self._send_stream.close()

        if self._worker:
            await self._worker.__aexit__(exc_type, exc_val, exc_tb)
            self._worker = None  # break reference loops if any

        await super().finish(exc_type, exc_val, exc_tb)
