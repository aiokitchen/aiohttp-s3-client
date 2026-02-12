import asyncio
import mmap
from collections.abc import Coroutine
from pathlib import Path
from typing import Any, BinaryIO

WriteFuture = Coroutine[Any, Any, None]


class Writer:
    def __init__(self, path: str | Path, file_size: int) -> None:
        self._fp: BinaryIO | None = None
        self._mm: mmap.mmap | None = None
        self._path = str(path)
        self._file_size = file_size

    def _sync_open(self) -> None:
        self._fp = open(self._path, "w+b")
        self._fp.truncate(self._file_size)
        if self._file_size > 0:
            self._mm = mmap.mmap(
                self._fp.fileno(),
                0,
                access=mmap.ACCESS_WRITE,
            )

    async def open(self) -> None:
        if self._fp is not None:
            return
        await asyncio.to_thread(self._sync_open)

    def close(self) -> None:
        if self._mm is not None:
            self._mm.flush()
            self._mm.close()
            self._mm = None
        if self._fp is not None:
            self._fp.close()
            self._fp = None

    @property
    def fd(self) -> int:
        if self._fp is None:
            raise RuntimeError("Writer is not open")
        return self._fp.fileno()

    @property
    def file_size(self) -> int:
        return self._file_size

    async def __aenter__(self) -> "Writer":
        await self.open()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        self.close()

    def _sync_write(self, data: bytes, offset: int) -> None:
        assert self._mm is not None
        self._mm[offset : offset + len(data)] = data

    def write(self, data: bytes, offset: int) -> WriteFuture:
        return asyncio.to_thread(self._sync_write, data, offset)


__all__ = (
    "Writer",
    "WriteFuture",
)
