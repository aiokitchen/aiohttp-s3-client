import asyncio
import hashlib
import os
import threading
from abc import ABC, abstractmethod
from collections.abc import Coroutine, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any, AsyncIterator, Awaitable, BinaryIO


@dataclass(frozen=True, slots=True)
class Chunk:
    data: bytes
    sha256: str = ''


@dataclass(frozen=True, slots=True)
class ChunkFuture(Awaitable[Chunk]):
    coro: Coroutine[Any, Any, Chunk]

    def __await__(self):
        return (yield from self.coro.__await__())

    async def __aiter__(self) -> AsyncIterator[Chunk]:
        yield await self.coro


class AbstractReader(ABC):
    def __init__(self, path: str | Path, compute_sha256: bool = True) -> None:
        self._fp: BinaryIO
        self._path = str(path)
        self._stat: os.stat_result | None = None
        self._compute_sha256 = compute_sha256

    def _sync_open(self) -> None:
        self._fp = open(self._path, "rb")
        self._stat = os.fstat(self._fp.fileno())

    async def open(self) -> None:
        await asyncio.to_thread(self._sync_open)

    def close(self) -> None:
        if hasattr(self, "_fp"):
            self._fp.close()
            del self._fp

    @property
    def fd(self) -> int:
        if not hasattr(self, "_fp"):
            raise RuntimeError("Reader is not open")
        return self._fp.fileno()

    @property
    def stat(self) -> os.stat_result:
        if self._stat is None:
            raise RuntimeError("Reader is not open")
        return self._stat

    async def __aenter__(self) -> "AbstractReader":
        await self.open()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        self.close()

    @abstractmethod
    def _read(self, size: int, offset: int) -> Coroutine[Any, Any, Chunk]: ...

    def chunked(self, size: int) -> Iterator[ChunkFuture]:
        file_size = self.stat.st_size
        offset = 0
        while offset < file_size:
            chunk_size = min(size, file_size - offset)
            yield ChunkFuture(self._read(chunk_size, offset))
            offset += chunk_size

    async def __aiter__(self) -> AsyncIterator[ChunkFuture]:
        for chunk_future in self.chunked(1024 * 1024):
            yield chunk_future


class UnixReader(AbstractReader):
    def _pread(self, size: int, offset: int) -> Chunk:
        data = os.pread(self.fd, size, offset)
        if self._compute_sha256:
            return Chunk(data=data, sha256=hashlib.sha256(data).hexdigest())
        return Chunk(data=data)

    async def _read(self, size: int, offset: int) -> Coroutine[Any, Any, Chunk]:
        return await asyncio.to_thread(self._pread, size, offset)


class IOReader(AbstractReader):
    def __init__(self, path: str | Path, compute_sha256: bool = True) -> None:
        super().__init__(path, compute_sha256=compute_sha256)
        self._lock = threading.Lock()

    def _locked_read(self, size: int, offset: int) -> Chunk:
        with self._lock:
            os.lseek(self.fd, offset, os.SEEK_SET)
            data = os.read(self.fd, size)
        if self._compute_sha256:
            return Chunk(data=data, sha256=hashlib.sha256(data).hexdigest())
        return Chunk(data=data)

    async def _read(self, size: int, offset: int) -> Coroutine[Any, Any, Chunk]:
        return await asyncio.to_thread(self._locked_read, size, offset)


Reader = UnixReader if hasattr(os, "pread") else IOReader

__all__ = (
    "AbstractReader", "Chunk", "ChunkFuture",
    "IOReader", "Reader", "UnixReader",
)
