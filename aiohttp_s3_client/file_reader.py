import asyncio
import hashlib
import logging
import os
import threading
from abc import ABC, abstractmethod
from collections.abc import Coroutine, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any, AsyncIterator, Awaitable, BinaryIO


log = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class Chunk:
    data: bytes
    sha256: str = ''


class ChunkFuture(Awaitable[Chunk]):
    __slots__ = ("_coro", "_result")

    def __init__(self, coro: Coroutine[Any, Any, Chunk]) -> None:
        self._coro = coro
        self._result: Chunk | None = None

    def __await__(self):
        if self._result is not None:
            return self._result
        result = yield from self._coro.__await__()
        self._result = result
        return result

    async def __aiter__(self) -> AsyncIterator[Chunk]:
        yield await self


class AbstractReader(ABC):
    DEFAULT_CHUNK_SIZE = 5 * 1024 * 1024
    # Most s3 endpoints support up to 10,000 parts in a multipart upload,
    # so we should not read more than that many chunks.
    MAX_CHUNKS = 10000

    def __init__(self, path: str | Path, compute_sha256: bool = True) -> None:
        self._fp: BinaryIO
        self._path = str(path)
        self._stat: os.stat_result | None = None
        self._compute_sha256 = compute_sha256

    def _sync_open(self) -> None:
        self._fp = open(self._path, "rb")
        self._stat = os.fstat(self._fp.fileno())

    async def open(self) -> None:
        if hasattr(self, "_fp"):
            return
        await asyncio.to_thread(self._sync_open)

    def close(self) -> None:
        if not hasattr(self, "_fp"):
            return
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
    async def _read(self, size: int, offset: int) -> Chunk: ...

    def chunked(self, size: int) -> Iterator[ChunkFuture]:
        file_size = self.stat.st_size
        offset = 0

        # Adjust chunk size to stay within the max number of chunks
        if file_size > size * self.MAX_CHUNKS:
            size = (file_size + self.MAX_CHUNKS - 1) // self.MAX_CHUNKS
            log.debug(
                "Adjusting chunk size to %d to avoid "
                "exceeding maximum number of chunks", size,
            )

        while offset < file_size:
            chunk_size = min(size, file_size - offset)
            yield ChunkFuture(self._read(chunk_size, offset))
            offset += chunk_size

    async def read(
        self, chunk_size: int = DEFAULT_CHUNK_SIZE,
    ) -> AsyncIterator[bytes]:
        for chunk_future in self.chunked(chunk_size):
            chunk = await chunk_future
            yield chunk.data

    def __aiter__(self) -> AsyncIterator[bytes]:
        return self.read()

    @classmethod
    def from_fp(
        cls, fp: BinaryIO, compute_sha256: bool = True,
    ) -> "AbstractReader":
        instance = cls.__new__(cls)
        instance._fp = fp
        instance._stat = os.fstat(fp.fileno())
        instance._compute_sha256 = compute_sha256
        fp.seek(0)
        return instance


class UnixReader(AbstractReader):
    """
    Uses os.pread to read file chunks without changing the file pointer,
    allowing for concurrent reads without locking.
    Really efficient on not small chunks. POSIX specific.
    """
    def _pread(self, size: int, offset: int) -> Chunk:
        data = os.pread(self.fd, size, offset)
        if self._compute_sha256:
            return Chunk(data=data, sha256=hashlib.sha256(data).hexdigest())
        return Chunk(data=data)

    async def _read(self, size: int, offset: int) -> Chunk:
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

    async def _read(self, size: int, offset: int) -> Chunk:
        return await asyncio.to_thread(self._locked_read, size, offset)


Reader = UnixReader if hasattr(os, "pread") else IOReader

__all__ = (
    "AbstractReader", "Chunk", "ChunkFuture",
    "IOReader", "Reader", "UnixReader",
)
