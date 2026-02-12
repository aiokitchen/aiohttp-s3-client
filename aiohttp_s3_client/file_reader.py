import asyncio
import hashlib
import logging
import mmap
import os
from collections.abc import Coroutine, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any, AsyncIterator, Awaitable, BinaryIO


log = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class Chunk:
    data: bytes
    sha256: str = ""


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


class Reader:
    DEFAULT_CHUNK_SIZE = 5 * 1024 * 1024
    # Most s3 endpoints support up to 10,000 parts in a multipart upload,
    # so we should not read more than that many chunks.
    MAX_CHUNKS = 10000

    def __init__(self, path: str | Path, compute_sha256: bool = True) -> None:
        self._path = str(path)
        self._stat: os.stat_result | None = None
        self._compute_sha256 = compute_sha256
        self._fp: BinaryIO | None = None
        self._mm: mmap.mmap | None = None

    def _sync_open(self) -> None:
        self._fp = open(self._path, "rb")
        self._stat = os.fstat(self._fp.fileno())
        if self._stat.st_size > 0:
            self._mm = mmap.mmap(
                self._fp.fileno(),
                0,
                access=mmap.ACCESS_READ,
            )

    async def open(self) -> None:
        if self._fp is not None:
            return
        await asyncio.to_thread(self._sync_open)

    def close(self) -> None:
        if self._mm is not None:
            self._mm.close()
            self._mm = None
        if self._fp is not None:
            self._fp.close()
            self._fp = None

    @property
    def fd(self) -> int:
        if self._fp is None:
            raise RuntimeError("Reader is not open")
        return self._fp.fileno()

    @property
    def stat(self) -> os.stat_result:
        if self._stat is None:
            raise RuntimeError("Reader is not open")
        return self._stat

    async def __aenter__(self) -> "Reader":
        await self.open()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        self.close()

    def _slice_read(self, size: int, offset: int) -> Chunk:
        assert self._mm is not None
        data = self._mm[offset : offset + size]
        if self._compute_sha256:
            return Chunk(data=data, sha256=hashlib.sha256(data).hexdigest())
        return Chunk(data=data)

    async def _read(self, size: int, offset: int) -> Chunk:
        return await asyncio.to_thread(self._slice_read, size, offset)

    def chunked(self, size: int) -> Iterator[ChunkFuture]:
        file_size = self.stat.st_size
        offset = 0

        # Adjust chunk size to stay within the max number of chunks
        if file_size > size * self.MAX_CHUNKS:
            size = (file_size + self.MAX_CHUNKS - 1) // self.MAX_CHUNKS
            log.debug(
                "Adjusting chunk size to %d to avoid "
                "exceeding maximum number of chunks",
                size,
            )

        while offset < file_size:
            chunk_size = min(size, file_size - offset)
            yield ChunkFuture(self._read(chunk_size, offset))
            offset += chunk_size

    async def read(
        self,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
    ) -> AsyncIterator[bytes]:
        for chunk_future in self.chunked(chunk_size):
            chunk = await chunk_future
            yield chunk.data

    def __aiter__(self) -> AsyncIterator[bytes]:
        return self.read()

    @classmethod
    def from_fp(
        cls,
        fp: BinaryIO,
        compute_sha256: bool = True,
    ) -> "Reader":
        instance = cls.__new__(cls)
        instance._compute_sha256 = compute_sha256
        instance._fp = fp
        fp.seek(0)
        instance._stat = os.fstat(fp.fileno())
        if instance._stat.st_size > 0:
            instance._mm = mmap.mmap(
                fp.fileno(),
                0,
                access=mmap.ACCESS_READ,
            )
        else:
            instance._mm = None
        return instance


__all__ = (
    "Chunk",
    "ChunkFuture",
    "Reader",
)
