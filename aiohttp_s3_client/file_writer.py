import asyncio
import os
import tempfile
import threading
from abc import ABC, abstractmethod
from collections.abc import Coroutine
from pathlib import Path
from typing import Any, BinaryIO

WriteFuture = Coroutine[Any, Any, None]


class AbstractWriter(ABC):
    def __init__(self, path: str | Path, file_size: int) -> None:
        self._fp: BinaryIO
        self._path = str(path)
        self._file_size = file_size

    def _sync_open(self) -> None:
        self._fp = open(self._path, "w+b")
        self._fp.truncate(self._file_size)

    async def open(self) -> None:
        await asyncio.to_thread(self._sync_open)

    def close(self) -> None:
        if hasattr(self, "_fp"):
            self._fp.close()
            del self._fp

    @property
    def fd(self) -> int:
        if not hasattr(self, "_fp"):
            raise RuntimeError("Writer is not open")
        return self._fp.fileno()

    @property
    def file_size(self) -> int:
        return self._file_size

    async def __aenter__(self) -> "AbstractWriter":
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
    def write(self, data: bytes, offset: int) -> WriteFuture: ...


class UnixWriter(AbstractWriter):
    """
    Writer implementation using os.pwrite, which allows writing to a file at a
    specific offset without changing the file pointer. Really efficient for
    concurrent writes to the same file from multiple threads or processes.
    But it is only available on Unix-like systems.
    """

    def _pwrite(self, data: bytes, offset: int) -> None:
        os.pwrite(self.fd, data, offset)

    def write(self, data: bytes, offset: int) -> WriteFuture:
        return asyncio.to_thread(self._pwrite, data, offset)


class IOWriter(AbstractWriter):
    ASSEMBLE_CHUNK_SIZE = 1024 * 1024

    def __init__(self, path: str | Path, file_size: int) -> None:
        super().__init__(path, file_size)
        self._tmp_dir: tempfile.TemporaryDirectory[str] | None = None
        self._parts: list[tuple[int, str]] = []
        self._lock = threading.Lock()

    def _sync_open(self) -> None:
        self._tmp_dir = tempfile.TemporaryDirectory(dir=Path(self._path).parent)

    def _write_to_tmp(self, data: bytes, offset: int) -> None:
        assert self._tmp_dir is not None
        tmp_path = os.path.join(self._tmp_dir.name, f"{offset:020d}")
        with open(tmp_path, "wb") as f:
            f.write(data)
        with self._lock:
            self._parts.append((offset, tmp_path))

    def write(self, data: bytes, offset: int) -> WriteFuture:
        return asyncio.to_thread(self._write_to_tmp, data, offset)

    def _assemble(self) -> None:
        self._parts.sort()
        with open(self._path, "wb") as out:
            out.truncate(self._file_size)
            for offset, tmp_path in self._parts:
                out.seek(offset)
                with open(tmp_path, "rb") as part:
                    while chunk := part.read(self.ASSEMBLE_CHUNK_SIZE):
                        out.write(chunk)

    def close(self) -> None:
        if self._tmp_dir is not None:
            self._assemble()
            self._tmp_dir.cleanup()
            self._tmp_dir = None

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        await asyncio.to_thread(self.close)


Writer = UnixWriter if hasattr(os, "pwrite") else IOWriter

__all__ = (
    "AbstractWriter",
    "IOWriter",
    "UnixWriter",
    "Writer",
    "WriteFuture",
)
