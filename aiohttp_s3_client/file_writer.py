import asyncio
import os
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
    def _pwrite(self, data: bytes, offset: int) -> None:
        os.pwrite(self.fd, data, offset)

    def write(self, data: bytes, offset: int) -> WriteFuture:
        return asyncio.to_thread(self._pwrite, data, offset)


class IOWriter(AbstractWriter):
    def __init__(self, path: str | Path, file_size: int) -> None:
        super().__init__(path, file_size)
        self._lock = threading.Lock()

    def _locked_write(self, data: bytes, offset: int) -> None:
        with self._lock:
            os.lseek(self.fd, offset, os.SEEK_SET)
            os.write(self.fd, data)

    def write(self, data: bytes, offset: int) -> WriteFuture:
        return asyncio.to_thread(self._locked_write, data, offset)


Writer = UnixWriter if hasattr(os, "pwrite") else IOWriter

__all__ = (
    "AbstractWriter", "IOWriter", "UnixWriter",
    "Writer", "WriteFuture",
)
