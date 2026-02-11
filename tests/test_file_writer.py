import asyncio
import os

import pytest

from aiohttp_s3_client.file_writer import (
    IOWriter,
    UnixWriter,
    Writer,
)


@pytest.fixture()
def target_file(tmp_path):
    return tmp_path / "output.bin"


@pytest.fixture()
def existing_file(tmp_path):
    p = tmp_path / "existing.bin"
    p.write_bytes(b"old content that should be overwritten")
    return p


async def test_writer_context_manager(target_file):
    async with Writer(target_file, 64) as writer:
        assert isinstance(writer.fd, int)


async def test_writer_pre_allocation(target_file):
    async with Writer(target_file, 256):
        pass
    assert target_file.stat().st_size == 256


async def test_writer_simple_write(target_file):
    data = b"Hello, world!"
    async with Writer(target_file, len(data)) as writer:
        await writer.write(data, 0)
    assert target_file.read_bytes() == data


async def test_writer_write_at_offset(target_file):
    file_size = 20
    async with Writer(target_file, file_size) as writer:
        await writer.write(b"Hello", 0)
        await writer.write(b"World", 10)
    content = target_file.read_bytes()
    assert content[:5] == b"Hello"
    assert content[10:15] == b"World"


async def test_writer_multiple_sequential_writes(target_file):
    data = b"ABCDEFGHIJKLMNOP"
    async with Writer(target_file, len(data)) as writer:
        for i in range(0, len(data), 4):
            await writer.write(data[i:i + 4], i)
    assert target_file.read_bytes() == data


async def test_writer_concurrent_writes(target_file):
    chunk_size = 100
    num_chunks = 10
    file_size = chunk_size * num_chunks
    chunks = [os.urandom(chunk_size) for _ in range(num_chunks)]

    async with Writer(target_file, file_size) as writer:
        await asyncio.gather(*(
            writer.write(chunks[i], i * chunk_size)
            for i in range(num_chunks)
        ))

    content = target_file.read_bytes()
    assert content == b"".join(chunks)


async def test_writer_zero_size_file(target_file):
    async with Writer(target_file, 0):
        pass
    assert target_file.stat().st_size == 0
    assert target_file.read_bytes() == b""


async def test_writer_overwrite_existing_file(existing_file):
    new_data = b"new content"
    async with Writer(existing_file, len(new_data)) as writer:
        await writer.write(new_data, 0)
    assert existing_file.read_bytes() == new_data


async def test_io_writer_writes_correctly(target_file):
    data = b"IOWriter test data"
    async with IOWriter(target_file, len(data)) as writer:
        await writer.write(data, 0)
    assert target_file.read_bytes() == data


async def test_io_writer_concurrent_writes(target_file):
    chunk_size = 100
    num_chunks = 10
    file_size = chunk_size * num_chunks
    chunks = [os.urandom(chunk_size) for _ in range(num_chunks)]

    async with IOWriter(target_file, file_size) as writer:
        await asyncio.gather(*(
            writer.write(chunks[i], i * chunk_size)
            for i in range(num_chunks)
        ))

    assert target_file.read_bytes() == b"".join(chunks)


@pytest.mark.skipif(
    not hasattr(os, "pwrite"), reason="os.pwrite not available",
)
async def test_unix_writer_writes_correctly(target_file):
    data = b"UnixWriter test data"
    async with UnixWriter(target_file, len(data)) as writer:
        await writer.write(data, 0)
    assert target_file.read_bytes() == data


@pytest.mark.skipif(
    not hasattr(os, "pwrite"), reason="os.pwrite not available",
)
async def test_unix_writer_concurrent_writes(target_file):
    chunk_size = 100
    num_chunks = 10
    file_size = chunk_size * num_chunks
    chunks = [os.urandom(chunk_size) for _ in range(num_chunks)]

    async with UnixWriter(target_file, file_size) as writer:
        await asyncio.gather(*(
            writer.write(chunks[i], i * chunk_size)
            for i in range(num_chunks)
        ))

    assert target_file.read_bytes() == b"".join(chunks)
