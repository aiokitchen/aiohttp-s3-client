import asyncio
import hashlib
import os

import pytest

from aiohttp_s3_client.file_reader import (
    IOReader,
    Reader,
    UnixReader,
)


@pytest.fixture()
def sample_file(tmp_path):
    p = tmp_path / "sample.bin"
    p.write_bytes(b"Hello, world! This is test data for file_reader.")
    return p


@pytest.fixture()
def empty_file(tmp_path):
    p = tmp_path / "empty.bin"
    p.write_bytes(b"")
    return p


async def test_reader_context_manager(sample_file):
    async with Reader(sample_file) as reader:
        assert isinstance(reader.fd, int)


async def test_reader_stat(sample_file):
    async with Reader(sample_file) as reader:
        assert reader.stat.st_size == len(sample_file.read_bytes())


@pytest.mark.parametrize("chunk_size", [4096, 65536])
async def test_reader_chunked_whole_file(sample_file, chunk_size):
    data = sample_file.read_bytes()
    async with Reader(sample_file) as reader:
        chunks = [await fut for fut in reader.chunked(chunk_size)]
    assert len(chunks) == 1
    assert chunks[0].data == data
    assert chunks[0].sha256 == hashlib.sha256(data).hexdigest()


@pytest.mark.parametrize("chunk_size", [5, 10, 17])
async def test_reader_chunked_multiple(sample_file, chunk_size):
    data = sample_file.read_bytes()
    expected_count = -(-len(data) // chunk_size)  # ceil division
    async with Reader(sample_file) as reader:
        chunks = [await fut for fut in reader.chunked(chunk_size)]
    assert len(chunks) == expected_count
    assert b"".join(c.data for c in chunks) == data


@pytest.mark.parametrize("chunk_size", [7, 20])
async def test_reader_chunked_sha256(sample_file, chunk_size):
    async with Reader(sample_file) as reader:
        chunks = [await fut for fut in reader.chunked(chunk_size)]
    for chunk in chunks:
        assert chunk.sha256 == hashlib.sha256(chunk.data).hexdigest()


async def test_reader_empty_file(empty_file):
    async with Reader(empty_file) as reader:
        chunks = [await fut for fut in reader.chunked(4096)]
    assert chunks == []


async def test_reader_compute_sha256_disabled(sample_file):
    async with Reader(sample_file, compute_sha256=False) as reader:
        chunks = [await fut for fut in reader.chunked(4096)]
    assert len(chunks) == 1
    assert chunks[0].sha256 == ""


async def test_reader_compute_sha256_enabled(sample_file):
    data = sample_file.read_bytes()
    async with Reader(sample_file, compute_sha256=True) as reader:
        chunks = [await fut for fut in reader.chunked(4096)]
    assert len(chunks) == 1
    assert chunks[0].sha256 == hashlib.sha256(data).hexdigest()


async def test_io_reader_reads_correctly(sample_file):
    data = sample_file.read_bytes()
    async with IOReader(sample_file) as reader:
        chunks = [await fut for fut in reader.chunked(4096)]
    assert b"".join(c.data for c in chunks) == data


@pytest.mark.skipif(
    not hasattr(os, "pread"),
    reason="os.pread not available",
)
async def test_unix_reader_reads_correctly(sample_file):
    data = sample_file.read_bytes()
    async with UnixReader(sample_file) as reader:
        chunks = [await fut for fut in reader.chunked(4096)]
    assert b"".join(c.data for c in chunks) == data


async def test_io_reader_concurrent_reads(tmp_path):
    p = tmp_path / "concurrent.bin"
    data = os.urandom(1024)
    p.write_bytes(data)

    async with IOReader(p) as reader:
        futures = list(reader.chunked(size=100))
        chunks = await asyncio.gather(*futures)
    assert b"".join(c.data for c in chunks) == data
