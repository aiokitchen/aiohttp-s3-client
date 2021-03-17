import os
import sys
from http import HTTPStatus
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Iterable

import pytest
from aiohttp import ClientSession
from yarl import URL

from aiohttp_s3_client import S3Client


@pytest.fixture()
async def s3_url() -> URL:
    return URL(os.getenv("S3_URL"))


@pytest.fixture
async def s3_client(loop, s3_url: URL):
    async with ClientSession(raise_for_status=True) as session:
        yield S3Client(url=s3_url, session=session)


@pytest.mark.parametrize("object_name", ("test/test", "/test/test"))
async def test_put_str(s3_url: URL, s3_client: S3Client, object_name):
    data = "hello, world"
    async with s3_client.put(object_name, data) as response:
        assert response.status == HTTPStatus.OK

    async with s3_client.get(object_name) as response:
        result = await response.text()
        assert result == data


@pytest.mark.parametrize("object_name", ("test/test", "/test/test"))
async def test_put_bytes(s3_url: URL, s3_client: S3Client, object_name):
    data = b"hello, world"
    async with s3_client.put(object_name, data) as response:
        assert response.status == HTTPStatus.OK

    async with s3_client.get(object_name) as response:
        result = await response.read()
        assert result == data


@pytest.mark.parametrize("object_name", ("test/test", "/test/test"))
async def test_put_async_iterable(s3_url: URL, s3_client: S3Client,
                                  object_name):
    async def async_iterable(iterable: Iterable[bytes]):
        for i in iterable:  # type: int
            yield i.to_bytes(1, sys.byteorder)

    data = b"hello, world"
    async with s3_client.put(object_name, async_iterable(data)) as response:
        assert response.status == HTTPStatus.OK

    async with s3_client.get(object_name) as response:
        result = await response.read()
        assert result == data


async def test_put_file(s3_url: URL, s3_client: S3Client):
    data = b"hello, world"

    with NamedTemporaryFile() as f:
        f.write(data)
        f.flush()

        # Test upload by file str path
        async with s3_client.put_file(f.name, "/test/test") as response:
            assert response.status == HTTPStatus.OK

        async with s3_client.get("/test/test") as response:
            result = await response.read()
            assert result == data

        # Test upload by file Path
        async with s3_client.put_file(Path(f.name), "/test/test2") as response:
            assert response.status == HTTPStatus.OK

        async with s3_client.get("/test/test2") as response:
            result = await response.read()
            assert result == data
