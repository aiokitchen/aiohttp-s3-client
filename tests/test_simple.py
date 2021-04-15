import gzip
import os
import sys
from http import HTTPStatus
from io import BytesIO
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Iterable

import pytest
from aiohttp import ClientSession
from yarl import URL

from aiohttp_s3_client import S3Client


@pytest.fixture()
async def s3_url() -> URL:
    return URL(os.getenv("S3_URL", "http://user:hackme@localhost:8000/"))


@pytest.fixture
async def s3_client(loop, s3_url: URL):
    async with ClientSession(
        raise_for_status=True, auto_decompress=False
    ) as session:
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
        async with s3_client.put_file("/test/test", f.name) as response:
            assert response.status == HTTPStatus.OK

        async with s3_client.get("/test/test") as response:
            result = await response.read()
            assert result == data

        # Test upload by file Path
        async with s3_client.put_file("/test/test2", Path(f.name)) as response:
            assert response.status == HTTPStatus.OK

        async with s3_client.get("/test/test2") as response:
            result = await response.read()
            assert result == data


async def test_url_path_with_colon(s3_url: URL, s3_client: S3Client):
    data = b"hello, world"
    key = "/some-path:with-colon.txt"
    async with s3_client.put(key, data) as response:
        assert response.status == HTTPStatus.OK

    async with s3_client.get(key) as response:
        result = await response.read()
        assert result == data


@pytest.mark.parametrize("object_name", ("test/test", "/test/test"))
async def test_put_compression(s3_url: URL, s3_client: S3Client, object_name):
    async def async_iterable(iterable: Iterable[bytes]):
        for i in iterable:  # type: int
            yield i.to_bytes(1, sys.byteorder)

    data = b"hello, world"
    async with s3_client.put(
        object_name, async_iterable(data), compress="gzip",
    ) as response:
        assert response.status == HTTPStatus.OK

    async with s3_client.get(object_name) as response:
        result = await response.read()
        # assert response.headers[hdrs.CONTENT_ENCODING] == "gzip"
        # FIXME: uncomment after update fakes3 image
        actual = gzip.GzipFile(fileobj=BytesIO(result)).read()
        assert actual == data
