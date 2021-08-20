import gzip
import sys
from http import HTTPStatus
from io import BytesIO
from pathlib import Path
from typing import Iterable

import pytest

from aiohttp_s3_client import S3Client


@pytest.mark.parametrize("object_name", ("test/test", "/test/test"))
async def test_put_str(s3_client: S3Client, object_name):
    data = "hello, world"
    resp = await s3_client.put(object_name, data)
    assert resp.status == HTTPStatus.OK

    resp = await s3_client.get(object_name)
    result = await resp.text()
    assert result == data


@pytest.mark.parametrize("object_name", ("test/test", "/test/test"))
async def test_put_bytes(s3_client: S3Client, s3_read, object_name):
    data = b"hello, world"
    resp = await s3_client.put(object_name, data)
    assert resp.status == HTTPStatus.OK
    assert (await s3_read()) == data


@pytest.mark.parametrize("object_name", ("test/test", "/test/test"))
async def test_put_async_iterable(s3_client: S3Client, s3_read, object_name):
    async def async_iterable(iterable: Iterable[bytes]):
        for i in iterable:  # type: int
            yield i.to_bytes(1, sys.byteorder)

    data = b"hello, world"
    resp = await s3_client.put(object_name, async_iterable(data))
    assert resp.status == HTTPStatus.OK

    assert (await s3_read()) == data


async def test_put_file(s3_client: S3Client, s3_read, tmp_path):
    data = b"hello, world"

    with (tmp_path / "hello.txt").open("wb") as f:
        f.write(data)
        f.flush()

        # Test upload by file str path
        resp = await s3_client.put_file("/test/test", f.name)
        assert resp.status == HTTPStatus.OK

        assert (await s3_read("/test/test")) == data

        # Test upload by file Path
        resp = await s3_client.put_file("/test/test2", Path(f.name))
        assert resp.status == HTTPStatus.OK

        assert (await s3_read("/test/test2")) == data


async def test_url_path_with_colon(s3_client: S3Client, s3_read):
    data = b"hello, world"
    key = "/some-path:with-colon.txt"
    resp = await s3_client.put(key, data)
    assert resp.status == HTTPStatus.OK

    assert (await s3_read(key)) == data


@pytest.mark.parametrize("object_name", ("test/test", "/test/test"))
async def test_put_compression(s3_client: S3Client, s3_read, object_name):
    async def async_iterable(iterable: Iterable[bytes]):
        for i in iterable:  # type: int
            yield i.to_bytes(1, sys.byteorder)

    data = b"hello, world"
    resp = await s3_client.put(
        object_name, async_iterable(data), compress="gzip",
    )
    assert resp.status == HTTPStatus.OK

    result = await s3_read()
    # assert resp.headers[hdrs.CONTENT_ENCODING] == "gzip"
    # FIXME: uncomment after update fakes3 image
    actual = gzip.GzipFile(fileobj=BytesIO(result)).read()
    assert actual == data
