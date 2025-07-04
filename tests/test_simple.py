import asyncio
import gzip
import sys
from http import HTTPStatus
from io import BytesIO
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from freezegun import freeze_time
from yarl import URL

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
    async def async_iterable(iterable: bytes):
        for i in iterable:
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


async def test_list_objects_v2(s3_client: S3Client, s3_read, tmp_path):
    data = b"hello, world"

    with (tmp_path / "hello.txt").open("wb") as f:
        f.write(data)
        f.flush()

        resp = await s3_client.put_file("/test/list/test1", f.name)
        assert resp.status == HTTPStatus.OK

        resp = await s3_client.put_file("/test/list/test2", f.name)
        assert resp.status == HTTPStatus.OK

        # Test list file
        batch = 0
        async for result, prefixes in s3_client.list_objects_v2(
            prefix="test/list/",
            delimiter="/",
            max_keys=1,
        ):
            batch += 1
            assert result[0].key == f"test/list/test{batch}"
            assert result[0].size == len(data)


async def test_list_objects_v2_prefix(s3_client: S3Client, s3_read, tmp_path):
    data = b"hello, world"

    with (tmp_path / "hello.txt").open("wb") as f:
        f.write(data)
        f.flush()

        resp = await s3_client.put_file("/test2/list1/test1", f.name)
        assert resp.status == HTTPStatus.OK

        resp = await s3_client.put_file("/test2/list2/test2", f.name)
        assert resp.status == HTTPStatus.OK

        # Test list file
        batch = 0

        async for result, prefixes in s3_client.list_objects_v2(
            prefix="test2/",
            delimiter="/",
        ):
            batch += 1
            assert len(result) == 0
            assert prefixes[0] == "test2/list1/"
            assert prefixes[1] == "test2/list2/"


async def test_url_path_with_colon(s3_client: S3Client, s3_read):
    data = b"hello, world"
    key = "/some-path:with-colon.txt"
    resp = await s3_client.put(key, data)
    assert resp.status == HTTPStatus.OK

    assert (await s3_read(key)) == data


@pytest.mark.parametrize("object_name", ("test/test", "/test/test"))
async def test_put_compression(s3_client: S3Client, s3_read, object_name):
    async def async_iterable(iterable: bytes):
        for i in iterable:
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


@pytest.mark.parametrize('method,given_url', [
    # Simple test
    ('GET', 'test/object'),

    # Check method name is ok in lowercase
    ('get', 'test/object'),

    # Absolute path
    ('GET', '/test/object'),

    # Check URL object with path is given
    ('get', URL('./test/object')),

    # Check URL object with path is given
    ('get', URL('/test/object')),
])
def test_presign_non_absolute_url(s3_client, s3_url, method, given_url):
    presigned = s3_client.presign_url('get', 'test/object')
    assert presigned.is_absolute()
    assert presigned.scheme == s3_url.scheme
    assert presigned.host == s3_url.host
    assert presigned.port == s3_url.port
    assert presigned.path == (s3_url / str(given_url).lstrip('/')).path


@pytest.mark.parametrize('method,given_url', [
    # String url
    ('GET', 'https://absolute-url:123/path'),

    # URL object
    ('GET', URL('https://absolute-url:123/path')),
])
def test_presign_absolute_url(s3_client, method, given_url):
    presigned = s3_client.presign_url(method, given_url)
    assert presigned.is_absolute()

    url_object = URL(given_url)
    assert presigned.scheme == url_object.scheme
    assert presigned.host == url_object.host
    assert presigned.port == url_object.port
    assert presigned.path == url_object.path


@freeze_time("2024-01-01")
def test_presign_url(s3_client, s3_url):
    url = s3_client.presign_url('get', URL('./example'))
    assert url.path == (s3_url / 'example').path
    assert url.query == {
        'X-Amz-Algorithm': 'AWS4-HMAC-SHA256',
        'X-Amz-Content-Sha256': 'UNSIGNED-PAYLOAD',
        'X-Amz-Credential': 'user/20240101/us-east-1/s3/aws4_request',
        'X-Amz-Date': '20240101T000000Z',
        'X-Amz-Expires': '86400',
        'X-Amz-SignedHeaders': 'host',
        'X-Amz-Signature': (
            '7359f1286edf554b0eab363e3c93ee32855b8d429d975fdb0a5d2cb7ad5c0db0'
        )
    }


def test_zero_file_upload_chunked(s3_client: S3Client, s3_url):
    s3_client._session = MagicMock(s3_client._session)
    s3_client.request("POST", "/blank", data=b"")
    assert s3_client._session.request.call_count == 1
    call = s3_client._session.request.mock_calls[-1]
    assert call.kwargs.get("chunked", True)


def test_zero_file_upload_not_chunked(s3_client: S3Client, s3_url):
    s3_client._session = MagicMock(s3_client._session)
    s3_client.request("POST", "/blank", data=b"", data_length=0)
    assert s3_client._session.request.call_count == 1
    call = s3_client._session.request.mock_calls[-1]
    assert call.kwargs.get("chunked", None) is None


def test_chunked_and_signature(s3_client: S3Client, s3_url):
    s3_client._session = MagicMock(s3_client._session)

    async def gen():
        await asyncio.sleep(0.0)
        yield b""

    s3_client.request("POST", "/blank", data=gen())
    assert s3_client._session.request.call_count == 1
    call = s3_client._session.request.mock_calls[-1]
    assert call.kwargs.get("chunked") is True
    headers = call.kwargs.get("headers")
    trealer = 'STREAMING-UNSIGNED-PAYLOAD-TRAILER'
    assert headers.get('x-amz-content-sha256') == trealer
