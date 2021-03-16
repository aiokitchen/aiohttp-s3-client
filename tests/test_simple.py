import os
import sys
from http import HTTPStatus
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
        yield S3Client(
            url=s3_url,
            session=session,
            access_key_id="tester",
            secret_access_key="hackme"
        )


async def test_put_str(s3_url: URL, s3_client: S3Client):
    data = "hello, world"
    async with s3_client.put("test/test", data) as response:
        assert response.status == HTTPStatus.OK

    async with s3_client.get("test/test") as response:
        result = await response.text()
        assert result == data


async def test_put_bytes(s3_url: URL, s3_client: S3Client):
    data = b"hello, world"
    async with s3_client.put("test/test", data) as response:
        assert response.status == HTTPStatus.OK

    async with s3_client.get("test/test") as response:
        result = await response.read()
        assert result == data


async def test_put_async_iterable(s3_url: URL, s3_client: S3Client):
    async def async_iterable(iterable: Iterable[bytes]):
        for i in iterable:  # type: int
            yield i.to_bytes(1, sys.byteorder)

    data = b"hello, world"
    async with s3_client.put("test/test", async_iterable(data)) as response:
        assert response.status == HTTPStatus.OK

    async with s3_client.get("test/test") as response:
        result = await response.read()
        assert result == data
