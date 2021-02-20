import os

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
        )


async def test_simple(s3_url: URL, s3_client: S3Client):
    async with s3_client.put("test/test", "hello world") as response:
        data = await response.read()

    assert data
