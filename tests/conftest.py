import os
from typing import Optional

import pytest
from aiohttp import ClientSession
from yarl import URL

from aiohttp_s3_client import S3Client


@pytest.fixture
async def s3_client(event_loop, s3_url: URL):
    async with ClientSession(
        raise_for_status=False, auto_decompress=False,
    ) as session:
        yield S3Client(
            url=s3_url,
            region="us-east-1",
            session=session,
        )


@pytest.fixture
async def s3_url() -> URL:
    return URL(os.getenv("S3_URL", "http://user:hackme@localhost:8000/"))


@pytest.fixture
def object_name() -> str:
    return "/test/test"


@pytest.fixture
def s3_read(s3_client: S3Client, object_name):

    async def do_read(custom_object_name: Optional[str] = None):
        s3_key = custom_object_name or object_name
        return await (await s3_client.get(s3_key)).read()

    return do_read
