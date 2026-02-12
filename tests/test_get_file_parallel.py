import asyncio
import os
import secrets

import pytest

from aiohttp_s3_client import S3Client
from aiohttp_s3_client.client import AwsDownloadError, AwsError


async def test_get_file_parallel(s3_client: S3Client, tmpdir):
    data = b"Hello world! " * 100
    object_name = "foo/bar.txt"
    await s3_client.put(object_name, data)
    await s3_client.get_file_parallel(
        object_name,
        tmpdir / "bar.txt",
        workers_count=4,
    )
    assert (tmpdir / "bar.txt").read_binary() == data


async def test_get_file_that_changed_in_process_error(
    s3_client: S3Client,
    tmpdir,
):
    object_name = "test/test"

    def iterable():
        for _ in range(8):  # type: int
            yield secrets.token_hex(1024 * 1024 * 5).encode()

    await s3_client.put_multipart(
        object_name,
        iterable(),
        workers_count=4,
    )

    async def upload():
        await asyncio.sleep(0.05)
        await s3_client.put_multipart(
            object_name,
            iterable(),
            workers_count=4,
        )

    with pytest.raises(AwsError) as err:
        await asyncio.gather(
            s3_client.get_file_parallel(
                object_name,
                tmpdir / "temp.dat",
                workers_count=4,
                range_step=128,
            ),
            upload(),
        )

    assert err.type is AwsDownloadError
    assert err.value.message.startswith(
        "Got wrong status code 412 on range download of test/test",
    )
    assert not os.path.exists(tmpdir / "temp.dat")
