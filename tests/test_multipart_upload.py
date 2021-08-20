from http import HTTPStatus

import pytest

from aiohttp_s3_client import S3Client


async def test_multipart_file_upload(s3_client: S3Client, s3_read, tmp_path):
    data = b"hello, world" * 1024

    with (tmp_path / "hello.txt").open("wb") as f:
        f.write(data)
        f.flush()

        resp = await s3_client.put_file_multipart(
            "/test/test",
            f.name,
            part_size=1024,
        )

    assert (await s3_read("/test/test")) == data


@pytest.mark.parametrize("calculate_content_sha256", [True, False])
@pytest.mark.parametrize("workers_count", [1, 2])
async def test_multipart_stream_upload(
    calculate_content_sha256, workers_count,
    s3_client: S3Client, s3_read, tmp_path,
):

    def iterable():
        for _ in range(8):  # type: int
            yield b"hello world" * 1024

    resp = await s3_client.put_multipart(
        "/test/test",
        iterable(),
        calculate_content_sha256=calculate_content_sha256,
        workers_count=workers_count,
    )

    assert (await s3_read("/test/test")) == b"hello world" * 1024 * 8
