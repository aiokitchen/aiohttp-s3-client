import pytest
from aiohttp import ClientSession

from aiohttp_s3_client import S3Client


def iterable():
    for _ in range(8):  # type: int
        yield b"hello world" * 1024


async def test_multipart_file_upload(s3_client: S3Client, s3_read, tmp_path):
    data = b"hello, world" * 1024

    with (tmp_path / "hello.txt").open("wb") as f:
        f.write(data)
        f.flush()

        await s3_client.put_file_multipart(
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
    await s3_client.put_multipart(
        "/test/test",
        iterable(),
        calculate_content_sha256=calculate_content_sha256,
        workers_count=workers_count,
    )

    assert (await s3_read("/test/test")) == b"hello world" * 1024 * 8


async def test_multipart_hooks(
    s3_url, s3_read, tmp_path,
):

    parts_uploaded = 0
    workers_started = 0

    async def on_multipart_worker_start(ctx):
        nonlocal workers_started
        workers_started += 1
        return {}

    async def on_part_upload(ctx):
        nonlocal parts_uploaded
        parts_uploaded += 1
        return {}

    async with ClientSession(
        raise_for_status=False, auto_decompress=False,
    ) as session:
        s3_client = S3Client(
            url=s3_url,
            region="us-east-1",
            session=session,
            on_part_upload=on_part_upload,
            on_multipart_worker_start=on_multipart_worker_start,
        )
        await s3_client.put_multipart(
            "/test/test",
            iterable(),
            workers_count=4,
        )

    assert (await s3_read("/test/test")) == b"hello world" * 1024 * 8
    assert parts_uploaded == 8
    assert workers_started == 4
