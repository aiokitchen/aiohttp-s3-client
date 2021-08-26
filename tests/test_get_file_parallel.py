from aiohttp_s3_client import S3Client


async def test_get_file_parallel(s3_client: S3Client, tmpdir):
    data = b"Hello world! " * 100
    object_name = "foo/bar.txt"
    resp = await s3_client.put(object_name, data)
    await s3_client.get_file_parallel(
        object_name,
        tmpdir / "bar.txt",
        workers_count=4,
    )
    assert (tmpdir / "bar.txt").read_binary() == data


async def test_get_file_parallel_without_pwrite(
    s3_client: S3Client, tmpdir, monkeypatch,
):
    monkeypatch.delattr("os.pwrite")
    data = b"Hello world! " * 100
    object_name = "foo/bar.txt"
    resp = await s3_client.put(object_name, data)
    await s3_client.get_file_parallel(
        object_name,
        tmpdir / "bar.txt",
        workers_count=4,
    )
    assert (tmpdir / "bar.txt").read_binary() == data
