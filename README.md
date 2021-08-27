aiohttp-s3-client
================

[![PyPI - License](https://img.shields.io/pypi/l/aiohttp-s3-client)](https://pypi.org/project/aiohttp-s3-client) [![Wheel](https://img.shields.io/pypi/wheel/aiohttp-s3-client)](https://pypi.org/project/aiohttp-s3-client) [![Mypy](http://www.mypy-lang.org/static/mypy_badge.svg)]() [![PyPI](https://img.shields.io/pypi/v/aiohttp-s3-client)](https://pypi.org/project/aiohttp-s3-client) [![PyPI](https://img.shields.io/pypi/pyversions/aiohttp-s3-client)](https://pypi.org/project/aiohttp-s3-client) [![Coverage Status](https://coveralls.io/repos/github/mosquito/aiohttp-s3-client/badge.svg?branch=master)](https://coveralls.io/github/mosquito/aiohttp-s3-client?branch=master) ![tox](https://github.com/mosquito/aiohttp-s3-client/workflows/tox/badge.svg?branch=master)

The simple module for putting and getting object from Amazon S3 compatible endpoints

## Installation

```bash
pip install aiohttp-s3-client
```

## Usage

```python
from http import HTTPStatus

from aiohttp import ClientSession
from aiohttp_s3_client import S3Client


async with ClientSession(raise_for_status=True) as session:
    client = S3Client(
        url="http://s3-url",
        session=session,
        access_key_id="key-id",
        secret_access_key="hackme",
        region="us-east-1"
    )

    # Upload str object to bucket "bucket" and key "str"
    async with client.put("bucket/str", "hello, world") as resp:
        assert resp.status == HTTPStatus.OK

    # Upload bytes object to bucket "bucket" and key "bytes"
    resp = await client.put("bucket/bytes", b"hello, world")
    assert resp.status == HTTPStatus.OK

    # Upload AsyncIterable to bucket "bucket" and key "iterable"
    async def gen():
        yield b'some bytes'

    resp = await client.put("bucket/file", gen())
    assert resp.status == HTTPStatus.OK

    # Upload file to bucket "bucket" and key "file"
    resp = await client.put_file("bucket/file", "/path_to_file")
    assert resp.status == HTTPStatus.OK

    # Check object exists using bucket+key
    resp = await client.head("bucket/key")
    assert resp == HTTPStatus.OK

    # Get object by bucket+key
    resp = await client.get("bucket/key")
    data = await resp.read()

    # Delete object using bucket+key
    resp = await client.delete("bucket/key")
    assert resp == HTTPStatus.NO_CONTENT
```

Bucket may be specified as subdomain or in object name:
```python
client = S3Client(url="http://bucket.your-s3-host", ...)
resp = await client.put("key", gen())

client = S3Client(url="http://your-s3-host", ...)
resp = await client.put("bucket/key", gen())

client = S3Client(url="http://your-s3-host/bucket", ...)
resp = await client.put("key", gen())
```

Auth may be specified with keywords or in URL:
```python
client = S3Client(url="http://your-s3-host", access_key_id="key_id",
                  secret_access_key="access_key", ...)

client = S3Client(url="http://key_id:access_key@your-s3-host", ...)
```

## Multipart upload

For uploading large files [multipart uploading](https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html)
can be used. It allows you to asynchronously upload multiple parts of a file
to S3.
S3Client handles retries of part uploads and calculates part hash for integrity checks.

```python
client = S3Client()
await client.put_file_multipart(
    "test/bigfile.csv",
    headers={
    	"Content-Type": "text/csv",
    },
    workers_count=8,
)
```

## Parallel download to file

S3 supports `GET` requests with `Range` header. It's possible to download
objects in parallel with multiple connections for speedup.
S3Client handles retries of partial requests and makes sure that file won't
changed during download with `ETag` header.
If your system supports `pwrite` syscall (linux, macos, etc) it will be used to
write simultaneously to a single file. Otherwise, each worker will have own file
which will be concatenated after downloading.

```python
client = S3Client()
await client.get_file_parallel(
    "dump/bigfile.csv",
    "/home/user/bigfile.csv",
    workers_count=8,
)
```
