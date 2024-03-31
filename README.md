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
    async with await client.put("bucket/bytes", b"hello, world") as resp:
        assert resp.status == HTTPStatus.OK

    # Upload AsyncIterable to bucket "bucket" and key "iterable"
    async def gen():
        yield b'some bytes'

    async with client.put("bucket/file", gen()) as resp:
        assert resp.status == HTTPStatus.OK

    # Upload file to bucket "bucket" and key "file"
    async with client.put_file("bucket/file", "/path_to_file") as resp:
        assert resp.status == HTTPStatus.OK

    # Check object exists using bucket+key
    async with client.head("bucket/key") as resp:
        assert resp == HTTPStatus.OK

    # Get object by bucket+key
    async with client.get("bucket/key") as resp:
        data = await resp.read()

    # Delete object using bucket+key
    async with client.delete("bucket/key") as resp:
        assert resp == HTTPStatus.NO_CONTENT

    # List objects by prefix
    async for result, prefixes in client.list_objects_v2("bucket/", prefix="prefix"):
        # Each result is a list of metadata objects representing an object
        # stored in the bucket.  Each prefixes is a list of common prefixes
        do_work(result, prefixes)
```

Bucket may be specified as subdomain or in object name:

```python
import aiohttp
from aiohttp_s3_client import S3Client


client = S3Client(url="http://bucket.your-s3-host",
                  session=aiohttp.ClientSession())
async with client.put("key", gen()) as resp:
    ...

client = S3Client(url="http://your-s3-host",
                  session=aiohttp.ClientSession())
async with await client.put("bucket/key", gen()) as resp:
    ...

client = S3Client(url="http://your-s3-host/bucket",
                  session=aiohttp.ClientSession())
async with client.put("key", gen()) as resp:
    ...
```

Auth may be specified with keywords or in URL:
```python
import aiohttp
from aiohttp_s3_client import S3Client

client_credentials_as_kw = S3Client(
    url="http://your-s3-host",
    access_key_id="key_id",
    secret_access_key="access_key",
    session=aiohttp.ClientSession(),
)

client_credentials_in_url = S3Client(
    url="http://key_id:access_key@your-s3-host",
    session=aiohttp.ClientSession(),
)
```

## Credentials

By default `S3Client` trying to collect all available credentials from keyword
arguments like `access_key_id=` and `secret_access_key=`, after that from the
username and password from passed `url` argument, so the next step is environment
variables parsing and the last source for collection is the config file.

You can pass credentials explicitly using `aiohttp_s3_client.credentials`
module.

### `aiohttp_s3_client.credentials.StaticCredentials`

```python
import aiohttp
from aiohttp_s3_client import S3Client
from aiohttp_s3_client.credentials import StaticCredentials

credentials = StaticCredentials(
    access_key_id='aaaa',
    secret_access_key='bbbb',
    region='us-east-1',
)
client = S3Client(
    url="http://your-s3-host",
    session=aiohttp.ClientSession(),
    credentials=credentials,
)
```

### `aiohttp_s3_client.credentials.URLCredentials`

```python
import aiohttp
from aiohttp_s3_client import S3Client
from aiohttp_s3_client.credentials import URLCredentials

url = "http://key@hack-me:your-s3-host"
credentials = URLCredentials(url, region="us-east-1")
client = S3Client(
    url="http://your-s3-host",
    session=aiohttp.ClientSession(),
    credentials=credentials,
)
```

### `aiohttp_s3_client.credentials.EnvironmentCredentials`

```python
import aiohttp
from aiohttp_s3_client import S3Client
from aiohttp_s3_client.credentials import EnvironmentCredentials

credentials = EnvironmentCredentials(region="us-east-1")
client = S3Client(
    url="http://your-s3-host",
    session=aiohttp.ClientSession(),
    credentials=credentials,
)
```

### `aiohttp_s3_client.credentials.ConfigCredentials`

Using user config file:

```python
import aiohttp
from aiohttp_s3_client import S3Client
from aiohttp_s3_client.credentials import ConfigCredentials


credentials = ConfigCredentials()   # Will be used ~/.aws/credentials config
client = S3Client(
    url="http://your-s3-host",
    session=aiohttp.ClientSession(),
    credentials=credentials,
)
```

Using the custom config location:

```python
import aiohttp
from aiohttp_s3_client import S3Client
from aiohttp_s3_client.credentials import ConfigCredentials


credentials = ConfigCredentials("~/.my-custom-aws-credentials")
client = S3Client(
    url="http://your-s3-host",
    session=aiohttp.ClientSession(),
    credentials=credentials,
)
```

### `aiohttp_s3_client.credentials.merge_credentials`

This function collect all passed credentials instances and return a new one
which contains all non-blank fields from passed instances. The first argument
has more priority.


```python
import aiohttp
from aiohttp_s3_client import S3Client
from aiohttp_s3_client.credentials import (
    ConfigCredentials, EnvironmentCredentials, merge_credentials
)

credentials = merge_credentials(
    EnvironmentCredentials(),
    ConfigCredentials(),
)
client = S3Client(
    url="http://your-s3-host",
    session=aiohttp.ClientSession(),
    credentials=credentials,
)
```


### `aiohttp_s3_client.credentials.MetadataCredentials`

Trying to get credentials from the metadata service:

```python
import aiohttp
from aiohttp_s3_client import S3Client
from aiohttp_s3_client.credentials import MetadataCredentials

credentials = MetadataCredentials()

# start refresh credentials from metadata server
await credentials.start()
client = S3Client(
    url="http://your-s3-host",
    session=aiohttp.ClientSession(),
)
await credentials.stop()
```

## Multipart upload

For uploading large files [multipart uploading](https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html)
can be used. It allows you to asynchronously upload multiple parts of a file
to S3.
S3Client handles retries of part uploads and calculates part hash for integrity checks.

```python
import aiohttp
from aiohttp_s3_client import S3Client


client = S3Client(url="http://your-s3-host", session=aiohttp.ClientSession())
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
be changed during download with `ETag` header.
If your system supports `pwrite` syscall (Linux, macOS, etc.) it will be used to
write simultaneously to a single file. Otherwise, each worker will have own file
which will be concatenated after downloading.

```python
import aiohttp
from aiohttp_s3_client import S3Client


client = S3Client(url="http://your-s3-host", session=aiohttp.ClientSession())

await client.get_file_parallel(
    "dump/bigfile.csv",
    "/home/user/bigfile.csv",
    workers_count=8,
)
```
