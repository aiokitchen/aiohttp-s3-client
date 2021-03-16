aiohttp-s3-client
================

[![PyPI - License](https://img.shields.io/pypi/l/aiohttp-s3-client)](https://pypi.org/project/aiohttp-s3-client) [![Wheel](https://img.shields.io/pypi/wheel/aiohttp-s3-client)](https://pypi.org/project/aiohttp-s3-client) [![PyPI](https://img.shields.io/pypi/v/aiohttp-s3-client)](https://pypi.org/project/aiohttp-s3-client) [![PyPI](https://img.shields.io/pypi/pyversions/aiohttp-s3-client)](https://pypi.org/project/aiohttp-s3-client) [![Coverage Status](https://coveralls.io/repos/github/mosquito/aiohttp-s3-client/badge.svg?branch=master)](https://coveralls.io/github/mosquito/aiohttp-s3-client?branch=master) ![tox](https://github.com/mosquito/aiohttp-s3-client/workflows/tox/badge.svg?branch=master)

This module is the simplest way to enable compression support for `aiohttp` server applications globally.

Installation
------------

```bash
pip install aiohttp-s3-client
```

Usage
-------

```python
from http import HTTPStatus

from aiohttp import ClientSession
from aiohttp_s3_client import S3Client


async with ClientSession(raise_for_status=True) as session:
    client = S3Client(
        url="http://s3-url",
        session=session,
        access_key_id="key-id",
        secret_access_key="hackme"
    )

    # Upload str object to bucket "bucket" and key "str"
    resp = await client.put("/bucket/str", "hello, world")
    assert resp.status == HTTPStatus.OK

    # Upload bytes object to bucket "bucket" and key "bytes"
    resp = await client.put("/bucket/bytes", b"hello, world")
    assert resp.status == HTTPStatus.OK

    # Upload bytes object to bucket "bucket" and key "bytes"
    resp = await client.put_file("/path_to_file", "/bucket/file")
    assert resp.status == HTTPStatus.OK
```
