aiohttp-compress
================

[![PyPI - License](https://img.shields.io/pypi/l/aiohttp-compress)](https://pypi.org/project/aiohttp-compress) [![Wheel](https://img.shields.io/pypi/wheel/aiohttp-compress)](https://pypi.org/project/aiohttp-compress) [![PyPI](https://img.shields.io/pypi/v/aiohttp-compress)](https://pypi.org/project/aiohttp-compress) [![PyPI](https://img.shields.io/pypi/pyversions/aiohttp-compress)](https://pypi.org/project/aiohttp-compress) [![Coverage Status](https://coveralls.io/repos/github/mosquito/aiohttp-compress/badge.svg?branch=master)](https://coveralls.io/github/mosquito/aiohttp-compress?branch=master) ![tox](https://github.com/mosquito/aiohttp-compress/workflows/tox/badge.svg?branch=master)

This module is the simplest way to enable compression support for `aiohttp` server applications globally.

Installation
------------

```bash
pip install aiohttp-compress
```

Example
-------

```python
from aiohttp import web
from aiohttp_s3_client import compress_middleware


async def handle(request):
    name = request.match_info.get(
        'name', "Anonymous"
    )
    text = "Hello, " + name
    return web.Response(text=text)


app = web.Application()
app.middlewares.append(compress_middleware)
app.add_routes([
    web.get('/', handle),
    web.get('/{name}', handle)
])

if __name__ == '__main__':
    web.run_app(app)
```
# aiohttp-s3-client
