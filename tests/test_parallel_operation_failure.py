from http import HTTPStatus

import pytest
from aiohttp import hdrs, web
from aiohttp.test_utils import TestServer
from yarl import URL

from aiohttp_s3_client.client import AwsDownloadError, AwsUploadError


@pytest.mark.timeout(1)
async def test_multipart_upload_failure(s3_client, s3_mock_calls):

    def iterable():
        for _ in range(8):  # type: int
            yield b"hello world" * 1024

    with pytest.raises(AwsUploadError):
        await s3_client.put_multipart(
            "/test/test",
            iterable(),
            workers_count=4,
            part_upload_tries=3,
        )


# @pytest.mark.timeout(1)
async def test_parallel_download_failure(s3_client, s3_mock_calls, tmpdir):
    with pytest.raises(AwsDownloadError):
        await s3_client.get_file_parallel(
            "foo/bar.txt",
            tmpdir / "bar.txt",
            workers_count=4,
        )


@pytest.fixture
def s3_url(s3_mock_url):
    return s3_mock_url


CREATE_MP_UPLOAD_RESPONSE = """\
<?xml version="1.0" encoding="UTF-8"?>
<InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <UploadId>EXAMPLEJZ6e0YupT2h66iePQCc9IEbYbDUy4RTpMeoSMLPRp8Z5o1u8feSRonpvnWsKKG35tI2LB9VDPiCgTy.Gq2VxQLYjrue4Nq.NBdqI-</UploadId>
</InitiateMultipartUploadResult>
"""


@pytest.fixture
def s3_mock_port(aiomisc_unused_port_factory) -> int:
    return aiomisc_unused_port_factory()


@pytest.fixture
def s3_mock_url(s3_mock_port, localhost):
    return URL.build(
        scheme="http",
        host=localhost,
        port=s3_mock_port,
        user="user",
        password="hackme",
    )


@pytest.fixture
async def s3_mock_server(s3_mock_port, localhost):
    app = web.Application()
    app["calls"] = []
    app.router.add_put("/{key:[^{}]+}", s3_mock_put_object_handler)
    app.router.add_post("/{key:[^{}]+}", s3_mock_post_object_handler)
    app.router.add_get("/{key:[^{}]+}", s3_mock_get_object_handler)
    # Not used right now but tests failed with it
    # app.router.add_head("/{key:[^{}]+}", s3_mock_head_object_handler)
    server = TestServer(app, host=localhost, port=s3_mock_port)
    await server.start_server()
    try:
        yield server
    finally:
        await server.close()


@pytest.fixture
def s3_mock_calls(s3_mock_server):
    return s3_mock_server.app["calls"]


async def s3_mock_put_object_handler(request):
    request.payload = await request.read()
    request.app["calls"].append(request)
    return web.Response(
        body=b"",
        status=HTTPStatus.INTERNAL_SERVER_ERROR,
    )


async def s3_mock_post_object_handler(request):
    request.payload = await request.read()
    request.app["calls"].append(request)

    if "uploads" in request.query:
        return web.Response(body=CREATE_MP_UPLOAD_RESPONSE)

    return web.Response(body=b"")


async def s3_mock_head_object_handler(request):
    return web.Response(
        headers={
            hdrs.CONTENT_LENGTH: str((1024**2) * 16),
            hdrs.ETAG: "7e10e7d25dc4581d89b9285be5f384fd",
        },
    )


async def s3_mock_get_object_handler(request):
    return web.Response(
        status=HTTPStatus.INTERNAL_SERVER_ERROR,
        headers={
            hdrs.CONTENT_LENGTH: str((1024**2) * 5),
            hdrs.ETAG: "7e10e7d25dc4581d89b9285be5f384fd",
        },
    )
