import asyncio
import hashlib
from types import SimpleNamespace

import pytest
from aiohttp import ClientSession, web
from aiohttp.test_utils import TestServer

from aiohttp_s3_client import S3Client
from aiohttp_s3_client.client import AwsUploadError, MultipartUploader


SUCCESS = (
    b"<CompleteMultipartUploadResult>"
    b"<ETag>ok</ETag></CompleteMultipartUploadResult>"
)
ERROR = (
    b"<Error><Code>InternalError</Code><Message>Please retry</Message></Error>"
)
DATA = b"test part"
SHA256 = hashlib.sha256(DATA).hexdigest()


@pytest.fixture
async def multipart_backend():
    state = SimpleNamespace(
        calls=[],
        completion_bodies=[SUCCESS],
        completion_status=200,
        abort_statuses=[204],
        create_status=200,
        part_status=200,
        abort_guard=lambda: None,
        completion_started=asyncio.Event(),
        release_completion=asyncio.Event(),
        pause_completion=False,
    )

    async def handle(request):
        await request.read()
        if request.method == "POST" and "uploads" in request.query:
            state.calls.append("create")
            return web.Response(
                status=state.create_status,
                body=b"<InitiateMultipartUploadResult><UploadId>upload-id</UploadId></InitiateMultipartUploadResult>",
            )
        assert request.query["uploadId"] == "upload-id"
        if request.method == "PUT":
            state.calls.append("part")
            return web.Response(
                status=state.part_status, headers={"ETag": '"part-etag"'}
            )
        if request.method == "POST":
            state.calls.append("complete")
            state.completion_started.set()
            if state.pause_completion:
                await state.release_completion.wait()
            payload = state.completion_bodies[0]
            if len(state.completion_bodies) > 1:
                state.completion_bodies.pop(0)
            return web.Response(status=state.completion_status, body=payload)
        assert request.method == "DELETE"
        state.abort_guard()
        state.calls.append("abort")
        status = state.abort_statuses[0]
        if len(state.abort_statuses) > 1:
            state.abort_statuses.pop(0)
        return web.Response(status=status)

    app = web.Application()
    app.router.add_route("*", "/{key:.*}", handle)
    async with TestServer(app, host="127.0.0.1") as server:
        async with ClientSession() as session:
            client = S3Client(
                session=session,
                url=server.make_url("/bucket"),
                access_key_id="test-key",
                secret_access_key="test-secret",
                region="us-east-1",
            )
            try:
                yield client, state
            finally:
                state.release_completion.set()


@pytest.mark.parametrize(
    "payload",
    [
        SUCCESS,
        b' \n<?xml version="1.0"?><CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"/>',
    ],
)
async def test_completion_accepts_success_xml(multipart_backend, payload):
    client, state = multipart_backend
    state.completion_bodies = [payload]
    async with client.multipart_upload(object_name="key") as upload:
        await upload.put_part(data=DATA, content_sha256=SHA256)
    assert state.calls == ["create", "part", "complete"]


@pytest.mark.parametrize(
    "payload",
    [
        ERROR,
        b'<Error xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Code>InternalError</Code></Error>',
        b"",
        b"<broken",
        b"<Unexpected/>",
    ],
)
async def test_completion_error_retries_then_aborts(multipart_backend, payload):
    client, state = multipart_backend
    state.completion_bodies = [payload]
    with pytest.raises(AwsUploadError):
        async with client.multipart_upload(object_name="key") as upload:
            await upload.put_part(data=DATA, content_sha256=SHA256)
    assert state.calls == [
        "create",
        "part",
        "complete",
        "complete",
        "complete",
        "abort",
    ]


async def test_embedded_error_can_recover(multipart_backend):
    client, state = multipart_backend
    state.completion_bodies = [ERROR, SUCCESS]
    async with client.multipart_upload(object_name="key") as upload:
        await upload.put_part(data=DATA, content_sha256=SHA256)
    assert state.calls == ["create", "part", "complete", "complete"]


async def test_body_error_is_preserved_when_abort_fails(
    multipart_backend, caplog
):
    client, state = multipart_backend
    state.abort_statuses = [503]
    original = ValueError("source read failed")
    with pytest.raises(ValueError) as raised:
        async with client.multipart_upload(object_name="key"):
            raise original
    assert raised.value is original
    assert state.calls == ["create", "abort", "abort", "abort"]
    assert "Failed to abort multipart upload" in caplog.text


@pytest.mark.parametrize("statuses", [[204], [404], [503, 204]])
async def test_explicit_abort_skips_completion(multipart_backend, statuses):
    client, state = multipart_backend
    state.abort_statuses = list(statuses)
    async with client.multipart_upload(object_name="key") as upload:
        await upload.abort()
    assert state.calls == ["create"] + ["abort"] * len(statuses)


async def test_manual_completion_is_not_repeated(multipart_backend):
    client, state = multipart_backend
    async with client.multipart_upload(object_name="key") as upload:
        await upload.put_part(data=DATA, content_sha256=SHA256)
        await upload.complete()
    assert state.calls == ["create", "part", "complete"]


@pytest.mark.parametrize("during_completion", [False, True])
async def test_task_cancellation_aborts(multipart_backend, during_completion):
    client, state = multipart_backend
    entered = asyncio.Event()
    state.pause_completion = during_completion

    async def operation():
        async with client.multipart_upload(object_name="key") as upload:
            await upload.put_part(data=DATA, content_sha256=SHA256)
            entered.set()
            if not during_completion:
                await asyncio.Future()

    task = asyncio.create_task(operation())
    try:
        await asyncio.wait_for(
            state.completion_started.wait()
            if during_completion
            else entered.wait(),
            timeout=2,
        )
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=2)
    finally:
        state.release_completion.set()
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
    assert state.calls[-1] == "abort"
    assert state.calls.count("complete") == int(during_completion)


@pytest.mark.parametrize("phase", ["create", "part", "complete"])
async def test_http_failure_aborts_only_created_upload(
    multipart_backend, phase
):
    client, state = multipart_backend
    setattr(
        state,
        f"{phase}_status" if phase != "complete" else "completion_status",
        503,
    )
    with pytest.raises(AwsUploadError):
        async with client.multipart_upload(object_name="key") as upload:
            await upload.put_part(data=DATA, content_sha256=SHA256)
    assert state.calls.count("abort") == int(phase != "create")


@pytest.mark.parametrize("from_file", [False, True])
async def test_parallel_parts_stop_before_abort(
    multipart_backend, monkeypatch, tmp_path, from_file
):
    client, state = multipart_backend
    active = set()
    second_started = asyncio.Event()
    stopped = []
    original = ValueError("part failed")

    async def part_uploader(self, part_no, data, content_sha256, **kwargs):
        active.add(part_no)
        try:
            if part_no == 1:
                await second_started.wait()
                raise original
            second_started.set()
            await asyncio.Future()
        finally:
            active.remove(part_no)
            stopped.append(part_no)

    monkeypatch.setattr(MultipartUploader, "_part_uploader", part_uploader)

    def assert_stopped():
        assert not active
        assert {1, 2} <= set(stopped)

    state.abort_guard = assert_stopped
    with pytest.raises(ValueError) as raised:
        if from_file:
            path = tmp_path / "parts.bin"
            path.write_bytes(b"x" * 12)
            await client.put_file_multipart(
                object_name="key",
                file_path=path,
                part_size=4,
                workers_count=2,
            )
        else:
            await client.put_multipart(
                object_name="key",
                data=[b"x" * 12],
                part_size=4,
                workers_count=2,
            )
    assert raised.value is original
    assert state.calls == ["create", "abort"]


@pytest.mark.parametrize("from_file", [False, True])
async def test_parallel_upload_completes(
    multipart_backend, tmp_path, from_file
):
    client, state = multipart_backend
    if from_file:
        path = tmp_path / "parts.bin"
        path.write_bytes(b"x" * 12)
        await client.put_file_multipart(
            object_name="key",
            file_path=path,
            part_size=4,
            workers_count=2,
        )
    else:
        await client.put_multipart(
            object_name="key",
            data=[b"x" * 12],
            part_size=4,
            workers_count=2,
        )
    assert state.calls == ["create", "part", "part", "part", "complete"]
