import asyncio
import hashlib
from types import SimpleNamespace
from xml.etree import ElementTree as ET

import pytest
from aiohttp import ClientSession, TraceConfig, web
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
def raise_for_status():
    return False


@pytest.fixture
async def multipart_backend(raise_for_status):
    state = SimpleNamespace(
        calls=[],
        uploaded_parts={},
        completed_parts=[],
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
        payload = await request.read()
        if request.method == "POST" and "uploads" in request.query:
            state.calls.append("create")
            return web.Response(
                status=state.create_status,
                body=b"<InitiateMultipartUploadResult><UploadId>upload-id</UploadId></InitiateMultipartUploadResult>",
            )
        assert request.query["uploadId"] == "upload-id"
        if request.method == "PUT":
            state.calls.append("part")
            state.uploaded_parts[int(request.query["partNumber"])] = payload
            return web.Response(
                status=state.part_status, headers={"ETag": '"part-etag"'}
            )
        if request.method == "POST":
            state.calls.append("complete")
            state.completed_parts = []
            for part in ET.fromstring(payload).findall(".//{*}PartNumber"):
                assert part.text is not None
                state.completed_parts.append(int(part.text))
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
        async with ClientSession(raise_for_status=raise_for_status) as session:
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


@pytest.mark.parametrize("raise_for_status", [False, True])
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


@pytest.mark.parametrize("raise_for_status", [False, True])
@pytest.mark.parametrize("statuses", [[204], [404], [503, 204], [503, 404]])
async def test_explicit_abort_skips_completion(multipart_backend, statuses):
    client, state = multipart_backend
    state.abort_statuses = list(statuses)
    async with client.multipart_upload(object_name="key") as upload:
        await upload.abort()
    assert state.calls == ["create"] + ["abort"] * len(statuses)
    assert upload._upload_id is None


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
        path.write_bytes(b"abcdefghijkl")
        await client.put_file_multipart(
            object_name="key",
            file_path=path,
            part_size=4,
            workers_count=2,
        )
    else:
        await client.put_multipart(
            object_name="key",
            data=[b"abcdefghijkl"],
            part_size=4,
            workers_count=2,
        )
    assert state.calls == ["create", "part", "part", "part", "complete"]

    assert state.uploaded_parts == {1: b"abcd", 2: b"efgh", 3: b"ijkl"}
    assert state.completed_parts == [1, 2, 3]


@pytest.mark.parametrize("from_file", [False, True])
async def test_cancellation_preserves_request_cleanup(tmp_path, from_file):
    requests_started = asyncio.Event()
    release_requests = asyncio.Event()
    release_cleanup = asyncio.Event()
    parts = set()
    events = []
    failed_requests = 0

    async def handle(request):
        await request.read()

        if request.method == "POST" and "uploads" in request.query:
            return web.Response(
                body=b"<InitiateMultipartUploadResult><UploadId>upload-id</UploadId></InitiateMultipartUploadResult>",
            )

        if request.method == "PUT":
            parts.add(request.query["partNumber"])
            if len(parts) == 2:
                requests_started.set()

            await release_requests.wait()
            return web.Response(headers={"ETag": '"part-etag"'})

        assert request.method == "DELETE"
        events.append("abort")
        return web.Response(status=204)

    async def on_request_exception(session, context, params):
        nonlocal failed_requests
        if params.method != "PUT":
            return

        failed_requests += 1
        if failed_requests == 1:
            task = asyncio.current_task()
            assert task is not None
            loop = asyncio.get_running_loop()
            # Let gather wake the parent before releasing the other cleanup.
            task.add_done_callback(
                lambda _: loop.call_soon(release_cleanup.set)
            )

        else:
            events.append("cleanup_started")
            await release_cleanup.wait()
            events.append("cleanup_completed")

    trace = TraceConfig()
    trace.on_request_exception.append(on_request_exception)
    app = web.Application()
    app.router.add_route("*", "/{key:.*}", handle)

    async with (
        TestServer(app, host="127.0.0.1") as server,
        ClientSession(trace_configs=[trace]) as session,
    ):
        client = S3Client(
            session=session,
            url=server.make_url("/bucket"),
            access_key_id="test-key",
            secret_access_key="test-secret",
            region="us-east-1",
        )
        if from_file:
            path = tmp_path / "parts.bin"
            path.write_bytes(b"x" * 8)
            operation = client.put_file_multipart(
                object_name="key",
                file_path=path,
                part_size=4,
                workers_count=2,
            )
        else:
            operation = client.put_multipart(
                object_name="key",
                data=[b"x" * 8],
                part_size=4,
                workers_count=2,
            )

        task = asyncio.create_task(operation)
        try:
            await asyncio.wait_for(requests_started.wait(), timeout=2)
            task.cancel()

            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(task, timeout=2)
        finally:
            release_cleanup.set()
            release_requests.set()
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    assert events == ["cleanup_started", "cleanup_completed", "abort"]


@pytest.mark.parametrize("raise_for_status", [False, True])
@pytest.mark.parametrize("status", [403, 500])
async def test_abort_failure_retries_and_keeps_upload_id(
    multipart_backend, status
):
    client, state = multipart_backend
    state.abort_statuses = [status]
    upload = client.multipart_upload(object_name="key")
    await upload.create()

    with pytest.raises(AwsUploadError) as raised:
        await upload.abort()

    assert raised.value.status == status
    assert state.calls == ["create", "abort", "abort", "abort"]
    assert upload._upload_id == "upload-id"


@pytest.mark.parametrize("from_file", [False, True])
async def test_failure_cancels_retry_with_stale_cancellation_count(
    multipart_backend, monkeypatch, tmp_path, from_file
):
    client, state = multipart_backend
    retry_started = asyncio.Event()
    release_retry = asyncio.Event()
    events = []
    attempts = 0
    original = ValueError("another part failed")

    async def part_uploader(self, part_no, data, content_sha256, **kwargs):
        nonlocal attempts
        task = asyncio.current_task()
        assert task is not None

        if part_no == 1:
            attempts += 1
            if attempts == 1:
                # aiohttp 3.8 timeouts translate cancellation to TimeoutError
                # without decrementing the task's cancellation counter.
                task.cancel()
                try:
                    await asyncio.sleep(0)
                except asyncio.CancelledError:
                    raise asyncio.TimeoutError from None

            assert task.cancelling() > 0
            retry_started.set()
            try:
                await release_retry.wait()
                events.append("retry_completed")
            except asyncio.CancelledError:
                events.append("retry_cancelled")
                raise
        else:
            await retry_started.wait()
            loop = asyncio.get_running_loop()
            # Give the parent a chance to cancel the retry before it finishes.
            task.add_done_callback(lambda _: loop.call_soon(release_retry.set))
            raise original

    def assert_retry_stopped():
        assert events == ["retry_cancelled"]

    monkeypatch.setattr(MultipartUploader, "_part_uploader", part_uploader)
    state.abort_guard = assert_retry_stopped

    with pytest.raises(ValueError) as raised:
        if from_file:
            path = tmp_path / "parts.bin"
            path.write_bytes(b"x" * 8)
            await client.put_file_multipart(
                object_name="key",
                file_path=path,
                part_size=4,
                workers_count=2,
            )
        else:
            await client.put_multipart(
                object_name="key",
                data=[b"x" * 8],
                part_size=4,
                workers_count=2,
            )

    assert raised.value is original
    assert attempts == 2
    assert events == ["retry_cancelled"]
    assert state.calls == ["create", "abort"]
