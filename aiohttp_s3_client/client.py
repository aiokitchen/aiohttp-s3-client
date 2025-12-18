import asyncio
import hashlib
import io
import logging
import os
import threading
import typing as t
from functools import partial, cached_property
from http import HTTPStatus
from itertools import chain
from mimetypes import guess_type
from mmap import PAGESIZE
from pathlib import Path
from tempfile import NamedTemporaryFile
from urllib.parse import quote

from aiohttp import ClientSession, hdrs
# noinspection PyProtectedMember
from aiohttp.client import (
    _RequestContextManager as RequestContextManager,
    ClientResponse,
)
from aiohttp.client_exceptions import ClientError, ClientResponseError
from aiomisc import asyncbackoff, threaded, threaded_iterable
from aws_request_signer import UNSIGNED_PAYLOAD
from multidict import CIMultiDict, CIMultiDictProxy
from yarl import URL

from aiohttp_s3_client.credentials import (
    AbstractCredentials, collect_credentials,
)
from aiohttp_s3_client.xml import (
    AwsObjectMeta, create_complete_upload_request,
    parse_create_multipart_upload_id, parse_list_objects,
)

log = logging.getLogger(__name__)

CHUNK_SIZE = 2 ** 16
DONE = object()
EMPTY_STR_HASH = hashlib.sha256(b"").hexdigest()
PART_SIZE = 5 * 1024 * 1024  # 5MB

HeadersType = t.Union[t.Dict, CIMultiDict, CIMultiDictProxy]

threaded_iterable_constrained = threaded_iterable(max_size=2)


class AwsError(ClientResponseError):
    def __init__(
        self, resp: ClientResponse, message: str, *history: ClientResponse
    ):
        super().__init__(
            headers=resp.headers,
            history=(resp, *history),
            message=message,
            request_info=resp.request_info,
            status=resp.status,
        )


class AwsUploadError(AwsError):
    pass


class AwsDownloadError(AwsError):
    pass


@threaded
def unlink_path(path: Path) -> None:
    path.unlink(missing_ok=True)


@threaded
def concat_files(
    target_file: Path, files: t.List[t.IO[bytes]], buffer_size: int,
) -> None:
    with target_file.open("ab") as fp:
        for file in files:
            file.seek(0)
            while True:
                chunk = file.read(buffer_size)
                if not chunk:
                    break
                fp.write(chunk)
            file.close()


@threaded
def write_from_start(
    file: io.BytesIO, chunk: bytes, range_start: int, pos: int,
) -> None:
    file.seek(pos - range_start)
    file.write(chunk)


@threaded
def pwrite_absolute_pos(
    fd: int, chunk: bytes, range_start: int, pos: int,
) -> None:
    os.pwrite(fd, chunk, pos)


@threaded_iterable_constrained
def gen_without_hash(
    stream: t.Iterable[bytes],
) -> t.Generator[t.Tuple[None, bytes], None, None]:
    for data in stream:
        yield (None, data)


@threaded_iterable_constrained
def gen_with_hash(
    stream: t.Iterable[bytes],
) -> t.Generator[t.Tuple[str, bytes], None, None]:
    for data in stream:
        yield hashlib.sha256(data).hexdigest(), data


def file_sender(
    file_name: t.Union[str, Path], chunk_size: int = CHUNK_SIZE,
) -> t.Iterable[bytes]:
    with open(file_name, "rb") as fp:
        while True:
            data = fp.read(chunk_size)
            if not data:
                break
            yield data


async_file_sender = threaded_iterable_constrained(file_sender)

DataType = t.Union[bytes, str, t.AsyncIterable[bytes]]
ParamsType = t.Optional[t.Mapping[str, str]]


class S3Client:
    def __init__(
        self, session: ClientSession, url: t.Union[URL, str],
        secret_access_key: t.Optional[str] = None,
        access_key_id: t.Optional[str] = None,
        session_token: t.Optional[str] = None,
        region: str = "",
        credentials: t.Optional[AbstractCredentials] = None,
    ):
        url = URL(url)
        if credentials is None:
            credentials = collect_credentials(
                url=url,
                access_key_id=access_key_id,
                region=region,
                secret_access_key=secret_access_key,
                session_token=session_token,
            )

        if not credentials:
            raise ValueError(
                f"Credentials {credentials!r} is incomplete",
            )

        self._url = URL(url).with_user(None).with_password(None)
        self._session = session
        self._credentials = credentials

    @property
    def url(self) -> URL:
        return self._url

    def request(
        self, method: str, path: str,
        headers: t.Optional[HeadersType] = None,
        params: ParamsType = None,
        data: t.Optional[DataType] = None,
        data_length: t.Optional[int] = None,
        content_sha256: t.Optional[str] = None,
        **kwargs,
    ) -> RequestContextManager:
        if isinstance(data, bytes):
            data_length = len(data)
        elif isinstance(data, str):
            data = data.encode()
            data_length = len(data)

        headers = self._prepare_headers(headers)
        if data_length is not None and data_length >= 0:
            headers[hdrs.CONTENT_LENGTH] = str(data_length)
        elif data is not None:
            kwargs["chunked"] = True

        if kwargs.get("chunked"):
            if content_sha256:
                log.warning(
                    "content_sha256 will be ignored because "
                    "content is chunked"
                )
            # https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-auth-using-authorization-header.html
            content_sha256 = "STREAMING-UNSIGNED-PAYLOAD-TRAILER"

        if data is not None and content_sha256 is None:
            content_sha256 = UNSIGNED_PAYLOAD

        url = (self._url / path.lstrip("/"))
        url = url.with_path(quote(url.path), encoded=True).with_query(params)

        headers = self._make_headers(headers)
        headers.extend(
            self._credentials.signer.sign_with_headers(
                method, str(url), headers=headers, content_hash=content_sha256,
            ),
        )
        return self._session.request(
            method, url, headers=headers, data=data, **kwargs,
        )

    def get(self, object_name: str, **kwargs) -> RequestContextManager:
        return self.request("GET", object_name, **kwargs)

    def head(
        self, object_name: str,
        content_sha256=EMPTY_STR_HASH,
        **kwargs,
    ) -> RequestContextManager:
        return self.request(
            "HEAD", object_name, content_sha256=content_sha256, **kwargs,
        )

    def delete(
        self, object_name: str,
        content_sha256=EMPTY_STR_HASH,
        **kwargs,
    ) -> RequestContextManager:
        return self.request(
            "DELETE", object_name, content_sha256=content_sha256, **kwargs,
        )

    @staticmethod
    def _make_headers(headers: t.Optional[HeadersType]) -> CIMultiDict:
        headers = CIMultiDict(headers or {})
        return headers

    def _prepare_headers(
        self, headers: t.Optional[HeadersType],
        file_path: str = "",
    ) -> CIMultiDict:
        headers = self._make_headers(headers)

        if hdrs.CONTENT_TYPE not in headers:
            content_type = guess_type(file_path)[0]
            if content_type is None:
                content_type = "application/octet-stream"

            headers[hdrs.CONTENT_TYPE] = content_type

        return headers

    def put(
        self, object_name: str,
        data: t.Union[bytes, str, t.AsyncIterable[bytes]], **kwargs,
    ) -> RequestContextManager:
        return self.request("PUT", object_name, data=data, **kwargs)

    def post(
        self, object_name: str,
        data: t.Union[None, bytes, str, t.AsyncIterable[bytes]] = None,
        **kwargs,
    ) -> RequestContextManager:
        return self.request("POST", object_name, data=data, **kwargs)

    def put_file(
        self, object_name: t.Union[str, Path],
        file_path: t.Union[str, Path],
        *, headers: t.Optional[HeadersType] = None,
        chunk_size: int = CHUNK_SIZE, content_sha256: t.Optional[str] = None,
    ) -> RequestContextManager:

        headers = self._prepare_headers(headers, str(file_path))
        return self.put(
            str(object_name),
            headers=headers,
            data=async_file_sender(
                file_path,
                chunk_size=chunk_size,
            ),
            data_length=os.stat(file_path).st_size,
            content_sha256=content_sha256,
        )

    @asyncbackoff(
        None, None, 0,
        max_tries=3, exceptions=(ClientError,),
    )
    async def _create_multipart_upload(
        self,
        object_name: str,
        headers: t.Optional[HeadersType] = None,
    ) -> str:
        async with self.post(
            object_name,
            headers=headers,
            params={"uploads": 1},
            content_sha256=EMPTY_STR_HASH,
        ) as resp:
            payload = await resp.read()
            if resp.status != HTTPStatus.OK:
                raise AwsUploadError(
                    resp,
                    (
                        f"Wrong status code {resp.status} from s3 "
                        f"with message {payload.decode()}."
                    ),
                )
            return parse_create_multipart_upload_id(payload)

    @asyncbackoff(
        None, None, 0,
        max_tries=3, exceptions=(AwsUploadError, ClientError),
    )
    async def _complete_multipart_upload(
        self,
        upload_id: str,
        object_name: str,
        parts: t.List[t.Tuple[int, str]],
    ) -> None:
        complete_upload_request = create_complete_upload_request(parts)
        async with self.post(
            object_name,
            headers={"Content-Type": "text/xml"},
            params={"uploadId": upload_id},
            data=complete_upload_request,
            content_sha256=hashlib.sha256(complete_upload_request).hexdigest(),
        ) as resp:
            if resp.status != HTTPStatus.OK:
                payload = await resp.text()
                raise AwsUploadError(
                    resp,
                    (
                        f"Wrong status code {resp.status} from s3 "
                        f"with message {payload}."
                    ),
                )

    async def _put_part(
        self,
        upload_id: str,
        object_name: str,
        part_no: int,
        data: bytes,
        content_sha256: str,
        **kwargs,
    ) -> str:
        async with self.put(
            object_name,
            params={"partNumber": part_no, "uploadId": upload_id},
            data=data,
            content_sha256=content_sha256,
            **kwargs,
        ) as resp:
            payload = await resp.text()
            if resp.status != HTTPStatus.OK:
                raise AwsUploadError(
                    resp,
                    (
                        f"Wrong status code {resp.status} from s3 "
                        f"with message {payload}."
                    ),
                )
            return resp.headers["Etag"].strip('"')

    @staticmethod
    async def _part_uploader(parts_queue: asyncio.Queue) -> None:
        while True:
            coro = await parts_queue.get()
            if coro is DONE:
                break
            await coro

    async def put_file_multipart(
        self,
        object_name: t.Union[str, Path],
        file_path: t.Union[str, Path],
        *,
        headers: t.Optional[HeadersType] = None,
        part_size: int = PART_SIZE,
        workers_count: int = 1,
        max_size: t.Optional[int] = None,
        part_upload_tries: int = 3,
        calculate_content_sha256: bool = True,
        **kwargs,
    ) -> None:
        """
        Upload data from a file with multipart upload

        object_name: key in s3
        file_path: path to a file for upload
        headers: additional headers, such as Content-Type
        part_size: size of a chunk to send (recommended: >5Mb)
        workers_count: count of coroutines for asyncronous parts uploading
        max_size: maximum size of a queue with data to send (should be
            at least `workers_count`)
        part_upload_tries: how many times trying to put part to s3 before fail
        calculate_content_sha256: whether to calculate sha256 hash of a part
            for integrity purposes
        """
        log.debug(
            "Going to multipart upload %s to %s with part size %d",
            file_path, object_name, part_size,
        )
        await self.put_multipart(
            object_name,
            file_sender(
                file_path,
                chunk_size=part_size,
            ),
            headers=headers,
            workers_count=workers_count,
            max_size=max_size,
            part_upload_tries=part_upload_tries,
            calculate_content_sha256=calculate_content_sha256,
            **kwargs,
        )

    @staticmethod
    async def _parts_generator(
        gen: t.AsyncIterable[t.Tuple[str, bytes]],
        uploader: 'MultipartUploader',
        queue: asyncio.Queue,
        workers_count: int,
    ) -> None:
        async with gen:     # type: ignore
            log.debug("Starting parts generator")
            no = 1
            async for part_hash, part in gen:
                log.debug("Reading part %d (%d bytes)", no, len(part))
                no += 1
                await queue.put(
                    uploader.put_part(part, content_sha256=part_hash)
                )

        for _ in range(workers_count):
            await queue.put(DONE)

    async def put_multipart(
        self,
        object_name: t.Union[str, Path],
        data: t.Iterable[bytes],
        *,
        headers: t.Optional[HeadersType] = None,
        workers_count: int = 1,
        max_size: t.Optional[int] = None,
        part_upload_tries: int = 3,
        calculate_content_sha256: bool = True,
        **kwargs,
    ) -> None:
        """
        Send data from iterable with multipart upload

        object_name: key in s3
        data: any iterable that returns chunks of bytes
        headers: additional headers, such as Content-Type
        workers_count: count of coroutines for asyncronous parts uploading
        max_size: maximum size of a queue with data to send (should be
            at least `workers_count`)
        part_upload_tries: how many times trying to put part to s3 before fail
        calculate_content_sha256: whether to calculate sha256 hash of a part
            for integrity purposes
        """
        if workers_count < 1:
            raise ValueError(
                f"Workers count should be > 0. Got {workers_count}",
            )
        max_size = max_size or workers_count

        async with MultipartUploader(
            self,
            object_name,
            headers=headers,
            max_retries=part_upload_tries,
            **kwargs,
        ) as uploader:
            parts_queue: asyncio.Queue = asyncio.Queue(maxsize=max_size)
            workers = [
                asyncio.create_task(self._part_uploader(parts_queue))
                for _ in range(workers_count)
            ]

            if calculate_content_sha256:
                gen = gen_with_hash(data)
            else:
                gen = gen_without_hash(data)

            parts_generator = asyncio.create_task(
                self._parts_generator(gen, uploader, parts_queue, workers_count)
            )
            try:
                part_no, *_ = await asyncio.gather(parts_generator, *workers)
            except Exception:
                for task in chain([parts_generator], workers):
                    if not task.done():
                        task.cancel()
                raise

    async def _download_range(
        self,
        object_name: str,
        writer: t.Callable[[bytes, int, int], t.Coroutine],
        *,
        etag: str,
        range_start: int,
        req_range_start: int,
        req_range_end: int,
        buffer_size: int,
        headers: t.Optional[HeadersType] = None,
        **kwargs,
    ) -> None:
        """
        Downloading range [req_range_start:req_range_end] to `file`
        """
        log.debug(
            "Downloading %s from %d to %d",
            object_name,
            req_range_start,
            req_range_end,
        )
        if not headers:
            headers = {}
        headers = headers.copy()
        headers["Range"] = f"bytes={req_range_start}-{req_range_end}"
        headers["If-Match"] = etag

        pos = req_range_start
        async with self.get(object_name, headers=headers, **kwargs) as resp:
            if resp.status not in (HTTPStatus.PARTIAL_CONTENT, HTTPStatus.OK):
                raise AwsDownloadError(
                    resp,
                    (
                        f"Got wrong status code {resp.status} on "
                        f"range download of {object_name}"
                    ),
                )
            while True:
                chunk = await resp.content.read(buffer_size)
                if not chunk:
                    break
                await writer(chunk, range_start, pos)
                pos += len(chunk)

    async def _download_worker(
        self,
        object_name: str,
        writer: t.Callable[[bytes, int, int], t.Coroutine],
        *,
        etag: str,
        range_step: int,
        range_start: int,
        range_end: int,
        buffer_size: int,
        range_get_tries: int = 3,
        headers: t.Optional[HeadersType] = None,
        **kwargs,
    ) -> None:
        """
        Downloads data in range `[range_start, range_end)`
        with step `range_step` to file `file_path`.
        Uses `etag` to make sure that file wasn't changed in the process.
        """
        log.debug(
            "Starting download worker for range [%d:%d]",
            range_start,
            range_end,
        )
        backoff = asyncbackoff(
            None, None,
            max_tries=range_get_tries,
            exceptions=(ClientError,),
        )
        req_range_end = range_start
        for req_range_start in range(range_start, range_end, range_step):
            req_range_end += range_step
            if req_range_end > range_end:
                req_range_end = range_end
            await backoff(self._download_range)(
                object_name,
                writer,
                etag=etag,
                range_start=range_start,
                req_range_start=req_range_start,
                req_range_end=req_range_end - 1,
                buffer_size=buffer_size,
                headers=headers,
                **kwargs,
            )

    async def get_file_parallel(
        self,
        object_name: t.Union[str, Path],
        file_path: t.Union[str, Path],
        *,
        headers: t.Optional[HeadersType] = None,
        range_step: int = PART_SIZE,
        workers_count: int = 1,
        range_get_tries: int = 3,
        buffer_size: int = PAGESIZE * 32,
        **kwargs,
    ) -> None:
        """
        Download object in parallel with requests with Range.
        If file will change while download is in progress -
            error will be raised.

        object_name: s3 key to download
        file_path: target file path
        headers: additional headers
        range_step: how much data will be downloaded in single HTTP request
        workers_count: count of parallel workers
        range_get_tries: count of tries to download each range
        buffer_size: size of a buffer for on the fly data
        """
        file_path = Path(file_path)
        async with self.head(str(object_name), headers=headers) as resp:
            if resp.status != HTTPStatus.OK:
                raise AwsDownloadError(
                    resp,
                    (
                        f"Got response for HEAD request for "
                        f"{object_name} of a wrong status {resp.status}"
                    ),
                )
            etag = resp.headers["Etag"]
            file_size = int(resp.headers["Content-Length"])
            log.debug(
                "Object's %s etag is %s and size is %d",
                object_name,
                etag,
                file_size,
            )

        workers = []
        files: t.List[t.IO[bytes]] = []
        worker_range_size = file_size // workers_count
        range_end = 0
        file: t.IO[bytes]
        try:
            with file_path.open("w+b") as fp:
                for range_start in range(0, file_size, worker_range_size):
                    range_end += worker_range_size
                    if range_end > file_size:
                        range_end = file_size
                    if hasattr(os, "pwrite"):
                        writer = partial(pwrite_absolute_pos, fp.fileno())
                    else:
                        if range_start:
                            file = NamedTemporaryFile(dir=file_path.parent)
                            files.append(file)
                        else:
                            file = fp
                        writer = partial(
                            write_from_start, file  # type: ignore
                        )
                    workers.append(
                        self._download_worker(
                            str(object_name),
                            writer,  # type: ignore
                            buffer_size=buffer_size,
                            etag=etag,
                            headers=headers,
                            range_end=range_end,
                            range_get_tries=range_get_tries,
                            range_start=range_start,
                            range_step=range_step,
                            **kwargs,
                        ),
                    )

                await asyncio.gather(*workers)

            if files:
                # First part already in `file_path`
                log.debug("Joining %d parts to %s", len(files) + 1, file_path)
                await concat_files(
                    file_path, files, buffer_size=buffer_size,
                )
        except Exception:
            log.exception(
                "Error on file download. Removing possibly incomplete file %s",
                file_path,
            )
            await unlink_path(file_path)
            raise

    async def list_objects_v2(
        self,
        object_name: t.Union[str, Path] = "/",
        *,
        bucket: t.Optional[str] = None,
        prefix: t.Optional[t.Union[str, Path]] = None,
        delimiter: t.Optional[str] = None,
        max_keys: t.Optional[int] = None,
        start_after: t.Optional[str] = None,
    ) -> t.AsyncIterator[t.Tuple[t.List[AwsObjectMeta], t.List[str]]]:
        """
        List objects in bucket.

        Returns an iterator over lists of metadata objects, each corresponding
        to an individual response result (typically limited to 1000 keys).

        object_name:
            path to listing endpoint, defaults to '/'; a `bucket` value is
            prepended to this value if provided.
        prefix:
            limits the response to keys that begin with the specified
            prefix
        delimiter: a delimiter is a character you use to group keys
        max_keys: maximum number of keys returned in the response
        start_after: keys to start listing after
        """

        params = {
            "list-type": "2",
        }

        if prefix:
            params["prefix"] = str(prefix)

        if delimiter:
            params["delimiter"] = delimiter

        if max_keys:
            params["max-keys"] = str(max_keys)

        if start_after:
            params["start-after"] = start_after

        if bucket is not None:
            object_name = f"/{bucket}"

        while True:
            async with self.get(str(object_name), params=params) as resp:
                if resp.status != HTTPStatus.OK:
                    raise AwsDownloadError(
                        resp,
                        (
                            "Got response with wrong status for GET request "
                            f"for {object_name} with prefix '{prefix}'"
                        ),
                    )
                payload = await resp.read()
                metadata, prefixes, cont_token = parse_list_objects(payload)
                if not metadata and not prefixes:
                    break
                yield metadata, prefixes
                if not cont_token:
                    break
                params["continuation-token"] = cont_token

    def presign_url(
        self,
        method: str,
        url: t.Union[str, URL],
        headers: t.Optional[HeadersType] = None,
        content_sha256: t.Optional[str] = None,
        expires: int = 86400,
    ) -> URL:
        """
        Make presigned url which will expire in specified amount of seconds

        method: HTTP method
        url: object key or absolute URL
        headers: optional headers you would like to pass
        content_sha256:
        expires: amount of seconds presigned url would be usable
        """
        if content_sha256 is None:
            content_sha256 = UNSIGNED_PAYLOAD

        _url = URL(url)
        if not _url.is_absolute():
            _url = self._url / str(_url)

        return URL(self._credentials.signer.presign_url(
            method=method.upper(),
            url=str(_url),
            headers=headers,
            content_hash=content_sha256,
            expires=expires
        ))

    def multipart_upload(self, object_name: str) -> 'MultipartUploader':
        """
        Get S3MultipartUploader for object_name

        object_name: key in s3
        """
        return MultipartUploader(self, str(object_name))


class MultipartUploader:
    def __init__(
        self,
        client: S3Client,
        object_name: str | Path,
        headers: t.Optional[HeadersType] = None,
        max_retries: int = 3,
        retry_when: t.Iterable[t.Type[Exception]] = (
            AwsUploadError,
            ClientError,
            OSError,
            asyncio.TimeoutError
        ),
    ):
        self.__client = client
        self._object_name = str(object_name)
        self._headers = headers
        self._upload_id: t.Optional[str] = None
        self._part_no = 1
        self._parts: t.Dict[int, t.Optional[str]] = {}
        self._part_no_lock = threading.Lock()
        self._retry_policy = asyncbackoff(
            None, None, 0,
            max_tries=max_retries, exceptions=tuple(retry_when),
        )

    async def _create(self):
        async with self.__client.post(
            self._object_name,
            headers=self._headers,
            params={"uploads": 1},
            content_sha256=EMPTY_STR_HASH,
        ) as resp:
            payload = await resp.read()
            if resp.status != HTTPStatus.OK:
                raise AwsUploadError(
                    resp,
                    (
                        f"Wrong status code {resp.status} from s3 "
                        f"with message {payload.decode()}."
                    ),
                )
            self._upload_id = parse_create_multipart_upload_id(payload)
            log.debug(
                "Got upload id %s for %s", self._upload_id, self._object_name
            )

    @cached_property
    def create(self) -> t.Callable[..., t.Coroutine[t.Any, t.Any, None]]:
        if self._upload_id is not None:
            raise RuntimeError("Multipart upload already created")
        return self._retry_policy(self._create)

    async def _complete(self) -> None:
        if self._upload_id is None:
            raise RuntimeError("Multipart upload not created")

        parts = []
        for part_no in sorted(self._parts):
            etag = self._parts[part_no]
            if etag is None:
                raise RuntimeError(f"Part {part_no} was not uploaded")
            parts.append((part_no, etag))

        complete_upload_request = create_complete_upload_request(parts)
        async with self.__client.post(
            self._object_name,
            headers={"Content-Type": "text/xml"},
            params={"uploadId": self._upload_id},
            data=complete_upload_request,
            content_sha256=hashlib.sha256(complete_upload_request).hexdigest(),
        ) as resp:
            if resp.status != HTTPStatus.OK:
                payload = await resp.text()
                raise AwsUploadError(
                    resp,
                    (
                        f"Wrong status code {resp.status} from s3 "
                        f"with message {payload}."
                    ),
                )

    @cached_property
    def complete(self) -> t.Callable[..., t.Coroutine[t.Any, t.Any, None]]:
        return self._retry_policy(self._complete)

    async def _part_uploader(
        self,
        part_no: int,
        data: t.Union[bytes, str, t.AsyncIterable[bytes]],
        content_sha256: str,
        **kwargs
    ) -> None:
        if self._upload_id is None:
            raise RuntimeError("Multipart upload not created")

        async with self.__client.put(
            self._object_name,
            params={"partNumber": part_no, "uploadId": self._upload_id},
            data=data,
            content_sha256=content_sha256,
            **kwargs,
        ) as resp:
            payload = await resp.text()
            if resp.status != HTTPStatus.OK:
                raise AwsUploadError(
                    resp,
                    (
                        f"Wrong status code {resp.status} from s3 "
                        f"with message {payload}."
                    ),
                )
        self._parts[part_no] = resp.headers["Etag"].strip('"')

    def put_part(
        self,
        data: t.Union[bytes, str, t.AsyncIterable[bytes]],
        content_sha256: str,
        **kwargs
    ) -> t.Coroutine[None, None, None]:
        if self._upload_id is None:
            raise RuntimeError("Multipart upload not created")

        # Force lock part number assignment, in normal usage this should not
        # be a bottleneck
        with self._part_no_lock:
            part_no = self._part_no
            self._part_no += 1
            self._parts[part_no] = None

        uploader = self._retry_policy(self._part_uploader)
        return uploader(part_no, data, content_sha256, **kwargs)

    async def __aenter__(self) -> 'MultipartUploader':
        await self.create()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        if exc_type is not None:
            return
        await self.complete()
