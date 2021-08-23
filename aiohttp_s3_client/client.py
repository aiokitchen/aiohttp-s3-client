import asyncio
import hashlib
import logging
import os
import typing as t
from collections import deque
from http import HTTPStatus
from mimetypes import guess_type
from pathlib import Path
from urllib.parse import quote

from aiohttp import ClientSession, hdrs
from aiohttp.client import _RequestContextManager as RequestContextManager
from aiohttp.client_exceptions import ClientError
from aiohttp.typedefs import LooseHeaders
from aiomisc import asyncbackoff, threaded_iterable
from aws_request_signer import UNSIGNED_PAYLOAD, AwsRequestSigner
from multidict import CIMultiDict
from yarl import URL

from aiohttp_s3_client.xml import (
    create_complete_upload_request, parse_create_multipart_upload_id,
)


log = logging.getLogger(__name__)


CHUNK_SIZE = 2 ** 16
DONE = object()
EMPTY_STR_HASH = hashlib.sha256(b"").hexdigest()
PART_SIZE = 5 * 1024 * 1024  # 5MB


threaded_iterable_constrained = threaded_iterable(max_size=2)


class AwsError(ClientError):
    pass


class AwsUploadError(AwsError):
    pass


class AwsNotFoundError(AwsError):
    pass


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
        self, session: ClientSession, url: URL,
        secret_access_key: str = None,
        access_key_id: str = None,
        region: str = "",
    ):
        access_key_id = access_key_id or url.user
        secret_access_key = secret_access_key or url.password

        if not access_key_id:
            raise ValueError(
                "access_key id must be passed as argument "
                "or as username in the url",
            )
        if not secret_access_key:
            raise ValueError(
                "secret_access_key id must be passed as argument "
                "or as username in the url",
            )

        self._url = URL(url).with_user(None).with_password(None)
        self._session = session
        self._signer = AwsRequestSigner(
            region=region, service="s3", access_key_id=access_key_id,
            secret_access_key=secret_access_key,
        )

    @property
    def url(self):
        return self._url

    def request(
        self, method: str, path: str,
        headers: LooseHeaders = None,
        params: ParamsType = None,
        data: t.Optional[DataType] = None,
        data_length: t.Optional[int] = None,
        content_sha256: str = None,
        **kwargs,
    ) -> RequestContextManager:
        if isinstance(data, bytes):
            data_length = len(data)
        elif isinstance(data, str):
            data = data.encode()
            data_length = len(data)

        headers = self._prepare_headers(headers)
        if data_length:
            headers[hdrs.CONTENT_LENGTH] = str(data_length)
        elif data is not None:
            kwargs["chunked"] = True

        if data is not None and content_sha256 is None:
            content_sha256 = UNSIGNED_PAYLOAD

        url = (self._url / path.lstrip("/"))
        url = url.with_path(quote(url.path), encoded=True).with_query(params)

        headers = self._make_headers(headers)
        headers.extend(
            self._signer.sign_with_headers(
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
    def _make_headers(headers: t.Optional[LooseHeaders]) -> CIMultiDict:
        headers = CIMultiDict(headers or {})
        return headers

    def _prepare_headers(
        self, headers: t.Optional[LooseHeaders],
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
    ):
        return self.request("POST", object_name, data=data, **kwargs)

    def put_file(
        self, object_name: t.Union[str, Path],
        file_path: t.Union[str, Path],
        *, headers: LooseHeaders = None,
        chunk_size: int = CHUNK_SIZE, content_sha256: str = None,
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
        headers: LooseHeaders = None,
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
                    f"Wrong status code {resp.status} from s3 with message "
                    f"{payload.decode()}.",
                )
            return parse_create_multipart_upload_id(payload)

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
                    f"Wrong status code {resp.status} from s3 with message "
                    f"{payload}.",
                )
            return resp.headers["Etag"].strip('"')

    @asyncbackoff(
        None, None, 0,
        max_tries=3, exceptions=(AwsUploadError, ClientError),
    )
    async def _complete_multipart_upload(
        self,
        upload_id: str,
        object_name: str,
        parts: t.List[t.Tuple[int, str]],
    ):
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
                    f"Wrong status code {resp.status} from s3 with message "
                    f"{payload}.",
                )

    async def _part_uploader(
        self,
        upload_id: str,
        object_name: str,
        parts_queue: asyncio.Queue,
        results_queue: deque,
        part_upload_tries: int,
        **kwargs,
    ):
        backoff = asyncbackoff(
            None, None,
            max_tries=part_upload_tries,
            exceptions=(ClientError,),
        )
        while True:
            msg = await parts_queue.get()
            if msg is DONE:
                break
            part_no, part_hash, part = msg
            etag = await backoff(self._put_part)(  # type: ignore
                upload_id=upload_id,
                object_name=object_name,
                part_no=part_no,
                data=part,
                content_sha256=part_hash,
                **kwargs,
            )
            log.debug(
                "Etag for part %d of %s is %s", part_no, upload_id, etag,
            )
            results_queue.append((part_no, etag))

    async def put_file_multipart(
        self,
        object_name: t.Union[str, Path],
        file_path: t.Union[str, Path],
        *,
        headers: LooseHeaders = None,
        part_size: int = PART_SIZE,
        workers_count: int = 1,
        max_size: t.Optional[int] = None,
        part_upload_tries: int = 3,
        calculate_content_sha256: bool = True,
        **kwargs,
    ):
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

    async def put_multipart(
        self,
        object_name: t.Union[str, Path],
        data: t.Iterable[bytes],
        *,
        headers: LooseHeaders = None,
        workers_count: int = 1,
        max_size: t.Optional[int] = None,
        part_upload_tries: int = 3,
        calculate_content_sha256: bool = True,
        **kwargs,
    ):
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

        upload_id = await self._create_multipart_upload(  # type: ignore
            str(object_name),
            headers=headers,
        )
        log.debug("Got upload id %s for %s", upload_id, object_name)

        part_no = 1
        parts_queue: asyncio.Queue = asyncio.Queue(maxsize=max_size)
        results_queue: deque = deque()
        workers = [
            asyncio.create_task(
                self._part_uploader(
                    upload_id,
                    str(object_name),
                    parts_queue,
                    results_queue,
                    part_upload_tries,
                    **kwargs,
                ),
            )
            for _ in range(workers_count)
        ]

        if calculate_content_sha256:
            gen = gen_with_hash(data)
        else:
            gen = gen_without_hash(data)

        async with gen:
            async for part_hash, part in gen:
                log.debug(
                    "Reading part %d (%d bytes) of upload %s to %s",
                    part_no, len(part), upload_id, object_name,
                )
                await parts_queue.put((part_no, part_hash, part))
                part_no += 1

        for _ in range(workers_count):
            await parts_queue.put(DONE)

        await asyncio.gather(*workers)

        log.debug(
            "All parts (#%d) of %s are uploaded to %s",
            part_no - 1, upload_id, object_name,
        )

        # Parts should be in ascending order
        parts = sorted(results_queue, key=lambda x: x[0])
        await self._complete_multipart_upload(  # type: ignore
            upload_id, object_name, parts,
        )
