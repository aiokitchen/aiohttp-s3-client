import asyncio
import os
import typing as t
from concurrent.futures.thread import ThreadPoolExecutor
from mimetypes import guess_type
from pathlib import Path
from urllib.parse import quote

from aiohttp import ClientSession, hdrs
from aiohttp.client import _RequestContextManager as RequestContextManager
from aiohttp.typedefs import LooseHeaders
from aws_request_signer import UNSIGNED_PAYLOAD, AwsRequestSigner
from multidict import CIMultiDict
from yarl import URL


CHUNK_SIZE = 2 ** 16


async def file_sender(
    file_name: t.Union[str, Path], executor=None,
    chunk_size: int = CHUNK_SIZE
):
    loop = asyncio.get_event_loop()
    fp = await loop.run_in_executor(executor, open, file_name, "rb")

    try:
        data = await loop.run_in_executor(executor, fp.read, chunk_size)
        while data:
            yield data
            data = await loop.run_in_executor(executor, fp.read, chunk_size)

        if data:
            yield data
    finally:
        await loop.run_in_executor(executor, fp.close)


DataType = t.Union[bytes, str, t.AsyncIterable[bytes]]
ParamsType = t.Optional[t.Mapping[str, str]]


class S3Client:
    def __init__(
        self, session: ClientSession, url: URL,
        secret_access_key: str = None,
        access_key_id: str = None,
        region: str = "",
        executor: ThreadPoolExecutor = None,
    ):
        access_key_id = access_key_id or url.user
        secret_access_key = secret_access_key or url.password

        if not access_key_id:
            raise ValueError("access_key id must be passed as argument "
                             "or as username in the url")
        if not secret_access_key:
            raise ValueError("secret_access_key id must be passed as argument "
                             "or as username in the url")

        self._url = url.with_user(None).with_password(None)
        self._session = session
        self._executor = executor
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
        **kwargs
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
            headers[hdrs.CONTENT_ENCODING] = "chunked"

        if data is not None and content_sha256 is None:
            content_sha256 = UNSIGNED_PAYLOAD

        url = (self._url / path.lstrip('/')).with_query(params)
        url = str(url.with_path(quote(url.path), encoded=True))

        headers = self._make_headers(headers)
        headers.extend(
            self._signer.sign_with_headers(
                method, url, headers=headers, content_hash=content_sha256
            )
        )
        return self._session.request(
            method, url, headers=headers, data=data, **kwargs
        )

    def get(self, object_name: str, **kwargs) -> RequestContextManager:
        return self.request("GET", object_name, **kwargs)

    def head(self, object_name: str, **kwargs) -> RequestContextManager:
        return self.request("HEAD", object_name, **kwargs)

    def delete(self, object_name: str, **kwargs) -> RequestContextManager:
        return self.request("DELETE", object_name, **kwargs)

    @staticmethod
    def _make_headers(headers: t.Optional[LooseHeaders]) -> LooseHeaders:
        headers = CIMultiDict(headers or {})
        return headers

    def _prepare_headers(
        self, headers: t.Optional[LooseHeaders],
        file_path: str = "",
    ) -> LooseHeaders:
        headers = self._make_headers(headers)

        if hdrs.CONTENT_TYPE not in headers:
            headers[hdrs.CONTENT_TYPE] = guess_type(file_path)[0]

        if headers[hdrs.CONTENT_TYPE] is None:
            headers[hdrs.CONTENT_TYPE] = "application/octet-stream"

        return headers

    def put(
        self, object_name: str,
        data: t.Union[bytes, str, t.AsyncIterable[bytes]], **kwargs
    ):
        return self.request("PUT", object_name, data=data, **kwargs)

    def put_file(
        self, object_name: t.Union[str, Path],
        file_path: t.Union[str, Path],
        *, headers: LooseHeaders = None,
        chunk_size: int = CHUNK_SIZE, content_sha256: str = None
    ):

        headers = self._prepare_headers(headers, str(file_path))
        return self.put(
            object_name,
            headers=headers,
            data=file_sender(
                file_path,
                executor=self._executor,
                chunk_size=chunk_size,
            ),
            data_length=os.stat(file_path).st_size,
            content_sha256=content_sha256,
        )
