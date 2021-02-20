import asyncio
import os
from concurrent.futures.thread import ThreadPoolExecutor
from mimetypes import guess_type
from pathlib import Path
from typing import Optional, Union

from aiohttp import ClientSession, hdrs
from aiohttp.typedefs import LooseHeaders
from multidict import CIMultiDict

from aws_request_signer import AwsRequestSigner, UNSIGNED_PAYLOAD
from yarl import URL

CHUNK_SIZE = 2 ** 16


async def file_sender(
    file_name: Path, executor=None,
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

        self.__url = url.with_user(None).with_password(None)
        self.__session = session
        self.__executor = executor
        self.__signer = AwsRequestSigner(
            region=region, service="s3", access_key_id=access_key_id,
            secret_access_key=secret_access_key,
        )

    def get(self, object_name: str, headers: LooseHeaders = None):
        url = str(self.__url / object_name)
        headers = self._make_headers(headers)
        headers = self.__signer.sign_with_headers(
            "GET", url, headers=headers
        )

        return self.__session.get(url, headers=headers)

    @staticmethod
    def _make_headers(headers: Optional[LooseHeaders]) -> LooseHeaders:
        headers = CIMultiDict(headers or {})
        return headers

    def _prepare_headers(
        self, headers: Optional[LooseHeaders],
        file_path: str = "",
    ) -> LooseHeaders:
        headers = self._make_headers(headers)

        if hdrs.CONTENT_TYPE not in headers:
            headers[hdrs.CONTENT_TYPE] = guess_type(file_path)[0]

        if headers[hdrs.CONTENT_TYPE] is None:
            headers[hdrs.CONTENT_TYPE] = "application/octet-stream"

        return headers

    def put(self, object_name: str, data, *,
            data_length: int = None,
            headers: LooseHeaders = None,
            content_sha256: str = None):

        if isinstance(data, bytes):
            data_length = len(data)
        elif isinstance(data, str):
            data = data.encode()
            data_length = len(data)

        if not data_length:
            raise ValueError("You must specify data_length argument")

        headers = self._prepare_headers(headers)
        headers[hdrs.CONTENT_LENGTH] = str(data_length)

        url = str(self.__url / object_name)
        headers = self.__signer.sign_with_headers(
            "PUT", url, headers=headers,
            content_hash=content_sha256 or UNSIGNED_PAYLOAD,
        )

        return self.__session.put(url, data=data, headers=headers)

    def put_file(
        self, file_path: Path,
        object_name: Union[str, Path] = None,
        *, headers: LooseHeaders = None,
        chunk_size: int = CHUNK_SIZE, content_sha256: str = None
    ):

        headers = self._prepare_headers(headers, str(file_path))

        return self.put(
            object_name,
            headers=headers,
            data=file_sender(
                file_path,
                executor=self.__executor,
                chunk_size=chunk_size,
            ),
            data_length=os.stat(file_path).st_size,
            content_sha256=content_sha256,
        )
