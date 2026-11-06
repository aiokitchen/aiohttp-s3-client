from datetime import datetime, timezone
from sys import intern
from typing import NamedTuple
from xml.etree import ElementTree as ET


class AwsObjectMeta(NamedTuple):
    etag: str
    key: str
    last_modified: datetime
    size: int
    storage_class: str


def parse_create_multipart_upload_id(payload: bytes) -> str:
    upload_id = ET.fromstring(payload).findtext("{*}UploadId")
    if not upload_id:
        raise ValueError(f"Upload id not found in {payload!r}")
    return upload_id


def create_complete_upload_request(parts: list[tuple[int, str]]) -> bytes:
    root = ET.Element("CompleteMultipartUpload")

    for part_no, etag in parts:
        part_el = ET.SubElement(root, "Part")
        etag_el = ET.SubElement(part_el, "ETag")
        etag_el.text = etag
        part_number_el = ET.SubElement(part_el, "PartNumber")
        part_number_el.text = str(part_no)

    return ET.tostring(root, encoding="UTF-8", xml_declaration=True)


def validate_complete_upload_response(payload: bytes) -> None:
    try:
        root = ET.fromstring(payload.lstrip())
    except ET.ParseError as exc:
        raise ValueError(
            f"Invalid multipart completion XML: {payload!r}"
        ) from exc

    if root.tag.rsplit("}", 1)[-1] != "CompleteMultipartUploadResult":
        raise ValueError(f"Multipart completion failed: {payload!r}")


def parse_list_objects(
    payload: bytes,
) -> tuple[
    list[AwsObjectMeta],
    list[str],
    str | None,
]:
    root = ET.fromstring(payload)
    result = []
    prefixes = [
        el.text
        for el in root.iterfind("{*}CommonPrefixes/{*}Prefix")
        if el.text
    ]
    for el in root.iterfind("{*}Contents"):
        etag = key = last_modified = size = storage_class = None
        for child in el:
            tag = child.tag[child.tag.rfind("}") + 1 :]
            text = child.text
            if text is None:
                continue
            if tag == "ETag":
                etag = text
            elif tag == "Key":
                key = text
            elif tag == "LastModified":
                assert text[-1] == "Z"
                last_modified = datetime.fromisoformat(text[:-1]).replace(
                    tzinfo=timezone.utc,
                )
            elif tag == "Size":
                size = int(text)
            elif tag == "StorageClass":
                storage_class = intern(text)
        if (
            etag
            and key
            and last_modified
            and size is not None
            and storage_class
        ):
            meta = AwsObjectMeta(etag, key, last_modified, size, storage_class)
            result.append(meta)
    continuation_token = root.findtext("{*}NextContinuationToken")
    return result, prefixes, continuation_token
