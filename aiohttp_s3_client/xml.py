from typing import List, Tuple

from xml.etree import ElementTree as ET


NS = "http://s3.amazonaws.com/doc/2006-03-01/"


def parse_create_multipart_upload_id(payload: bytes) -> str:
    root = ET.fromstring(payload)
    uploadid_el = root.find(f"{{{NS}}}UploadId")
    if uploadid_el is None:
        uploadid_el = root.find("UploadId")
    if uploadid_el is None or uploadid_el.text is None:
        raise ValueError(f"Upload id not found in {payload!r}")
    return uploadid_el.text


def create_complete_upload_request(parts: List[Tuple[int, str]]) -> bytes:
    ET.register_namespace("", NS)
    root = ET.Element(f"{{{NS}}}CompleteMultipartUpload")

    for part_no, etag in parts:
        part_el = ET.SubElement(root, "Part")
        etag_el = ET.SubElement(part_el, "ETag")
        etag_el.text = etag
        part_number_el = ET.SubElement(part_el, "PartNumber")
        part_number_el.text = str(part_no)

    return (
        b'<?xml version="1.0" encoding="UTF-8"?>' +
        ET.tostring(root, encoding="UTF-8")
    )
