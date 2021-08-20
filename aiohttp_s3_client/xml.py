from typing import List, Tuple

from lxml import etree as ET


def parse_create_multipart_upload_id(payload: bytes) -> str:
    root = ET.fromstring(payload)
    return next(root.iter("{*}UploadId")).text


def create_complete_upload_request(parts: List[Tuple[int, str]]) -> bytes:
    ns = "http://s3.amazonaws.com/doc/2006-03-01/"
    root = ET.Element(f"{{{ns}}}CompleteMultipartUpload", nsmap={None: ns})

    for part_no, etag in parts:
        part_el = ET.SubElement(root, "Part")
        etag_el = ET.SubElement(part_el, "ETag")
        etag_el.text = etag
        part_number_el = ET.SubElement(part_el, "PartNumber")
        part_number_el.text = str(part_no)

    return ET.tostring(root, xml_declaration=True, encoding="UTF-8")
