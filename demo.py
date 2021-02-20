import datetime
import hashlib

import requests
from requests_toolbelt.auth.handler import AuthHandler

from aws_request_signer import UNSIGNED_PAYLOAD, AwsRequestSigner
from aws_request_signer.requests import AwsAuth

AWS_REGION = ""
AWS_ACCESS_KEY_ID = "minio"
AWS_SECRET_ACCESS_KEY = "supersecret"

URL = "http://ubuntu:9000/test/test/test"


def main() -> None:
    # Demo content for our target file.
    content = b"hello world"
    content_hash = hashlib.sha256(content).hexdigest()

    # Create a request signer instance.
    request_signer = AwsRequestSigner(
        AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, "s3"
    )

    #
    # Use AWS request signer to generate authentication headers.
    #

    # The headers we'll provide and want to sign.
    headers = {
        "Content-Type": "text/plain",
        "Content-Length": str(len(content)),
    }

    # Add the authentication headers.
    headers.update(
        request_signer.sign_with_headers("PUT", URL, headers, content_hash)
    )

    # Make the request.
    r = requests.put(URL, headers=headers, data=content)
    r.raise_for_status()

    #
    # Use AWS request signer to generate a pre-signed URL.
    #

    # The headers we'll provide and want to sign.
    headers = {"Content-Type": "text/plain", "Content-Length": str(len(content))}

    # Generate the pre-signed URL that includes the authentication
    # parameters. Allow the client to determine the contents by
    # settings the content_has to UNSIGNED-PAYLOAD.
    presigned_url = request_signer.presign_url("PUT", URL, headers, UNSIGNED_PAYLOAD)

    # Perform the request.
    r = requests.put(presigned_url, headers=headers, data=content)
    r.raise_for_status()

    #
    # Use AWS request signer for requests helper to perform requests.
    #

    # Create a requests session and assign auth handler.
    session = requests.Session()
    session.auth = AuthHandler(
        {
            "http://ubuntu:9000": AwsAuth(
                AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, "s3"
            )
        }
    )

    # Perform the request.
    r = session.put(URL, data=content)
    r.raise_for_status()

    #
    # Use AWS request signer to sign an S3 POST policy request.
    #

    # Create a policy, only restricting bucket and expiration.
    expiration = datetime.datetime.utcnow() + datetime.timedelta(minutes=1)
    policy = {
        "expiration": expiration.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "conditions": [{"bucket": "demo"}],
    }

    # Get the required form fields to use the policy.
    fields = request_signer.sign_s3_post_policy(policy)

    # Post the form data to the bucket endpoint.
    # Set key (filename) to hello_world.txt.
    r = requests.post(
        URL.rsplit("/", 1)[0],
        data={"key": "test/hello_world.txt", "Content-Type": "text/plain", **fields},
        files={"file": content},
    )
    r.raise_for_status()


if __name__ == "__main__":
    main()
