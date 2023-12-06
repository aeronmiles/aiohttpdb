import hashlib
import hmac
from typing import Optional


def sign_sha256(host: str, endpoint: str, secret: str, request: Optional[str]) -> str:
    # Encoding the secret key for HMAC
    encoded_secret = secret.encode("utf-8")

    # Using an empty string if 'request' is None
    encoded_request = None if request is None else request.encode("utf-8")

    # Creating the HMAC signature
    signature = hmac.new(encoded_secret, encoded_request, hashlib.sha256).hexdigest()

    # Constructing the request body
    req_body = f"signature={signature}"
    if request:
        req_body = f"{request}&{req_body}"

    # Constructing and returning the final URL
    return f"{host}{endpoint}?{req_body}"