import hashlib
import hmac
from typing import Optional
from httpxdb.error import ParameterRequiredError


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


def flatten_params(params):
    flat_params = []
    for key, value in params.items():
        if isinstance(value, list):
            # Append each list item as a separate parameter
            for item in value:
                flat_params.append((f"{key}", item))
        else:
            flat_params.append((key, value))
    return flat_params


def check_required_parameter(value, name):
    if not value and value != 0:
        raise ParameterRequiredError([name])