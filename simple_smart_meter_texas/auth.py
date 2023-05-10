import os
import requests

from urllib.parse import urljoin
from . import logger
from .config import (
    BASE_URL,
    DEFAULT_USER_AGENT,
    DEFAULT_USERNAME_ENV_VARIABLE,
    DEFAULT_PASSWORD_ENV_VARIABLE,
)

AUTH_URL = urljoin(
    BASE_URL,
    "api/user/authenticate",
)


def _request_authentication(
    username: str,
    password: str,
):
    logger.debug(f"Submitting GET request to {BASE_URL:s}...")

    # submit initial get request to main URL (required for auth request to work)
    resp = requests.get(
        BASE_URL,
        headers={"User-Agent": DEFAULT_USER_AGENT},
    )

    logger.debug(f"Recieved response to GET request to {BASE_URL:s}.")

    # raise exception if request doesn't work
    if resp.status_code != 200:
        raise requests.exceptions.HTTPError(
            f"GET request to {BASE_URL:s} returned status code {resp.status_code:d}"
        )

    logger.debug(
        f"Submitting POST request to {AUTH_URL:s} w/ username {username} and password..."
    )

    # submit post request to authentication API
    resp = requests.post(
        AUTH_URL,
        headers={
            "User-Agent": DEFAULT_USER_AGENT,
        },
        json={
            "username": username,
            "password": password,
            "rememberMe": "true",
        },
    )

    logger.debug(
        f"Recieved response to POST request to {AUTH_URL:s} w/ username {username} and password."
    )

    # raise exception if request doesn't work
    if resp.status_code != 200:
        raise requests.exceptions.HTTPError(
            f"POST Request to a {AUTH_URL:s} returned status code {resp.status_code:d}"
        )

    # return auth response body as JSON, as well as constructed header
    return resp.json()


def get_session_token(
    username: str = os.environ.get(DEFAULT_USERNAME_ENV_VARIABLE, None),
    password: str = os.environ.get(DEFAULT_PASSWORD_ENV_VARIABLE, None),
) -> dict:
    if not username or not isinstance(username, str):
        raise ValueError("username input argument is unspecified or is not a string!")

    if not password or not isinstance(password, str):
        raise ValueError("password input argument is unspecified or is not a string!")

    return _request_authentication(username, password).get("token")
