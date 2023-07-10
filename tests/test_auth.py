import pytest
import requests_mock
from requests.exceptions import HTTPError

from simple_smart_meter_texas.config import BASE_URL
from simple_smart_meter_texas.auth import (
    AUTH_URL,
    _request_authentication,
    get_session_token,
)


@pytest.fixture
def example_auth_response():
    """example auth response for use in unit-testing

    Returns
    -------
    dict
        simulated auth response with session token
    """
    return {"token": "1234.abcdefg"}


def test_request_authentication(example_auth_response):
    """tests authentication request request error handling using mock"""

    # test nominal request
    with requests_mock.Mocker() as m:
        # define get request to BASE_URL response
        m.get(BASE_URL)

        # define post request to AUTH_URL response
        m.post(AUTH_URL, json=example_auth_response)

        assert (
            _request_authentication(
                "dummy_username",
                "dummy_password",
            )
            == example_auth_response
        )

    # test website 404 case
    with requests_mock.Mocker() as m:
        # define get request to BASE_URL response
        m.get(BASE_URL, status_code=404)

        with pytest.raises(HTTPError):
            _request_authentication(
                "dummy_username",
                "dummy_password",
            )

    # test auth api 404 case
    with requests_mock.Mocker() as m:
        # define get request to BASE_URL response
        m.get(BASE_URL)

        # define post request to AUTH_URL response
        m.post(AUTH_URL, status_code=404)

        with pytest.raises(HTTPError):
            _request_authentication(
                "dummy_username",
                "dummy_password",
            )


def test_get_session_token(example_auth_response):
    """tests authentication request input error handling using mock"""

    # test nominal behavior
    with requests_mock.Mocker() as m:
        # define get request to BASE_URL response
        m.get(BASE_URL)

        # define post request to AUTH_URL response
        m.post(AUTH_URL, json=example_auth_response)

        # test nominal case
        assert get_session_token(
            "dummy_username",
            "dummy_password",
        ) == example_auth_response.get("token")

        # test bad username
        with pytest.raises(ValueError):
            get_session_token(
                "",
                "dummy_password",
            )

        with pytest.raises(ValueError):
            get_session_token(
                username="",
                password="dummy_password",
            )

        with pytest.raises(ValueError):
            get_session_token(
                password="dummy_password",
            )

        # test bad password
        with pytest.raises(ValueError):
            get_session_token(
                "dummy_username",
                "",
            )

        with pytest.raises(ValueError):
            get_session_token(
                username="dummy_username",
                password="",
            )

        with pytest.raises(ValueError):
            get_session_token(
                username="dummy_username",
            )
