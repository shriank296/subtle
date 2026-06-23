import json
import logging

import requests

from brit_common.config import CLIENT_DISABLE_SSL

logger = logging.getLogger(__name__)


class OAuth2Exception(Exception):
    pass


class HeaderException(Exception):
    pass


class HttpRequest:
    def __init__(self, base_url: str, auth_credentials: dict | None = None) -> None:
        authentication_credentials: dict = auth_credentials or {}
        self._base_url: str = base_url
        self._headers: dict = {}

    def update_headers(self, headers: dict) -> None:
        try:
            self._headers.update(headers)
        except HeaderException as e:
            logger.error(f"Failed to update headers with error: {str(e)}")
            raise HeaderException(e)

    def make_request(
        self, endpoint: str, method: str, data: dict | None = None, params: dict | None = None  # type: ignore
    ) -> requests.Response:
        url: str = f"{self._base_url}{endpoint}"
        return requests.request(
            method,
            url,
            data=json.dumps(data),
            params=params,
            headers=self._headers,
            verify=CLIENT_DISABLE_SSL,
        )


class OAuth2(HttpRequest):
    def __init__(self, base_url, auth_credentials: dict | None = None) -> None:
        authentication_credentials = auth_credentials or {}
        try:
            self.tenant_id = authentication_credentials["tenant_id"]
            self.client_id = authentication_credentials["client_id"]
            self.client_secret = authentication_credentials["client_secret"]
            self.scope = authentication_credentials["scope"]
            self._base_url = base_url
        except KeyError as error:
            logger.error(f"Env variable: {error}")
            raise
        self._headers = {"Authorization": f"Bearer {self._get_access_code()}"}

    def _get_access_code(self) -> dict:
        token_endpoint: str = (
            f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"
        )
        response: requests.Response = requests.post(
            token_endpoint,
            data={
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "scope": self.scope,
            },
        )
        if response.status_code != 200:
            error_message: str = f"Error getting access token: {response.json()}"
            logger.error(error_message)
            raise OAuth2Exception(error_message)

        else:
            access_token = response.json()["access_token"]
            return access_token
