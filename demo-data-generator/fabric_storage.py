"""Conditional OneLake Blob operations for durable producer state."""

import re
import uuid
from email.utils import formatdate
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import HTTPRedirectHandler, Request, build_opener


class StorageError(RuntimeError):
    pass


class StorageConflict(StorageError):
    pass


class StorageOutcomeUnknown(StorageError):
    pass


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class OneLakeStore:
    def __init__(self, workspace_id, lakehouse_id, token_provider, *, timeout=30, opener=None):
        try:
            self.workspace_id = str(uuid.UUID(workspace_id))
            self.lakehouse_id = str(uuid.UUID(lakehouse_id))
        except (ValueError, TypeError, AttributeError) as exc:
            raise ValueError("OneLake workspace and Lakehouse must be UUIDs") from exc
        if not callable(token_provider):
            raise ValueError("A storage token provider is required")
        if not isinstance(timeout, (int, float)) or isinstance(timeout, bool) or timeout <= 0:
            raise ValueError("Storage timeout must be positive")
        self.token_provider = token_provider
        self.timeout = timeout
        self.opener = opener or build_opener(_NoRedirect()).open

    def _url(self, path):
        if (not isinstance(path, str) or not path.startswith("Files/")
                or not re.fullmatch(r"[A-Za-z0-9._/-]+", path)
                or any(part in ("", ".", "..") for part in path.split("/"))):
            raise ValueError("OneLake path must be a safe Files descendant")
        return (f"https://onelake.blob.fabric.microsoft.com/{self.workspace_id}/"
                f"{self.lakehouse_id}/{quote(path, safe='/')}")

    @staticmethod
    def _etag(value):
        if not isinstance(value, str) or not value or "\r" in value or "\n" in value:
            raise StorageError("OneLake did not provide a usable ETag")
        return value

    def _request(self, method, path, *, data=None, condition=None):
        url = self._url(path)
        try:
            token = self.token_provider()
        except Exception:
            raise StorageError("Storage token acquisition failed") from None
        if not isinstance(token, str) or not token:
            raise StorageError("Storage token provider returned no token")
        headers = {"Authorization": f"Bearer {token}", "x-ms-version": "2021-08-06",
                   "x-ms-date": formatdate(usegmt=True)}
        if method == "PUT":
            headers.update({"x-ms-blob-type": "BlockBlob", "Content-Type": "application/octet-stream"})
        if condition:
            headers.update(condition)
        request = Request(url, data=data, headers=headers, method=method)
        try:
            with self.opener(request, timeout=self.timeout) as response:
                status = response.status
                body = response.read()
                response_headers = response.headers
        except HTTPError as exc:
            with exc:
                code = exc.headers.get("x-ms-error-code", "")
                if exc.code == 404:
                    raise FileNotFoundError(f"OneLake object not found: {path}") from None
                if exc.code == 412 or (exc.code == 409 and code == "BlobAlreadyExists"):
                    raise StorageConflict(f"OneLake conditional {method} rejected for {path}") from None
                raise StorageError(f"OneLake {method} failed (HTTP {exc.code}, {code or 'no error code'})") from None
        except (URLError, TimeoutError, OSError) as exc:
            raise StorageOutcomeUnknown(f"OneLake {method} outcome unknown ({type(exc).__name__})") from None
        rejected = response_headers.get("x-ms-rejected-headers", "").lower()
        if any(name.lower() in rejected for name in (condition or {})):
            raise StorageError("OneLake rejected a conditional request header")
        expected = {"GET": (200,), "PUT": (201,), "DELETE": (200, 202)}[method]
        if status not in expected:
            raise StorageError(f"Unexpected OneLake {method} status {status}")
        return body, response_headers

    def read(self, path):
        body, headers = self._request("GET", path)
        return body, self._etag(headers.get("ETag"))

    def create(self, path, data):
        if not isinstance(data, bytes):
            raise TypeError("OneLake payload must be bytes")
        _, headers = self._request("PUT", path, data=data, condition={"If-None-Match": "*"})
        return self._etag(headers.get("ETag"))

    def replace(self, path, data, etag):
        if not isinstance(data, bytes):
            raise TypeError("OneLake payload must be bytes")
        _, headers = self._request("PUT", path, data=data, condition={"If-Match": self._etag(etag)})
        return self._etag(headers.get("ETag"))

    def delete(self, path, etag):
        self._request("DELETE", path, condition={"If-Match": self._etag(etag)})
