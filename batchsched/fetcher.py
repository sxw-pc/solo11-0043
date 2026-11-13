"""Local-only HTTP fetching with a hard target allow-list.

The allow-list is checked before a socket is opened.  There is no DNS
resolution of public names: only loopback hosts are accepted.
"""

from __future__ import annotations

from dataclasses import dataclass
import time
from typing import Callable
from urllib.parse import urlparse

import requests

from . import config

AuditHook = Callable[[str, str], None]


@dataclass(frozen=True)
class FetchResponse:
    content: bytes
    status_code: int
    content_type: str
    elapsed_ms: int
    url: str


class FetchError(RuntimeError):
    def __init__(self, error_class: str, detail: str):
        if error_class not in config.ERROR_CLASSES:
            raise ValueError(f"unknown fetch error class: {error_class}")
        self.error_class = error_class
        self.detail = detail
        super().__init__(f"{error_class}: {detail}")


@dataclass(frozen=True)
class TargetCheck:
    allowed: bool
    reason: str
    host: str | None


def check_target(url: str) -> TargetCheck:
    """Validate scheme/host/credentials without performing any I/O."""

    try:
        parsed = urlparse(url)
    except ValueError:
        return TargetCheck(False, "malformed_url", None)
    host = (parsed.hostname or "").lower()
    if parsed.scheme not in ("http", "https"):
        return TargetCheck(False, "scheme_not_allowed", host or None)
    if parsed.username is not None or parsed.password is not None:
        return TargetCheck(False, "credentials_not_allowed", host or None)
    if host not in config.ALLOWED_HOSTS:
        return TargetCheck(False, "host_not_allowlisted", host or None)
    try:
        parsed.port
    except ValueError:
        return TargetCheck(False, "invalid_port", host or None)
    return TargetCheck(True, "allowlisted", host)


class Fetcher:
    def __init__(
        self,
        base_url: str = config.DEFAULT_BASE_URL,
        timeout_seconds: float = 2.0,
        max_bytes: int = config.MAX_RESPONSE_BYTES,
        audit_hook: AuditHook | None = None,
        session: requests.Session | None = None,
    ):
        if not 0 < timeout_seconds <= config.MAX_TIMEOUT_SECONDS:
            raise ValueError("timeout_seconds outside allowed range")
        if not 1 <= max_bytes <= config.MAX_RESPONSE_BYTES:
            raise ValueError("max_bytes outside allowed range")
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.max_bytes = max_bytes
        self.audit_hook = audit_hook
        if session is None:
            session = requests.Session()
            session.trust_env = False
        self.session = session

    def resolve(self, source: str) -> str:
        if source.startswith(("http://", "https://")):
            return source
        return f"{self.base_url}/{source.lstrip('/')}"

    def fetch(self, source: str) -> FetchResponse:
        url = self.resolve(source)
        target = check_target(url)
        if not target.allowed:
            self._audit(
                "fetch_rejected",
                f"host={target.host or '<none>'} reason={target.reason}",
            )
            raise FetchError("network", f"target rejected: {target.reason}")
        started = time.monotonic()
        try:
            response = self.session.get(
                url,
                timeout=(self.timeout_seconds, self.timeout_seconds),
                stream=True,
                allow_redirects=False,
                headers={"User-Agent": f"{config.APP_NAME}/{config.APP_VERSION}"},
            )
        except requests.exceptions.Timeout as exc:
            raise FetchError("timeout", "request timed out") from exc
        except requests.exceptions.RequestException as exc:
            raise FetchError("network", exc.__class__.__name__) from exc
        except OSError as exc:
            raise FetchError("network", exc.__class__.__name__) from exc

        with response:
            status = int(response.status_code)
            if 300 <= status < 400:
                raise FetchError("http_4xx", f"redirect refused (status={status})")
            if status >= 500:
                raise FetchError("http_5xx", f"server error (status={status})")
            if status >= 400:
                raise FetchError("http_4xx", f"client error (status={status})")
            declared = response.headers.get("Content-Length")
            if declared:
                try:
                    if int(declared) > self.max_bytes:
                        raise FetchError("too_large", "declared body too large")
                except ValueError:
                    pass
            chunks: list[bytes] = []
            size = 0
            try:
                for chunk in response.iter_content(chunk_size=16 * 1024):
                    if not chunk:
                        continue
                    size += len(chunk)
                    if size > self.max_bytes:
                        raise FetchError("too_large", "response exceeded byte limit")
                    chunks.append(chunk)
            except requests.exceptions.Timeout as exc:
                raise FetchError("timeout", "body read timed out") from exc
            except requests.exceptions.RequestException as exc:
                raise FetchError("network", exc.__class__.__name__) from exc
            body = b"".join(chunks)
            if not body:
                raise FetchError("empty_body", "response body was empty")
            elapsed_ms = int((time.monotonic() - started) * 1000)
            return FetchResponse(
                content=body,
                status_code=status,
                content_type=response.headers.get("Content-Type", ""),
                elapsed_ms=elapsed_ms,
                url=url,
            )

    def _audit(self, event: str, detail: str) -> None:
        if self.audit_hook is not None:
            self.audit_hook(event, detail)
