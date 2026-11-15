"""Local-only HTTP fetching with a hard target allow-list.

The allow-list is checked before a socket is opened.  There is no DNS
resolution of public names: only loopback hosts are accepted.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import time
from typing import Any, Callable
from urllib.parse import (
    parse_qsl,
    urlencode,
    urlparse,
    urlsplit,
    urlunsplit,
)

import requests

from . import config

AuditHook = Callable[[str, str], None]


def canonical_header_name(name: Any) -> str | None:
    """Map a planned header name onto the fixed allow-list, case-insensitively."""

    if not isinstance(name, str):
        return None
    return config.REQUEST_HEADER_NAMES.get(name.casefold())


def query_value_problem(key: str, value: Any) -> str | None:
    """Return why a query value is unacceptable, or ``None`` when valid.

    Values are deliberately restricted to short integer scalars (YAML integers
    or their decimal string form).  Floats, booleans, containers and any text
    outside the per-key range are rejected.
    """

    limits = config.QUERY_PARAM_RULES.get(key)
    if limits is None:
        return f"query parameter '{key}' is not allowed"
    minimum, maximum = limits
    if isinstance(value, str):
        if len(value) > config.MAX_QUERY_VALUE_LENGTH:
            return "value exceeds the maximum length"
        if not value.isdigit():
            return "value must be a short integer scalar"
        number = int(value)
    elif isinstance(value, int) and not isinstance(value, bool):
        number = value
    else:
        return "value must be a short integer scalar"
    if not minimum <= number <= maximum:
        return f"value must be between {minimum} and {maximum}"
    return None


def header_value_problem(name: str, value: Any) -> str | None:
    if not isinstance(value, str):
        return "value must be a string"
    if not value:
        return "value must not be empty"
    if len(value) > config.MAX_HEADER_VALUE_LENGTH:
        return "value exceeds the maximum length"
    if any(ord(char) < 32 or ord(char) > 126 for char in value):
        return "value must contain printable ASCII only"
    if name == "User-Agent" and not value.startswith(config.USER_AGENT_PREFIX):
        return f"value must start with '{config.USER_AGENT_PREFIX}'"
    return None


def source_query_keys(source: str) -> frozenset[str]:
    """Return the query keys already embedded in a planned source path."""

    try:
        query = urlsplit(source).query
    except ValueError:
        return frozenset()
    return frozenset(
        key for key, _value in parse_qsl(query, keep_blank_values=True)
    )


def build_url(
    base_url: str, source: str, query_params: Mapping[str, Any] | None = None
) -> str:
    """Resolve a source path and append validated query parameters.

    Validation is repeated here so the fetch path cannot be driven with values
    that never passed plan loading.  ``base_url`` is only used for relative
    sources.  Existing source query keys are preserved; a declared key that
    already exists is a hard conflict and raises ``ValueError`` instead of
    being overwritten or duplicated.
    """

    if source.startswith(("http://", "https://")):
        resolved = source
    else:
        resolved = f"{base_url}/{source.lstrip('/')}"
    query_params = query_params or {}
    if not isinstance(query_params, Mapping):
        raise ValueError("query_params must be a mapping")
    for key, value in query_params.items():
        if not isinstance(key, str):
            raise ValueError("query parameter names must be strings")
        problem = query_value_problem(key, value)
        if problem is not None:
            raise ValueError(f"query parameter '{key}': {problem}")
    if not query_params:
        return resolved
    parts = urlsplit(resolved)
    existing = parse_qsl(parts.query, keep_blank_values=True)
    existing_keys = {key for key, _value in existing}
    conflicts = sorted(set(query_params) & existing_keys)
    if conflicts:
        raise ValueError(
            "query key(s) already present in source: " + ", ".join(conflicts)
        )
    pairs = list(existing) + [
        (key, _scalar_text(value)) for key, value in query_params.items()
    ]
    return urlunsplit(parts._replace(query=urlencode(pairs)))


def merge_headers(headers: Mapping[str, Any] | None = None) -> dict[str, str]:
    """Merge planned headers over the fixed project User-Agent."""

    merged = {"User-Agent": f"{config.APP_NAME}/{config.APP_VERSION}"}
    seen: set[str] = set()
    for raw_name, value in (headers or {}).items():
        canonical = canonical_header_name(raw_name)
        if canonical is None:
            raise ValueError(f"request header '{raw_name}' is not allowed")
        folded = raw_name.casefold()
        if folded in seen:
            raise ValueError(f"request header '{canonical}' is declared more than once")
        seen.add(folded)
        problem = header_value_problem(canonical, value)
        if problem is not None:
            raise ValueError(f"request header '{canonical}': {problem}")
        merged[canonical] = value
    return merged


def _scalar_text(value: Any) -> str:
    return value if isinstance(value, str) else str(value)


def _host_hint(base_url: str, source: str) -> str:
    candidate = source if source.startswith(("http://", "https://")) else (
        f"{base_url}/{source.lstrip('/')}"
    )
    try:
        return (urlsplit(candidate).hostname or "").lower() or "<none>"
    except ValueError:
        return "<none>"


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
        return build_url(self.base_url, source)

    def fetch(
        self,
        source: str,
        query_params: Mapping[str, Any] | None = None,
        headers: Mapping[str, Any] | None = None,
    ) -> FetchResponse:
        query_params = query_params or {}
        try:
            if not isinstance(query_params, Mapping):
                raise ValueError("query_params must be a mapping")
            if headers is not None and not isinstance(headers, Mapping):
                raise ValueError("headers must be a mapping")
            url = build_url(self.base_url, source, query_params)
            request_headers = merge_headers(headers)
        except ValueError as exc:
            # Never fall back to the bare source: a rejected extension is a
            # runtime rejection, audited and classified as a network failure.
            hint = _host_hint(self.base_url, source)
            self._audit("fetch_rejected", f"host={hint} reason=invalid_request")
            raise FetchError("network", f"request rejected: {exc}") from exc
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
                headers=request_headers,
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
