from __future__ import annotations

import pytest

from batchsched.fetcher import FetchError, Fetcher, check_target


class SpySession:
    def __init__(self):
        self.calls = 0

    def get(self, *args, **kwargs):
        self.calls += 1
        raise AssertionError("network must not be touched")


def test_successful_loopback_fetch(mock_base_url):
    fetcher = Fetcher(base_url=mock_base_url, timeout_seconds=1.0)
    response = fetcher.fetch("/api/catalog")
    assert response.status_code == 200
    assert b"catalog" in response.content
    assert "application/json" in response.content_type
    assert response.media_family == "json"


def test_response_media_family_classification(mock_base_url):
    fetcher = Fetcher(base_url=mock_base_url, timeout_seconds=1.0)
    text_response = fetcher.fetch("/api/text")
    assert text_response.media_family == "text"
    large_response = fetcher.fetch("/api/large")
    assert large_response.media_family is None


def test_target_allow_list_and_credentials():
    assert check_target("http://127.0.0.1:5600/api/catalog").allowed
    assert check_target("http://localhost/api/catalog").allowed
    assert check_target("http://[::1]:5600/api/catalog").allowed
    assert not check_target("http://example.invalid/x").allowed
    assert not check_target("http://127.0.0.1.evil.invalid/x").allowed
    assert not check_target("ftp://127.0.0.1/x").allowed
    assert not check_target("http://user:pass@127.0.0.1/x").allowed


def test_rejected_target_is_audited_without_http_call(ledger, active_run):
    spy = SpySession()
    fetcher = Fetcher(
        base_url="http://127.0.0.1:5600",
        audit_hook=lambda event, detail: ledger.add_audit(
            active_run, event, detail, "2026-01-01T00:00:00Z"
        ),
        session=spy,
    )
    with pytest.raises(FetchError) as caught:
        fetcher.fetch("http://example.invalid/data")
    assert caught.value.error_class == "network"
    assert spy.calls == 0
    audit = ledger.get_audit(active_run)
    assert audit and audit[0]["event"] == "fetch_rejected"
    assert "example.invalid" in audit[0]["detail"]


@pytest.mark.parametrize(
    ("path", "error_class"),
    [("/api/server-error", "http_5xx"), ("/api/not-found", "http_4xx")],
)
def test_http_error_classification(mock_base_url, path, error_class):
    fetcher = Fetcher(base_url=mock_base_url, timeout_seconds=1.0)
    with pytest.raises(FetchError) as caught:
        fetcher.fetch(path)
    assert caught.value.error_class == error_class


def test_empty_body_classification(mock_base_url):
    fetcher = Fetcher(base_url=mock_base_url, timeout_seconds=1.0)
    with pytest.raises(FetchError) as caught:
        fetcher.fetch("/api/empty")
    assert caught.value.error_class == "empty_body"


def test_oversized_body_classification(mock_base_url):
    fetcher = Fetcher(base_url=mock_base_url, timeout_seconds=1.0, max_bytes=1024)
    with pytest.raises(FetchError) as caught:
        fetcher.fetch("/api/large")
    assert caught.value.error_class == "too_large"


def test_timeout_classification(mock_base_url):
    fetcher = Fetcher(base_url=mock_base_url, timeout_seconds=0.1)
    with pytest.raises(FetchError) as caught:
        fetcher.fetch("/api/timeout")
    assert caught.value.error_class == "timeout"


def test_constructor_limits():
    with pytest.raises(ValueError):
        Fetcher(timeout_seconds=0)
    with pytest.raises(ValueError):
        Fetcher(max_bytes=0)
