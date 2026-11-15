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


def test_build_url_appends_query_and_preserves_existing_keys():
    from batchsched.fetcher import build_url

    assert build_url(
        "http://127.0.0.1:5600", "/api/concurrency", {"delay_ms": 25}
    ) == "http://127.0.0.1:5600/api/concurrency?delay_ms=25"
    assert build_url(
        "http://127.0.0.1:5600", "/api/concurrency?mode=fast", {"delay_ms": "25"}
    ) == "http://127.0.0.1:5600/api/concurrency?mode=fast&delay_ms=25"
    assert build_url("http://127.0.0.1:5600", "/api/catalog", {}) == (
        "http://127.0.0.1:5600/api/catalog"
    )


def test_build_url_rejects_conflicts_and_unvalidated_values():
    from batchsched.fetcher import build_url

    with pytest.raises(ValueError) as caught:
        build_url(
            "http://127.0.0.1:5600",
            "/api/concurrency?delay_ms=100",
            {"delay_ms": 20},
        )
    assert "already present" in str(caught.value)
    with pytest.raises(ValueError):
        build_url("http://127.0.0.1:5600", "/api/concurrency", {"offset": 1})
    with pytest.raises(ValueError):
        build_url("http://127.0.0.1:5600", "/api/concurrency", {"delay_ms": 999})


def test_merge_headers_is_allow_listed_and_keeps_user_agent():
    from batchsched.fetcher import merge_headers

    assert merge_headers() == {"User-Agent": "batchsched/0.1.0"}
    merged = merge_headers({"accept": "application/json", "x-request-id": "abc"})
    assert merged == {
        "User-Agent": "batchsched/0.1.0",
        "Accept": "application/json",
        "X-Request-ID": "abc",
    }
    with pytest.raises(ValueError):
        merge_headers({"authorization": "Bearer x"})
    with pytest.raises(ValueError):
        merge_headers({"user-agent": "evil/1.0"})


def test_fetch_applies_query_and_headers(mock_base_url):
    fetcher = Fetcher(base_url=mock_base_url, timeout_seconds=1.0)
    response = fetcher.fetch(
        "/api/concurrency",
        query_params={"delay_ms": 10},
        headers={"X-Request-ID": "fetch-001", "Accept": "application/json"},
    )
    assert response.status_code == 200
    assert response.url.endswith("/api/concurrency?delay_ms=10")
    assert b'"delay_ms":10' in response.content


def test_runtime_rejection_is_audited_and_never_falls_back(mock_base_url, ledger, active_run):
    from batchsched.fetcher import FetchError

    class RecordingSession:
        def __init__(self):
            self.calls = []

        def get(self, *args, **kwargs):
            self.calls.append((args, kwargs))
            raise AssertionError("network must not be touched")

    session = RecordingSession()
    fetcher = Fetcher(
        base_url=mock_base_url,
        audit_hook=lambda event, detail: ledger.add_audit(
            active_run, event, detail, "2026-01-01T00:00:00Z"
        ),
        session=session,
    )
    with pytest.raises(FetchError) as caught:
        fetcher.fetch(
            "/api/concurrency",
            query_params={"delay_ms": 999},
        )
    assert caught.value.error_class == "network"
    assert session.calls == []
    audit = ledger.get_audit(active_run)
    assert audit and audit[-1]["event"] == "fetch_rejected"
    assert "invalid_request" in audit[-1]["detail"]

    with pytest.raises(FetchError) as caught:
        fetcher.fetch("/api/catalog", headers={"authorization": "Bearer x"})
    assert caught.value.error_class == "network"
    assert session.calls == []
    assert ledger.get_audit(active_run)[-1]["event"] == "fetch_rejected"


def test_query_extension_to_unlisted_absolute_host_is_audited(ledger, active_run):
    from batchsched.fetcher import FetchError

    spy = SpySession()
    fetcher = Fetcher(
        base_url="http://127.0.0.1:5600",
        audit_hook=lambda event, detail: ledger.add_audit(
            active_run, event, detail, "2026-01-01T00:00:00Z"
        ),
        session=spy,
    )
    with pytest.raises(FetchError) as caught:
        fetcher.fetch(
            "http://example.invalid/data", query_params={"delay_ms": 10}
        )
    assert caught.value.error_class == "network"
    assert spy.calls == 0
    audit = ledger.get_audit(active_run)
    assert audit[-1]["event"] == "fetch_rejected"
    assert "example.invalid" in audit[-1]["detail"]
