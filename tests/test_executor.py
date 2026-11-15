from __future__ import annotations

import hashlib

from batchsched import config
from batchsched.executor import JobExecutor
from batchsched.fetcher import Fetcher
from batchsched.models import JobSpec, RetryPolicy


def _job(
    source: str,
    *,
    retry: RetryPolicy | None = None,
    params: dict | None = None,
) -> JobSpec:
    return JobSpec(
        id="job_one",
        source=source,
        retry=retry or RetryPolicy(),
        params=params or {},
    )


def _executor(
    mock_base_url,
    ledger,
    run_id,
    output_dir,
    sleeps,
    timeout=1.0,
    max_bytes=config.MAX_RESPONSE_BYTES,
    max_total_delay_ms=None,
):
    fetcher = Fetcher(
        base_url=mock_base_url, timeout_seconds=timeout, max_bytes=max_bytes
    )
    return JobExecutor(
        fetcher,
        ledger,
        run_id,
        output_dir,
        sleep_fn=lambda seconds: sleeps.append(seconds),
        max_total_delay_ms=max_total_delay_ms,
    )


def test_flaky_endpoint_succeeds_on_third_attempt(
    mock_base_url, ledger, active_run, output_dir
):
    sleeps: list[float] = []
    executor = _executor(mock_base_url, ledger, active_run, output_dir, sleeps)
    retry = RetryPolicy(mode="fixed", max_attempts=3, base_delay_ms=50)
    outcome = executor.execute(_job("/api/flaky", retry=retry))
    assert outcome.status == config.JOB_DONE
    attempts = ledger.get_attempts(active_run)
    assert [item["attempt_no"] for item in attempts] == [1, 2, 3]
    assert [item["error_class"] for item in attempts] == [
        "http_5xx",
        "http_5xx",
        None,
    ]
    assert sleeps == [0.05, 0.05]
    assert ledger.get_job_results(active_run)[0].status == config.JOB_DONE


def test_http_4xx_is_not_retried(mock_base_url, ledger, active_run, output_dir):
    sleeps: list[float] = []
    executor = _executor(mock_base_url, ledger, active_run, output_dir, sleeps)
    retry = RetryPolicy(mode="fixed", max_attempts=4, base_delay_ms=50)
    outcome = executor.execute(_job("/api/not-found", retry=retry))
    assert outcome.status == config.JOB_FAILED
    assert outcome.error_class == "http_4xx"
    assert sleeps == []
    assert len(ledger.get_attempts(active_run)) == 1


def test_timeout_retries_then_fails(mock_base_url, ledger, active_run, output_dir):
    sleeps: list[float] = []
    executor = _executor(
        mock_base_url, ledger, active_run, output_dir, sleeps, timeout=0.1
    )
    retry = RetryPolicy(mode="fixed", max_attempts=2, base_delay_ms=50)
    outcome = executor.execute(_job("/api/timeout", retry=retry))
    assert outcome.status == config.JOB_FAILED
    assert outcome.error_class == "timeout"
    assert len(sleeps) == 1
    attempts = ledger.get_attempts(active_run)
    assert [item["error_class"] for item in attempts] == ["timeout", "timeout"]


def test_backoff_cap_short_circuits_retry(
    mock_base_url, ledger, active_run, output_dir
):
    sleeps: list[float] = []
    executor = _executor(
        mock_base_url,
        ledger,
        active_run,
        output_dir,
        sleeps,
        max_total_delay_ms=150,
    )
    retry = RetryPolicy(mode="fixed", max_attempts=5, base_delay_ms=1000)
    outcome = executor.execute(_job("/api/server-error", retry=retry))
    assert outcome.status == config.JOB_FAILED
    assert sleeps == [0.15]
    assert len(ledger.get_attempts(active_run)) == 2


def test_too_large_is_not_retryable(mock_base_url, ledger, active_run, output_dir):
    sleeps: list[float] = []
    executor = _executor(
        mock_base_url, ledger, active_run, output_dir, sleeps, max_bytes=1024
    )
    retry = RetryPolicy(mode="fixed", max_attempts=3, base_delay_ms=50)
    outcome = executor.execute(_job("/api/large", retry=retry))
    assert outcome.error_class == "too_large"
    assert sleeps == []


def test_content_type_precondition_fails_without_retry(
    mock_base_url, ledger, active_run, output_dir
):
    sleeps: list[float] = []
    executor = _executor(mock_base_url, ledger, active_run, output_dir, sleeps)
    retry = RetryPolicy(mode="fixed", max_attempts=3, base_delay_ms=50)
    outcome = executor.execute(
        _job(
            "/api/text",
            retry=retry,
            params={"expect_content_type": "application/json"},
        )
    )
    assert outcome.status == config.JOB_FAILED
    assert outcome.error_class == "parse_precondition"
    assert "content type mismatch" in (outcome.reason or "")
    assert sleeps == []


def test_empty_body_classification(mock_base_url, ledger, active_run, output_dir):
    executor = _executor(mock_base_url, ledger, active_run, output_dir, [])
    retry = RetryPolicy(mode="fixed", max_attempts=1, base_delay_ms=50)
    outcome = executor.execute(_job("/api/empty", retry=retry))
    assert outcome.error_class == "empty_body"


def test_output_file_digest_and_relative_path(
    mock_base_url, ledger, active_run, output_dir
):
    executor = _executor(mock_base_url, ledger, active_run, output_dir, [])
    retry = RetryPolicy(mode="fixed", max_attempts=1, base_delay_ms=50)
    outcome = executor.execute(_job("/api/catalog", retry=retry))
    target = output_dir / "job_one.raw"
    content = target.read_bytes()
    assert hashlib.sha256(content).hexdigest() == outcome.sha256
    assert outcome.size_bytes == len(content)
    assert outcome.output_path == f"work/output/runs/{active_run}/job_one.raw"
    assert not outcome.output_path.startswith("/")
    attempt = ledger.get_attempts(active_run)[0]
    assert attempt["duration_ms"] >= 0
    assert attempt["backoff_ms"] == 0


def test_attempt_number_continues_after_existing_attempt(
    mock_base_url, ledger, active_run, output_dir
):
    from batchsched.models import AttemptRecord

    ledger.record_attempt(
        AttemptRecord(
            run_id=active_run,
            job_id="job_one",
            attempt_no=1,
            error_class="timeout",
            duration_ms=5,
            backoff_ms=0,
            started_at=config.utc_now_iso(),
        )
    )
    executor = _executor(mock_base_url, ledger, active_run, output_dir, [])
    retry = RetryPolicy(mode="fixed", max_attempts=1, base_delay_ms=50)
    outcome = executor.execute(_job("/api/catalog", retry=retry))
    assert outcome.status == config.JOB_DONE
    attempts = ledger.get_attempts(active_run)
    assert [item["attempt_no"] for item in attempts] == [1, 2]


def test_executor_applies_query_and_header_extensions(
    mock_base_url, ledger, active_run, output_dir
):
    import json

    executor = _executor(mock_base_url, ledger, active_run, output_dir, [])
    retry = RetryPolicy(mode="fixed", max_attempts=1, base_delay_ms=50)
    outcome = executor.execute(
        _job(
            "/api/concurrency",
            retry=retry,
            params={
                "query": {"delay_ms": 60},
                "headers": {"Accept": "application/json"},
            },
        )
    )
    assert outcome.status == config.JOB_DONE
    payload = json.loads((output_dir / "job_one.raw").read_text())
    assert payload["delay_ms"] == 60


def test_executor_preserves_source_query_and_appends_params(
    mock_base_url, ledger, active_run, output_dir
):
    import json

    executor = _executor(mock_base_url, ledger, active_run, output_dir, [])
    retry = RetryPolicy(mode="fixed", max_attempts=1, base_delay_ms=50)
    outcome = executor.execute(
        _job(
            "/api/concurrency?mode=fast",
            retry=retry,
            params={"query": {"delay_ms": "15"}},
        )
    )
    assert outcome.status == config.JOB_DONE
    payload = json.loads((output_dir / "job_one.raw").read_text())
    assert payload["delay_ms"] == 15


def test_executor_runtime_rejection_is_network_and_audited(
    mock_base_url, ledger, active_run, output_dir
):
    audit_events: list[tuple[str, str]] = []

    def audit(event: str, detail: str) -> None:
        audit_events.append((event, detail))
        ledger.add_audit(active_run, event, detail)

    sleeps: list[float] = []
    fetcher = Fetcher(base_url=mock_base_url, timeout_seconds=1.0, audit_hook=audit)
    executor = JobExecutor(
        fetcher, ledger, active_run, output_dir, sleep_fn=sleeps.append
    )
    retry = RetryPolicy(mode="fixed", max_attempts=3, base_delay_ms=50)
    outcome = executor.execute(
        _job(
            "/api/concurrency",
            retry=retry,
            params={"query": {"delay_ms": 999}},
        )
    )
    assert outcome.status == config.JOB_FAILED
    assert outcome.error_class == "network"
    attempts = ledger.get_attempts(active_run)
    assert [item["error_class"] for item in attempts] == ["network"] * 3
    assert len(sleeps) == 2
    assert len([event for event in audit_events if event[0] == "fetch_rejected"]) == 3
    rejected = [event for event in audit_events if event[0] == "fetch_rejected"]
    assert rejected and "invalid_request" in rejected[0][1]


def test_executor_rejected_extension_is_not_silently_dropped(
    mock_base_url, ledger, active_run, output_dir
):
    executor = _executor(mock_base_url, ledger, active_run, output_dir, [])
    retry = RetryPolicy(mode="fixed", max_attempts=1, base_delay_ms=50)
    outcome = executor.execute(
        _job(
            "/api/concurrency",
            retry=retry,
            params={"headers": {"authorization": "Bearer nope"}},
        )
    )
    assert outcome.status == config.JOB_FAILED
    assert outcome.error_class == "network"
    assert not (output_dir / "job_one.raw").exists()
