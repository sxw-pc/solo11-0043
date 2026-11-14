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
    return JobSpec(id="job_one", source=source, retry=retry or RetryPolicy(),
                   params=params or {})


def _executor(
    mock_base_url, ledger, run_id, output_dir, sleeps,
    timeout=1.0, max_bytes=config.MAX_RESPONSE_BYTES, max_total_delay_ms=None,
):
    fetcher = Fetcher(base_url=mock_base_url, timeout_seconds=timeout, max_bytes=max_bytes)
    return JobExecutor(
        fetcher, ledger, run_id, output_dir,
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


def test_within_budget_is_persisted_unchanged(
    mock_base_url, ledger, active_run, output_dir
):
    executor = _executor(mock_base_url, ledger, active_run, output_dir, [])
    retry = RetryPolicy(mode="fixed", max_attempts=1, base_delay_ms=50)
    outcome = executor.execute(
        _job("/api/catalog", retry=retry, params={"max_record_bytes": 4096})
    )
    assert outcome.status == config.JOB_DONE
    assert outcome.truncated is False
    assert outcome.original_size_bytes is None
    raw = (output_dir / "job_one.raw").read_bytes()
    assert hashlib.sha256(raw).hexdigest() == outcome.sha256
    assert outcome.size_bytes == len(raw)
    result = ledger.get_job_results(active_run)[0]
    assert result.truncated is False and result.original_size_bytes is None
    assert "body_truncated" not in [
        item["event"] for item in ledger.get_audit(active_run)
    ]


def test_json_over_budget_is_truncated_into_valid_output(
    mock_base_url, ledger, active_run, output_dir
):
    import json

    executor = _executor(mock_base_url, ledger, active_run, output_dir, [])
    retry = RetryPolicy(mode="fixed", max_attempts=1, base_delay_ms=50)
    source = "/api/large-json?records=120"
    original_size = len(Fetcher(base_url=mock_base_url).fetch(source).content)
    outcome = executor.execute(
        _job(source, retry=retry, params={"max_record_bytes": 600})
    )
    assert outcome.status == config.JOB_DONE
    assert outcome.truncated is True
    assert outcome.original_size_bytes == original_size
    assert outcome.size_bytes <= 600 < original_size
    persisted = (output_dir / "job_one.raw").read_bytes()
    assert len(persisted) == outcome.size_bytes
    assert hashlib.sha256(persisted).hexdigest() == outcome.sha256
    parsed = json.loads(persisted.decode("utf-8"))
    assert "records" in parsed and len(parsed["records"]) < 120
    result = ledger.get_job_results(active_run)[0]
    assert (result.truncated, result.original_size_bytes, result.size_bytes) == (
        True,
        original_size,
        outcome.size_bytes,
    )
    assert result.sha256 == outcome.sha256
    events = [
        item for item in ledger.get_audit(active_run)
        if item["event"] == "body_truncated"
    ]
    assert len(events) == 1
    assert f"original_bytes={original_size}" in events[0]["detail"]
    assert f"retained_bytes={outcome.size_bytes}" in events[0]["detail"]


def test_text_over_budget_uses_byte_prefix(
    mock_base_url, ledger, active_run, output_dir
):
    executor = _executor(mock_base_url, ledger, active_run, output_dir, [])
    retry = RetryPolicy(mode="fixed", max_attempts=1, base_delay_ms=50)
    outcome = executor.execute(
        _job("/api/text", retry=retry, params={"max_record_bytes": 10})
    )
    assert outcome.status == config.JOB_DONE and outcome.truncated is True
    persisted = (output_dir / "job_one.raw").read_bytes()
    assert persisted == b"plain text"
    assert len(persisted) == outcome.size_bytes == 10
    assert outcome.original_size_bytes == len(b"plain text fixture\n")
    assert hashlib.sha256(persisted).hexdigest() == outcome.sha256


def test_unsafe_media_type_over_budget_fails_as_too_large(
    mock_base_url, ledger, active_run, output_dir
):
    executor = _executor(
        mock_base_url, ledger, active_run, output_dir, [], max_bytes=128 * 1024
    )
    retry = RetryPolicy(mode="fixed", max_attempts=3, base_delay_ms=50)
    outcome = executor.execute(
        _job("/api/large", retry=retry, params={"max_record_bytes": 1024})
    )
    assert outcome.status == config.JOB_FAILED
    assert outcome.error_class == "too_large"
    assert "not safely truncatable" in (outcome.reason or "")
    assert not (output_dir / "job_one.raw").exists()
    result = ledger.get_job_results(active_run)[0]
    assert result.status == config.JOB_FAILED
    assert result.truncated is False and result.original_size_bytes is None
    assert "body_truncated" not in [
        item["event"] for item in ledger.get_audit(active_run)
    ]


def test_truncation_is_deterministic_across_executions(
    mock_base_url, ledger, tmp_path
):
    retry = RetryPolicy(mode="fixed", max_attempts=1, base_delay_ms=50)
    digests = []
    for run_id in ("run-det-1", "run-det-2"):
        ledger.create_run(run_id, "plans/test.yaml", config.utc_now_iso())
        out_dir = tmp_path / run_id
        out_dir.mkdir(parents=True, exist_ok=True)
        executor = _executor(mock_base_url, ledger, run_id, out_dir, [])
        outcome = executor.execute(
            _job(
                "/api/large-json?records=80",
                retry=retry,
                params={"max_record_bytes": 500},
            )
        )
        digests.append((outcome.sha256, outcome.size_bytes))
    assert digests[0] == digests[1]
