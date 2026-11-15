from __future__ import annotations

import hashlib

from batchsched import config
from batchsched.executor import JobExecutor
from batchsched.fetcher import FetchResponse, Fetcher
from batchsched.models import AttemptRecord, JobSpec, RetryPolicy


class _ScriptedFetcher:
    """Fetch a sequence of fixed bodies, one per call, without any network."""

    def __init__(self, bodies: list[bytes]):
        self.bodies = list(bodies)
        self.calls = 0

    def fetch(self, source: str) -> FetchResponse:
        body = self.bodies[min(self.calls, len(self.bodies) - 1)]
        self.calls += 1
        return FetchResponse(
            content=body,
            status_code=200,
            content_type="application/json; charset=utf-8",
            elapsed_ms=1,
            url=source,
        )


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


def _run_job_again(ledger, run_id, output_dir, bodies):
    fetcher = _ScriptedFetcher(bodies)
    executor = JobExecutor(fetcher, ledger, run_id, output_dir)
    outcome = executor.execute(_job("/api/changing"))
    assert outcome.status == config.JOB_DONE
    return outcome


def test_retry_with_changed_content_versions_and_backs_up_old_artifact(
    ledger, active_run, output_dir
):
    """A later success must preserve the superseded artifact and record it."""

    first = _run_job_again(ledger, active_run, output_dir, [b'{"v": 1}'])
    first_digest = first.sha256
    target = output_dir / "job_one.raw"
    assert target.read_bytes() == b'{"v": 1}'

    second = _run_job_again(ledger, active_run, output_dir, [b'{"v": 2}'])
    second_digest = second.sha256
    assert second_digest != first_digest

    # Final artifact is the newest bytes, and job_results still has one row.
    assert target.read_bytes() == b'{"v": 2}'
    results = ledger.get_job_results(active_run)
    assert len(results) == 1
    assert results[0].sha256 == second_digest

    # Old bytes survive under a digest-bearing backup name.
    backup = output_dir / f"job_one.{first_digest[:8]}.raw.bak"
    assert backup.exists()
    assert backup.read_bytes() == b'{"v": 1}'
    assert hashlib.sha256(backup.read_bytes()).hexdigest() == first_digest

    revisions = ledger.get_artifact_revisions(active_run)
    assert len(revisions) == 1
    revision = revisions[0]
    assert revision.job_id == "job_one"
    assert revision.revision_no == 1
    assert revision.old_sha256 == first_digest
    assert revision.new_sha256 == second_digest
    assert revision.backup_path.endswith(
        f"job_one.{first_digest[:8]}.raw.bak"
    )
    assert not revision.backup_path.startswith("/")

    revised = [
        item for item in ledger.get_audit(active_run)
        if item["event"] == "output_revised"
    ]
    assert len(revised) == 1
    assert first_digest in revised[0]["detail"]
    assert second_digest in revised[0]["detail"]
    # No staging file is left behind.
    assert not list(output_dir.glob("*.tmp"))


def test_retry_with_identical_content_does_not_version(
    ledger, active_run, output_dir
):
    body = b'{"stable": true}'
    first = _run_job_again(ledger, active_run, output_dir, [body])
    second = _run_job_again(ledger, active_run, output_dir, [body])
    assert first.sha256 == second.sha256
    assert ledger.get_artifact_revisions(active_run) == []
    assert not list(output_dir.glob("*.raw.bak"))
    assert not [
        item for item in ledger.get_audit(active_run)
        if item["event"] == "output_revised"
    ]
    assert len(ledger.get_job_results(active_run)) == 1


def test_third_version_chains_old_digests(
    ledger, active_run, output_dir
):
    bodies = [b"one", b"two", b"three"]
    outcomes = [
        _run_job_again(ledger, active_run, output_dir, [body]) for body in bodies
    ]
    revisions = ledger.get_artifact_revisions(active_run)
    assert [item.revision_no for item in revisions] == [1, 2]
    assert [item.old_sha256 for item in revisions] == [
        outcomes[0].sha256,
        outcomes[1].sha256,
    ]
    assert [item.new_sha256 for item in revisions] == [
        outcomes[1].sha256,
        outcomes[2].sha256,
    ]
    for revision in revisions:
        backup = output_dir / revision.backup_path.rsplit("/", 1)[-1]
        assert backup.exists()
    assert (output_dir / "job_one.raw").read_bytes() == b"three"
    assert len(ledger.get_job_results(active_run)) == 1


def test_revision_uses_final_artifact_not_tmp_name(
    ledger, active_run, output_dir
):
    """The staged ``.tmp`` name carries the *new* digest, yet versioning keys
    off the pre-existing final artifact; a same-digest rewrite must be a
    no-op even though a fresh temp file was created.
    """

    body = b"identical payload"
    _run_job_again(ledger, active_run, output_dir, [body])
    before = sorted(p.name for p in output_dir.iterdir())
    _run_job_again(ledger, active_run, output_dir, [body])
    after = sorted(p.name for p in output_dir.iterdir())
    assert before == after
    assert ledger.get_artifact_revisions(active_run) == []


def test_retry_then_change_lands_one_final_version(
    ledger, active_run, output_dir
):
    """A job that fails, retries, and finally lands different bytes than its
    previous run produces exactly one backup and one revision row.
    """

    from batchsched.fetcher import FetchError

    class _FlakyThenContent:
        def __init__(self):
            self.calls = 0

        def fetch(self, source):
            self.calls += 1
            if self.calls == 1:
                raise FetchError("http_5xx", "transient")
            return FetchResponse(
                content=b"changed-after-retry",
                status_code=200,
                content_type="application/json",
                elapsed_ms=1,
                url=source,
            )

    _run_job_again(ledger, active_run, output_dir, [b"before"])
    sleeps: list[float] = []
    executor = JobExecutor(
        _FlakyThenContent(),
        ledger,
        active_run,
        output_dir,
        sleep_fn=lambda seconds: sleeps.append(seconds),
    )
    retry = RetryPolicy(mode="fixed", max_attempts=3, base_delay_ms=10)
    outcome = executor.execute(_job("/api/x", retry=retry))
    assert outcome.status == config.JOB_DONE
    assert outcome.sha256 == hashlib.sha256(b"changed-after-retry").hexdigest()
    assert len(sleeps) == 1
    assert sleeps == [0.01]
    revisions = ledger.get_artifact_revisions(active_run)
    assert len(revisions) == 1
    assert revisions[0].new_sha256 == outcome.sha256
    assert (output_dir / "job_one.raw").read_bytes() == b"changed-after-retry"
    assert len(ledger.get_job_results(active_run)) == 1
