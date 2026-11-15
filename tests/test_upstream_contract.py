"""Upstream artifact contract: the downstream job validates every direct
upstream's persisted artifact (required_fields / min_records) before it
issues any HTTP request, aggregates all violations, and fails closed with
a single non-retried parse_precondition attempt."""

from __future__ import annotations

import json
from pathlib import Path

from batchsched import config
from batchsched.executor import JobExecutor
from batchsched.fetcher import FetchResponse
from batchsched.models import JobResult, JobSpec, RetryPolicy
from batchsched.plan_loader import parse_plan_text
from batchsched.scheduler import execute_once


class SpyFetcher:
    """Fetcher stand-in: proves no HTTP is issued and serves a fixed body."""

    def __init__(self, payload: bytes = b'{"dataset": "downstream", "rows": [1]}'):
        self.payload = payload
        self.calls: list[str] = []

    def fetch(self, source: str) -> FetchResponse:
        self.calls.append(source)
        return FetchResponse(
            content=self.payload,
            status_code=200,
            content_type="application/json",
            elapsed_ms=1,
            url=f"http://127.0.0.1{source}",
        )


def _job(depends_on=("up_a", "up_b"), params=None, retry=None) -> JobSpec:
    return JobSpec(
        id="aggregate",
        source="/api/summary",
        depends_on=tuple(depends_on),
        retry=retry or RetryPolicy(mode="fixed", max_attempts=3, base_delay_ms=10),
        params=params or {},
    )


def _executor(ledger, run_id, output_dir, fetcher, sleeps) -> JobExecutor:
    return JobExecutor(fetcher, ledger, run_id, output_dir, sleep_fn=sleeps.append)


def _write_artifact(output_dir: Path, job_id: str, document) -> None:
    (output_dir / f"{job_id}.raw").write_text(json.dumps(document), encoding="utf-8")


def test_all_upstreams_checked_and_failures_aggregated(
    ledger, active_run, output_dir
):
    _write_artifact(output_dir, "up_a", {"dataset": "a"})
    _write_artifact(output_dir, "up_b", {"dataset": "b", "rows": [1]})
    fetcher = SpyFetcher()
    sleeps: list[float] = []
    executor = _executor(ledger, active_run, output_dir, fetcher, sleeps)
    job = _job(params={"required_fields": ["dataset", "rows"], "min_records": 2})
    outcome = executor.execute(job)
    assert outcome.status == config.JOB_FAILED
    assert outcome.error_class == "parse_precondition"
    reason = outcome.reason or ""
    assert reason.startswith("parse_precondition: upstream artifact contract:")
    assert "upstream 'up_a': missing required field(s): rows" in reason
    assert "upstream 'up_a': record count 0 below min_records 2" in reason
    assert "upstream 'up_b': record count 1 below min_records 2" in reason
    assert fetcher.calls == []
    assert sleeps == []
    attempts = ledger.get_attempts(active_run, "aggregate")
    assert [item["error_class"] for item in attempts] == ["parse_precondition"]
    result = ledger.get_job_results(active_run)[0]
    assert result.status == config.JOB_FAILED
    assert result.reason == reason
    events = [item["event"] for item in ledger.get_audit(active_run)]
    assert "job_failed" in events
    assert "upstream_contract_blocked" in events


def test_first_satisfied_upstream_does_not_release_job(
    ledger, active_run, output_dir
):
    _write_artifact(output_dir, "up_a", {"dataset": "ok", "rows": [1, 2, 3]})
    _write_artifact(output_dir, "up_z", {"dataset": "z"})
    fetcher = SpyFetcher()
    executor = _executor(ledger, active_run, output_dir, fetcher, [])
    job = _job(
        depends_on=("up_a", "up_z"),
        params={"required_fields": ["rows"], "min_records": 2},
    )
    outcome = executor.execute(job)
    assert outcome.status == config.JOB_FAILED
    assert "upstream 'up_z'" in (outcome.reason or "")
    assert fetcher.calls == []


def test_contract_satisfied_allows_fetch(ledger, active_run, output_dir):
    _write_artifact(output_dir, "up_a", {"dataset": "a", "rows": [1, 2]})
    _write_artifact(output_dir, "up_b", {"dataset": "b", "rows": [1, 2, 3]})
    fetcher = SpyFetcher()
    executor = _executor(ledger, active_run, output_dir, fetcher, [])
    job = _job(params={"required_fields": ["dataset"], "min_records": 2})
    outcome = executor.execute(job)
    assert outcome.status == config.JOB_DONE
    assert fetcher.calls == ["/api/summary"]
    assert (output_dir / "aggregate.raw").is_file()


def test_missing_artifact_blocks_fetch(ledger, active_run, output_dir):
    fetcher = SpyFetcher()
    executor = _executor(ledger, active_run, output_dir, fetcher, [])
    job = _job(depends_on=("up_a",), params={"min_records": 1})
    outcome = executor.execute(job)
    assert outcome.error_class == "parse_precondition"
    assert "upstream 'up_a': artifact missing" in (outcome.reason or "")
    assert fetcher.calls == []


def test_empty_artifact_blocks_fetch(ledger, active_run, output_dir):
    (output_dir / "up_a.raw").write_bytes(b"")
    fetcher = SpyFetcher()
    executor = _executor(ledger, active_run, output_dir, fetcher, [])
    job = _job(depends_on=("up_a",), params={"min_records": 1})
    outcome = executor.execute(job)
    assert "upstream 'up_a': artifact is empty" in (outcome.reason or "")
    assert fetcher.calls == []


def test_invalid_json_artifact_blocks_fetch(ledger, active_run, output_dir):
    (output_dir / "up_a.raw").write_bytes(b"not json {")
    fetcher = SpyFetcher()
    executor = _executor(ledger, active_run, output_dir, fetcher, [])
    job = _job(depends_on=("up_a",), params={"min_records": 1})
    outcome = executor.execute(job)
    assert "upstream 'up_a': artifact is not valid JSON" in (outcome.reason or "")
    assert fetcher.calls == []


def test_malformed_contract_declaration_fails_closed(ledger, active_run, output_dir):
    _write_artifact(output_dir, "up_a", {"dataset": "a", "rows": [1]})
    fetcher = SpyFetcher()
    executor = _executor(ledger, active_run, output_dir, fetcher, [])
    job = _job(depends_on=("up_a",), params={"required_fields": "dataset"})
    outcome = executor.execute(job)
    assert outcome.error_class == "parse_precondition"
    assert "required_fields must be a list" in (outcome.reason or "")
    assert fetcher.calls == []


def test_no_contract_params_skips_upstream_check(ledger, active_run, output_dir):
    fetcher = SpyFetcher()
    executor = _executor(ledger, active_run, output_dir, fetcher, [])
    job = _job(depends_on=("up_a",))
    outcome = executor.execute(job)
    assert outcome.status == config.JOB_DONE
    assert fetcher.calls == ["/api/summary"]


def test_contract_failure_never_retried(ledger, active_run, output_dir):
    _write_artifact(output_dir, "up_a", {"dataset": "a"})
    fetcher = SpyFetcher()
    sleeps: list[float] = []
    executor = _executor(ledger, active_run, output_dir, fetcher, sleeps)
    retry = RetryPolicy(
        mode="fixed",
        max_attempts=3,
        base_delay_ms=10,
        retryable=frozenset({"parse_precondition"}),
    )
    job = _job(depends_on=("up_a",), params={"min_records": 1}, retry=retry)
    outcome = executor.execute(job)
    assert outcome.status == config.JOB_FAILED
    assert sleeps == []
    assert len(ledger.get_attempts(active_run, "aggregate")) == 1


def test_scheduler_blocks_downstream_before_fetch(ledger, active_run, output_dir):
    plan = parse_plan_text(
        """
name: contract-gate
concurrency: 1
jobs:
  - id: fetch_seed
    source: /api/seed
  - id: aggregate
    source: /api/summary
    depends_on: [fetch_seed]
    params:
      required_fields: [dataset]
      min_records: 5
"""
    )
    fetcher = SpyFetcher(payload=b'{"dataset": "seed", "rows": [1]}')
    executor = JobExecutor(fetcher, ledger, active_run, output_dir)
    outcome = execute_once(plan, ledger, active_run, executor)
    assert outcome.status == config.RUN_FAILED
    assert outcome.states["fetch_seed"] == config.JOB_DONE
    assert outcome.states["aggregate"] == config.JOB_FAILED
    assert fetcher.calls == ["/api/seed"]
    attempts = ledger.get_attempts(active_run, "aggregate")
    assert [item["error_class"] for item in attempts] == ["parse_precondition"]
    result = {item.job_id: item for item in ledger.get_job_results(active_run)}
    assert "record count 1 below min_records 5" in (result["aggregate"].reason or "")
    events = [item["event"] for item in ledger.get_audit(active_run)]
    assert "upstream_contract_blocked" in events


def test_failed_upstream_propagates_upstream_failed_not_contract(
    ledger, active_run, fake_executor_cls
):
    plan = parse_plan_text(
        """
name: propagate-failed
concurrency: 1
jobs:
  - id: fetch_seed
    source: /api/seed
  - id: aggregate
    source: /api/summary
    depends_on: [fetch_seed]
    params:
      required_fields: [dataset]
      min_records: 1
"""
    )
    executor = fake_executor_cls(ledger, active_run, failures={"fetch_seed"})
    outcome = execute_once(plan, ledger, active_run, executor)
    assert outcome.states["fetch_seed"] == config.JOB_FAILED
    assert outcome.states["aggregate"] == config.JOB_SKIPPED
    result = {item.job_id: item for item in ledger.get_job_results(active_run)}
    assert result["aggregate"].reason == "upstream_failed"
    assert ledger.get_attempts(active_run, "aggregate") == []
    assert "aggregate" not in executor.executed
    events = [item["event"] for item in ledger.get_audit(active_run)]
    assert "upstream_contract_blocked" not in events


def test_skipped_upstream_propagates_upstream_failed_not_contract(
    ledger, active_run, fake_executor_cls
):
    plan = parse_plan_text(
        """
name: propagate-skipped
concurrency: 1
jobs:
  - id: fetch_bad
    source: /api/bad
  - id: fetch_mid
    source: /api/mid
    depends_on: [fetch_bad]
  - id: aggregate
    source: /api/summary
    depends_on: [fetch_mid]
    params:
      required_fields: [dataset]
      min_records: 1
"""
    )
    executor = fake_executor_cls(ledger, active_run, failures={"fetch_bad"})
    outcome = execute_once(plan, ledger, active_run, executor)
    assert outcome.states["fetch_mid"] == config.JOB_SKIPPED
    assert outcome.states["aggregate"] == config.JOB_SKIPPED
    result = {item.job_id: item for item in ledger.get_job_results(active_run)}
    assert result["aggregate"].reason == "upstream_failed"
    assert ledger.get_attempts(active_run, "aggregate") == []
    events = [item["event"] for item in ledger.get_audit(active_run)]
    assert "upstream_contract_blocked" not in events


def test_resume_reads_done_upstream_artifact(ledger, active_run, output_dir):
    plan = parse_plan_text(
        """
name: resume-contract
concurrency: 1
jobs:
  - id: fetch_seed
    source: /api/seed
  - id: aggregate
    source: /api/summary
    depends_on: [fetch_seed]
    params:
      required_fields: [dataset]
      min_records: 2
"""
    )
    ledger.save_job_result(
        JobResult(
            run_id=active_run,
            job_id="fetch_seed",
            status=config.JOB_DONE,
            sha256="b" * 64,
            size_bytes=40,
            output_path=f"work/output/runs/{active_run}/fetch_seed.raw",
        )
    )
    _write_artifact(output_dir, "fetch_seed", {"dataset": "seed", "rows": [1, 2]})
    fetcher = SpyFetcher()
    executor = JobExecutor(fetcher, ledger, active_run, output_dir)
    outcome = execute_once(plan, ledger, active_run, executor)
    assert outcome.status == config.RUN_COMPLETED
    assert outcome.states["fetch_seed"] == config.JOB_DONE
    assert fetcher.calls == ["/api/summary"]
