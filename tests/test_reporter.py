from __future__ import annotations

import json

import pytest

from batchsched import config
from batchsched.models import AttemptRecord, JobResult
from batchsched.reporter import build_report, to_json, to_markdown


def _seed_report(ledger, run_id, tmp_path):
    ledger.create_run(
        run_id, str(tmp_path / "report-plan.yaml"), "2026-01-01T00:00:00Z"
    )
    ledger.finish_run(
        run_id,
        config.RUN_FAILED,
        "2026-01-01T00:00:02Z",
        {"job_counts": {"done": 1, "failed": 1, "skipped": 1}},
    )
    ledger.save_job_result(
        JobResult(
            run_id=run_id,
            job_id="fetch_done",
            status=config.JOB_DONE,
            sha256="a" * 64,
            size_bytes=10,
            output_path=f"work/output/runs/{run_id}/fetch_done.raw",
        )
    )
    ledger.save_job_result(
        JobResult(
            run_id=run_id,
            job_id="fetch_failed",
            status=config.JOB_FAILED,
            reason="http_5xx: exhausted",
        )
    )
    ledger.save_job_result(
        JobResult(
            run_id=run_id,
            job_id="fetch_skipped",
            status=config.JOB_SKIPPED,
            reason="upstream_failed",
        )
    )
    ledger.record_attempt(
        AttemptRecord(
            run_id=run_id,
            job_id="fetch_done",
            attempt_no=1,
            error_class=None,
            duration_ms=5,
            backoff_ms=0,
            started_at="2026-01-01T00:00:00Z",
        )
    )
    ledger.record_attempt(
        AttemptRecord(
            run_id=run_id,
            job_id="fetch_failed",
            attempt_no=1,
            error_class="http_5xx",
            duration_ms=7,
            backoff_ms=50,
            started_at="2026-01-01T00:00:01Z",
        )
    )
    ledger.record_attempt(
        AttemptRecord(
            run_id=run_id,
            job_id="fetch_failed",
            attempt_no=2,
            error_class="http_5xx",
            duration_ms=8,
            backoff_ms=0,
            started_at="2026-01-01T00:00:02Z",
        )
    )
    ledger.add_audit(
        run_id, "resumed", "completed=0 failed=0 skipped=0 pending=3",
        "2026-01-01T00:00:00Z",
    )


def test_json_summary_structure(ledger, tmp_path):
    run_id = "run-report-0001"
    _seed_report(ledger, run_id, tmp_path)
    report = build_report(ledger, run_id)
    assert report["run"]["run_id"] == run_id
    assert report["run"]["status"] == config.RUN_FAILED
    assert report["job_counts"] == {
        "done": 1,
        "failed": 1,
        "skipped": 1,
        "recorded_total": 3,
    }
    assert report["total_attempts"] == 3
    assert report["failure_class_counts"] == {"http_5xx": 2}
    assert report["outputs"] == [
        f"work/output/runs/{run_id}/fetch_done.raw"
    ]
    assert report["resume_history"][0]["detail"].startswith("completed=0")
    assert len(report["jobs"]) == 3


def test_json_and_markdown_contain_no_absolute_paths(ledger, tmp_path):
    run_id = "run-report-0001"
    _seed_report(ledger, run_id, tmp_path)
    report = build_report(ledger, run_id)
    json_text = to_json(report)
    markdown = to_markdown(report)
    assert str(tmp_path) not in json_text
    assert str(tmp_path) not in markdown
    assert "report-plan.yaml" in markdown
    parsed = json.loads(json_text)
    assert parsed["run"]["plan"] == "report-plan.yaml"


def test_markdown_sections_and_attempt_details(ledger, tmp_path):
    run_id = "run-report-0001"
    _seed_report(ledger, run_id, tmp_path)
    markdown = to_markdown(build_report(ledger, run_id))
    assert "## Jobs by status" in markdown
    assert "## Failure and retry details" in markdown
    assert "## Output files" in markdown
    assert "## Resume history" in markdown
    assert "## Audit event summary" in markdown
    assert "`fetch_failed`" in markdown
    assert "http_5xx" in markdown
    assert "| 2 |" in markdown
    assert "upstream_failed" in markdown


def test_report_is_deterministic(ledger, tmp_path):
    run_id = "run-report-0001"
    _seed_report(ledger, run_id, tmp_path)
    first = to_json(build_report(ledger, run_id))
    second = to_json(build_report(ledger, run_id))
    assert first == second


def test_missing_report_raises_key_error(ledger):
    with pytest.raises(KeyError):
        build_report(ledger, "run-not-found")
