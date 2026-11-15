from __future__ import annotations

import sqlite3

import pytest

from batchsched import config
from batchsched.ledger import Ledger
from batchsched.models import (
    ArtifactRevision,
    AttemptRecord,
    JobResult,
)


def _attempt(run_id: str, attempt_no: int = 1, error_class: str | None = None):
    return AttemptRecord(
        run_id=run_id,
        job_id="job_one",
        attempt_no=attempt_no,
        error_class=error_class,
        duration_ms=7,
        backoff_ms=0,
        started_at=config.utc_now_iso(),
    )


def test_schema_tables_and_primary_keys(tmp_path):
    ledger = Ledger(tmp_path / "db.sqlite3")
    with sqlite3.connect(ledger.db_path) as connection:
        names = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        attempt_pk = connection.execute("PRAGMA table_info(attempts)").fetchall()
        result_pk = connection.execute("PRAGMA table_info(job_results)").fetchall()
    assert {
        "runs",
        "attempts",
        "job_results",
        "audit",
        "artifact_revisions",
        "job_dependencies",
    } <= names
    assert [row[1] for row in attempt_pk if row[5]] == [
        "run_id",
        "job_id",
        "attempt_no",
    ]
    assert [row[1] for row in result_pk if row[5]] == ["run_id", "job_id"]


def test_create_get_list_and_finish_run(ledger):
    ledger.create_run("run-a", "plans/a.yaml", "2026-01-01T00:00:00Z")
    ledger.create_run("run-b", "plans/b.yaml", "2026-01-02T00:00:00Z")
    ledger.finish_run(
        "run-a", config.RUN_COMPLETED, "2026-01-01T00:00:05Z", {"jobs": 1}
    )
    run = ledger.get_run("run-a")
    assert run["status"] == config.RUN_COMPLETED
    assert run["summary"] == {"jobs": 1}
    assert ledger.get_run("missing") is None
    assert [item["run_id"] for item in ledger.list_runs()] == ["run-b", "run-a"]


def test_attempt_primary_key_prevents_duplicates(ledger, active_run):
    ledger.record_attempt(_attempt(active_run, 1, "timeout"))
    with pytest.raises(sqlite3.IntegrityError):
        ledger.record_attempt(_attempt(active_run, 1, "network"))


def test_job_result_upsert_keeps_one_row(ledger, active_run):
    first = JobResult(
        run_id=active_run,
        job_id="job_one",
        status=config.JOB_DONE,
        sha256="a" * 64,
        size_bytes=3,
        output_path="work/output/runs/x/job_one.raw",
    )
    ledger.save_job_result(first)
    ledger.save_job_result(
        JobResult(
            run_id=active_run,
            job_id="job_one",
            status=config.JOB_FAILED,
            reason="http_5xx: nope",
        )
    )
    results = ledger.get_job_results(active_run)
    assert len(results) == 1
    assert results[0].status == config.JOB_FAILED
    assert results[0].sha256 is None


def test_attempt_sequence_and_next_number(ledger, active_run):
    ledger.record_attempt(_attempt(active_run, 1, "http_5xx"))
    ledger.record_attempt(_attempt(active_run, 2, "http_5xx"))
    attempts = ledger.get_attempts(active_run)
    assert [item["attempt_no"] for item in attempts] == [1, 2]
    assert ledger.next_attempt_no(active_run, "job_one") == 3
    assert ledger.next_attempt_no(active_run, "job_other") == 1


def test_audit_order_and_output_paths(ledger, active_run):
    ledger.add_audit(active_run, "first", "detail-1", "2026-01-01T00:00:00Z")
    ledger.add_audit(active_run, "resumed", "completed=0", "2026-01-01T00:00:01Z")
    assert [item["event"] for item in ledger.get_audit(active_run)] == [
        "first",
        "resumed",
    ]
    ledger.save_job_result(
        JobResult(
            run_id=active_run,
            job_id="job_b",
            status=config.JOB_DONE,
            output_path="work/output/runs/x/job_b.raw",
        )
    )
    ledger.save_job_result(
        JobResult(
            run_id=active_run,
            job_id="job_a",
            status=config.JOB_DONE,
            output_path="work/output/runs/x/job_a.raw",
        )
    )
    assert ledger.output_paths(active_run) == [
        "work/output/runs/x/job_a.raw",
        "work/output/runs/x/job_b.raw",
    ]


def test_run_details_aggregate(ledger, active_run):
    ledger.record_attempt(_attempt(active_run, 1, "timeout"))
    ledger.save_job_result(
        JobResult(run_id=active_run, job_id="job_one", status=config.JOB_FAILED)
    )
    ledger.save_job_result(
        JobResult(run_id=active_run, job_id="job_two", status=config.JOB_DONE)
    )
    details = ledger.get_run_details(active_run)
    assert details["job_counts"] == {"done": 1, "failed": 1, "skipped": 0}
    assert details["total_attempts"] == 1
    assert len(details["jobs"]) == 2
    assert ledger.get_run_details("missing") is None


def test_artifact_revision_rows_are_append_only(ledger, active_run):
    revision = ArtifactRevision(
        run_id=active_run,
        job_id="job_one",
        revision_no=1,
        old_sha256="a" * 64,
        new_sha256="b" * 64,
        backup_path=f"work/output/runs/{active_run}/job_one.{'a' * 8}.raw.bak",
        at="2026-01-01T00:00:00Z",
    )
    ledger.save_artifact_revision(revision)
    ledger.save_artifact_revision(
        ArtifactRevision(
            run_id=active_run,
            job_id="job_one",
            revision_no=2,
            old_sha256="b" * 64,
            new_sha256="c" * 64,
            backup_path=f"work/output/runs/{active_run}/job_one.{'b' * 8}.raw.bak",
            at="2026-01-01T00:00:01Z",
        )
    )
    rows = ledger.get_artifact_revisions(active_run)
    assert [item.revision_no for item in rows] == [1, 2]
    assert rows[0].old_sha256 == "a" * 64
    assert rows[1].backup_path.endswith("job_one.bbbbbbbb.raw.bak")
    assert ledger.next_revision_no(active_run, "job_one") == 3
    assert ledger.next_revision_no(active_run, "job_other") == 1
    # The revision table does not disturb the single job_results row.
    assert ledger.get_job_results(active_run) == []


def test_job_dependencies_roundtrip(ledger, active_run):
    ledger.save_job_dependencies(active_run, "job_b", ["job_a"])
    ledger.save_job_dependencies(active_run, "job_c", ["job_a", "job_b"])
    assert ledger.get_job_dependencies(active_run) == {
        "job_b": ["job_a"],
        "job_c": ["job_a", "job_b"],
    }
