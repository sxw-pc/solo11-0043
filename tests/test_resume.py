from __future__ import annotations

from threading import Event

import pytest

from batchsched import config
from batchsched.executor import JobExecutor
from batchsched.fetcher import Fetcher
from batchsched.models import JobResult
from batchsched.plan_loader import parse_plan_text
from batchsched.resume import ResumeError, load_resume_state, resume_run
from batchsched.scheduler import Scheduler

PLAN_TEXT = """
name: resume-demo
concurrency: 1
jobs:
  - id: fetch_a
    source: /api/catalog
  - id: fetch_b
    source: /api/orders
  - id: fetch_c
    source: /api/summary
    depends_on: [fetch_a, fetch_b]
"""

CHAIN_TEXT = """
name: resume-chain
concurrency: 1
jobs:
  - id: fetch_a
    source: /api/catalog
  - id: fetch_b
    source: /api/orders
    depends_on: [fetch_a]
  - id: fetch_c
    source: /api/summary
    depends_on: [fetch_b]
"""


def _executor(ledger, run_id, base_url, output_root):
    return JobExecutor(
        Fetcher(base_url=base_url, timeout_seconds=1.0),
        ledger,
        run_id,
        output_root / run_id,
    )


def test_interrupt_then_resume_matches_uninterrupted_hashes(
    mock_base_url, ledger, tmp_path
):
    plan = parse_plan_text(PLAN_TEXT)
    baseline_id = "run-baseline"
    partial_id = "run-partial"
    ledger.create_run(baseline_id, "plans/resume.yaml", config.utc_now_iso())
    ledger.create_run(partial_id, "plans/resume.yaml", config.utc_now_iso())
    baseline = Scheduler(
        plan,
        ledger,
        baseline_id,
        _executor(ledger, baseline_id, mock_base_url, tmp_path / "out"),
    ).run()
    assert baseline.status == config.RUN_COMPLETED
    baseline_hashes = {
        item.job_id: item.sha256 for item in ledger.get_job_results(baseline_id)
    }

    stop = Event()

    def interrupt(event_name, job_id):
        if event_name == "job_done" and job_id == "fetch_a":
            stop.set()

    interrupted = Scheduler(
        plan,
        ledger,
        partial_id,
        _executor(ledger, partial_id, mock_base_url, tmp_path / "out"),
        stop_event=stop,
        on_event=interrupt,
    ).run()
    assert interrupted.status == config.RUN_INTERRUPTED
    assert interrupted.states["fetch_b"] == config.JOB_PENDING
    before_attempts = len(ledger.get_attempts(partial_id, "fetch_a"))

    state = load_resume_state(ledger, plan, partial_id)
    assert state.completed == ("fetch_a",)
    assert state.pending == ("fetch_b", "fetch_c")

    resumed = resume_run(
        ledger,
        plan,
        partial_id,
        _executor(ledger, partial_id, mock_base_url, tmp_path / "out"),
    )
    assert resumed.status == config.RUN_COMPLETED
    results = ledger.get_job_results(partial_id)
    assert len(results) == 3
    resumed_hashes = {item.job_id: item.sha256 for item in results}
    assert resumed_hashes == baseline_hashes
    assert len(ledger.get_attempts(partial_id, "fetch_a")) == before_attempts
    assert any(item["event"] == "resumed" for item in ledger.get_audit(partial_id))
    assert ledger.get_run(partial_id)["status"] == config.RUN_COMPLETED
    for result in results:
        assert (tmp_path / "out" / partial_id / f"{result.job_id}.raw").exists()


def test_resume_keeps_failed_upstream_and_reports_skips(
    mock_base_url, ledger, tmp_path, fake_executor_cls
):
    plan = parse_plan_text(CHAIN_TEXT)
    run_id = "run-failed-chain"
    ledger.create_run(run_id, "plans/resume.yaml", config.utc_now_iso())
    ledger.save_job_result(
        JobResult(
            run_id=run_id,
            job_id="fetch_a",
            status=config.JOB_FAILED,
            reason="http_5xx: exhausted",
        )
    )
    executor = fake_executor_cls(ledger, run_id)
    outcome = resume_run(ledger, plan, run_id, executor)
    assert outcome.status == config.RUN_FAILED
    assert outcome.states["fetch_a"] == config.JOB_FAILED
    assert outcome.states["fetch_b"] == config.JOB_SKIPPED
    assert outcome.states["fetch_c"] == config.JOB_SKIPPED
    assert executor.executed == []
    results = {item.job_id: item for item in ledger.get_job_results(run_id)}
    assert results["fetch_b"].reason == "upstream_failed"
    assert results["fetch_c"].status == config.JOB_SKIPPED


def test_resume_completed_run_does_not_dispatch(ledger):
    plan = parse_plan_text(PLAN_TEXT)
    run_id = "run-already-done"
    ledger.create_run(run_id, "plans/resume.yaml", config.utc_now_iso())
    for job_id in ("fetch_a", "fetch_b", "fetch_c"):
        ledger.save_job_result(
            JobResult(
                run_id=run_id,
                job_id=job_id,
                status=config.JOB_DONE,
                sha256="f" * 64,
                size_bytes=3,
                output_path=f"work/output/runs/{run_id}/{job_id}.raw",
            )
        )

    class ForbiddenExecutor:
        def execute(self, job):
            raise AssertionError(f"should not execute {job.id}")

    outcome = resume_run(ledger, plan, run_id, ForbiddenExecutor())
    assert outcome.status == config.RUN_COMPLETED
    assert any(item["event"] == "resumed" for item in ledger.get_audit(run_id))


def test_missing_run_and_unknown_ledger_job(ledger):
    plan = parse_plan_text(PLAN_TEXT)
    with pytest.raises(ResumeError, match="run not found"):
        load_resume_state(ledger, plan, "run-missing")
    run_id = "run-unknown-job"
    ledger.create_run(run_id, "plans/resume.yaml", config.utc_now_iso())
    ledger.save_job_result(
        JobResult(run_id=run_id, job_id="ghost_job", status=config.JOB_DONE)
    )
    with pytest.raises(ResumeError, match="missing from plan"):
        load_resume_state(ledger, plan, run_id)


def test_resume_state_counts(ledger):
    plan = parse_plan_text(PLAN_TEXT)
    run_id = "run-counts"
    ledger.create_run(run_id, "plans/resume.yaml", config.utc_now_iso())
    ledger.save_job_result(
        JobResult(run_id=run_id, job_id="fetch_a", status=config.JOB_DONE)
    )
    ledger.save_job_result(
        JobResult(run_id=run_id, job_id="fetch_b", status=config.JOB_FAILED)
    )
    state = load_resume_state(ledger, plan, run_id)
    assert state.completed == ("fetch_a",)
    assert state.failed == ("fetch_b",)
    assert state.skipped == ()
    assert state.pending == ("fetch_c",)
    assert state.summary()["pending"] == 1


def test_resume_appends_dispatch_without_reordering_done_jobs(
    mock_base_url, ledger, tmp_path
):
    plan = parse_plan_text(PLAN_TEXT)
    run_id = "run-dispatch-resume"
    ledger.create_run(run_id, "plans/resume.yaml", config.utc_now_iso())
    stop = Event()

    def interrupt(event_name, job_id):
        if event_name == "job_done" and job_id == "fetch_a":
            stop.set()

    first = Scheduler(
        plan,
        ledger,
        run_id,
        _executor(ledger, run_id, mock_base_url, tmp_path / "out"),
        stop_event=stop,
        on_event=interrupt,
    ).run()
    assert first.status == config.RUN_INTERRUPTED
    assert ledger.get_dispatch_order(run_id) == ["fetch_a"]

    resumed = resume_run(
        ledger,
        plan,
        run_id,
        _executor(ledger, run_id, mock_base_url, tmp_path / "out"),
    )
    assert resumed.status == config.RUN_COMPLETED
    order = ledger.get_dispatch_order(run_id)
    assert order == ["fetch_a", "fetch_b", "fetch_c"]
    assert list(resumed.dispatch_order) == order
