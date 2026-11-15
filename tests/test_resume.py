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


class _ScriptedFetcher:
    """Serves each source a distinct body per call; no network is touched."""

    def __init__(self, bodies_by_source: dict[str, list[bytes]]):
        self.bodies_by_source = {
            source: list(bodies) for source, bodies in bodies_by_source.items()
        }
        self.calls: dict[str, int] = {}

    def fetch(self, source: str):
        from batchsched.fetcher import FetchResponse

        index = self.calls.get(source, 0)
        bodies = self.bodies_by_source[source]
        self.calls[source] = index + 1
        return FetchResponse(
            content=bodies[min(index, len(bodies) - 1)],
            status_code=200,
            content_type="application/json; charset=utf-8",
            elapsed_ms=1,
            url=source,
        )


def _scripted_executor(ledger, run_id, output_root, bodies_by_source):
    return JobExecutor(
        _ScriptedFetcher(bodies_by_source),
        ledger,
        run_id,
        output_root / run_id,
    )


def test_resume_does_not_rewrite_done_artifact_even_if_source_changes(
    ledger, tmp_path
):
    """Done jobs stay untouched on resume: no re-execution, no backup,
    no revision row and byte-identical output.
    """

    run_id = "run-resume-no-rewrite"
    ledger.create_run(run_id, "plans/resume.yaml", config.utc_now_iso())
    plan = parse_plan_text(PLAN_TEXT)
    output_root = tmp_path / "out"

    # Only fetch_a has landed; fetch_b and fetch_c are still pending.
    first = _scripted_executor(
        ledger,
        run_id,
        output_root,
        {"/api/catalog": [b"a-original"]},
    )
    a_outcome = first.execute(plan.jobs["fetch_a"])
    assert a_outcome.status == config.JOB_DONE
    raw_a = output_root / run_id / "fetch_a.raw"
    before_bytes = raw_a.read_bytes()
    before_result = ledger.get_job_result(run_id, "fetch_a")
    assert before_result is not None and before_result.sha256 == a_outcome.sha256

    # The upstream source would now serve different bytes, but fetch_a is done.
    resumed_executor = _scripted_executor(
        ledger,
        run_id,
        output_root,
        {
            "/api/catalog": [b"a-CHANGED"],
            "/api/orders": [b"b-recovered"],
            "/api/summary": [b"c-recovered"],
        },
    )
    outcome = resume_run(ledger, plan, run_id, resumed_executor)
    assert outcome.status == config.RUN_COMPLETED

    # fetch_a was not executed again despite its changed source.
    assert "/api/catalog" not in resumed_executor.fetcher.calls
    assert raw_a.read_bytes() == before_bytes
    after_result = ledger.get_job_result(run_id, "fetch_a")
    assert after_result is not None
    assert after_result.sha256 == before_result.sha256
    assert ledger.get_artifact_revisions(run_id, "fetch_a") == []
    assert not list((output_root / run_id).glob("fetch_a.*.raw.bak"))
    assert not [
        item for item in ledger.get_audit(run_id)
        if item["event"] == "output_revised"
    ]

    # The pending jobs did land normally.
    assert (output_root / run_id / "fetch_b.raw").read_bytes() == b"b-recovered"
    assert (output_root / run_id / "fetch_c.raw").read_bytes() == b"c-recovered"
