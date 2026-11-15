"""Cross-run artifact reuse during resume."""

from __future__ import annotations

import hashlib

from batchsched import config
from batchsched.executor import JobExecutor
from batchsched.fetcher import Fetcher
from batchsched.models import JobResult
from batchsched.plan_loader import parse_plan_text
from batchsched.reporter import build_report, to_markdown
from batchsched.resume import resume_run
from batchsched.scheduler import Scheduler

CHAIN_TEXT = (
    "name: reuse-chain\nconcurrency: 1\njobs:\n"
    "  - {id: fetch_a, source: /api/catalog}\n"
    "  - {id: fetch_b, source: /api/orders, depends_on: [fetch_a]}\n"
    "  - {id: fetch_c, source: /api/summary, depends_on: [fetch_b]}\n"
)
PLAN_PATH = "plans/reuse.yaml"
DONOR_ID = "run-donor"
RESUME_ID = "run-resume"
JOBS = ("fetch_a", "fetch_b", "fetch_c")


def _executor(ledger, run_id, base_url, base_dir):
    return JobExecutor(
        Fetcher(base_url=base_url, timeout_seconds=1.0), ledger, run_id,
        base_dir / "work" / "output" / "runs" / run_id,
    )


def _seed_donor(ledger, base_dir, plan, job_ids, status=config.RUN_COMPLETED):
    """Create a finished donor run with done rows and artifact files."""

    ledger.create_run(DONOR_ID, PLAN_PATH, "2026-01-01T00:00:00Z")
    ledger.finish_run(DONOR_ID, status, "2026-01-01T00:01:00Z")
    for job_id in job_ids:
        payload = f"{DONOR_ID}:{job_id}".encode()
        target = base_dir / "work" / "output" / "runs" / DONOR_ID / f"{job_id}.raw"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(payload)
        ledger.save_job_result(JobResult(
            run_id=DONOR_ID, job_id=job_id, status=config.JOB_DONE,
            sha256=hashlib.sha256(payload).hexdigest(), size_bytes=len(payload),
            output_path=config.run_output_relative(DONOR_ID, f"{job_id}.raw"),
            job_fingerprint=plan.jobs[job_id].fingerprint(),
        ))


def _resume(ledger, plan, tmp_path, executor):
    ledger.create_run(RESUME_ID, PLAN_PATH, "2026-01-02T00:00:00Z")
    return resume_run(ledger, plan, RESUME_ID, executor, base_dir=tmp_path)


def _results(ledger):
    return {r.job_id: r for r in ledger.get_job_results(RESUME_ID)}


def _events(ledger):
    return [item["event"] for item in ledger.get_audit(RESUME_ID)]


class ForbiddenExecutor:
    def execute(self, job):
        raise AssertionError(f"should not execute {job.id}")


def test_resume_reuses_matching_artifacts(mock_base_url, ledger, tmp_path):
    plan = parse_plan_text(CHAIN_TEXT)
    ledger.create_run(DONOR_ID, PLAN_PATH, "2026-01-01T00:00:00Z")
    donor = Scheduler(
        plan, ledger, DONOR_ID, _executor(ledger, DONOR_ID, mock_base_url, tmp_path)
    ).run()
    assert donor.status == config.RUN_COMPLETED
    donor_results = {r.job_id: r for r in ledger.get_job_results(DONOR_ID)}

    outcome = _resume(ledger, plan, tmp_path, ForbiddenExecutor())
    assert outcome.status == config.RUN_COMPLETED
    results = _results(ledger)
    assert set(results) == set(JOBS)
    for job_id, result in results.items():
        assert (result.status, result.reused_from) == (config.JOB_DONE, DONOR_ID)
        assert result.sha256 == donor_results[job_id].sha256
        assert result.output_path == f"work/output/runs/{DONOR_ID}/{job_id}.raw"
    assert _events(ledger).count("artifact_reused") == 3
    assert ledger.get_attempts(RESUME_ID) == []
    report = build_report(ledger, RESUME_ID, base_dir=tmp_path)
    assert report["outputs"] == []
    assert report["reused_outputs"] == sorted(
        f"work/output/runs/{DONOR_ID}/{job}.raw" for job in JOBS
    )


def test_reuse_requires_dependencies_done(ledger, tmp_path, fake_executor_cls):
    """Downstream donor hits are ignored while an upstream job reruns."""

    plan = parse_plan_text(CHAIN_TEXT)
    _seed_donor(ledger, tmp_path, plan, ["fetch_b"])  # no donor row for fetch_a
    executor = fake_executor_cls(ledger, RESUME_ID)
    assert _resume(ledger, plan, tmp_path, executor).status == config.RUN_COMPLETED
    assert sorted(executor.executed) == sorted(JOBS)
    assert all(r.reused_from is None for r in _results(ledger).values())
    assert "artifact_reused" not in _events(ledger)


def test_missing_donor_file_falls_back_to_execution(
    ledger, tmp_path, fake_executor_cls
):
    """A deleted donor artifact falls back cleanly, with no reuse record."""

    plan = parse_plan_text(CHAIN_TEXT)
    _seed_donor(ledger, tmp_path, plan, JOBS)
    (tmp_path / "work" / "output" / "runs" / DONOR_ID / "fetch_c.raw").unlink()
    executor = fake_executor_cls(ledger, RESUME_ID)
    assert _resume(ledger, plan, tmp_path, executor).status == config.RUN_COMPLETED
    assert executor.executed == ["fetch_c"]
    results = _results(ledger)
    assert results["fetch_a"].reused_from == DONOR_ID
    assert results["fetch_b"].reused_from == DONOR_ID
    fallback = results["fetch_c"]
    assert fallback.status == config.JOB_DONE and fallback.reused_from is None
    assert fallback.output_path == f"work/output/runs/{RESUME_ID}/fetch_c.raw"
    assert _events(ledger).count("artifact_reused") == 2
    assert "artifact_reuse_miss" in _events(ledger)


def test_missing_upstream_artifact_blocks_downstream_reuse(
    ledger, tmp_path, fake_executor_cls
):
    """Losing the upstream donor file cascades: everything below re-executes."""

    plan = parse_plan_text(CHAIN_TEXT)
    _seed_donor(ledger, tmp_path, plan, JOBS)
    (tmp_path / "work" / "output" / "runs" / DONOR_ID / "fetch_a.raw").unlink()
    executor = fake_executor_cls(ledger, RESUME_ID)
    assert _resume(ledger, plan, tmp_path, executor).status == config.RUN_COMPLETED
    assert sorted(executor.executed) == sorted(JOBS)
    assert all(r.reused_from is None for r in _results(ledger).values())


def test_skipped_donor_row_is_not_a_reuse_hit(ledger, tmp_path, fake_executor_cls):
    """A skipped donor row may share the fingerprint but has no artifact."""

    plan = parse_plan_text(CHAIN_TEXT)
    _seed_donor(ledger, tmp_path, plan, [], status=config.RUN_FAILED)
    ledger.save_job_result(JobResult(
        run_id=DONOR_ID, job_id="fetch_a", status=config.JOB_SKIPPED,
        reason="upstream_failed",
        job_fingerprint=plan.jobs["fetch_a"].fingerprint(),
    ))
    executor = fake_executor_cls(ledger, RESUME_ID)
    assert _resume(ledger, plan, tmp_path, executor).status == config.RUN_COMPLETED
    assert "fetch_a" in executor.executed
    assert _results(ledger)["fetch_a"].reused_from is None


def test_fingerprint_mismatch_is_not_a_reuse_hit(
    ledger, tmp_path, fake_executor_cls
):
    plan = parse_plan_text(CHAIN_TEXT)
    _seed_donor(ledger, tmp_path, plan, ["fetch_a"])
    ledger.save_job_result(JobResult(  # same job, but a stale fingerprint
        run_id=DONOR_ID, job_id="fetch_a", status=config.JOB_DONE, sha256="1" * 64,
        size_bytes=11, output_path=config.run_output_relative(DONOR_ID, "fetch_a.raw"),
        job_fingerprint="0" * 64,
    ))
    executor = fake_executor_cls(ledger, RESUME_ID)
    assert _resume(ledger, plan, tmp_path, executor).status == config.RUN_COMPLETED
    assert "fetch_a" in executor.executed
    assert _results(ledger)["fetch_a"].reused_from is None


def test_report_separates_produced_and_reused_outputs(ledger, tmp_path):
    ledger.create_run(RESUME_ID, PLAN_PATH, "2026-01-02T00:00:00Z")
    ledger.finish_run(RESUME_ID, config.RUN_COMPLETED, "2026-01-02T00:01:00Z")
    ledger.save_job_result(JobResult(
        run_id=RESUME_ID, job_id="fetch_a", status=config.JOB_DONE, sha256="a" * 64,
        size_bytes=3, output_path=f"work/output/runs/{RESUME_ID}/fetch_a.raw",
        job_fingerprint="f" * 64,
    ))
    ledger.save_job_result(JobResult(
        run_id=RESUME_ID, job_id="fetch_b", status=config.JOB_DONE, sha256="b" * 64,
        size_bytes=3, output_path=f"work/output/runs/{DONOR_ID}/fetch_b.raw",
        job_fingerprint="e" * 64, reused_from=DONOR_ID,
    ))
    report = build_report(ledger, RESUME_ID, base_dir=tmp_path)
    assert report["outputs"] == [f"work/output/runs/{RESUME_ID}/fetch_a.raw"]
    assert report["reused_outputs"] == [f"work/output/runs/{DONOR_ID}/fetch_b.raw"]
    jobs = {job["job_id"]: job for job in report["jobs"]}
    assert jobs["fetch_a"]["reused_from"] is None
    assert jobs["fetch_b"]["reused_from"] == DONOR_ID
    markdown = to_markdown(report)
    assert "Reused from earlier runs:" in markdown
    assert DONOR_ID in markdown
