"""Run-level job_budget quotas: validation, scheduling, resume and reports."""

from __future__ import annotations

import json
import threading
import time
from pathlib import Path

import pytest

from batchsched import config
from batchsched.cli import main as cli_main
from batchsched.models import (
    AttemptRecord,
    ExecutionOutcome,
    JobResult,
    PlanValidationError,
)
from batchsched.plan_loader import load_plan, parse_plan_text
from batchsched.reporter import build_report, to_markdown
from batchsched.scheduler import Scheduler, execute_once

ROOT = Path(__file__).resolve().parents[1]


class BudgetExecutor:
    """Executor recording one attempt per job and failing selected jobs
    with a chosen error class."""

    def __init__(self, ledger, run_id, failures=None, delay=0.0):
        self.ledger = ledger
        self.run_id = run_id
        self.failures = dict(failures or {})
        self.delay = delay
        self.lock = threading.Lock()
        self.executed: list[str] = []
        self.active = 0
        self.max_active = 0

    def execute(self, job):
        with self.lock:
            self.executed.append(job.id)
            self.active += 1
            self.max_active = max(self.max_active, self.active)
        try:
            if self.delay:
                time.sleep(self.delay)
            error_class = self.failures.get(job.id)
            attempt_no = self.ledger.next_attempt_no(self.run_id, job.id)
            self.ledger.record_attempt(
                AttemptRecord(
                    run_id=self.run_id,
                    job_id=job.id,
                    attempt_no=attempt_no,
                    error_class=error_class,
                    duration_ms=1,
                    backoff_ms=0,
                    started_at=config.utc_now_iso(),
                )
            )
            if error_class is not None:
                reason = f"{error_class}: synthetic"
                self.ledger.save_job_result(
                    JobResult(
                        run_id=self.run_id,
                        job_id=job.id,
                        status=config.JOB_FAILED,
                        reason=reason,
                    )
                )
                return ExecutionOutcome(
                    job_id=job.id,
                    status=config.JOB_FAILED,
                    reason=reason,
                    error_class=error_class,
                    attempts=attempt_no,
                )
            self.ledger.save_job_result(
                JobResult(
                    run_id=self.run_id,
                    job_id=job.id,
                    status=config.JOB_DONE,
                    sha256="0" * 64,
                    size_bytes=1,
                    output_path=f"work/output/runs/{self.run_id}/{job.id}.raw",
                )
            )
            return ExecutionOutcome(
                job_id=job.id, status=config.JOB_DONE, attempts=attempt_no
            )
        finally:
            with self.lock:
                self.active -= 1


def _plan(job_ids, concurrency=3, budget=None, depends=None):
    lines = ["name: budget-plan", f"concurrency: {concurrency}"]
    if budget:
        lines.append("job_budget:")
        lines.extend(f"  {key}: {value}" for key, value in budget.items())
    lines.append("jobs:")
    for job_id in job_ids:
        lines.append(f"  - id: {job_id}")
        lines.append("    source: /api/catalog")
        deps = (depends or {}).get(job_id)
        if deps:
            lines.append(f"    depends_on: [{', '.join(deps)}]")
    return parse_plan_text("\n".join(lines) + "\n")


def _results_by_job(ledger, run_id):
    return {r.job_id: r for r in ledger.get_job_results(run_id)}


# --- plan validation -------------------------------------------------------


def test_job_budget_defaults_to_unlimited():
    plan = parse_plan_text(
        "name: p\njobs:\n  - id: a\n    source: /api/catalog\n"
    )
    assert plan.job_budget == {}


def test_job_budget_valid_entries_accepted():
    plan = parse_plan_text(
        """
name: p
job_budget:
  http_4xx: 1
  network: 0
jobs:
  - id: a
    source: /api/catalog
"""
    )
    assert plan.job_budget == {"http_4xx": 1, "network": 0}


@pytest.mark.parametrize("value", ["-1", "1.5", "'1'", "true"])
def test_job_budget_rejects_bad_values(value):
    with pytest.raises(PlanValidationError) as caught:
        parse_plan_text(
            "name: p\n"
            "job_budget:\n"
            f"  http_4xx: {value}\n"
            "jobs:\n"
            "  - id: a\n"
            "    source: /api/catalog\n"
        )
    assert any(i.code == "invalid_job_budget" for i in caught.value.issues)


@pytest.mark.parametrize("key", ["http_404", "404"])
def test_job_budget_rejects_unknown_class(key):
    with pytest.raises(PlanValidationError) as caught:
        parse_plan_text(
            "name: p\n"
            "job_budget:\n"
            f"  {key}: 1\n"
            "jobs:\n"
            "  - id: a\n"
            "    source: /api/catalog\n"
        )
    issues = [i for i in caught.value.issues if i.code == "invalid_job_budget"]
    assert issues
    assert "unknown error class" in issues[0].message


def test_job_budget_rejects_non_mapping():
    with pytest.raises(PlanValidationError) as caught:
        parse_plan_text(
            "name: p\n"
            "job_budget: [http_4xx]\n"
            "jobs:\n"
            "  - id: a\n"
            "    source: /api/catalog\n"
        )
    assert any(i.code == "invalid_job_budget" for i in caught.value.issues)


def test_demo_plan_carries_job_budget():
    plan = load_plan(ROOT / "plans" / "demo.yaml")
    assert plan.job_budget == {"http_4xx": 1}


# --- scheduler behaviour ---------------------------------------------------


def test_ample_budget_behaves_like_before(ledger, active_run):
    plan = _plan(["a", "b", "c"], concurrency=2, budget={"http_4xx": 5})
    executor = BudgetExecutor(ledger, active_run, failures={"a": "http_4xx"})
    outcome = Scheduler(plan, ledger, active_run, executor).run()
    assert outcome.status == config.RUN_FAILED
    assert outcome.states == {
        "a": config.JOB_FAILED,
        "b": config.JOB_DONE,
        "c": config.JOB_DONE,
    }
    assert sorted(executor.executed) == ["a", "b", "c"]
    assert ledger.budget_downgraded_jobs(active_run) == []
    summary = ledger.get_run(active_run)["summary"]
    assert summary["budget"]["limits"] == {"http_4xx": 5}
    assert summary["budget"]["terminal"] == {"http_4xx": 1}
    assert summary["budget"]["exhausted"] == []


def test_exhausted_budget_skips_pending_without_attempts(ledger, active_run):
    plan = _plan(["a", "b", "c"], concurrency=1, budget={"http_4xx": 1})
    executor = BudgetExecutor(ledger, active_run, failures={"a": "http_4xx"})
    outcome = Scheduler(plan, ledger, active_run, executor).run()
    assert outcome.status == config.RUN_FAILED
    assert outcome.states["a"] == config.JOB_FAILED
    assert outcome.states["b"] == config.JOB_SKIPPED
    assert outcome.states["c"] == config.JOB_SKIPPED
    assert executor.executed == ["a"]
    results = _results_by_job(ledger, active_run)
    assert results["b"].reason == config.BUDGET_EXHAUSTED_REASON
    assert results["c"].reason == config.BUDGET_EXHAUSTED_REASON
    attempted = {row["job_id"] for row in ledger.get_attempts(active_run)}
    assert attempted == {"a"}
    assert ledger.budget_downgraded_jobs(active_run) == ["b", "c"]


def test_inflight_jobs_finish_when_budget_trips(ledger, active_run):
    """Concurrency 3 with quota 1: three 404 jobs run together; the first
    to finish exhausts the quota, but the other two already dispatched must
    complete with their own attempts and failed results.  Only jobs still
    pending at that point are downgraded."""

    plan = _plan(["a", "b", "c", "d", "e"], concurrency=3, budget={"http_4xx": 1})
    failures = {"a": "http_4xx", "b": "http_4xx", "c": "http_4xx"}
    executor = BudgetExecutor(ledger, active_run, failures=failures, delay=0.05)
    outcome = Scheduler(plan, ledger, active_run, executor).run()
    assert executor.max_active == 3
    assert sorted(executor.executed) == ["a", "b", "c"]
    for job_id in ("a", "b", "c"):
        assert outcome.states[job_id] == config.JOB_FAILED
    for job_id in ("d", "e"):
        assert outcome.states[job_id] == config.JOB_SKIPPED
    attempted: dict[str, int] = {}
    for row in ledger.get_attempts(active_run):
        attempted[row["job_id"]] = attempted.get(row["job_id"], 0) + 1
    assert attempted == {"a": 1, "b": 1, "c": 1}
    results = _results_by_job(ledger, active_run)
    for job_id in ("a", "b", "c"):
        assert results[job_id].status == config.JOB_FAILED
        assert results[job_id].reason.startswith("http_4xx")
    for job_id in ("d", "e"):
        assert results[job_id].reason == config.BUDGET_EXHAUSTED_REASON
    summary = ledger.get_run(active_run)["summary"]
    assert summary["budget"]["terminal"] == {"http_4xx": 3}
    assert summary["budget"]["exhausted"] == ["http_4xx"]
    assert summary["budget"]["downgraded"] == ["d", "e"]
    assert summary["budget"]["dispatched"] == 0


def test_budget_classes_are_independent(ledger, active_run):
    plan = _plan(["a", "b", "c"], concurrency=1, budget={"http_4xx": 1})
    executor = BudgetExecutor(ledger, active_run, failures={"a": "http_5xx"})
    outcome = Scheduler(plan, ledger, active_run, executor).run()
    assert sorted(executor.executed) == ["a", "b", "c"]
    assert outcome.states["b"] == config.JOB_DONE
    assert outcome.states["c"] == config.JOB_DONE
    assert ledger.budget_downgraded_jobs(active_run) == []
    summary = ledger.get_run(active_run)["summary"]
    assert summary["budget"]["terminal"] == {"http_5xx": 1}
    assert summary["budget"]["exhausted"] == []


def test_each_class_trips_only_its_own_quota(ledger, active_run):
    plan = _plan(
        ["a", "b", "c"],
        concurrency=1,
        budget={"http_4xx": 1, "http_5xx": 2},
    )
    executor = BudgetExecutor(
        ledger, active_run, failures={"a": "http_5xx", "b": "http_4xx"}
    )
    outcome = Scheduler(plan, ledger, active_run, executor).run()
    # a's http_5xx failure stays below the http_5xx quota and never touches
    # the http_4xx counter; b's http_4xx failure trips its own quota.
    assert outcome.states["a"] == config.JOB_FAILED
    assert outcome.states["b"] == config.JOB_FAILED
    assert outcome.states["c"] == config.JOB_SKIPPED
    assert ledger.budget_downgraded_jobs(active_run) == ["c"]
    summary = ledger.get_run(active_run)["summary"]
    assert summary["budget"]["terminal"] == {"http_4xx": 1, "http_5xx": 1}
    assert summary["budget"]["exhausted"] == ["http_4xx"]


def test_upstream_failed_still_propagates_under_budget(ledger, active_run):
    plan = _plan(
        ["a", "b", "c"],
        concurrency=1,
        budget={"http_4xx": 1},
        depends={"b": ["a"]},
    )
    executor = BudgetExecutor(ledger, active_run, failures={"a": "http_4xx"})
    Scheduler(plan, ledger, active_run, executor).run()
    results = _results_by_job(ledger, active_run)
    assert results["a"].status == config.JOB_FAILED
    assert results["b"].reason == "upstream_failed"
    assert results["c"].reason == config.BUDGET_EXHAUSTED_REASON


def test_zero_budget_halts_dispatch_immediately(ledger, active_run):
    """A zero quota means the class is already at its limit, so no job may
    enter execution at all."""

    plan = _plan(["a", "b"], concurrency=1, budget={"http_4xx": 0})
    executor = BudgetExecutor(ledger, active_run)
    outcome = Scheduler(plan, ledger, active_run, executor).run()
    assert executor.executed == []
    assert set(outcome.states.values()) == {config.JOB_SKIPPED}
    assert ledger.budget_downgraded_jobs(active_run) == ["a", "b"]


# --- resume / ledger -------------------------------------------------------


def test_resume_seeds_terminal_failures_into_budget(ledger, active_run):
    plan = _plan(["a", "b"], concurrency=1, budget={"http_4xx": 1})
    ledger.save_job_result(
        JobResult(
            run_id=active_run,
            job_id="a",
            status=config.JOB_FAILED,
            reason="http_4xx: gone",
        )
    )
    executor = BudgetExecutor(ledger, active_run)
    outcome = execute_once(plan, ledger, active_run, executor)
    assert executor.executed == []
    assert outcome.states["a"] == config.JOB_FAILED
    assert outcome.states["b"] == config.JOB_SKIPPED
    results = _results_by_job(ledger, active_run)
    assert results["b"].reason == config.BUDGET_EXHAUSTED_REASON


def test_budget_skips_stay_skipped_on_resume(ledger, active_run):
    plan = _plan(["a", "b"], concurrency=1, budget={"http_4xx": 1})
    executor = BudgetExecutor(ledger, active_run, failures={"a": "http_4xx"})
    Scheduler(plan, ledger, active_run, executor).run()
    assert ledger.budget_downgraded_jobs(active_run) == ["b"]
    again = BudgetExecutor(ledger, active_run)
    outcome = execute_once(plan, ledger, active_run, again)
    assert again.executed == []
    assert outcome.states["b"] == config.JOB_SKIPPED
    assert len(ledger.get_attempts(active_run)) == 1


def test_ledger_budget_views(ledger, active_run):
    ledger.save_job_result(
        JobResult(
            run_id=active_run,
            job_id="f1",
            status=config.JOB_FAILED,
            reason="http_4xx: missing",
        )
    )
    ledger.save_job_result(
        JobResult(
            run_id=active_run,
            job_id="f2",
            status=config.JOB_FAILED,
            reason="http_4xx: gone",
        )
    )
    ledger.save_job_result(
        JobResult(
            run_id=active_run,
            job_id="f3",
            status=config.JOB_FAILED,
            reason="network: boom",
        )
    )
    ledger.save_job_result(
        JobResult(
            run_id=active_run,
            job_id="s1",
            status=config.JOB_SKIPPED,
            reason=config.BUDGET_EXHAUSTED_REASON,
        )
    )
    ledger.save_job_result(
        JobResult(
            run_id=active_run,
            job_id="s2",
            status=config.JOB_SKIPPED,
            reason="upstream_failed",
        )
    )
    ledger.save_job_result(
        JobResult(run_id=active_run, job_id="ok", status=config.JOB_DONE)
    )
    assert ledger.terminal_failure_classes(active_run) == {
        "http_4xx": 2,
        "network": 1,
    }
    assert ledger.budget_downgraded_jobs(active_run) == ["s1"]


# --- reporting -------------------------------------------------------------


def test_report_reflects_budget_downgrades(ledger, active_run):
    plan = _plan(["a", "b", "c", "d"], concurrency=3, budget={"http_4xx": 1})
    failures = {"a": "http_4xx", "b": "http_4xx", "c": "http_4xx"}
    executor = BudgetExecutor(ledger, active_run, failures=failures, delay=0.05)
    Scheduler(plan, ledger, active_run, executor).run()
    report = build_report(ledger, active_run)
    assert report["budget"]["limits"] == {"http_4xx": 1}
    assert report["budget"]["terminal_failures"] == {"http_4xx": 3}
    assert report["budget"]["exhausted"] == ["http_4xx"]
    assert report["budget"]["downgraded_jobs"] == ["d"]
    assert report["failure_class_counts"] == {"http_4xx": 3}
    job_d = next(job for job in report["jobs"] if job["job_id"] == "d")
    assert job_d["reason"] == config.BUDGET_EXHAUSTED_REASON
    assert job_d["attempts"] == []
    markdown = to_markdown(report)
    assert "## Job budget" in markdown
    assert "http_4xx=1" in markdown
    assert "http_4xx=3" in markdown
    assert "`d`" in markdown
    assert "budget_exhausted" in markdown


def test_report_without_budget(ledger, active_run):
    plan = _plan(["a"], concurrency=1)
    executor = BudgetExecutor(ledger, active_run)
    Scheduler(plan, ledger, active_run, executor).run()
    report = build_report(ledger, active_run)
    assert report["budget"]["limits"] == {}
    assert report["budget"]["downgraded_jobs"] == []
    assert "No job budget configured." in to_markdown(report)


# --- end to end ------------------------------------------------------------


def test_demo_plan_runs_with_job_budget(tmp_path, mock_base_url, capsys):
    code = cli_main(
        [
            "--work-dir",
            str(tmp_path),
            "run",
            "--plan",
            str(ROOT / "plans" / "demo.yaml"),
            "--base-url",
            mock_base_url,
        ]
    )
    payload = json.loads(capsys.readouterr().out)
    assert code == 0
    assert payload["status"] == "completed"
    assert payload["job_counts"]["done"] == 6
