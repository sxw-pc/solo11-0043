from __future__ import annotations

from threading import Event

from batchsched import config
from batchsched.models import JobResult, PlanDag
from batchsched.plan_loader import parse_plan_text
from batchsched.scheduler import Scheduler, execute_once


def _parallel_plan(count: int = 4, concurrency: int = 2) -> PlanDag:
    jobs = "".join(
        f"  - id: job_{index}\n    source: /api/catalog\n" for index in range(count)
    )
    return parse_plan_text(
        f"name: parallel\nconcurrency: {concurrency}\njobs:\n{jobs}"
    )


def test_scheduler_completes_all_jobs(ledger, active_run, fake_executor_cls):
    plan = _parallel_plan(3, 2)
    executor = fake_executor_cls(ledger, active_run, delay=0.01)
    outcome = Scheduler(plan, ledger, active_run, executor).run()
    assert outcome.status == config.RUN_COMPLETED
    assert set(outcome.states.values()) == {config.JOB_DONE}
    results = ledger.get_job_results(active_run)
    assert len(results) == 3
    assert ledger.get_run(active_run)["status"] == config.RUN_COMPLETED


def test_plan_concurrency_limits_peak(ledger, active_run, fake_executor_cls):
    plan = _parallel_plan(6, 2)
    executor = fake_executor_cls(ledger, active_run, delay=0.06)
    outcome = Scheduler(plan, ledger, active_run, executor).run()
    assert outcome.status == config.RUN_COMPLETED
    assert executor.max_active == 2


def test_explicit_worker_limit_cannot_exceed_plan(ledger, active_run, fake_executor_cls):
    plan = _parallel_plan(4, 3)
    executor = fake_executor_cls(ledger, active_run, delay=0.05)
    Scheduler(plan, ledger, active_run, executor, max_workers=1).run()
    assert executor.max_active == 1
    assert len(executor.executed) == 4


def test_failed_dependency_skips_downstream(ledger, active_run, fake_executor_cls):
    plan = parse_plan_text(
        """
name: failure
concurrency: 2
jobs:
  - id: fetch_bad
    source: /api/catalog
  - id: build_a
    source: /api/orders
    depends_on: [fetch_bad]
  - id: build_b
    source: /api/orders
    depends_on: [build_a]
  - id: independent
    source: /api/summary
"""
    )
    executor = fake_executor_cls(ledger, active_run, failures={"fetch_bad"})
    outcome = Scheduler(plan, ledger, active_run, executor).run()
    assert outcome.status == config.RUN_FAILED
    assert outcome.states["fetch_bad"] == config.JOB_FAILED
    assert outcome.states["build_a"] == config.JOB_SKIPPED
    assert outcome.states["build_b"] == config.JOB_SKIPPED
    assert outcome.states["independent"] == config.JOB_DONE
    results = {item.job_id: item for item in ledger.get_job_results(active_run)}
    assert results["build_a"].reason == "upstream_failed"
    assert "build_a" not in executor.executed


def test_existing_done_job_is_not_executed(ledger, active_run, fake_executor_cls):
    plan = parse_plan_text(
        """
name: seeded
concurrency: 1
jobs:
  - id: fetch_done
    source: /api/catalog
  - id: fetch_next
    source: /api/orders
    depends_on: [fetch_done]
"""
    )
    ledger.save_job_result(
        JobResult(
            run_id=active_run,
            job_id="fetch_done",
            status=config.JOB_DONE,
            sha256="a" * 64,
            size_bytes=10,
            output_path=f"work/output/runs/{active_run}/fetch_done.raw",
        )
    )
    executor = fake_executor_cls(ledger, active_run)
    outcome = execute_once(plan, ledger, active_run, executor)
    assert outcome.status == config.RUN_COMPLETED
    assert executor.executed == ["fetch_next"]


def test_stop_event_interrupts_without_dispatching(ledger, active_run, fake_executor_cls):
    plan = _parallel_plan(3, 2)
    executor = fake_executor_cls(ledger, active_run)
    stop = Event()
    stop.set()
    outcome = Scheduler(plan, ledger, active_run, executor, stop_event=stop).run()
    assert outcome.interrupted is True
    assert outcome.status == config.RUN_INTERRUPTED
    assert executor.executed == []
    assert ledger.get_run(active_run)["status"] == config.RUN_INTERRUPTED


def test_worker_exception_becomes_failed_result(ledger, active_run, fake_executor_cls):
    plan = _parallel_plan(2, 1)
    executor = fake_executor_cls(ledger, active_run, raise_for={"job_0"})
    outcome = Scheduler(plan, ledger, active_run, executor).run()
    assert outcome.status == config.RUN_FAILED
    result = next(
        item for item in ledger.get_job_results(active_run) if item.job_id == "job_0"
    )
    assert result.status == config.JOB_FAILED
    assert "worker_exception" in result.reason
