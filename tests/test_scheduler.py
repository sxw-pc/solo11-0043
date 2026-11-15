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


PRIORITY_PLAN = """
name: priority
concurrency: 1
jobs:
  - id: root_low
    source: /api/catalog
    priority: -5
  - id: root_high
    source: /api/catalog
    priority: 20
  - id: root_mid_b
    source: /api/catalog
  - id: root_mid_a
    source: /api/catalog
  - id: layer_two_high
    source: /api/orders
    depends_on: [root_low]
    priority: 99
"""


def test_dispatch_uses_layer_then_priority_then_id(ledger, active_run, fake_executor_cls):
    plan = parse_plan_text(PRIORITY_PLAN)
    executor = fake_executor_cls(ledger, active_run)
    outcome = Scheduler(plan, ledger, active_run, executor).run()
    expected = ["root_high", "root_mid_a", "root_mid_b", "root_low", "layer_two_high"]
    assert executor.executed == expected
    assert list(outcome.dispatch_order) == expected
    assert ledger.get_dispatch_order(active_run) == expected


def test_priority_never_dispatches_job_before_its_dependency(
    ledger, active_run, fake_executor_cls
):
    plan = parse_plan_text(PRIORITY_PLAN)
    executor = fake_executor_cls(ledger, active_run)
    Scheduler(plan, ledger, active_run, executor, max_workers=8).run()
    order = ledger.get_dispatch_order(active_run)
    assert order.index("layer_two_high") > order.index("root_low")
    assert order.count("layer_two_high") == 1


def test_dispatch_order_is_unique_under_repeated_frontier_queries(
    ledger, active_run, fake_executor_cls
):
    plan = _parallel_plan(6, 2)
    executor = fake_executor_cls(ledger, active_run, delay=0.02)
    outcome = Scheduler(plan, ledger, active_run, executor).run()
    order = list(outcome.dispatch_order)
    assert sorted(order) == sorted(executor.executed)
    assert len(order) == len(set(order)) == 6


def test_same_priority_order_does_not_leak_pool_submission(ledger, active_run, fake_executor_cls):
    plan = _parallel_plan(6, 3)
    executor = fake_executor_cls(ledger, active_run, delay=0.01)
    outcome = Scheduler(plan, ledger, active_run, executor).run()
    assert list(outcome.dispatch_order) == [f"job_{index}" for index in range(6)]


def test_resume_appends_new_dispatch_after_recorded_history(
    ledger, active_run, fake_executor_cls
):
    plan = parse_plan_text(PRIORITY_PLAN)
    ledger.save_job_result(
        JobResult(run_id=active_run, job_id="root_low", status=config.JOB_DONE)
    )
    ledger.record_dispatch(active_run, "root_low")
    executor = fake_executor_cls(ledger, active_run)
    outcome = execute_once(plan, ledger, active_run, executor)
    order = ledger.get_dispatch_order(active_run)
    assert order[0] == "root_low"
    assert order[1:] == ["root_high", "root_mid_a", "root_mid_b", "layer_two_high"]
    assert list(outcome.dispatch_order) == order
    assert "root_low" not in executor.executed


def test_history_dispatched_but_unfinished_job_is_dispatched_once_more(
    ledger, active_run, fake_executor_cls
):
    """A prior attempt left no result: it stays pending and is dispatched once."""

    plan = _parallel_plan(3, 1)
    executor = fake_executor_cls(ledger, active_run)
    outcome = Scheduler(plan, ledger, active_run, executor).run()
    order = ledger.get_dispatch_order(active_run)
    assert order == ["job_0", "job_1", "job_2"]
    assert list(outcome.dispatch_order) == order
    assert executor.executed == ["job_0", "job_1", "job_2"]
