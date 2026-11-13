"""Concurrent, dependency-aware dispatch loop."""

from __future__ import annotations

from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from threading import Event
from typing import Callable

from . import config
from .dag import blocked_jobs, frontier
from .ledger import Ledger
from .models import (
    ExecutionOutcome,
    JobResult,
    PlanDag,
    SchedulerOutcome,
    terminal_states,
)

EventHook = Callable[[str, str], None]


class Scheduler:
    def __init__(
        self,
        plan: PlanDag,
        ledger: Ledger,
        run_id: str,
        executor: object,
        max_workers: int | None = None,
        initial_results: list[JobResult] | None = None,
        stop_event: Event | None = None,
        on_event: EventHook | None = None,
    ):
        self.plan = plan
        self.ledger = ledger
        self.run_id = run_id
        self.executor = executor
        self.max_workers = _worker_limit(plan.concurrency, max_workers)
        self.stop_event = stop_event
        self.on_event = on_event
        self.states = {job_id: config.JOB_PENDING for job_id in plan.jobs}
        for job_id, status in terminal_states(initial_results or []).items():
            if job_id in self.states:
                self.states[job_id] = status

    def run(self) -> SchedulerOutcome:
        interrupted = False
        futures: dict[Future[ExecutionOutcome], str] = {}
        try:
            with ThreadPoolExecutor(
                max_workers=self.max_workers, thread_name_prefix="batchsched"
            ) as pool:
                while True:
                    self._mark_blocked()
                    if self.stop_event is not None and self.stop_event.is_set():
                        interrupted = True
                    if not interrupted:
                        self._dispatch(pool, futures)
                    if not futures:
                        break
                    done, _ = wait(
                        futures, timeout=0.1, return_when=FIRST_COMPLETED
                    )
                    for future in done:
                        job_id = futures.pop(future)
                        self._consume(job_id, future)
                    if interrupted and all(
                        state != config.JOB_PENDING for state in self.states.values()
                    ):
                        break
                if not interrupted:
                    self._resolve_stuck_pending()
        except KeyboardInterrupt:
            interrupted = True

        status = self._final_status(interrupted)
        counts = self._counts()
        summary = {
            "job_counts": counts,
            "total_attempts": len(self.ledger.get_attempts(self.run_id)),
            "interrupted": interrupted,
        }
        self.ledger.finish_run(
            self.run_id, status, config.utc_now_iso(), summary=summary
        )
        self.ledger.add_audit(
            self.run_id,
            "run_finished",
            f"status={status} done={counts[config.JOB_DONE]}"
            f" failed={counts[config.JOB_FAILED]}"
            f" skipped={counts[config.JOB_SKIPPED]}",
        )
        return SchedulerOutcome(
            run_id=self.run_id,
            status=status,
            states=dict(self.states),
            interrupted=interrupted,
        )

    def _dispatch(
        self,
        pool: ThreadPoolExecutor,
        futures: dict[Future[ExecutionOutcome], str],
    ) -> None:
        free = self.max_workers - len(futures)
        if free <= 0:
            return
        completed = {job for job, state in self.states.items() if state == config.JOB_DONE}
        failed = {job for job, state in self.states.items() if state == config.JOB_FAILED}
        skipped = {job for job, state in self.states.items() if state == config.JOB_SKIPPED}
        available = frontier(self.plan, completed, failed, skipped)
        available = [
            job_id
            for job_id in available
            if self.states[job_id] == config.JOB_PENDING
        ]
        for job_id in available[:free]:
            self.states[job_id] = config.JOB_RUNNING
            futures[pool.submit(self.executor.execute, self.plan.jobs[job_id])] = job_id
            self._emit("job_started", job_id)

    def _consume(self, job_id: str, future: Future[ExecutionOutcome]) -> None:
        try:
            outcome = future.result()
            status = outcome.status
            reason = outcome.reason
        except Exception as exc:  # defensive isolation around a worker
            status = config.JOB_FAILED
            reason = f"network: worker_exception_{exc.__class__.__name__}"
            self.ledger.save_job_result(
                JobResult(
                    run_id=self.run_id,
                    job_id=job_id,
                    status=status,
                    reason=reason,
                )
            )
            self.ledger.add_audit(
                self.run_id, "job_failed", f"job={job_id} reason={reason}"
            )
        if status not in config.JOB_TERMINAL:
            status = config.JOB_FAILED
            reason = reason or "invalid_executor_status"
            self.ledger.save_job_result(
                JobResult(
                    run_id=self.run_id,
                    job_id=job_id,
                    status=status,
                    reason=reason,
                )
            )
        self.states[job_id] = status
        self._emit(f"job_{status}", job_id)

    def _mark_blocked(self) -> None:
        for job_id, reason in blocked_jobs(self.plan, self.states):
            self.states[job_id] = config.JOB_SKIPPED
            self.ledger.save_job_result(
                JobResult(
                    run_id=self.run_id,
                    job_id=job_id,
                    status=config.JOB_SKIPPED,
                    reason=reason,
                )
            )
            self.ledger.add_audit(
                self.run_id, "job_skipped", f"job={job_id} reason={reason}"
            )
            self._emit("job_skipped", job_id)

    def _resolve_stuck_pending(self) -> None:
        """A valid DAG should never reach this; fail closed if it does."""

        for job_id, state in sorted(self.states.items()):
            if state != config.JOB_PENDING:
                continue
            reason = "unresolved_dependency"
            self.states[job_id] = config.JOB_FAILED
            self.ledger.save_job_result(
                JobResult(
                    run_id=self.run_id,
                    job_id=job_id,
                    status=config.JOB_FAILED,
                    reason=reason,
                )
            )
            self.ledger.add_audit(
                self.run_id, "job_failed", f"job={job_id} reason={reason}"
            )

    def _final_status(self, interrupted: bool) -> str:
        if interrupted or any(
            state in (config.JOB_PENDING, config.JOB_RUNNING)
            for state in self.states.values()
        ):
            return config.RUN_INTERRUPTED
        if all(state == config.JOB_DONE for state in self.states.values()):
            return config.RUN_COMPLETED
        return config.RUN_FAILED

    def _counts(self) -> dict[str, int]:
        counts = {
            config.JOB_DONE: 0,
            config.JOB_FAILED: 0,
            config.JOB_SKIPPED: 0,
            config.JOB_PENDING: 0,
            config.JOB_RUNNING: 0,
        }
        for state in self.states.values():
            counts[state] = counts.get(state, 0) + 1
        return counts

    def _emit(self, event: str, job_id: str) -> None:
        if self.on_event is not None:
            self.on_event(event, job_id)


def _worker_limit(plan_concurrency: int, requested: int | None) -> int:
    if requested is None:
        requested = plan_concurrency
    if requested < 1:
        raise ValueError("max_workers must be >= 1")
    return min(plan_concurrency, requested, config.MAX_WORKERS)


def execute_once(
    plan: PlanDag,
    ledger: Ledger,
    run_id: str,
    executor: object,
    **kwargs: object,
) -> SchedulerOutcome:
    """Small convenience wrapper used by tests and the CLI."""

    existing = ledger.get_job_results(run_id)
    scheduler = Scheduler(
        plan, ledger, run_id, executor, initial_results=existing, **kwargs
    )
    return scheduler.run()
