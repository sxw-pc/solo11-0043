"""Resume an interrupted run from persisted terminal job results."""

from __future__ import annotations

from dataclasses import dataclass
from threading import Event
from typing import Any

from . import config
from .ledger import Ledger
from .models import JobResult, PlanDag, SchedulerOutcome
from .scheduler import Scheduler


class ResumeError(RuntimeError):
    pass


@dataclass(frozen=True)
class ResumeState:
    run_id: str
    plan_path: str
    completed: tuple[str, ...]
    failed: tuple[str, ...]
    skipped: tuple[str, ...]
    pending: tuple[str, ...]
    results: tuple[JobResult, ...]

    def summary(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "completed": len(self.completed),
            "failed": len(self.failed),
            "skipped": len(self.skipped),
            "pending": len(self.pending),
        }


def load_resume_state(
    ledger: Ledger, plan: PlanDag, run_id: str
) -> ResumeState:
    run = ledger.get_run(run_id)
    if run is None:
        raise ResumeError(f"run not found: {run_id}")
    results = ledger.get_job_results(run_id)
    by_job = {result.job_id: result for result in results}
    unknown = sorted(set(by_job) - set(plan.jobs))
    if unknown:
        raise ResumeError("ledger contains job(s) missing from plan: " + ", ".join(unknown))
    completed = tuple(sorted(job for job, r in by_job.items() if r.status == config.JOB_DONE))
    failed = tuple(sorted(job for job, r in by_job.items() if r.status == config.JOB_FAILED))
    skipped = tuple(sorted(job for job, r in by_job.items() if r.status == config.JOB_SKIPPED))
    pending = tuple(sorted(set(plan.jobs) - set(by_job)))
    return ResumeState(
        run_id=run_id,
        plan_path=run["plan_path"],
        completed=completed,
        failed=failed,
        skipped=skipped,
        pending=pending,
        results=tuple(results),
    )


def resume_run(
    ledger: Ledger,
    plan: PlanDag,
    run_id: str,
    executor: object,
    max_workers: int | None = None,
    stop_event: Event | None = None,
) -> SchedulerOutcome:
    state = load_resume_state(ledger, plan, run_id)
    ledger.update_run_status(run_id, config.RUN_RUNNING)
    ledger.add_audit(
        run_id,
        "resumed",
        "completed={} failed={} skipped={} pending={}".format(
            len(state.completed),
            len(state.failed),
            len(state.skipped),
            len(state.pending),
        ),
    )
    scheduler = Scheduler(
        plan=plan,
        ledger=ledger,
        run_id=run_id,
        executor=executor,
        max_workers=max_workers,
        initial_results=list(state.results),
        stop_event=stop_event,
    )
    return scheduler.run()
