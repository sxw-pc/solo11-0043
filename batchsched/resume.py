"""Resume an interrupted run from persisted terminal job results."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from threading import Event
from typing import Any

from . import config
from .dag import topological_order
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
    base_dir: str | Path | None = None,
) -> SchedulerOutcome:
    state = load_resume_state(ledger, plan, run_id)
    ledger.update_run_status(run_id, config.RUN_RUNNING)
    ledger.add_audit(
        run_id, "resumed",
        "completed={} failed={} skipped={} pending={}".format(
            len(state.completed), len(state.failed),
            len(state.skipped), len(state.pending),
        ),
    )
    results = _apply_artifact_reuse(
        ledger, plan, run_id, list(state.results), base_dir
    )
    scheduler = Scheduler(
        plan=plan,
        ledger=ledger,
        run_id=run_id,
        executor=executor,
        max_workers=max_workers,
        initial_results=results,
        stop_event=stop_event,
    )
    return scheduler.run()


def _apply_artifact_reuse(
    ledger: Ledger,
    plan: PlanDag,
    run_id: str,
    results: list[JobResult],
    base_dir: str | Path | None,
) -> list[JobResult]:
    """Adopt artifacts from earlier finished runs for still-pending jobs.

    A job is eligible only once every dependency is already ``done`` in this
    run or was itself just reused; otherwise the job executes as usual.  A
    donor hit whose artifact file has disappeared falls back to execution
    without leaving any reuse record behind.
    """
    run = ledger.get_run(run_id)
    if run is None:
        raise ResumeError(f"run not found: {run_id}")
    base = Path(base_dir) if base_dir is not None else Path.cwd()
    by_job = {result.job_id: result for result in results}
    available = {r.job_id for r in results if r.status == config.JOB_DONE}
    for job_id in topological_order(plan):
        if job_id in by_job:
            continue
        job = plan.jobs[job_id]
        if not all(dep in available for dep in job.depends_on):
            continue
        fingerprint = job.fingerprint()
        donor = ledger.find_reusable_artifact(
            run["plan_path"], job_id, fingerprint,
            exclude_run_id=run_id, before_started_at=run["started_at"],
        )
        if donor is None:
            continue
        artifact = Path(donor.output_path or "")
        if not artifact.is_absolute():
            artifact = base / artifact
        if not artifact.is_file():
            ledger.add_audit(
                run_id, "artifact_reuse_miss",
                f"job={job_id} source_run={donor.run_id}"
                f" reason=missing_artifact path={donor.output_path}",
            )
            continue
        reused = JobResult(
            run_id=run_id, job_id=job_id, status=config.JOB_DONE,
            sha256=donor.sha256, size_bytes=donor.size_bytes,
            output_path=donor.output_path, job_fingerprint=fingerprint,
            reused_from=donor.run_id,
        )
        ledger.save_job_result(reused)
        ledger.add_audit(
            run_id, "artifact_reused",
            f"job={job_id} source_run={donor.run_id} path={donor.output_path}",
        )
        by_job[job_id] = reused
        available.add(job_id)
    return list(by_job.values())
