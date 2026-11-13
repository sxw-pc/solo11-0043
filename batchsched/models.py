"""Core data structures shared by the loader, scheduler and reporters."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from . import config


@dataclass(frozen=True)
class ValidationIssue:
    code: str
    message: str
    line: int | None = None
    job_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"code": self.code, "message": self.message}
        if self.line is not None:
            payload["line"] = self.line
        if self.job_id is not None:
            payload["job_id"] = self.job_id
        return payload


class PlanValidationError(ValueError):
    """Raised when a plan cannot be accepted; carries structured issues."""

    def __init__(self, issues: list[ValidationIssue]):
        self.issues = sorted(
            issues,
            key=lambda item: (item.line is None, item.line or 0, item.code, item.message),
        )
        super().__init__(f"plan validation failed with {len(issues)} issue(s)")

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": False,
            "error_count": len(self.issues),
            "errors": [issue.to_dict() for issue in self.issues],
        }


@dataclass(frozen=True)
class RetryPolicy:
    mode: str = "fixed"
    max_attempts: int = 3
    base_delay_ms: int = 100
    retryable: frozenset[str] | None = None
    max_total_delay_ms: int = config.MAX_BACKOFF_TOTAL_MS

    def can_retry(self, error_class: str) -> bool:
        retryable = (
            self.retryable
            if self.retryable is not None
            else config.DEFAULT_RETRYABLE
        )
        return error_class in retryable


@dataclass(frozen=True)
class JobSpec:
    id: str
    source: str
    depends_on: tuple[str, ...] = ()
    retry: RetryPolicy = field(default_factory=RetryPolicy)
    params: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "source": self.source,
            "depends_on": list(self.depends_on),
            "retry": {
                "mode": self.retry.mode,
                "max_attempts": self.retry.max_attempts,
                "base_delay_ms": self.retry.base_delay_ms,
                "retryable": (
                    sorted(self.retry.retryable)
                    if self.retry.retryable is not None
                    else None
                ),
                "max_total_delay_ms": self.retry.max_total_delay_ms,
            },
            "params": dict(self.params),
        }


@dataclass(frozen=True)
class RunRecord:
    run_id: str
    plan_path: str
    started_at: str
    status: str = config.RUN_RUNNING
    finished_at: str | None = None
    summary: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class PlanDag:
    name: str
    concurrency: int
    jobs: dict[str, JobSpec]
    plan_path: str = ""

    @property
    def job_ids(self) -> list[str]:
        return sorted(self.jobs)

    def job(self, job_id: str) -> JobSpec:
        return self.jobs[job_id]

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "concurrency": self.concurrency,
            "job_count": len(self.jobs),
            "jobs": [self.jobs[job_id].to_dict() for job_id in self.job_ids],
        }


@dataclass(frozen=True)
class AttemptRecord:
    run_id: str
    job_id: str
    attempt_no: int
    error_class: str | None
    duration_ms: int
    backoff_ms: int
    started_at: str


@dataclass(frozen=True)
class JobResult:
    run_id: str
    job_id: str
    status: str
    sha256: str | None = None
    size_bytes: int | None = None
    output_path: str | None = None
    reason: str | None = None

    @property
    def is_terminal(self) -> bool:
        return self.status in config.JOB_TERMINAL


@dataclass(frozen=True)
class ExecutionOutcome:
    job_id: str
    status: str
    reason: str | None = None
    sha256: str | None = None
    size_bytes: int | None = None
    output_path: str | None = None
    error_class: str | None = None
    attempts: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "job_id": self.job_id,
            "status": self.status,
            "reason": self.reason,
            "sha256": self.sha256,
            "size_bytes": self.size_bytes,
            "output_path": self.output_path,
            "error_class": self.error_class,
            "attempts": self.attempts,
        }


@dataclass(frozen=True)
class SchedulerOutcome:
    run_id: str
    status: str
    states: dict[str, str]
    interrupted: bool = False

    @property
    def counts(self) -> dict[str, int]:
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


def terminal_states(
    results: list[JobResult],
) -> dict[str, str]:
    """Convert ledger results into the scheduler's initial state map."""

    return {
        result.job_id: result.status
        for result in results
        if result.status in (config.JOB_DONE, config.JOB_FAILED, config.JOB_SKIPPED)
    }


Attempt = AttemptRecord
