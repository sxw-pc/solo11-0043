"""Execute one job: fetch, precondition check, persist raw bytes, retry."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path
import hashlib
import json
import time
from typing import Callable

from . import config
from .fetcher import FetchError, Fetcher
from .ledger import Ledger
from .models import (
    AttemptRecord,
    ExecutionOutcome,
    JobResult,
    JobSpec,
    artifact_record_count,
    contract_param_errors,
)
from .retry import next_backoff_ms

SleepFn = Callable[[float], None]


class JobExecutor:
    def __init__(
        self,
        fetcher: Fetcher,
        ledger: Ledger,
        run_id: str,
        output_dir: str | Path,
        sleep_fn: SleepFn | None = None,
        max_total_delay_ms: int | None = None,
    ):
        self.fetcher = fetcher
        self.ledger = ledger
        self.run_id = run_id
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.sleep_fn = sleep_fn or time.sleep
        if max_total_delay_ms is not None:
            if not 0 <= max_total_delay_ms <= config.MAX_BACKOFF_TOTAL_MS:
                raise ValueError("max_total_delay_ms outside allowed range")
        self.max_total_delay_ms = max_total_delay_ms

    def execute(self, job: JobSpec) -> ExecutionOutcome:
        policy = job.retry
        if self.max_total_delay_ms is not None:
            policy = replace(
                policy,
                max_total_delay_ms=min(
                    policy.max_total_delay_ms, self.max_total_delay_ms
                ),
            )
        attempt_no = self.ledger.next_attempt_no(self.run_id, job.id)
        contract_failures = self._check_upstream_contracts(job)
        if contract_failures:
            return self._record_contract_failure(job, attempt_no, contract_failures)
        total_backoff_ms = 0
        while True:
            started_at = config.utc_now_iso()
            started = time.perf_counter()
            error_class: str | None = None
            detail = ""
            try:
                response = self.fetcher.fetch(job.source)
                self._check_preconditions(job, response.content, response.content_type)
                digest = hashlib.sha256(response.content).hexdigest()
                size_bytes = len(response.content)
                self._write_output(job.id, response.content, digest)
                ledger_path = config.run_output_relative(self.run_id, f"{job.id}.raw")
                self.ledger.record_attempt(
                    AttemptRecord(
                        run_id=self.run_id,
                        job_id=job.id,
                        attempt_no=attempt_no,
                        error_class=None,
                        duration_ms=_elapsed_ms(started),
                        backoff_ms=0,
                        started_at=started_at,
                    )
                )
                self.ledger.save_job_result(
                    JobResult(
                        run_id=self.run_id,
                        job_id=job.id,
                        status=config.JOB_DONE,
                        sha256=digest,
                        size_bytes=size_bytes,
                        output_path=ledger_path,
                        reason=None,
                    )
                )
                self.ledger.add_audit(
                    self.run_id,
                    "job_done",
                    f"job={job.id} attempt={attempt_no} bytes={size_bytes}",
                )
                return ExecutionOutcome(
                    job_id=job.id,
                    status=config.JOB_DONE,
                    sha256=digest,
                    size_bytes=size_bytes,
                    output_path=ledger_path,
                    attempts=attempt_no,
                )
            except FetchError as exc:
                error_class = exc.error_class
                detail = _safe_detail(exc.detail)
            except Exception as exc:  # defensive: every failure gets a class
                error_class = "network"
                detail = f"unexpected_{exc.__class__.__name__}"

            duration_ms = _elapsed_ms(started)
            delay = next_backoff_ms(policy, attempt_no, total_backoff_ms)
            backoff_ms = delay if policy.can_retry(error_class) else None
            self.ledger.record_attempt(
                AttemptRecord(
                    run_id=self.run_id,
                    job_id=job.id,
                    attempt_no=attempt_no,
                    error_class=error_class,
                    duration_ms=duration_ms,
                    backoff_ms=backoff_ms or 0,
                    started_at=started_at,
                )
            )
            reason = f"{error_class}: {detail}"
            if backoff_ms is None:
                self._record_failure(job.id, reason)
                return ExecutionOutcome(
                    job_id=job.id,
                    status=config.JOB_FAILED,
                    reason=reason,
                    error_class=error_class,
                    attempts=attempt_no,
                )
            total_backoff_ms += backoff_ms
            self.ledger.add_audit(
                self.run_id,
                "job_retry",
                f"job={job.id} attempt={attempt_no} error={error_class}"
                f" backoff_ms={backoff_ms}",
            )
            self.sleep_fn(backoff_ms / 1000.0)
            attempt_no += 1

    def _check_preconditions(
        self, job: JobSpec, content: bytes, content_type: str
    ) -> None:
        expected_type = job.params.get("expect_content_type")
        if expected_type is not None:
            if not isinstance(expected_type, str):
                raise FetchError("parse_precondition", "expect_content_type must be text")
            if expected_type.lower() not in content_type.lower():
                raise FetchError(
                    "parse_precondition",
                    f"content type mismatch: expected {expected_type}",
                )
        min_bytes = job.params.get("min_bytes")
        if min_bytes is not None:
            if not isinstance(min_bytes, int) or isinstance(min_bytes, bool):
                raise FetchError("parse_precondition", "min_bytes must be an integer")
            if len(content) < min_bytes:
                raise FetchError(
                    "parse_precondition",
                    f"body shorter than min_bytes ({len(content)} < {min_bytes})",
                )

    def _check_upstream_contracts(self, job: JobSpec) -> list[str]:
        """Validate every direct upstream's persisted artifact against the
        contract declared in ``job.params``.  All upstreams are checked and
        every violation aggregated; empty result means the job may fetch."""

        required_fields = job.params.get("required_fields")
        min_records = job.params.get("min_records")
        if required_fields is None and min_records is None:
            return []
        declaration_errors = contract_param_errors(job.params)
        if declaration_errors:
            return declaration_errors
        failures: list[str] = []
        for dep in sorted(job.depends_on):
            failures.extend(
                self._validate_upstream_artifact(dep, required_fields, min_records)
            )
        return failures

    def _validate_upstream_artifact(
        self,
        dep: str,
        required_fields: list[str] | None,
        min_records: int | None,
    ) -> list[str]:
        prefix = f"upstream '{dep}'"
        path = self.output_dir / f"{dep}.raw"
        if not path.is_file():
            return [f"{prefix}: artifact missing ({path.name})"]
        try:
            raw = path.read_bytes()
        except OSError:
            return [f"{prefix}: artifact unreadable"]
        if not raw.strip():
            return [f"{prefix}: artifact is empty"]
        try:
            document = json.loads(raw)
        except json.JSONDecodeError:
            return [f"{prefix}: artifact is not valid JSON"]
        failures: list[str] = []
        if required_fields:
            if not isinstance(document, dict):
                failures.append(f"{prefix}: artifact is not a JSON object")
            else:
                missing = [name for name in required_fields if name not in document]
                if missing:
                    failures.append(
                        f"{prefix}: missing required field(s): {', '.join(missing)}"
                    )
        if min_records is not None:
            records = artifact_record_count(document)
            if records < min_records:
                failures.append(
                    f"{prefix}: record count {records} below min_records {min_records}"
                )
        return failures

    def _record_contract_failure(
        self, job: JobSpec, attempt_no: int, failures: list[str]
    ) -> ExecutionOutcome:
        """Persist the parse_precondition evidence exactly once; no retry."""

        self.ledger.record_attempt(
            AttemptRecord(
                run_id=self.run_id,
                job_id=job.id,
                attempt_no=attempt_no,
                error_class="parse_precondition",
                duration_ms=0,
                backoff_ms=0,
                started_at=config.utc_now_iso(),
            )
        )
        detail = _safe_detail("; ".join(failures))
        reason = f"parse_precondition: upstream artifact contract: {detail}"
        self._record_failure(job.id, reason)
        self.ledger.add_audit(
            self.run_id,
            "upstream_contract_blocked",
            f"job={job.id} upstreams={len(job.depends_on)} failures={len(failures)}",
        )
        return ExecutionOutcome(
            job_id=job.id,
            status=config.JOB_FAILED,
            reason=reason,
            error_class="parse_precondition",
            attempts=attempt_no,
        )

    def _write_output(self, job_id: str, content: bytes, digest: str) -> Path:
        target = self.output_dir / f"{job_id}.raw"
        temporary = self.output_dir / f".{job_id}.{digest[:8]}.tmp"
        try:
            temporary.write_bytes(content)
            temporary.replace(target)
        except OSError as exc:
            try:
                temporary.unlink(missing_ok=True)
            except OSError:
                pass
            raise FetchError(
                "parse_precondition", f"output write failed: {exc.__class__.__name__}"
            ) from exc
        return target

    def _record_failure(self, job_id: str, reason: str) -> None:
        self.ledger.save_job_result(
            JobResult(
                run_id=self.run_id,
                job_id=job_id,
                status=config.JOB_FAILED,
                reason=reason,
            )
        )
        self.ledger.add_audit(self.run_id, "job_failed", f"job={job_id} reason={reason}")


def _elapsed_ms(started: float) -> int:
    return max(0, int((time.perf_counter() - started) * 1000))


def _safe_detail(detail: str) -> str:
    compact = " ".join(str(detail).split())
    return compact[:240]
