"""Deterministic JSON and Markdown run reports with relative paths only."""

from __future__ import annotations

from collections import Counter
from pathlib import Path
import json
from typing import Any

from . import config
from .ledger import Ledger

_STATUS_ORDER = (config.JOB_DONE, config.JOB_FAILED, config.JOB_SKIPPED)


def build_report(
    ledger: Ledger, run_id: str, base_dir: str | Path | None = None
) -> dict[str, Any]:
    details = ledger.get_run_details(run_id)
    if details is None:
        raise KeyError(run_id)
    base = Path(base_dir) if base_dir is not None else Path.cwd()
    run = details["run"]
    results = sorted(details["jobs"], key=lambda item: item["job_id"])
    attempts = sorted(
        details["attempts"],
        key=lambda item: (item["job_id"], int(item["attempt_no"])),
    )
    counts = {
        config.JOB_DONE: 0,
        config.JOB_FAILED: 0,
        config.JOB_SKIPPED: 0,
    }
    for result in results:
        counts[result["status"]] = counts.get(result["status"], 0) + 1
    failure_counts: Counter[str] = Counter(
        str(item["error_class"]) for item in attempts if item["error_class"]
    )
    audit_counts = Counter(str(item["event"]) for item in details["audit"])
    contract_blocked = audit_counts.get("upstream_contract_blocked", 0)
    total_duration_ms = sum(int(item["duration_ms"]) for item in attempts)
    outputs = sorted(
        _relative_path(result["output_path"], base, run_id)
        for result in results
        if result.get("output_path")
    )
    jobs = []
    for result in results:
        job_attempts = [
            {
                "attempt_no": int(item["attempt_no"]),
                "error_class": item["error_class"],
                "duration_ms": int(item["duration_ms"]),
                "backoff_ms": int(item["backoff_ms"]),
                "started_at": item["started_at"],
            }
            for item in attempts
            if item["job_id"] == result["job_id"]
        ]
        jobs.append(
            {
                "job_id": result["job_id"],
                "status": result["status"],
                "sha256": result.get("sha256"),
                "size_bytes": result.get("size_bytes"),
                "output_path": _relative_path(
                    result.get("output_path"), base, run_id
                ),
                "reason": _safe_text(result.get("reason"), base),
                "attempts": job_attempts,
            }
        )
    return {
        "schema_version": 1,
        "run": {
            "run_id": run_id,
            "plan": _plan_path(run.get("plan_path"), base),
            "status": run.get("status"),
            "started_at": run.get("started_at"),
            "finished_at": run.get("finished_at"),
        },
        "job_counts": {
            "done": counts.get(config.JOB_DONE, 0),
            "failed": counts.get(config.JOB_FAILED, 0),
            "skipped": counts.get(config.JOB_SKIPPED, 0),
            "recorded_total": len(results),
        },
        "total_attempts": len(attempts),
        "total_duration_ms": total_duration_ms,
        "total_attempt_duration_ms": total_duration_ms,
        "failure_class_counts": dict(sorted(failure_counts.items())),
        "upstream_contract_blocked": contract_blocked,
        "outputs": outputs,
        "jobs": jobs,
        "resume_history": [
            {
                "detail": _safe_text(item.get("detail"), base),
                "at": item.get("at"),
            }
            for item in details["audit"]
            if item["event"] == "resumed"
        ],
        "audit_summary": dict(sorted(audit_counts.items())),
    }


def to_json(report: dict[str, Any]) -> str:
    return json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2) + "\n"


def to_markdown(report: dict[str, Any]) -> str:
    run = report["run"]
    counts = report["job_counts"]
    lines = [
        f"# Batch run report: {run['run_id']}",
        "",
        "## Summary",
        "",
        f"- Plan: `{run['plan']}`",
        f"- Status: **{run['status']}**",
        f"- Started: {run['started_at']}",
        f"- Finished: {run['finished_at']}",
        f"- Jobs: done={counts['done']} failed={counts['failed']}"
        f" skipped={counts['skipped']} recorded={counts['recorded_total']}",
        f"- Total attempts: {report['total_attempts']}",
        f"- Total attempt duration: {report['total_attempt_duration_ms']} ms",
        f"- Failure classes: {_inline_classes(report['failure_class_counts'])}",
        f"- Upstream contract blocked: {report['upstream_contract_blocked']}",
        "",
        "## Jobs by status",
        "",
        "| Status | Job | Attempts | Bytes | SHA-256 (12) | Reason |",
        "| --- | --- | ---: | ---: | --- | --- |",
    ]
    for status in _STATUS_ORDER:
        for job in report["jobs"]:
            if job["status"] != status:
                continue
            lines.append(
                "| {status} | `{job}` | {attempts} | {size} | `{sha}` | {reason} |".format(
                    status=status,
                    job=job["job_id"],
                    attempts=len(job["attempts"]),
                    size=job["size_bytes"] if job["size_bytes"] is not None else "-",
                    sha=(job["sha256"] or "-")[:12],
                    reason=_cell(job["reason"] or "-"),
                )
            )
    lines.extend(["", "## Failure and retry details", ""])
    failure_jobs = [job for job in report["jobs"] if job["status"] != config.JOB_DONE]
    if not failure_jobs:
        lines.append("No failed or skipped jobs.")
    for job in failure_jobs:
        lines.append(f"### `{job['job_id']}` — {job['status']}")
        lines.append("")
        if job["reason"]:
            lines.append(f"Reason: {job['reason']}")
            lines.append("")
        if job["attempts"]:
            lines.extend(
                [
                    "| Attempt | Error class | Duration (ms) | Backoff (ms) | Started |",
                    "| ---: | --- | ---: | ---: | --- |",
                ]
            )
            for attempt in job["attempts"]:
                lines.append(
                    "| {n} | {err} | {duration} | {backoff} | {started} |".format(
                        n=attempt["attempt_no"],
                        err=attempt["error_class"] or "ok",
                        duration=attempt["duration_ms"],
                        backoff=attempt["backoff_ms"],
                        started=attempt["started_at"],
                    )
                )
        lines.append("")
    lines.extend(["", "## Output files", ""])
    if report["outputs"]:
        lines.extend(f"- `{path}`" for path in report["outputs"])
    else:
        lines.append("No output files.")
    lines.extend(["", "## Resume history", ""])
    if report["resume_history"]:
        for event in report["resume_history"]:
            lines.append(f"- {event['at']}: {event['detail'] or '-'}")
    else:
        lines.append("No resume events.")
    lines.extend(["", "## Audit event summary", ""])
    if report["audit_summary"]:
        lines.extend(
            f"- `{event}`: {count}"
            for event, count in report["audit_summary"].items()
        )
    else:
        lines.append("No audit events.")
    lines.append("")
    return "\n".join(lines)


def _plan_path(value: str | None, base: Path) -> str:
    if not value:
        return "<unknown>"
    candidate = Path(value)
    if not candidate.is_absolute():
        return candidate.as_posix()
    return config.display_path(candidate, base)


def _relative_path(value: str | None, base: Path, run_id: str) -> str | None:
    if not value:
        return None
    candidate = Path(value)
    if not candidate.is_absolute():
        return candidate.as_posix().lstrip("./")
    try:
        relative = candidate.resolve().relative_to(base.resolve())
        return relative.as_posix()
    except (OSError, ValueError):
        return f"work/output/runs/{run_id}/{candidate.name}"


def _safe_text(value: str | None, base: Path) -> str | None:
    if value is None:
        return None
    text = str(value)
    for root in {str(base.resolve()), str(Path.cwd().resolve())}:
        if root:
            text = text.replace(root, ".")
    return text.replace("\r", " ").replace("\n", " ")


def _cell(value: str) -> str:
    return value.replace("|", "\\|")


def _inline_classes(counts: dict[str, int]) -> str:
    if not counts:
        return "none"
    return ", ".join(f"{name}={count}" for name, count in counts.items())
