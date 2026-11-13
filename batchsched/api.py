"""Minimal read-only Flask API for ledger inspection."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from flask import Flask, jsonify, request

from . import config
from .ledger import Ledger

_LOCAL_ADDRESSES = {"127.0.0.1", "::1", "localhost"}


def register_api(
    app: Flask, ledger: Ledger, base_dir: str | Path | None = None
) -> None:
    base = Path(base_dir) if base_dir is not None else Path.cwd()

    @app.before_request
    def _loopback_only():
        if request.remote_addr not in _LOCAL_ADDRESSES:
            return jsonify({"error": "loopback clients only"}), 403
        return None

    @app.get("/api/runs")
    def list_runs():
        runs = [_public_run(run, base) for run in ledger.list_runs()]
        return jsonify({"count": len(runs), "runs": runs})

    @app.get("/api/runs/<run_id>")
    def get_run(run_id: str):
        details = ledger.get_run_details(run_id)
        if details is None:
            return jsonify({"error": "run not found", "run_id": run_id}), 404
        return jsonify(
            {
                "run": _public_run(details["run"], base),
                "job_counts": details["job_counts"],
                "total_attempts": details["total_attempts"],
                "jobs": details["jobs"],
                "attempts": details["attempts"],
                "audit": [_public_audit(event, base) for event in details["audit"]],
            }
        )


def _public_run(run: dict[str, Any], base: Path) -> dict[str, Any]:
    plan_path = run.get("plan_path") or "<unknown>"
    candidate = Path(plan_path)
    if candidate.is_absolute():
        plan_path = config.display_path(candidate, base)
    return {
        "run_id": run["run_id"],
        "plan": str(plan_path).replace("\\", "/"),
        "status": run["status"],
        "started_at": run["started_at"],
        "finished_at": run["finished_at"],
        "summary": run.get("summary", {}),
    }


def _public_audit(event: dict[str, Any], base: Path) -> dict[str, Any]:
    detail = str(event.get("detail", ""))
    for root in {str(base.resolve()), str(Path.cwd().resolve())}:
        detail = detail.replace(root, ".")
    return {"event": event["event"], "detail": detail, "at": event["at"]}
