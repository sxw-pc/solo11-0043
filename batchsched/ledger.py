"""SQLite execution ledger.  Every SQL statement in the project lives here."""

from __future__ import annotations

from collections import Counter
from contextlib import contextmanager
from pathlib import Path
import json
import sqlite3
from typing import Any, Iterator

from . import config
from .models import AttemptRecord, JobResult, error_class_of_reason

_SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
  run_id TEXT PRIMARY KEY,
  plan_path TEXT NOT NULL,
  started_at TEXT NOT NULL,
  finished_at TEXT,
  status TEXT NOT NULL,
  summary_json TEXT NOT NULL DEFAULT '{}'
);
CREATE TABLE IF NOT EXISTS attempts (
  run_id TEXT NOT NULL,
  job_id TEXT NOT NULL,
  attempt_no INTEGER NOT NULL,
  error_class TEXT,
  duration_ms INTEGER NOT NULL,
  backoff_ms INTEGER NOT NULL DEFAULT 0,
  started_at TEXT NOT NULL,
  PRIMARY KEY (run_id, job_id, attempt_no)
);
CREATE TABLE IF NOT EXISTS job_results (
  run_id TEXT NOT NULL,
  job_id TEXT NOT NULL,
  status TEXT NOT NULL,
  sha256 TEXT,
  size_bytes INTEGER,
  output_path TEXT,
  reason TEXT,
  PRIMARY KEY (run_id, job_id)
);
CREATE TABLE IF NOT EXISTS audit (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id TEXT NOT NULL,
  event TEXT NOT NULL,
  detail TEXT NOT NULL DEFAULT '',
  at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_attempts_run ON attempts(run_id, job_id, attempt_no);
CREATE INDEX IF NOT EXISTS idx_audit_run ON audit(run_id, id);
"""


class Ledger:
    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.initialize()

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(
            self.db_path, timeout=10.0, isolation_level="DEFERRED"
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout=10000")
        connection.execute("PRAGMA foreign_keys=ON")
        try:
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def initialize(self) -> None:
        with self._connection() as connection:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.executescript(_SCHEMA)

    def create_run(
        self,
        run_id: str,
        plan_path: str,
        started_at: str,
        status: str = config.RUN_RUNNING,
    ) -> None:
        with self._connection() as connection:
            connection.execute(
                "INSERT INTO runs(run_id, plan_path, started_at, status)"
                " VALUES (?, ?, ?, ?)",
                (run_id, plan_path, started_at, status),
            )

    def ensure_run(
        self,
        run_id: str,
        plan_path: str,
        started_at: str,
        status: str = config.RUN_RUNNING,
    ) -> None:
        with self._connection() as connection:
            connection.execute(
                "INSERT OR IGNORE INTO runs(run_id, plan_path, started_at, status)"
                " VALUES (?, ?, ?, ?)",
                (run_id, plan_path, started_at, status),
            )

    def finish_run(
        self,
        run_id: str,
        status: str,
        finished_at: str,
        summary: dict[str, Any] | None = None,
    ) -> None:
        payload = json.dumps(summary or {}, ensure_ascii=False, sort_keys=True)
        with self._connection() as connection:
            connection.execute(
                "UPDATE runs SET status = ?, finished_at = ?, summary_json = ?"
                " WHERE run_id = ?",
                (status, finished_at, payload, run_id),
            )

    def update_run_status(self, run_id: str, status: str) -> None:
        with self._connection() as connection:
            connection.execute(
                "UPDATE runs SET status = ? WHERE run_id = ?", (status, run_id)
            )

    def record_attempt(self, attempt: AttemptRecord) -> None:
        with self._connection() as connection:
            connection.execute(
                "INSERT INTO attempts(run_id, job_id, attempt_no, error_class,"
                " duration_ms, backoff_ms, started_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    attempt.run_id,
                    attempt.job_id,
                    attempt.attempt_no,
                    attempt.error_class,
                    attempt.duration_ms,
                    attempt.backoff_ms,
                    attempt.started_at,
                ),
            )

    def save_job_result(self, result: JobResult) -> None:
        with self._connection() as connection:
            connection.execute(
                "INSERT INTO job_results(run_id, job_id, status, sha256, size_bytes,"
                " output_path, reason) VALUES (?, ?, ?, ?, ?, ?, ?)"
                " ON CONFLICT(run_id, job_id) DO UPDATE SET"
                " status=excluded.status, sha256=excluded.sha256,"
                " size_bytes=excluded.size_bytes, output_path=excluded.output_path,"
                " reason=excluded.reason",
                (
                    result.run_id,
                    result.job_id,
                    result.status,
                    result.sha256,
                    result.size_bytes,
                    result.output_path,
                    result.reason,
                ),
            )

    def add_audit(
        self, run_id: str, event: str, detail: str = "", at: str | None = None
    ) -> None:
        with self._connection() as connection:
            connection.execute(
                "INSERT INTO audit(run_id, event, detail, at) VALUES (?, ?, ?, ?)",
                (run_id, event, detail, at or config.utc_now_iso()),
            )

    def next_attempt_no(self, run_id: str, job_id: str) -> int:
        with self._connection() as connection:
            row = connection.execute(
                "SELECT COALESCE(MAX(attempt_no), 0) AS n FROM attempts"
                " WHERE run_id = ? AND job_id = ?",
                (run_id, job_id),
            ).fetchone()
        return int(row["n"]) + 1

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        with self._connection() as connection:
            row = connection.execute(
                "SELECT * FROM runs WHERE run_id = ?", (run_id,)
            ).fetchone()
        return _run_row(row) if row else None

    def list_runs(self) -> list[dict[str, Any]]:
        with self._connection() as connection:
            rows = connection.execute(
                "SELECT * FROM runs ORDER BY started_at DESC, run_id DESC"
            ).fetchall()
        return [_run_row(row) for row in rows]

    def get_job_results(self, run_id: str) -> list[JobResult]:
        with self._connection() as connection:
            rows = connection.execute(
                "SELECT * FROM job_results WHERE run_id = ? ORDER BY job_id",
                (run_id,),
            ).fetchall()
        return [
            JobResult(
                run_id=row["run_id"],
                job_id=row["job_id"],
                status=row["status"],
                sha256=row["sha256"],
                size_bytes=row["size_bytes"],
                output_path=row["output_path"],
                reason=row["reason"],
            )
            for row in rows
        ]

    def get_attempts(
        self, run_id: str, job_id: str | None = None
    ) -> list[dict[str, Any]]:
        sql = "SELECT * FROM attempts WHERE run_id = ?"
        params: list[Any] = [run_id]
        if job_id is not None:
            sql += " AND job_id = ?"
            params.append(job_id)
        sql += " ORDER BY job_id, attempt_no"
        with self._connection() as connection:
            rows = connection.execute(sql, params).fetchall()
        return [dict(row) for row in rows]

    def terminal_failure_classes(self, run_id: str) -> dict[str, int]:
        """Per-class counts of jobs in a terminal ``failed`` state."""

        counts: Counter[str] = Counter()
        for result in self.get_job_results(run_id):
            if result.status != config.JOB_FAILED:
                continue
            error_class = error_class_of_reason(result.reason)
            if error_class is not None:
                counts[error_class] += 1
        return dict(sorted(counts.items()))

    def budget_downgraded_jobs(self, run_id: str) -> list[str]:
        """Jobs skipped because a job-budget class reached its quota."""

        return sorted(
            result.job_id
            for result in self.get_job_results(run_id)
            if result.status == config.JOB_SKIPPED
            and result.reason == config.BUDGET_EXHAUSTED_REASON
        )

    def get_audit(self, run_id: str) -> list[dict[str, Any]]:
        with self._connection() as connection:
            rows = connection.execute(
                "SELECT event, detail, at FROM audit WHERE run_id = ? ORDER BY id",
                (run_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    def get_run_details(self, run_id: str) -> dict[str, Any] | None:
        run = self.get_run(run_id)
        if run is None:
            return None
        results = self.get_job_results(run_id)
        attempts = self.get_attempts(run_id)
        audit = self.get_audit(run_id)
        counts = {
            config.JOB_DONE: 0,
            config.JOB_FAILED: 0,
            config.JOB_SKIPPED: 0,
        }
        for result in results:
            counts[result.status] = counts.get(result.status, 0) + 1
        return {
            "run": run,
            "job_counts": counts,
            "total_attempts": len(attempts),
            "jobs": [_result_dict(result) for result in results],
            "attempts": attempts,
            "audit": audit,
        }

    def output_paths(self, run_id: str) -> list[str]:
        with self._connection() as connection:
            rows = connection.execute(
                "SELECT output_path FROM job_results WHERE run_id = ?"
                " AND output_path IS NOT NULL ORDER BY job_id",
                (run_id,),
            ).fetchall()
        return [str(row["output_path"]) for row in rows]


def _run_row(row: sqlite3.Row) -> dict[str, Any]:
    payload = dict(row)
    try:
        payload["summary"] = json.loads(payload.pop("summary_json") or "{}")
    except json.JSONDecodeError:
        payload["summary"] = {}
    return payload


def _result_dict(result: JobResult) -> dict[str, Any]:
    return {
        "job_id": result.job_id,
        "status": result.status,
        "sha256": result.sha256,
        "size_bytes": result.size_bytes,
        "output_path": result.output_path,
        "reason": result.reason,
    }
