"""Artifact-version and dependency persistence for the SQLite ledger.

These helpers are mixed into :class:`batchsched.ledger.Ledger` so every SQL
statement stays inside the ledger layer while the main module remains small.
"""

from __future__ import annotations

from contextlib import contextmanager
import json
from typing import Any, Iterator

from . import config
from .models import ArtifactRevision, JobResult

ARTIFACT_SCHEMA = """
CREATE TABLE IF NOT EXISTS artifact_revisions (
  run_id TEXT NOT NULL,
  job_id TEXT NOT NULL,
  revision_no INTEGER NOT NULL,
  old_sha256 TEXT NOT NULL,
  new_sha256 TEXT NOT NULL,
  backup_path TEXT NOT NULL,
  at TEXT NOT NULL,
  PRIMARY KEY (run_id, job_id, revision_no)
);
CREATE TABLE IF NOT EXISTS job_dependencies (
  run_id TEXT NOT NULL,
  job_id TEXT NOT NULL,
  depends_on TEXT NOT NULL DEFAULT '[]',
  PRIMARY KEY (run_id, job_id)
);
CREATE INDEX IF NOT EXISTS idx_artifact_revisions_run
  ON artifact_revisions(run_id, job_id, revision_no);
"""


class ArtifactLedgerMixin:
    """Mixin; the host class supplies ``_connection`` and ``db_path``."""

    @contextmanager
    def _connection(self) -> Iterator[Any]:  # pragma: no cover - overridden
        raise NotImplementedError

    def save_artifact_revision(self, revision: ArtifactRevision) -> None:
        """Append one superseded-version row; the prior result stays in place."""

        with self._connection() as connection:
            connection.execute(
                "INSERT INTO artifact_revisions(run_id, job_id, revision_no,"
                " old_sha256, new_sha256, backup_path, at)"
                " VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    revision.run_id,
                    revision.job_id,
                    revision.revision_no,
                    revision.old_sha256,
                    revision.new_sha256,
                    revision.backup_path,
                    revision.at or config.utc_now_iso(),
                ),
            )

    def get_artifact_revisions(
        self, run_id: str, job_id: str | None = None
    ) -> list[ArtifactRevision]:
        sql = "SELECT * FROM artifact_revisions WHERE run_id = ?"
        params: list[Any] = [run_id]
        if job_id is not None:
            sql += " AND job_id = ?"
            params.append(job_id)
        sql += " ORDER BY job_id, revision_no"
        with self._connection() as connection:
            rows = connection.execute(sql, params).fetchall()
        return [
            ArtifactRevision(
                run_id=row["run_id"],
                job_id=row["job_id"],
                revision_no=int(row["revision_no"]),
                old_sha256=row["old_sha256"],
                new_sha256=row["new_sha256"],
                backup_path=row["backup_path"],
                at=row["at"],
            )
            for row in rows
        ]

    def next_revision_no(self, run_id: str, job_id: str) -> int:
        with self._connection() as connection:
            row = connection.execute(
                "SELECT COALESCE(MAX(revision_no), 0) AS n FROM artifact_revisions"
                " WHERE run_id = ? AND job_id = ?",
                (run_id, job_id),
            ).fetchone()
        return int(row["n"]) + 1

    def save_job_dependencies(
        self, run_id: str, job_id: str, depends_on: list[str] | tuple[str, ...]
    ) -> None:
        payload = json.dumps(sorted(depends_on), ensure_ascii=False)
        with self._connection() as connection:
            connection.execute(
                "INSERT INTO job_dependencies(run_id, job_id, depends_on)"
                " VALUES (?, ?, ?)"
                " ON CONFLICT(run_id, job_id) DO UPDATE SET"
                " depends_on=excluded.depends_on",
                (run_id, job_id, payload),
            )

    def get_job_dependencies(self, run_id: str) -> dict[str, list[str]]:
        with self._connection() as connection:
            rows = connection.execute(
                "SELECT job_id, depends_on FROM job_dependencies WHERE run_id = ?"
                " ORDER BY job_id",
                (run_id,),
            ).fetchall()
        result: dict[str, list[str]] = {}
        for row in rows:
            try:
                result[row["job_id"]] = sorted(json.loads(row["depends_on"] or "[]"))
            except json.JSONDecodeError:
                result[row["job_id"]] = []
        return result

    def get_job_result(self, run_id: str, job_id: str) -> JobResult | None:
        with self._connection() as connection:
            row = connection.execute(
                "SELECT * FROM job_results WHERE run_id = ? AND job_id = ?",
                (run_id, job_id),
            ).fetchone()
        if row is None:
            return None
        return JobResult(
            run_id=row["run_id"],
            job_id=row["job_id"],
            status=row["status"],
            sha256=row["sha256"],
            size_bytes=row["size_bytes"],
            output_path=row["output_path"],
            reason=row["reason"],
        )
