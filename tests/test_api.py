from __future__ import annotations

from flask import Flask

from batchsched import config
from batchsched.api import register_api
from batchsched.models import AttemptRecord, JobResult


def _client(ledger, base_dir=None):
    app = Flask(__name__)
    register_api(app, ledger, base_dir=base_dir)
    return app.test_client()


def _seed(ledger, run_id):
    ledger.save_job_result(
        JobResult(
            run_id=run_id,
            job_id="fetch_one",
            status=config.JOB_DONE,
            sha256="a" * 64,
            size_bytes=12,
            output_path=f"work/output/runs/{run_id}/fetch_one.raw",
        )
    )
    ledger.record_attempt(
        AttemptRecord(
            run_id=run_id,
            job_id="fetch_one",
            attempt_no=1,
            error_class=None,
            duration_ms=4,
            backoff_ms=0,
            started_at=config.utc_now_iso(),
        )
    )
    ledger.add_audit(run_id, "job_done", "job=fetch_one attempt=1")


def test_empty_runs_list(ledger):
    response = _client(ledger).get("/api/runs")
    assert response.status_code == 200
    assert response.get_json() == {"count": 0, "runs": []}


def test_runs_list_and_absolute_plan_sanitized(ledger, tmp_path):
    plan = tmp_path / "secret-plan.yaml"
    ledger.create_run("run-api-1", str(plan), "2026-01-01T00:00:00Z")
    response = _client(ledger).get("/api/runs")
    payload = response.get_json()
    assert payload["count"] == 1
    assert payload["runs"][0]["run_id"] == "run-api-1"
    assert str(tmp_path) not in str(payload)
    assert payload["runs"][0]["plan"].endswith("secret-plan.yaml")


def test_run_details_structure(ledger, active_run):
    _seed(ledger, active_run)
    response = _client(ledger).get(f"/api/runs/{active_run}")
    assert response.status_code == 200
    payload = response.get_json()
    assert payload["run"]["run_id"] == active_run
    assert payload["job_counts"]["done"] == 1
    assert payload["total_attempts"] == 1
    assert payload["jobs"][0]["job_id"] == "fetch_one"
    assert payload["attempts"][0]["attempt_no"] == 1
    assert payload["audit"][0]["event"] == "job_done"


def test_missing_run_is_json_404(ledger):
    response = _client(ledger).get("/api/runs/does-not-exist")
    assert response.status_code == 404
    assert response.get_json()["error"] == "run not found"


def test_non_loopback_client_is_forbidden(ledger):
    client = _client(ledger)
    response = client.get(
        "/api/runs", environ_base={"REMOTE_ADDR": "203.0.113.10"}
    )
    assert response.status_code == 403
    assert "loopback" in response.get_json()["error"]


def test_api_is_read_only(ledger):
    response = _client(ledger).post("/api/runs")
    assert response.status_code == 405
