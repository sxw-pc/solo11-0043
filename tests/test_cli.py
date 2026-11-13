from __future__ import annotations

import json
from pathlib import Path

from batchsched.cli import main

ROOT = Path(__file__).resolve().parents[1]


def test_cli_plan_validate_exit_codes(capsys):
    valid = main(["plan-validate", str(ROOT / "plans" / "demo.yaml")])
    valid_payload = json.loads(capsys.readouterr().out)
    assert valid == 0
    assert valid_payload["ok"] is True
    assert valid_payload["job_count"] == 6

    invalid = main(
        ["plan-validate", str(ROOT / "plans" / "demo_invalid_cycle.yaml")]
    )
    invalid_payload = json.loads(capsys.readouterr().out)
    assert invalid != 0
    assert invalid_payload["ok"] is False
    assert invalid_payload["errors"][0]["code"] == "cycle"


def test_cli_run_and_report(tmp_path, write_plan, mock_base_url, capsys):
    plan_path = write_plan(
        """
name: cli-plan
concurrency: 1
jobs:
  - id: fetch_cli
    source: /api/catalog
    retry:
      mode: fixed
      max_attempts: 1
      base_delay_ms: 50
"""
    )
    code = main(
        [
            "--work-dir",
            str(tmp_path),
            "run",
            "--plan",
            str(plan_path),
            "--base-url",
            mock_base_url,
        ]
    )
    run_payload = json.loads(capsys.readouterr().out)
    assert code == 0
    assert run_payload["status"] == "completed"
    run_id = run_payload["run_id"]
    assert (tmp_path / "work" / "ledger.sqlite3").exists()
    assert (tmp_path / "work" / "output" / "runs" / run_id).is_dir()

    report_code = main(
        ["--work-dir", str(tmp_path), "report", "--run", run_id, "--format", "json"]
    )
    report_text = capsys.readouterr().out
    report = json.loads(report_text)
    assert report_code == 0
    assert report["run"]["run_id"] == run_id
    assert report["job_counts"]["done"] == 1
    assert str(tmp_path) not in report_text


def test_cli_report_missing_run(tmp_path, capsys):
    code = main(
        ["--work-dir", str(tmp_path), "report", "--run", "run-missing"]
    )
    captured = capsys.readouterr()
    assert code == 1
    assert "run not found" in captured.err
