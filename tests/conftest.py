"""Shared fixtures: a real loopback mock server, temp ledgers, fake worker."""

from __future__ import annotations

from pathlib import Path
import socket
import threading
import time

import pytest
import requests

from batchsched import config
from batchsched.ledger import Ledger
from batchsched.mocksite.app import create_app
from batchsched.mocksite.server import create_local_server
from batchsched.models import ExecutionOutcome, JobResult


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@pytest.fixture(scope="session")
def mock_server():
    app = create_app(
        flaky_failures=2,
        slow_delay_seconds=0.01,
        timeout_delay_seconds=0.3,
    )
    local = create_local_server(app, "127.0.0.1", _free_port()).start_background()
    time.sleep(0.05)
    yield local
    local.stop()


@pytest.fixture
def mock_base_url(mock_server):
    session = requests.Session()
    session.trust_env = False
    try:
        response = session.post(
            f"{mock_server.base_url}/__mock/reset",
            timeout=1.0,
            proxies={"http": None, "https": None},
        )
        response.raise_for_status()
    finally:
        session.close()
    return mock_server.base_url


@pytest.fixture
def ledger(tmp_path):
    return Ledger(tmp_path / "ledger.sqlite3")


@pytest.fixture
def active_run(ledger):
    run_id = "run-test-0001"
    ledger.create_run(run_id, "plans/test.yaml", config.utc_now_iso())
    return run_id


@pytest.fixture
def output_dir(tmp_path):
    path = tmp_path / "runs" / "run-test-0001"
    path.mkdir(parents=True, exist_ok=True)
    return path


class FakeExecutor:
    """Thread-safe executor used to exercise scheduler control flow."""

    def __init__(
        self,
        ledger: Ledger,
        run_id: str,
        failures: set[str] | None = None,
        delay: float = 0.0,
        raise_for: set[str] | None = None,
    ):
        self.ledger = ledger
        self.run_id = run_id
        self.failures = failures or set()
        self.raise_for = raise_for or set()
        self.delay = delay
        self.lock = threading.Lock()
        self.active = 0
        self.max_active = 0
        self.executed: list[str] = []

    def execute(self, job):
        with self.lock:
            self.active += 1
            self.max_active = max(self.max_active, self.active)
            self.executed.append(job.id)
        try:
            if self.delay:
                time.sleep(self.delay)
            if job.id in self.raise_for:
                raise RuntimeError("synthetic worker crash")
            if job.id in self.failures:
                self.ledger.save_job_result(
                    JobResult(
                        run_id=self.run_id,
                        job_id=job.id,
                        status=config.JOB_FAILED,
                        reason="http_5xx: synthetic",
                    )
                )
                return ExecutionOutcome(
                    job_id=job.id,
                    status=config.JOB_FAILED,
                    reason="http_5xx: synthetic",
                    error_class="http_5xx",
                )
            self.ledger.save_job_result(
                JobResult(
                    run_id=self.run_id,
                    job_id=job.id,
                    status=config.JOB_DONE,
                    sha256="0" * 64,
                    size_bytes=1,
                    output_path=f"work/output/runs/{self.run_id}/{job.id}.raw",
                )
            )
            return ExecutionOutcome(job_id=job.id, status=config.JOB_DONE)
        finally:
            with self.lock:
                self.active -= 1


@pytest.fixture
def fake_executor_cls():
    return FakeExecutor


def make_plan(text: str):
    from batchsched.plan_loader import parse_plan_text

    return parse_plan_text(text)


@pytest.fixture
def plan_factory():
    return make_plan


@pytest.fixture
def write_plan(tmp_path):
    def _write(text: str, name: str = "plan.yaml") -> Path:
        path = tmp_path / name
        path.write_text(text, encoding="utf-8")
        return path

    return _write
