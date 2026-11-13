"""Global constants and path handling for batchsched.

The project intentionally has no network configuration beyond a local mock
site.  Every runtime rule that protects that boundary lives here so it is easy
to audit.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import uuid

APP_NAME = "batchsched"
APP_VERSION = "0.1.0"

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 5600
DEFAULT_BASE_URL = f"http://{DEFAULT_HOST}:{DEFAULT_PORT}"
ALLOWED_HOSTS = frozenset({"127.0.0.1", "localhost", "::1"})

MAX_JOBS = 200
MAX_WORKERS = 8
MAX_TIMEOUT_SECONDS = 10.0
MAX_RESPONSE_BYTES = 2 * 1024 * 1024
MAX_BACKOFF_TOTAL_MS = 10_000
MAX_ATTEMPTS = 5
MIN_ATTEMPT_DELAY_MS = 0
MAX_ATTEMPT_DELAY_MS = 10_000

ERROR_CLASSES = (
    "network",
    "http_5xx",
    "http_4xx",
    "too_large",
    "timeout",
    "empty_body",
    "parse_precondition",
)
DEFAULT_RETRYABLE = frozenset(
    {"network", "http_5xx", "timeout", "empty_body"}
)
NON_RETRYABLE = frozenset({"http_4xx", "too_large", "parse_precondition"})

JOB_ID_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$"
JOB_PENDING = "pending"
JOB_RUNNING = "running"
JOB_DONE = "done"
JOB_FAILED = "failed"
JOB_SKIPPED = "skipped"
JOB_TERMINAL = frozenset({JOB_DONE, JOB_FAILED, JOB_SKIPPED})

RUN_RUNNING = "running"
RUN_COMPLETED = "completed"
RUN_FAILED = "failed"
RUN_INTERRUPTED = "interrupted"


@dataclass(frozen=True)
class RuntimePaths:
    """All writable state is rooted below one directory."""

    base_dir: Path

    @property
    def work_dir(self) -> Path:
        return self.base_dir / "work"

    @property
    def db_path(self) -> Path:
        return self.work_dir / "ledger.sqlite3"

    @property
    def output_dir(self) -> Path:
        return self.work_dir / "output"

    @property
    def runs_dir(self) -> Path:
        return self.output_dir / "runs"

    @property
    def log_dir(self) -> Path:
        return self.work_dir / "logs"

    def ensure(self) -> "RuntimePaths":
        for path in (self.work_dir, self.output_dir, self.runs_dir, self.log_dir):
            path.mkdir(parents=True, exist_ok=True)
        return self


def utc_now_iso() -> str:
    """Return a compact UTC timestamp with a trailing Z."""

    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace(
        "+00:00", "Z"
    )


def new_run_id() -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"run-{stamp}-{uuid.uuid4().hex[:6]}"


def display_path(path: str | Path, base: Path | None = None) -> str:
    """Return a repository-relative, POSIX-style path when possible.

    Reports never expose absolute paths.  If a path lives outside the chosen
    base directory, only its name is retained.
    """

    candidate = Path(path)
    root = (base or Path.cwd()).resolve()
    try:
        relative = candidate.resolve().relative_to(root)
    except (ValueError, OSError):
        return candidate.name or "<unknown>"
    return relative.as_posix()


def run_output_relative(run_id: str, filename: str) -> str:
    return f"work/output/runs/{run_id}/{filename}"
