"""Command-line interface for validate, run, resume, report and serve."""

from __future__ import annotations

import argparse
from contextlib import contextmanager
import json
from pathlib import Path
import sys
from typing import Iterator
from urllib.parse import urlparse

from . import config
from .api import register_api
from .dag import topological_layers
from .executor import JobExecutor
from .fetcher import Fetcher, check_target
from .ledger import Ledger
from .mocksite.app import create_app
from .mocksite.server import (
    ensure_mock_site,
    mock_site_reachable,
    serve_forever,
)
from .models import PlanValidationError
from .plan_loader import load_plan
from .reporter import build_report, to_json, to_markdown
from .resume import ResumeError, resume_run
from .scheduler import Scheduler


def main(argv: list[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    handlers = {
        "serve": cmd_serve,
        "plan-validate": cmd_plan_validate,
        "run": cmd_run,
        "resume": cmd_resume,
        "report": cmd_report,
    }
    try:
        return handlers[args.command](args)
    except PlanValidationError as exc:
        _json_stdout(exc.to_dict())
        return 2
    except (ResumeError, RuntimeError, ValueError) as exc:
        _json_stderr({"ok": False, "error": str(exc)})
        return 1


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="batchsched")
    parser.add_argument(
        "--work-dir",
        default=".",
        help="directory containing work/ state (default: current directory)",
    )
    sub = parser.add_subparsers(dest="command", required=True)
    serve = sub.add_parser("serve", help="serve mock source and read-only API")
    serve.add_argument("--port", type=int, default=config.DEFAULT_PORT)
    validate = sub.add_parser("plan-validate", help="validate a YAML plan")
    validate.add_argument("plan")
    run = sub.add_parser("run", help="execute a plan")
    run.add_argument("--plan", required=True)
    run.add_argument("--base-url", default=config.DEFAULT_BASE_URL)
    resume = sub.add_parser("resume", help="resume an interrupted run")
    resume.add_argument("--run", required=True)
    resume.add_argument("--base-url", default=config.DEFAULT_BASE_URL)
    report = sub.add_parser("report", help="print a run report")
    report.add_argument("--run", required=True)
    report.add_argument("--format", choices=("json", "md"), default="json")
    report.add_argument("--out")
    return parser


def cmd_plan_validate(args: argparse.Namespace) -> int:
    plan = load_plan(args.plan)
    layers = topological_layers(plan)
    _json_stdout(
        {
            "ok": True,
            "name": plan.name,
            "job_count": len(plan.jobs),
            "concurrency": plan.concurrency,
            "layers": layers,
            "plan": _plan_label(Path(args.plan)),
            "diagnostics_ok": True,
        }
    )
    return 0


def cmd_run(args: argparse.Namespace) -> int:
    plan = load_plan(args.plan)
    paths = config.RuntimePaths(Path(args.work_dir)).ensure()
    ledger = Ledger(paths.db_path)
    run_id = config.new_run_id()
    with _local_mock(args.base_url) as base_url:
        ledger.create_run(
            run_id, _ledger_plan_path(Path(args.plan)), config.utc_now_iso()
        )
        outcome = _execute(plan, ledger, paths, run_id, base_url)
    report = build_report(ledger, run_id)
    _json_stdout(_outcome_payload(report, outcome.status))
    return _exit_code(outcome.status)


def cmd_resume(args: argparse.Namespace) -> int:
    paths = config.RuntimePaths(Path(args.work_dir)).ensure()
    ledger = Ledger(paths.db_path)
    run = ledger.get_run(args.run)
    if run is None:
        _json_stderr({"ok": False, "error": f"run not found: {args.run}"})
        return 1
    plan_path = _resolve_plan_path(run["plan_path"])
    plan = load_plan(plan_path)
    with _local_mock(args.base_url) as base_url:
        executor = _executor(ledger, paths, args.run, base_url)
        outcome = resume_run(ledger, plan, args.run, executor)
    report = build_report(ledger, args.run)
    _json_stdout(_outcome_payload(report, outcome.status))
    return _exit_code(outcome.status)


def cmd_report(args: argparse.Namespace) -> int:
    paths = config.RuntimePaths(Path(args.work_dir))
    ledger = Ledger(paths.db_path)
    try:
        report = build_report(ledger, args.run, base_dir=Path.cwd())
    except KeyError:
        _json_stderr({"ok": False, "error": f"run not found: {args.run}"})
        return 1
    rendered = to_json(report) if args.format == "json" else to_markdown(report)
    if args.out:
        Path(args.out).write_text(rendered, encoding="utf-8")
    print(rendered, end="")
    return 0


def cmd_serve(args: argparse.Namespace) -> int:
    host = config.DEFAULT_HOST
    paths = config.RuntimePaths(Path(args.work_dir)).ensure()
    ledger = Ledger(paths.db_path)
    app = create_app()
    register_api(app, ledger, base_dir=Path.cwd())
    serve_forever(app, host, args.port)
    return 0


def _execute(plan, ledger: Ledger, paths: config.RuntimePaths, run_id: str, base_url: str):
    executor = _executor(ledger, paths, run_id, base_url)
    scheduler = Scheduler(
        plan=plan,
        ledger=ledger,
        run_id=run_id,
        executor=executor,
        on_event=_progress,
    )
    return scheduler.run()


def _executor(
    ledger: Ledger, paths: config.RuntimePaths, run_id: str, base_url: str
) -> JobExecutor:
    def audit(event: str, detail: str) -> None:
        ledger.add_audit(run_id, event, detail)

    fetcher = Fetcher(base_url=base_url, audit_hook=audit)
    return JobExecutor(
        fetcher=fetcher,
        ledger=ledger,
        run_id=run_id,
        output_dir=paths.runs_dir / run_id,
    )


@contextmanager
def _local_mock(base_url: str) -> Iterator[str]:
    if not check_target(f"{base_url.rstrip('/')}/api/health").allowed:
        raise RuntimeError("base URL must target an allow-listed loopback host")
    if mock_site_reachable(base_url):
        yield base_url.rstrip("/")
        return
    parsed = urlparse(base_url)
    local, resolved = ensure_mock_site(parsed.hostname or config.DEFAULT_HOST, parsed.port or 80)
    try:
        yield resolved
    finally:
        if local is not None:
            local.stop()


def _outcome_payload(report: dict, status: str) -> dict:
    return {
        "ok": status == config.RUN_COMPLETED,
        "run_id": report["run"]["run_id"],
        "status": status,
        "job_counts": report["job_counts"],
        "total_attempts": report["total_attempts"],
        "outputs": report["outputs"],
    }


def _exit_code(status: str) -> int:
    if status == config.RUN_COMPLETED:
        return 0
    if status == config.RUN_INTERRUPTED:
        return 130
    return 1


def _resolve_plan_path(stored: str) -> Path:
    candidate = Path(stored)
    return candidate if candidate.is_absolute() else Path.cwd() / candidate


def _ledger_plan_path(path: Path) -> str:
    try:
        return path.resolve().relative_to(Path.cwd().resolve()).as_posix()
    except (OSError, ValueError):
        return str(path.resolve())


def _plan_label(path: Path) -> str:
    return config.display_path(path, Path.cwd())


def _progress(event: str, job_id: str) -> None:
    print(f"[{event}] {job_id}", file=sys.stderr, flush=True)


def _json_stdout(payload: dict) -> None:
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))


def _json_stderr(payload: dict) -> None:
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True), file=sys.stderr)
