"""YAML plan loading and strict validation with line-level diagnostics."""

from __future__ import annotations

from pathlib import Path
import yaml

from . import config
from .models import JobSpec, PlanDag, PlanValidationError, ValidationIssue
from .plan_schema import (
    ALLOWED_JOB,
    ALLOWED_TOP,
    check_duplicate_keys,
    child_node,
    find_cycle,
    is_int,
    job_nodes,
    key_lines,
    line_of,
    parse_job,
)


def load_plan(path: str | Path) -> PlanDag:
    plan_path = Path(path)
    try:
        text = plan_path.read_text(encoding="utf-8")
    except OSError as exc:
        raise PlanValidationError(
            [ValidationIssue("file_error", f"cannot read plan: {exc.__class__.__name__}")]
        ) from exc
    return parse_plan_text(text, display_name=str(plan_path))


def parse_plan_text(text: str, display_name: str = "<inline>") -> PlanDag:
    try:
        root_node = yaml.compose(text, Loader=yaml.SafeLoader)
        data = yaml.safe_load(text)
    except yaml.MarkedYAMLError as exc:
        line = (exc.problem_mark.line + 1) if exc.problem_mark else None
        raise PlanValidationError(
            [ValidationIssue("yaml_syntax", str(exc.problem or "invalid YAML"), line)]
        ) from exc
    except yaml.YAMLError as exc:
        raise PlanValidationError(
            [ValidationIssue("yaml_syntax", exc.__class__.__name__)]
        ) from exc

    issues: list[ValidationIssue] = []
    if not isinstance(data, dict):
        raise PlanValidationError(
            [ValidationIssue("root_type", "plan root must be a mapping", line_of(root_node))]
        )
    check_duplicate_keys(root_node, issues)
    top_lines = key_lines(root_node)
    for key in data:
        if key not in ALLOWED_TOP:
            issues.append(
                ValidationIssue(
                    "unknown_field",
                    f"unknown top-level field: {key}",
                    top_lines.get(key, line_of(root_node)),
                )
            )

    name = data.get("name", "unnamed-plan")
    if not isinstance(name, str) or not name.strip():
        issues.append(
            ValidationIssue(
                "invalid_name",
                "name must be a non-empty string",
                top_lines.get("name"),
            )
        )
        name = "unnamed-plan"

    concurrency = data.get("concurrency", 2)
    if not is_int(concurrency) or not 1 <= concurrency <= config.MAX_WORKERS:
        issues.append(
            ValidationIssue(
                "invalid_concurrency",
                f"concurrency must be an integer between 1 and {config.MAX_WORKERS}",
                top_lines.get("concurrency"),
            )
        )
        concurrency = 1

    raw_jobs = data.get("jobs")
    if not isinstance(raw_jobs, list) or not raw_jobs:
        issues.append(
            ValidationIssue(
                "invalid_jobs", "jobs must be a non-empty list", top_lines.get("jobs")
            )
        )
        raise PlanValidationError(issues)
    if len(raw_jobs) > config.MAX_JOBS:
        issues.append(
            ValidationIssue(
                "too_many_jobs",
                f"job count {len(raw_jobs)} exceeds limit {config.MAX_JOBS}",
                top_lines.get("jobs"),
            )
        )
        raw_jobs = raw_jobs[: config.MAX_JOBS]

    nodes = job_nodes(root_node)
    jobs: dict[str, JobSpec] = {}
    job_lines: dict[str, int] = {}
    for index, raw_job in enumerate(raw_jobs):
        node = nodes[index] if index < len(nodes) else None
        line = line_of(node) if node is not None else top_lines.get("jobs")
        if not isinstance(raw_job, dict):
            issues.append(ValidationIssue("job_type", "job must be a mapping", line))
            continue
        fields = key_lines(node)
        for key in raw_job:
            if key not in ALLOWED_JOB:
                issues.append(
                    ValidationIssue(
                        "unknown_field",
                        f"job field '{key}' is not allowed",
                        fields.get(key, line),
                    )
                )
        job = parse_job(
            raw_job, line, fields, issues, params_node=child_node(node, "params")
        )
        if job is None:
            continue
        if job.id in jobs:
            issues.append(
                ValidationIssue(
                    "duplicate_id", f"duplicate job id: {job.id}", line, job.id
                )
            )
            continue
        jobs[job.id] = job
        job_lines[job.id] = line or 0

    for job in jobs.values():
        for dep in job.depends_on:
            if dep not in jobs:
                issues.append(
                    ValidationIssue(
                        "dangling_dependency",
                        f"job '{job.id}' depends on unknown job '{dep}'",
                        job_lines[job.id],
                        job.id,
                    )
                )
    cycle = find_cycle(jobs)
    if cycle:
        issues.append(
            ValidationIssue(
                "cycle",
                "dependency cycle detected: " + " -> ".join(cycle),
                job_lines.get(cycle[0]),
                cycle[0],
            )
        )
    if issues:
        raise PlanValidationError(issues)
    assert isinstance(name, str)
    assert isinstance(concurrency, int)
    return PlanDag(
        name=name, concurrency=concurrency, jobs=jobs, plan_path=display_name
    )
