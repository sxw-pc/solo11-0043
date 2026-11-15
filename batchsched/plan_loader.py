"""YAML plan loading and strict validation with line-level diagnostics."""

from __future__ import annotations

from pathlib import Path
import yaml

from . import config
from .models import (
    JobSpec,
    PlanDag,
    PlanValidationError,
    ValidationIssue,
)
from .plan_schema import (
    ALLOWED_JOB,
    ALLOWED_TOP,
    build_graph_diagnostics,
    check_duplicate_keys,
    find_cycle,
    is_int,
    job_nodes,
    key_lines,
    line_of,
    parse_job,
    valid_source,
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
            [
                ValidationIssue(
                    "root_type", "plan root must be a mapping", line_of(root_node)
                )
            ],
            build_graph_diagnostics({}, [], [], None),
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
        raise PlanValidationError(
            issues, build_graph_diagnostics({}, [], [], None)
        )
    if len(raw_jobs) > config.MAX_JOBS:
        issues.append(
            ValidationIssue(
                "too_many_jobs",
                f"job count {len(raw_jobs)} exceeds limit {config.MAX_JOBS}",
                top_lines.get("jobs"),
            )
        )
        job_limit_discarded = [
            (_raw_job_label(raw_job, index), "job_limit")
            for index, raw_job in enumerate(raw_jobs[config.MAX_JOBS :], config.MAX_JOBS)
        ]
        raw_jobs = raw_jobs[: config.MAX_JOBS]
    else:
        job_limit_discarded = []

    nodes = job_nodes(root_node)
    jobs: dict[str, JobSpec] = {}
    job_lines: dict[str, int] = {}
    discarded: list[tuple[str, str]] = list(job_limit_discarded)
    discarded_ids: set[str] = {label for label, _ in job_limit_discarded}
    for index, raw_job in enumerate(raw_jobs):
        node = nodes[index] if index < len(nodes) else None
        line = line_of(node) if node is not None else top_lines.get("jobs")
        if not isinstance(raw_job, dict):
            issues.append(ValidationIssue("job_type", "job must be a mapping", line))
            label = _raw_job_label(raw_job, index)
            discarded.append((label, "job_type"))
            discarded_ids.add(label)
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
        job = parse_job(raw_job, line, fields, issues)
        if job is None:
            label = _raw_job_label(raw_job, index)
            discarded.append((label, "invalid_id"))
            discarded_ids.add(label)
            continue
        if job.id in jobs:
            issues.append(
                ValidationIssue(
                    "duplicate_id", f"duplicate job id: {job.id}", line, job.id
                )
            )
            discarded.append((job.id, "duplicate_id"))
            discarded_ids.add(job.id)
            continue
        if not valid_source(raw_job.get("source")):
            # The invalid source was already reported by parse_job.  The job
            # cannot run, so it is discarded from the effective graph; other
            # jobs may still name it, which is reported as a dangling
            # dependency, never as an island.
            discarded.append((job.id, "invalid_source"))
            discarded_ids.add(job.id)
            continue
        jobs[job.id] = job
        job_lines[job.id] = line or 0

    dangling: list[tuple[str, str, str]] = []
    for job_id in sorted(jobs):
        job = jobs[job_id]
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
                reason = (
                    "dependency_discarded"
                    if dep in discarded_ids
                    else "dependency_missing"
                )
                dangling.append((job.id, dep, reason))
    dangling.sort()
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
    diagnostics = build_graph_diagnostics(jobs, discarded, dangling, cycle)
    if issues:
        raise PlanValidationError(issues, diagnostics)
    assert isinstance(name, str)
    assert isinstance(concurrency, int)
    return PlanDag(
        name=name, concurrency=concurrency, jobs=jobs, plan_path=display_name
    )


def _raw_job_label(raw_job: object, index: int) -> str:
    """Stable label for a job that never became a valid :class:`JobSpec`."""

    if isinstance(raw_job, dict):
        raw_id = raw_job.get("id")
        if isinstance(raw_id, str):
            return raw_id
        if isinstance(raw_id, (int, float)) and not isinstance(raw_id, bool):
            return str(raw_id)
    return f"<job #{index + 1}>"

