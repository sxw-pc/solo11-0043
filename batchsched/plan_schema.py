"""Field-level plan validation and YAML line-number helpers."""

from __future__ import annotations

import re
from typing import Any

import yaml

from . import config
from .models import JobSpec, RetryPolicy, ValidationIssue

ALLOWED_TOP = {"name", "concurrency", "jobs"}
ALLOWED_JOB = {"id", "source", "depends_on", "retry", "params"}
ALLOWED_RETRY = {"mode", "max_attempts", "base_delay_ms", "retryable", "max_total_delay_ms"}
ID_RE = re.compile(config.JOB_ID_PATTERN)


def parse_job(
    raw: dict[str, Any],
    line: int | None,
    field_lines: dict[str, int],
    issues: list[ValidationIssue],
) -> JobSpec | None:
    job_id = raw.get("id")
    if not isinstance(job_id, str) or not ID_RE.fullmatch(job_id):
        issues.append(
            ValidationIssue(
                "invalid_id",
                "id must be 1-64 letters, digits, '_' or '-', starting with a letter or digit",
                field_lines.get("id", line),
            )
        )
        return None
    source = raw.get("source")
    if not valid_source(source):
        issues.append(
            ValidationIssue(
                "invalid_source",
                "source must be a local path beginning with '/' and containing no '://'",
                field_lines.get("source", line),
                job_id,
            )
        )
        source = "/"
    raw_deps = raw.get("depends_on", [])
    deps: list[str] = []
    if not isinstance(raw_deps, list) or any(not isinstance(dep, str) for dep in raw_deps):
        issues.append(
            ValidationIssue(
                "invalid_dependencies",
                "depends_on must be a list of job id strings",
                field_lines.get("depends_on", line),
                job_id,
            )
        )
    else:
        for dep in raw_deps:
            if dep == job_id:
                issues.append(
                    ValidationIssue(
                        "self_dependency",
                        f"job '{job_id}' cannot depend on itself",
                        field_lines.get("depends_on", line),
                        job_id,
                    )
                )
            elif dep in deps:
                issues.append(
                    ValidationIssue(
                        "duplicate_dependency",
                        f"job '{job_id}' lists dependency '{dep}' twice",
                        field_lines.get("depends_on", line),
                        job_id,
                    )
                )
            else:
                deps.append(dep)
    params = parse_params(raw.get("params", {}), field_lines.get("params", line), job_id, issues)
    retry = parse_retry(raw.get("retry"), field_lines.get("retry", line), job_id, issues)
    return JobSpec(
        id=job_id,
        source=str(source),
        depends_on=tuple(deps),
        retry=retry,
        params=dict(params),
    )


def parse_params(
    raw: Any,
    line: int | None,
    job_id: str,
    issues: list[ValidationIssue],
) -> dict[str, Any]:
    if not isinstance(raw, dict):
        issues.append(
            ValidationIssue("invalid_params", "params must be a mapping", line, job_id)
        )
        return {}
    params = dict(raw)
    budget = params.get("max_record_bytes")
    if budget is not None and (
        not is_int(budget) or not 1 <= budget <= config.MAX_RESPONSE_BYTES
    ):
        issues.append(
            ValidationIssue(
                "invalid_max_record_bytes",
                f"params.max_record_bytes must be an integer 1.."
                f"{config.MAX_RESPONSE_BYTES}",
                line,
                job_id,
            )
        )
        params.pop("max_record_bytes")
    return params


def parse_retry(
    raw: Any,
    line: int | None,
    job_id: str,
    issues: list[ValidationIssue],
) -> RetryPolicy:
    defaults = RetryPolicy()
    if raw is None:
        return defaults
    if not isinstance(raw, dict):
        issues.append(
            ValidationIssue("invalid_retry", "retry must be a mapping", line, job_id)
        )
        return defaults
    for key in raw:
        if key not in ALLOWED_RETRY:
            issues.append(
                ValidationIssue("unknown_field", f"unknown retry field: {key}", line, job_id)
            )

    def int_field(value: Any, code: str, message: str, low: int, high: int, fallback: int) -> int:
        if is_int(value) and low <= value <= high:
            return int(value)
        issues.append(ValidationIssue(code, message, line, job_id))
        return fallback

    mode = raw.get("mode", defaults.mode)
    if mode not in ("fixed", "exponential"):
        issues.append(
            ValidationIssue(
                "invalid_retry_mode",
                "retry.mode must be 'fixed' or 'exponential'",
                line,
                job_id,
            )
        )
        mode = defaults.mode
    attempts = int_field(
        raw.get("max_attempts", defaults.max_attempts),
        "invalid_max_attempts",
        f"retry.max_attempts must be 1..{config.MAX_ATTEMPTS}",
        1,
        config.MAX_ATTEMPTS,
        defaults.max_attempts,
    )
    delay = int_field(
        raw.get("base_delay_ms", defaults.base_delay_ms),
        "invalid_base_delay",
        f"retry.base_delay_ms must be {config.MIN_ATTEMPT_DELAY_MS}.."
        f"{config.MAX_ATTEMPT_DELAY_MS}",
        config.MIN_ATTEMPT_DELAY_MS,
        config.MAX_ATTEMPT_DELAY_MS,
        defaults.base_delay_ms,
    )
    cap = int_field(
        raw.get("max_total_delay_ms", config.MAX_BACKOFF_TOTAL_MS),
        "invalid_retry_cap",
        f"retry.max_total_delay_ms must be 0..{config.MAX_BACKOFF_TOTAL_MS}",
        0,
        config.MAX_BACKOFF_TOTAL_MS,
        config.MAX_BACKOFF_TOTAL_MS,
    )
    retryable_raw = raw.get("retryable")
    retryable: frozenset[str] | None = None
    if retryable_raw is not None:
        if not isinstance(retryable_raw, list) or any(
            not isinstance(item, str) for item in retryable_raw
        ):
            issues.append(
                ValidationIssue(
                    "invalid_retryable",
                    "retry.retryable must be a list of error classes",
                    line, job_id,
                )
            )
        else:
            unknown = sorted(set(retryable_raw) - set(config.ERROR_CLASSES))
            if unknown:
                issues.append(
                    ValidationIssue(
                        "invalid_retryable",
                        "unknown error class(es): " + ", ".join(unknown),
                        line, job_id,
                    )
                )
            retryable = frozenset(
                item for item in retryable_raw if item in config.ERROR_CLASSES
            )
    return RetryPolicy(
        mode=str(mode),
        max_attempts=attempts,
        base_delay_ms=delay,
        retryable=retryable,
        max_total_delay_ms=cap,
    )


def find_cycle(jobs: dict[str, JobSpec]) -> list[str] | None:
    visiting: set[str] = set()
    visited: set[str] = set()
    stack: list[str] = []

    def visit(job_id: str) -> list[str] | None:
        if job_id in visiting:
            start = stack.index(job_id)
            return stack[start:] + [job_id]
        if job_id in visited:
            return None
        visiting.add(job_id)
        stack.append(job_id)
        for dep in jobs[job_id].depends_on:
            if dep not in jobs:
                continue
            found = visit(dep)
            if found:
                return found
        stack.pop()
        visiting.remove(job_id)
        visited.add(job_id)
        return None

    for job_id in sorted(jobs):
        found = visit(job_id)
        if found:
            return found
    return None


def valid_source(source: Any) -> bool:
    if not isinstance(source, str) or not source.startswith("/") or source.startswith("//"):
        return False
    if "://" in source or "\\" in source or ".." in source:
        return False
    return not any(ord(ch) < 32 or ch.isspace() for ch in source)


def is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def line_of(node: Any) -> int | None:
    mark = getattr(node, "start_mark", None)
    return mark.line + 1 if mark is not None else None


def key_lines(node: Any) -> dict[str, int]:
    if not isinstance(node, yaml.MappingNode):
        return {}
    return {
        key: (line_of(value_node) or line_of(key_node) or 0)
        for key_node, value_node in node.value
        if isinstance((key := getattr(key_node, "value", None)), str)
    }


def job_nodes(root: Any) -> list[Any]:
    if not isinstance(root, yaml.MappingNode):
        return []
    for key_node, value_node in root.value:
        if getattr(key_node, "value", None) == "jobs" and isinstance(
            value_node, yaml.SequenceNode
        ):
            return list(value_node.value)
    return []


def check_duplicate_keys(node: Any, issues: list[ValidationIssue]) -> None:
    if isinstance(node, yaml.MappingNode):
        seen: set[str] = set()
        for key_node, value_node in node.value:
            key = getattr(key_node, "value", None)
            if isinstance(key, str):
                if key in seen:
                    issues.append(
                        ValidationIssue("duplicate_key", f"duplicate YAML key: {key}", line_of(key_node))
                    )
                seen.add(key)
            check_duplicate_keys(value_node, issues)
    elif isinstance(node, yaml.SequenceNode):
        for child in node.value:
            check_duplicate_keys(child, issues)
