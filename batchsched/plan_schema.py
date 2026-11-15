"""Field-level plan validation and YAML line-number helpers."""

from __future__ import annotations

import re
from typing import Any

import yaml

from . import config
from .fetcher import (
    canonical_header_name,
    header_value_problem,
    query_value_problem,
    source_query_keys,
)
from .models import JobSpec, RetryPolicy, ValidationIssue

ALLOWED_TOP = {"name", "concurrency", "jobs"}
ALLOWED_JOB = {"id", "source", "depends_on", "retry", "params"}
# Job-control params understood by the executor.  ``query`` and ``headers``
# are the controlled request-extension containers; allow-listed query keys
# may also be declared directly for compact plans.
JOB_CONTROL_PARAMS = {"expect_content_type", "min_bytes", "note"}
REQUEST_EXTENSION_PARAMS = {"query", "headers"}
ALLOWED_RETRY = {
    "mode",
    "max_attempts",
    "base_delay_ms",
    "retryable",
    "max_total_delay_ms",
}
ID_RE = re.compile(config.JOB_ID_PATTERN)


def parse_job(
    raw: dict[str, Any],
    line: int | None,
    field_lines: dict[str, int],
    issues: list[ValidationIssue],
    params_node: Any | None = None,
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
    params = parse_params(
        raw.get("params", {}),
        field_lines.get("params", line),
        params_node,
        source if isinstance(source, str) else "/",
        job_id,
        issues,
    )
    retry = parse_retry(raw.get("retry"), field_lines.get("retry", line), job_id, issues)
    return JobSpec(
        id=job_id,
        source=str(source),
        depends_on=tuple(deps),
        retry=retry,
        params=params,
    )


def parse_params(
    raw: Any,
    line: int | None,
    params_node: Any | None,
    source: str,
    job_id: str,
    issues: list[ValidationIssue],
) -> dict[str, Any]:
    """Validate job params and normalize request extensions.

    Request extensions are stored as ``params["query"]`` (query key -> scalar)
    and ``params["headers"]`` (canonical header name -> text).  Allow-listed
    query keys may be declared directly beside the control params; both forms
    are merged, but declaring the same key twice is rejected.
    """

    if not isinstance(raw, dict):
        issues.append(
            ValidationIssue(
                "invalid_params", "params must be a mapping", line, job_id
            )
        )
        return {}
    top_lines = _params_key_lines(params_node)
    normalized: dict[str, Any] = {}
    query: dict[str, Any] = {}
    query_lines: dict[str, int | None] = {}
    headers: dict[str, str] = {}
    source_keys = source_query_keys(source)

    def _add_query(key: str, value: Any, key_line: int | None) -> None:
        if key in query:
            issues.append(
                ValidationIssue(
                    "invalid_params",
                    f"query parameter '{key}' is declared twice",
                    key_line,
                    job_id,
                )
            )
            return
        query[key] = value
        query_lines[key] = key_line

    for key, value in raw.items():
        key_line = top_lines.get(key, line)
        if not isinstance(key, str):
            issues.append(
                ValidationIssue(
                    "invalid_params", "param names must be strings", key_line, job_id
                )
            )
            continue
        if key in REQUEST_EXTENSION_PARAMS:
            if key == "query":
                group_lines = _params_key_lines(child_node(params_node, "query"))
                for group_key, group_value in _parse_query_group(
                    value, key_line, params_node, "query", job_id, issues
                ).items():
                    _add_query(
                        group_key, group_value, group_lines.get(group_key, key_line)
                    )
            else:
                headers.update(
                    _parse_header_group(value, key_line, params_node, job_id, issues)
                )
        elif key in config.QUERY_PARAM_RULES:
            problem = query_value_problem(key, value)
            if problem is not None:
                issues.append(
                    ValidationIssue(
                        "invalid_params", f"query parameter '{key}': {problem}",
                        key_line, job_id,
                    )
                )
            else:
                _add_query(key, value, key_line)
        elif key in JOB_CONTROL_PARAMS:
            problem = _control_param_problem(key, value)
            if problem is not None:
                issues.append(
                    ValidationIssue(
                        "invalid_params", f"param '{key}': {problem}", key_line, job_id
                    )
                )
            else:
                normalized[key] = value
        else:
            issues.append(
                ValidationIssue(
                    "invalid_params", f"unknown param: {key}", key_line, job_id
                )
            )

    duplicates = sorted(set(query) & source_keys)
    if duplicates:
        issue_line = next(
            (query_lines[key] for key in duplicates if query_lines.get(key)),
            top_lines.get("query", line),
        )
        issues.append(
            ValidationIssue(
                "invalid_params",
                "query key(s) already present in source: " + ", ".join(duplicates),
                issue_line,
                job_id,
            )
        )
        for key in duplicates:
            query.pop(key, None)
    if query:
        normalized["query"] = query
    if headers:
        normalized["headers"] = headers
    return normalized


def _parse_query_group(
    raw: Any,
    line: int | None,
    params_node: Any | None,
    group: str,
    job_id: str,
    issues: list[ValidationIssue],
) -> dict[str, Any]:
    if not isinstance(raw, dict):
        issues.append(
            ValidationIssue(
                "invalid_params", "params.query must be a mapping", line, job_id
            )
        )
        return {}
    group_lines = _params_key_lines(child_node(params_node, group))
    result: dict[str, Any] = {}
    for key, value in raw.items():
        key_line = group_lines.get(key, line)
        if key not in config.QUERY_PARAM_RULES:
            issues.append(
                ValidationIssue(
                    "invalid_params", f"query parameter '{key}' is not allowed",
                    key_line, job_id,
                )
            )
            continue
        problem = query_value_problem(key, value)
        if problem is not None:
            issues.append(
                ValidationIssue(
                    "invalid_params", f"query parameter '{key}': {problem}",
                    key_line, job_id,
                )
            )
            continue
        if key in result:
            issues.append(
                ValidationIssue(
                    "invalid_params", f"query parameter '{key}' is declared twice",
                    key_line, job_id,
                )
            )
            continue
        result[key] = value
    return result


def _parse_header_group(
    raw: Any,
    line: int | None,
    params_node: Any | None,
    job_id: str,
    issues: list[ValidationIssue],
) -> dict[str, str]:
    if not isinstance(raw, dict):
        issues.append(
            ValidationIssue(
                "invalid_params", "params.headers must be a mapping", line, job_id
            )
        )
        return {}
    group_lines = _params_key_lines(child_node(params_node, "headers"))
    result: dict[str, str] = {}
    seen: set[str] = set()
    for name, value in raw.items():
        key_line = group_lines.get(name, line)
        canonical = canonical_header_name(name)
        if canonical is None:
            issues.append(
                ValidationIssue(
                    "invalid_params", f"request header '{name}' is not allowed",
                    key_line, job_id,
                )
            )
            continue
        folded = str(name).casefold()
        if folded in seen:
            issues.append(
                ValidationIssue(
                    "invalid_params", f"request header '{canonical}' is declared twice",
                    key_line, job_id,
                )
            )
            continue
        seen.add(folded)
        problem = header_value_problem(canonical, value)
        if problem is not None:
            issues.append(
                ValidationIssue(
                    "invalid_params", f"request header '{canonical}': {problem}",
                    key_line, job_id,
                )
            )
            continue
        result[canonical] = value
    return result


def _control_param_problem(key: str, value: Any) -> str | None:
    if key == "expect_content_type":
        if not isinstance(value, str) or not value.strip():
            return "must be a non-empty string"
        if len(value) > 128 or any(ch.isspace() for ch in value):
            return "must be a short token without whitespace"
        return None
    if key == "min_bytes":
        if not is_int(value) or value < 0:
            return "must be a non-negative integer"
        return None
    if key == "note":
        if not isinstance(value, str):
            return "must be a string"
        if len(value) > 256:
            return "must be at most 256 characters"
        return None
    return "is not allowed"


def child_node(node: Any, key: str) -> Any | None:
    if not isinstance(node, yaml.MappingNode):
        return None
    for key_node, value_node in node.value:
        if getattr(key_node, "value", None) == key:
            return value_node
    return None



def _params_key_lines(node: Any | None) -> dict[str, int]:
    return key_lines(node) if node is not None else {}


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
                ValidationIssue(
                    "unknown_field", f"unknown retry field: {key}", line, job_id
                )
            )
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
    attempts = raw.get("max_attempts", defaults.max_attempts)
    if not is_int(attempts) or not 1 <= attempts <= config.MAX_ATTEMPTS:
        issues.append(
            ValidationIssue(
                "invalid_max_attempts",
                f"retry.max_attempts must be 1..{config.MAX_ATTEMPTS}",
                line,
                job_id,
            )
        )
        attempts = defaults.max_attempts
    delay = raw.get("base_delay_ms", defaults.base_delay_ms)
    if not is_int(delay) or not (
        config.MIN_ATTEMPT_DELAY_MS <= delay <= config.MAX_ATTEMPT_DELAY_MS
    ):
        issues.append(
            ValidationIssue(
                "invalid_base_delay",
                f"retry.base_delay_ms must be {config.MIN_ATTEMPT_DELAY_MS}.."
                f"{config.MAX_ATTEMPT_DELAY_MS}",
                line,
                job_id,
            )
        )
        delay = defaults.base_delay_ms
    cap = raw.get("max_total_delay_ms", config.MAX_BACKOFF_TOTAL_MS)
    if not is_int(cap) or not 0 <= cap <= config.MAX_BACKOFF_TOTAL_MS:
        issues.append(
            ValidationIssue(
                "invalid_retry_cap",
                f"retry.max_total_delay_ms must be 0..{config.MAX_BACKOFF_TOTAL_MS}",
                line,
                job_id,
            )
        )
        cap = config.MAX_BACKOFF_TOTAL_MS
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
                    line,
                    job_id,
                )
            )
        else:
            unknown = sorted(set(retryable_raw) - set(config.ERROR_CLASSES))
            if unknown:
                issues.append(
                    ValidationIssue(
                        "invalid_retryable",
                        "unknown error class(es): " + ", ".join(unknown),
                        line,
                        job_id,
                    )
                )
            retryable = frozenset(
                item for item in retryable_raw if item in config.ERROR_CLASSES
            )
    return RetryPolicy(
        mode=str(mode),
        max_attempts=int(attempts),
        base_delay_ms=int(delay),
        retryable=retryable,
        max_total_delay_ms=int(cap),
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
    result: dict[str, int] = {}
    for key_node, value_node in node.value:
        key = getattr(key_node, "value", None)
        if isinstance(key, str):
            result[key] = line_of(value_node) or line_of(key_node) or 0
    return result


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
                        ValidationIssue(
                            "duplicate_key",
                            f"duplicate YAML key: {key}",
                            line_of(key_node),
                        )
                    )
                seen.add(key)
            check_duplicate_keys(value_node, issues)
    elif isinstance(node, yaml.SequenceNode):
        for child in node.value:
            check_duplicate_keys(child, issues)
