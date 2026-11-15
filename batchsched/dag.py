"""Pure DAG algorithms used by validation, scheduling and resume."""

from __future__ import annotations

from collections import deque
from typing import Iterable

from . import config
from .models import PlanDag


def topological_layers(dag: PlanDag) -> list[list[str]]:
    """Kahn layering with lexical ordering for deterministic runs."""

    indegree = {job_id: len(job.depends_on) for job_id, job in dag.jobs.items()}
    ready = sorted(job_id for job_id, degree in indegree.items() if degree == 0)
    layers: list[list[str]] = []
    seen = 0
    while ready:
        layer = list(ready)
        layers.append(layer)
        seen += len(layer)
        next_ready: list[str] = []
        for job_id in layer:
            for child in sorted(dag.jobs):
                if job_id not in dag.jobs[child].depends_on:
                    continue
                indegree[child] -= 1
                if indegree[child] == 0:
                    next_ready.append(child)
        ready = sorted(next_ready)
    if seen != len(dag.jobs):
        raise ValueError("DAG contains a cycle")
    return layers


def topological_order(dag: PlanDag) -> list[str]:
    return [job_id for layer in topological_layers(dag) for job_id in layer]


def layer_ranks(dag: PlanDag) -> dict[str, int]:
    """Zero-based topological layer for every job.

    Dispatch ordering may only reorder jobs that live in the same layer;
    comparing these ranks keeps an earlier layer strictly ahead of a later one.
    """

    return {
        job_id: index
        for index, layer in enumerate(topological_layers(dag))
        for job_id in layer
    }


def order_for_dispatch(
    dag: PlanDag,
    job_ids: Iterable[str],
    ranks: dict[str, int] | None = None,
) -> list[str]:
    """Rank already-executable jobs for dispatch.

    The key is explicit and total, so thread-pool submission order can never
    decide the outcome: topological layer first, then higher priority, then
    lexical job id.  Callers must only pass jobs from :func:`frontier`; this
    function never adds a job whose dependencies are unfinished.
    """

    layer_by_job = ranks if ranks is not None else layer_ranks(dag)
    missing = len(layer_by_job)

    def key(job_id: str) -> tuple[int, int, str]:
        job = dag.jobs[job_id]
        return (layer_by_job.get(job_id, missing), -job.priority, job_id)

    return sorted(job_ids, key=key)


def frontier(
    dag: PlanDag,
    completed: Iterable[str],
    failed: Iterable[str] = (),
    skipped: Iterable[str] = (),
) -> list[str]:
    """Jobs that are ready to dispatch right now.

    A job is ready only when every dependency is in ``completed``.  Jobs with
    failed or skipped dependencies are not executable; the scheduler marks
    those jobs skipped through :func:`blocked_jobs`.
    """

    done = set(completed)
    terminal = done | set(failed) | set(skipped)
    ready: list[str] = []
    for job_id in sorted(dag.jobs):
        if job_id in terminal:
            continue
        deps = dag.jobs[job_id].depends_on
        if all(dep in done for dep in deps):
            ready.append(job_id)
    return ready


def blocked_jobs(dag: PlanDag, states: dict[str, str]) -> list[tuple[str, str]]:
    """Return newly blocked ``(job_id, reason)`` pairs, including propagation.

    A dependency that failed or was skipped blocks its descendants.  Once a
    job is considered skipped here, its descendants are discovered in the same
    pass, so chains collapse deterministically.
    """

    working = dict(states)
    blocked: list[tuple[str, str]] = []
    changed = True
    while changed:
        changed = False
        for job_id in sorted(dag.jobs):
            if working.get(job_id) != config.JOB_PENDING:
                continue
            bad_dep = next(
                (
                    dep
                    for dep in dag.jobs[job_id].depends_on
                    if working.get(dep) in (config.JOB_FAILED, config.JOB_SKIPPED)
                ),
                None,
            )
            if bad_dep is not None:
                working[job_id] = config.JOB_SKIPPED
                blocked.append((job_id, "upstream_failed"))
                changed = True
    return blocked


def downstream_of(dag: PlanDag, job_ids: Iterable[str]) -> set[str]:
    """Transitive descendants of the supplied job ids (excluding seeds)."""

    seeds = set(job_ids)
    result: set[str] = set()
    queue: deque[str] = deque(sorted(seeds))
    while queue:
        current = queue.popleft()
        for job_id in sorted(dag.jobs):
            if current not in dag.jobs[job_id].depends_on or job_id in result:
                continue
            result.add(job_id)
            queue.append(job_id)
    return result


def initial_ready_set(dag: PlanDag) -> list[str]:
    return frontier(dag, completed=frozenset())
