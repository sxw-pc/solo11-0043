"""Pure DAG algorithms used by validation, scheduling and resume."""

from __future__ import annotations

from collections import deque
from typing import Iterable

from . import config
from .models import JobSpec, PlanDag


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


def _neighbors(jobs: dict[str, JobSpec]) -> dict[str, set[str]]:
    """Undirected adjacency over dependencies that exist in ``jobs``."""

    adjacency: dict[str, set[str]] = {job_id: set() for job_id in jobs}
    for job_id in jobs:
        for dep in jobs[job_id].depends_on:
            if dep in jobs:
                adjacency[job_id].add(dep)
                adjacency[dep].add(job_id)
    return adjacency


def weakly_connected_components(jobs: dict[str, JobSpec]) -> list[list[str]]:
    """Weakly connected components of the effective dependency graph.

    Only jobs present in ``jobs`` are nodes.  Declared dependencies whose name
    is absent are ignored: they are not nodes of the effective subgraph.
    Component members are sorted, and components are ordered by first member
    for deterministic output.
    """

    adjacency = _neighbors(jobs)
    seen: set[str] = set()
    components: list[list[str]] = []
    for start in sorted(jobs):
        if start in seen:
            continue
        seen.add(start)
        queue: deque[str] = deque([start])
        members: list[str] = []
        while queue:
            current = queue.popleft()
            members.append(current)
            for neighbor in sorted(adjacency[current]):
                if neighbor not in seen:
                    seen.add(neighbor)
                    queue.append(neighbor)
        components.append(sorted(members))
    components.sort(key=lambda component: component[0])
    return components


def _has_dangling_dependency(job: JobSpec, jobs: dict[str, JobSpec]) -> bool:
    return any(dep not in jobs for dep in job.depends_on)


def degree_classes(
    jobs: dict[str, JobSpec],
) -> tuple[list[str], list[str], list[str]]:
    """Classify zero-degree accepted jobs.

    Returns ``(isolated, dangling_only, edge_less)``:

    * ``edge_less`` is every accepted job with no incoming and no outgoing
      edge in the effective graph.  Only accepted jobs are nodes, so discarded
      nodes can never appear here.
    * ``dangling_only`` is the subset of that set whose declared dependencies
      all point at names absent from the effective graph.  These are reported
      separately to distinguish them from a genuinely standalone job.
    * ``isolated`` follows the suspicious-island rule literally: zero in/out
      degree in the effective graph, and accepted (never discarded).  It
      equals ``edge_less``; dangling-only jobs remain flagged separately.
    """

    adjacency = _neighbors(jobs)
    edge_less = [job_id for job_id in sorted(jobs) if not adjacency[job_id]]
    dangling_only = [
        job_id
        for job_id in edge_less
        if _has_dangling_dependency(jobs[job_id], jobs)
    ]
    return edge_less, dangling_only, edge_less


def isolated_jobs(jobs: dict[str, JobSpec]) -> list[str]:
    """Accepted jobs with no incoming and no outgoing effective edge."""

    return degree_classes(jobs)[0]


def cycle_summary(jobs: dict[str, JobSpec], cycle: list[str]) -> dict:
    """Describe a detected cycle using the effective graph.

    ``cycle`` follows :func:`batchsched.plan_schema.find_cycle` and repeats the
    first node at the end.  Entry order is the dependency walk order used by
    cycle detection, so ``order`` starts at ``cycle[0]``.  Degrees are counted
    over the whole effective graph with edges pointing from a job to each
    dependency: ``indegree`` is the number of dependencies a job has, and
    ``outdegree`` is the number of jobs that depend on it.
    """

    nodes: list[str] = []
    for job_id in cycle:
        if job_id not in nodes:
            nodes.append(job_id)
    node_set = set(nodes)
    indegree: dict[str, int] = {}
    outdegree: dict[str, int] = {}
    cycle_indegree: dict[str, int] = {}
    cycle_outdegree: dict[str, int] = {}
    cycle_dependencies: dict[str, list[str]] = {}
    for job_id in nodes:
        spec = jobs.get(job_id)
        deps = spec.depends_on if spec is not None else ()
        indegree[job_id] = sum(1 for dep in deps if dep in jobs)
        cycle_indegree[job_id] = sum(1 for dep in deps if dep in node_set)
        outdegree[job_id] = sum(
            1
            for other in jobs
            if other != job_id and job_id in jobs[other].depends_on
        )
        cycle_outdegree[job_id] = sum(
            1 for other in nodes if other != job_id and job_id in jobs[other].depends_on
        )
        cycle_dependencies[job_id] = [dep for dep in deps if dep in node_set]
    order = list(cycle)
    edges = [
        {"from": order[index], "to": order[index + 1]}
        for index in range(len(order) - 1)
    ]
    return {
        "order": order,
        "dependency_order": order,
        "nodes": nodes,
        "length": len(nodes),
        "edges": edges,
        "dependencies": {
            job_id: cycle_dependencies[job_id] for job_id in nodes
        },
        "jobs": [
            {
                "id": job_id,
                "indegree": indegree[job_id],
                "outdegree": outdegree[job_id],
                "in_degree": indegree[job_id],
                "out_degree": outdegree[job_id],
                "cycle_indegree": cycle_indegree[job_id],
                "cycle_outdegree": cycle_outdegree[job_id],
                "dependencies": cycle_dependencies[job_id],
            }
            for job_id in nodes
        ],
        "job_degrees": {
            job_id: {
                "indegree": indegree[job_id],
                "outdegree": outdegree[job_id],
            }
            for job_id in nodes
        },
    }
