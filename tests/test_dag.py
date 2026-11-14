from __future__ import annotations

import pytest

from batchsched import config
from batchsched.dag import (
    blocked_jobs,
    downstream_of,
    frontier,
    initial_ready_set,
    topological_layers,
    topological_order,
)
from batchsched.plan_loader import parse_plan_text

PLAN_TEXT = """
name: dag-test
concurrency: 2
jobs:
  - id: fetch_root
    source: /api/catalog
  - id: fetch_second
    source: /api/catalog
  - id: build_left
    source: /api/orders
    depends_on: [fetch_root]
  - id: build_right
    source: /api/orders
    depends_on: [fetch_root, fetch_second]
  - id: publish_report
    source: /api/summary
    depends_on: [build_left, build_right]
"""


@pytest.fixture
def dag():
    return parse_plan_text(PLAN_TEXT)


def test_topological_layers_and_order(dag):
    assert topological_layers(dag) == [
        ["fetch_root", "fetch_second"],
        ["build_left", "build_right"],
        ["publish_report"],
    ]
    order = topological_order(dag)
    assert order[0:2] == ["fetch_root", "fetch_second"]
    assert order[-1] == "publish_report"


def test_frontier_starts_with_roots(dag):
    assert initial_ready_set(dag) == ["fetch_root", "fetch_second"]
    assert frontier(dag, {"fetch_root"}) == ["build_left", "fetch_second"]
    assert frontier(dag, {"fetch_root", "fetch_second"}) == [
        "build_left",
        "build_right",
    ]


def test_frontier_excludes_terminal_and_failed_dependencies(dag):
    ready = frontier(
        dag,
        completed={"fetch_root", "fetch_second"},
        failed={"build_left"},
        skipped=set(),
    )
    assert ready == ["build_right"]
    assert "publish_report" not in ready


def test_blocked_jobs_propagate_to_descendants(dag):
    states = {job_id: config.JOB_PENDING for job_id in dag.jobs}
    states["fetch_root"] = config.JOB_FAILED
    blocked = blocked_jobs(dag, states)
    blocked_ids = [job_id for job_id, _ in blocked]
    assert blocked_ids == ["build_left", "build_right", "publish_report"]
    reasons = dict(blocked)
    assert reasons["build_left"] == "upstream_failed"
    assert reasons["publish_report"] == "upstream_failed"
    assert states["build_left"] == config.JOB_PENDING


def test_skipped_dependency_also_blocks(dag):
    states = {job_id: config.JOB_PENDING for job_id in dag.jobs}
    states["fetch_second"] = config.JOB_SKIPPED
    blocked = dict(blocked_jobs(dag, states))
    assert "build_right" in blocked
    assert blocked["build_right"] == "upstream_failed"


def test_downstream_of_transitive(dag):
    assert downstream_of(dag, {"fetch_root"}) == {
        "build_left",
        "build_right",
        "publish_report",
    }
    assert downstream_of(dag, ["publish_report"]) == set()


def test_topological_layers_cycle_detection():
    from batchsched.models import JobSpec, PlanDag

    cyclic = PlanDag(
        name="cyclic",
        concurrency=1,
        jobs={
            "job_a": JobSpec(id="job_a", source="/a", depends_on=("job_b",)),
            "job_b": JobSpec(id="job_b", source="/b", depends_on=("job_a",)),
        },
    )
    with pytest.raises(ValueError, match="cycle"):
        topological_layers(cyclic)
