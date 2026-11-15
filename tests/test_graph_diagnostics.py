"""Structural diagnostics attached to plan validation failures."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from batchsched.cli import main
from batchsched.dag import (
    cycle_summary,
    isolated_jobs,
    weakly_connected_components,
)
from batchsched.models import JobSpec, PlanValidationError
from batchsched.plan_loader import parse_plan_text
from batchsched.plan_schema import find_cycle

ROOT = Path(__file__).resolve().parents[1]

MIXED_PLAN = """
name: mixed
jobs:
  - id: good_root
    source: /api/catalog
  - id: "Bad ID"
    source: /api/catalog
  - id: good_child
    source: /api/orders
    depends_on: [good_root, "Bad ID"]
  - id: bad_source_job
    source: https://example.invalid/data
  - id: points_at_bad_source
    source: /api/summary
    depends_on: [bad_source_job]
  - id: lonely
    source: /api/catalog
  - id: points_at_ghost
    source: /api/catalog
    depends_on: [never_declared]
"""


def _fail(text: str) -> PlanValidationError:
    with pytest.raises(PlanValidationError) as caught:
        parse_plan_text(text)
    return caught.value


def _diagnostics(error: PlanValidationError) -> dict:
    payload = error.to_dict()
    assert payload["graph_diagnostics"]
    return payload["graph_diagnostics"]


def test_valid_plan_reports_diagnostics_ok(capsys):
    code = main(["plan-validate", str(ROOT / "plans" / "demo.yaml")])
    payload = json.loads(capsys.readouterr().out)
    assert code == 0
    assert payload["ok"] is True
    assert payload["diagnostics_ok"] is True
    # Existing success structure is unchanged.
    assert payload["job_count"] == 6
    assert payload["concurrency"] == 3
    assert payload["layers"]


def test_failure_payload_keeps_legacy_structure_and_adds_diagnostics(capsys):
    code = main(
        ["plan-validate", str(ROOT / "plans" / "demo_invalid_cycle.yaml")]
    )
    payload = json.loads(capsys.readouterr().out)
    assert code == 2
    assert payload["ok"] is False
    assert payload["error_count"] == 1
    assert payload["errors"][0]["code"] == "cycle"
    assert payload["errors"][0]["line"] == 5
    assert payload["graph_diagnostics"] is not None
    assert payload["diagnostics_ok"] is False


def test_cycle_summary_matches_find_cycle():
    jobs = {
        "alpha": JobSpec(id="alpha", source="/api/catalog", depends_on=("gamma",)),
        "beta": JobSpec(id="beta", source="/api/orders", depends_on=("alpha",)),
        "gamma": JobSpec(id="gamma", source="/api/summary", depends_on=("beta",)),
    }
    cycle = find_cycle(jobs)
    assert cycle == ["alpha", "gamma", "beta", "alpha"]
    summary = cycle_summary(jobs, cycle)
    assert summary["order"] == cycle
    assert summary["nodes"] == ["alpha", "gamma", "beta"]
    assert [edge["from"] for edge in summary["edges"]] == [
        "alpha",
        "gamma",
        "beta",
    ]
    assert [edge["to"] for edge in summary["edges"]] == [
        "gamma",
        "beta",
        "alpha",
    ]
    degrees = {item["id"]: item for item in summary["jobs"]}
    assert set(degrees) == {"alpha", "beta", "gamma"}
    assert all(item["indegree"] == 1 for item in degrees.values())
    assert all(item["outdegree"] == 1 for item in degrees.values())


def test_cycle_degrees_count_edges_outside_the_cycle():
    jobs = {
        # alpha -> gamma (cycle edge) and alpha -> root (external dependency)
        "alpha": JobSpec(
            id="alpha", source="/a", depends_on=("gamma", "root")
        ),
        "gamma": JobSpec(id="gamma", source="/c", depends_on=("beta",)),
        "beta": JobSpec(id="beta", source="/b", depends_on=("alpha",)),
        # Both depend on alpha from outside the cycle.
        "outsider_a": JobSpec(id="outsider_a", source="/d", depends_on=("alpha",)),
        "outsider_b": JobSpec(id="outsider_b", source="/e", depends_on=("alpha",)),
        "root": JobSpec(id="root", source="/f"),
    }
    cycle = find_cycle(jobs)
    assert cycle == ["alpha", "gamma", "beta", "alpha"]
    degrees = {
        item["id"]: item for item in cycle_summary(jobs, cycle)["jobs"]
    }
    # alpha has two dependencies (gamma, root); three jobs depend on it.
    assert degrees["alpha"]["indegree"] == 2
    assert degrees["alpha"]["outdegree"] == 3
    assert degrees["alpha"]["cycle_indegree"] == 1
    assert degrees["alpha"]["cycle_outdegree"] == 1
    assert degrees["gamma"]["indegree"] == 1
    assert degrees["gamma"]["outdegree"] == 1
    assert degrees["beta"]["indegree"] == 1
    assert degrees["beta"]["outdegree"] == 1


def test_invalid_cycle_plan_carries_cycle_summary():
    error = _fail((ROOT / "plans" / "demo_invalid_cycle.yaml").read_text())
    diagnostics = _diagnostics(error)
    assert diagnostics["has_cycle"] is True
    assert diagnostics["weakly_connected_component_count"] == 1
    assert diagnostics["weakly_connected_components"] == [
        ["fetch_alpha", "fetch_beta", "fetch_gamma"]
    ]
    assert diagnostics["isolated_jobs"] == []
    cycle = diagnostics["cycle"]
    assert cycle["order"] == [
        "fetch_alpha",
        "fetch_gamma",
        "fetch_beta",
        "fetch_alpha",
    ]
    assert [item["id"] for item in cycle["jobs"]] == [
        "fetch_alpha",
        "fetch_gamma",
        "fetch_beta",
    ]
    # Cycle summary is derived from the same detection result as the issue.
    cycle_issue = next(i for i in error.issues if i.code == "cycle")
    assert " -> ".join(cycle["order"]) in cycle_issue.message


def test_discarded_nodes_never_enter_effective_subgraph():
    error = _fail(MIXED_PLAN)
    diagnostics = _diagnostics(error)
    component_members = {
        job_id
        for component in diagnostics["weakly_connected_components"]
        for job_id in component
    }
    assert diagnostics["effective_job_count"] == 5
    assert component_members == {
        "good_root",
        "good_child",
        "points_at_bad_source",
        "lonely",
        "points_at_ghost",
    }
    # Discarded names are reported separately, never as graph nodes or islands.
    assert diagnostics["discarded_job_ids"] == ["Bad ID", "bad_source_job"]
    assert "Bad ID" not in component_members
    assert "bad_source_job" not in component_members
    assert "Bad ID" not in diagnostics["isolated_jobs"]
    assert "bad_source_job" not in diagnostics["isolated_jobs"]
    assert "Bad ID" not in diagnostics["edge_less_jobs"]
    assert "bad_source_job" not in diagnostics["edge_less_jobs"]


def test_dangling_dependency_distinct_from_discarded_and_islands():
    error = _fail(MIXED_PLAN)
    diagnostics = _diagnostics(error)
    dangling = {
        (item["job_id"], item["dependency"]): item["reason"]
        for item in diagnostics["dangling_dependencies"]
    }
    assert dangling[("good_child", "Bad ID")] == "dependency_discarded"
    assert dangling[("points_at_bad_source", "bad_source_job")] == (
        "dependency_discarded"
    )
    assert dangling[("points_at_ghost", "never_declared")] == "dependency_missing"
    # Suspicious islands: accepted jobs with zero in/out degree.  They are
    # reported with their effective-graph components, and never include any
    # discarded name.  Jobs that only point at absent names are additionally
    # classified as dangling_only so the two causes stay distinguishable.
    assert diagnostics["isolated_jobs"] == [
        "lonely",
        "points_at_bad_source",
        "points_at_ghost",
    ]
    assert diagnostics["dangling_only_jobs"] == [
        "points_at_bad_source",
        "points_at_ghost",
    ]
    assert diagnostics["edge_less_jobs"] == [
        "lonely",
        "points_at_bad_source",
        "points_at_ghost",
    ]
    assert "good_child" not in diagnostics["isolated_jobs"]


def test_component_count_uses_effective_edges():
    error = _fail(MIXED_PLAN)
    diagnostics = _diagnostics(error)
    assert diagnostics["weakly_connected_component_count"] == 4
    assert diagnostics["weakly_connected_components"] == [
        ["good_child", "good_root"],
        ["lonely"],
        ["points_at_bad_source"],
        ["points_at_ghost"],
    ]


def test_isolated_rule_ignores_discarded_nodes():
    jobs = {
        "root": JobSpec(id="root", source="/catalog"),
        "child": JobSpec(id="child", source="/orders", depends_on=("root",)),
        "island": JobSpec(id="island", source="/catalog"),
    }
    assert isolated_jobs(jobs) == ["island"]
    assert weakly_connected_components(jobs) == [["child", "root"], ["island"]]


def test_plan_validation_error_exposes_graph_diagnostics():
    error = _fail(
        """
jobs:
  - id: lone
    source: /api/catalog
  - id: broken
    source: "not-a-local-path"
"""
    )
    assert error.graph_diagnostics is not None
    assert error.to_dict()["graph_diagnostics"]["isolated_jobs"] == ["lone"]
    assert error.to_dict()["diagnostics_ok"] is False
    assert [issue.code for issue in error.issues] == ["invalid_source"]
