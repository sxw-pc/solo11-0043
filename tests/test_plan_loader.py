from __future__ import annotations

from pathlib import Path

import pytest

from batchsched.models import PlanValidationError
from batchsched.plan_loader import load_plan, parse_plan_text

ROOT = Path(__file__).resolve().parents[1]


def test_valid_plan_and_defaults():
    plan = parse_plan_text(
        """
name: sample
concurrency: 3
jobs:
  - id: fetch_one
    source: /api/catalog
  - id: fetch_two
    source: /api/orders
    depends_on: [fetch_one]
    params: {min_bytes: 2}
"""
    )
    assert plan.name == "sample"
    assert plan.concurrency == 3
    assert plan.job("fetch_two").depends_on == ("fetch_one",)
    assert plan.job("fetch_one").retry.mode == "fixed"
    assert plan.job("fetch_one").retry.max_attempts == 3
    assert plan.job("fetch_two").params == {"min_bytes": 2}
    assert plan.job("fetch_one").priority == 0


def test_priority_is_optional_and_validated():
    plan = parse_plan_text(
        """
jobs:
  - id: fetch_high
    source: /api/catalog
    priority: 7
  - id: fetch_low
    source: /api/orders
    priority: -3
  - id: fetch_default
    source: /api/summary
"""
    )
    assert plan.job("fetch_high").priority == 7
    assert plan.job("fetch_low").priority == -3
    assert plan.job("fetch_default").priority == 0
    priorities = {
        item["id"]: item["priority"] for item in plan.to_dict()["jobs"]
    }
    assert priorities == {"fetch_high": 7, "fetch_low": -3, "fetch_default": 0}

    bad = """
jobs:
  - id: fetch_one
    source: /api/catalog
    priority: 101
  - id: fetch_two
    source: /api/catalog
    priority: extreme
  - id: fetch_three
    source: /api/catalog
    priority: true
"""
    with pytest.raises(PlanValidationError) as caught:
        parse_plan_text(bad)
    issues = [issue for issue in caught.value.issues if issue.code == "invalid_priority"]
    assert len(issues) == 3
    assert [issue.line for issue in issues] == [5, 8, 11]


def test_demo_plan_loads():
    plan = load_plan(ROOT / "plans" / "demo.yaml")
    assert len(plan.jobs) == 6
    assert plan.job("summarize").depends_on == (
        "enrich_orders",
        "fetch_flaky",
        "fetch_slow",
    )


def test_invalid_cycle_gives_line_number():
    with pytest.raises(PlanValidationError) as caught:
        load_plan(ROOT / "plans" / "demo_invalid_cycle.yaml")
    cycle_issues = [issue for issue in caught.value.issues if issue.code == "cycle"]
    assert cycle_issues
    assert cycle_issues[0].line == 5
    assert "fetch_alpha" in cycle_issues[0].message


def test_duplicate_job_id():
    text = """
jobs:
  - id: fetch_one
    source: /api/catalog
  - id: fetch_one
    source: /api/orders
"""
    with pytest.raises(PlanValidationError) as caught:
        parse_plan_text(text)
    assert any(issue.code == "duplicate_id" for issue in caught.value.issues)


def test_dangling_dependency():
    text = """
jobs:
  - id: fetch_one
    source: /api/catalog
    depends_on: [missing_job]
"""
    with pytest.raises(PlanValidationError) as caught:
        parse_plan_text(text)
    issue = next(i for i in caught.value.issues if i.code == "dangling_dependency")
    assert "missing_job" in issue.message
    assert issue.line == 3


def test_self_and_duplicate_dependencies():
    self_text = """
jobs:
  - id: fetch_one
    source: /api/catalog
    depends_on: [fetch_one]
"""
    with pytest.raises(PlanValidationError) as caught:
        parse_plan_text(self_text)
    assert {issue.code for issue in caught.value.issues} == {"self_dependency"}

    duplicate_text = """
jobs:
  - id: fetch_one
    source: /api/catalog
  - id: fetch_two
    source: /api/orders
    depends_on: [fetch_one, fetch_one]
"""
    with pytest.raises(PlanValidationError) as caught:
        parse_plan_text(duplicate_text)
    codes = {issue.code for issue in caught.value.issues}
    assert codes == {"duplicate_dependency"}


def test_unknown_fields_are_rejected():
    text = """
mystery_top: true
jobs:
  - id: fetch_one
    source: /api/catalog
    mystery_job: 1
    retry:
      mode: fixed
      max_attempts: 1
      base_delay_ms: 50
      mystery_retry: nope
"""
    with pytest.raises(PlanValidationError) as caught:
        parse_plan_text(text)
    messages = " ".join(issue.message for issue in caught.value.issues)
    assert "mystery_top" in messages
    assert "mystery_job" in messages
    assert "mystery_retry" in messages


def test_invalid_id_and_source():
    bad_id = """
jobs:
  - id: "Bad ID"
    source: /api/catalog
"""
    with pytest.raises(PlanValidationError) as caught:
        parse_plan_text(bad_id)
    assert {issue.code for issue in caught.value.issues} == {"invalid_id"}

    bad_source = """
jobs:
  - id: fetch_one
    source: https://example.invalid/data
"""
    with pytest.raises(PlanValidationError) as caught:
        parse_plan_text(bad_source)
    assert {issue.code for issue in caught.value.issues} == {"invalid_source"}


def test_invalid_retry_values():
    text = """
jobs:
  - id: fetch_one
    source: /api/catalog
    retry:
      mode: sometimes
      max_attempts: 99
      base_delay_ms: 10001
      retryable: [http_5xx, alien_error]
      max_total_delay_ms: 999999
"""
    with pytest.raises(PlanValidationError) as caught:
        parse_plan_text(text)
    codes = {issue.code for issue in caught.value.issues}
    assert "invalid_retry_mode" in codes
    assert "invalid_max_attempts" in codes
    assert "invalid_base_delay" in codes
    assert "invalid_retryable" in codes
    assert "invalid_retry_cap" in codes


def test_concurrency_and_name_validation():
    text = """
name: ""
concurrency: 99
jobs:
  - id: fetch_one
    source: /api/catalog
"""
    with pytest.raises(PlanValidationError) as caught:
        parse_plan_text(text)
    codes = {issue.code for issue in caught.value.issues}
    assert "invalid_name" in codes
    assert "invalid_concurrency" in codes


def test_empty_jobs_and_root_type():
    with pytest.raises(PlanValidationError) as caught:
        parse_plan_text("name: x\njobs: []\n")
    assert caught.value.issues[0].code == "invalid_jobs"
    with pytest.raises(PlanValidationError) as caught:
        parse_plan_text("- not\n- a\n- mapping\n")
    assert caught.value.issues[0].code == "root_type"


def test_yaml_syntax_and_missing_file(tmp_path):
    with pytest.raises(PlanValidationError) as caught:
        parse_plan_text("jobs: [\n")
    assert caught.value.issues[0].code == "yaml_syntax"
    with pytest.raises(PlanValidationError) as caught:
        load_plan(tmp_path / "not-there.yaml")
    assert caught.value.issues[0].code == "file_error"


def test_job_count_limit():
    jobs = "".join(
        f"  - id: job_{index:03d}\n    source: /api/catalog\n" for index in range(201)
    )
    text = f"jobs:\n{jobs}"
    with pytest.raises(PlanValidationError) as caught:
        parse_plan_text(text)
    assert any(issue.code == "too_many_jobs" for issue in caught.value.issues)


def test_duplicate_yaml_key():
    text = """
jobs:
  - id: fetch_one
    source: /api/catalog
    source: /api/orders
"""
    with pytest.raises(PlanValidationError) as caught:
        parse_plan_text(text)
    assert any(issue.code == "duplicate_key" for issue in caught.value.issues)
