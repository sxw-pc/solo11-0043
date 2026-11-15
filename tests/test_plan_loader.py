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


def test_query_and_header_params_are_normalized():
    plan = parse_plan_text(
        """
name: params-plan
concurrency: 1
jobs:
  - id: timed_fetch
    source: /api/concurrency
    params:
      query:
        delay_ms: 25
      headers:
        accept: application/json
        x-request-id: run-abc-001
      min_bytes: 1
"""
    )
    job = plan.job("timed_fetch")
    assert job.params["query"] == {"delay_ms": 25}
    assert job.params["headers"] == {
        "Accept": "application/json",
        "X-Request-ID": "run-abc-001",
    }
    assert job.params["min_bytes"] == 1


def test_query_key_may_be_declared_directly_or_as_decimal_text():
    plan = parse_plan_text(
        """
jobs:
  - id: direct
    source: /api/concurrency
    params:
      delay_ms: 40
  - id: textual
    source: /api/concurrency
    params:
      query:
        delay_ms: "40"
"""
    )
    assert plan.job("direct").params["query"] == {"delay_ms": 40}
    assert plan.job("textual").params["query"] == {"delay_ms": "40"}


@pytest.mark.parametrize(
    "query_block",
    [
        "delay_ms: 10.5",
        "delay_ms: true",
        "delay_ms: [10]",
        "delay_ms: -1",
        "delay_ms: 501",
        "delay_ms: '12x'",
        "delay_ms: ''",
        "delay_ms: '1 0'",
        "delay_ms: " + ("1" * 65),
    ],
)
def test_query_values_must_be_typed_short_scalars(query_block):
    text = f"""
jobs:
  - id: fetch_one
    source: /api/concurrency
    params:
      query:
        {query_block}
"""
    with pytest.raises(PlanValidationError) as caught:
        parse_plan_text(text)
    issue = next(i for i in caught.value.issues if i.code == "invalid_params")
    assert "delay_ms" in issue.message
    assert issue.line is not None


def test_query_allow_list_rejects_unknown_keys_with_line_number():
    text = """
jobs:
  - id: fetch_one
    source: /api/concurrency
    params:
      query:
        offset: 5
"""
    with pytest.raises(PlanValidationError) as caught:
        parse_plan_text(text)
    issue = next(i for i in caught.value.issues if i.code == "invalid_params")
    assert "offset" in issue.message
    assert "not allowed" in issue.message
    assert issue.line == 7


def test_header_allow_list_is_small_and_user_agent_compatible():
    base = """
jobs:
  - id: fetch_one
    source: /api/catalog
    params:
      headers:
        {header}
"""
    with pytest.raises(PlanValidationError) as caught:
        parse_plan_text(base.format(header="authorization: Bearer token"))
    issue = next(i for i in caught.value.issues if i.code == "invalid_params")
    assert "authorization" in issue.message
    assert issue.line == 7

    for value in ("other-bot/1.0", "", "batchsched\\t0.1.0"):
        with pytest.raises(PlanValidationError) as caught:
            parse_plan_text(base.format(header=f'user-agent: "{value}"'))
        assert any(
            issue.code == "invalid_params" and "User-Agent" in issue.message
            for issue in caught.value.issues
        )

    plan = parse_plan_text(base.format(header="User-Agent: batchsched/0.1.0 (demo)"))
    assert plan.job("fetch_one").params["headers"] == {
        "User-Agent": "batchsched/0.1.0 (demo)"
    }


def test_header_names_case_insensitive_duplicate_and_unknown_params():
    duplicate = """
jobs:
  - id: fetch_one
    source: /api/catalog
    params:
      headers:
        accept: application/json
        Accept: text/plain
"""
    with pytest.raises(PlanValidationError) as caught:
        parse_plan_text(duplicate)
    assert any("declared twice" in issue.message for issue in caught.value.issues)

    unknown = """
jobs:
  - id: fetch_one
    source: /api/catalog
    params:
      mystery: 1
"""
    with pytest.raises(PlanValidationError) as caught:
        parse_plan_text(unknown)
    issue = next(i for i in caught.value.issues if i.code == "invalid_params")
    assert "mystery" in issue.message and issue.line == 6


def test_source_query_conflict_is_rejected_not_overwritten():
    conflict = """
jobs:
  - id: fetch_one
    source: /api/concurrency?delay_ms=100
    params:
      query:
        delay_ms: 20
"""
    with pytest.raises(PlanValidationError) as caught:
        parse_plan_text(conflict)
    issue = next(i for i in caught.value.issues if i.code == "invalid_params")
    assert "already present in source" in issue.message
    assert "delay_ms" in issue.message
    assert issue.line == 7

    direct_conflict = """
jobs:
  - id: fetch_one
    source: /api/concurrency?delay_ms=100
    params:
      delay_ms: 20
"""
    with pytest.raises(PlanValidationError) as caught:
        parse_plan_text(direct_conflict)
    assert any(
        issue.code == "invalid_params" and "already present" in issue.message
        for issue in caught.value.issues
    )


def test_source_with_unrelated_query_is_preserved():
    plan = parse_plan_text(
        """
jobs:
  - id: fetch_one
    source: /api/concurrency?mode=fast
    params:
      query:
        delay_ms: 30
"""
    )
    assert plan.job("fetch_one").source == "/api/concurrency?mode=fast"
    assert plan.job("fetch_one").params["query"] == {"delay_ms": 30}


def test_source_validity_still_applies_with_params():
    for bad_source in (
        "https://example.invalid/api/concurrency",
        "//api/concurrency",
        "/api/../concurrency",
        "/api/con currency",
    ):
        with pytest.raises(PlanValidationError) as caught:
            parse_plan_text(
                f"""
jobs:
  - id: fetch_one
    source: "{bad_source}"
    params:
      query:
        delay_ms: 10
"""
            )
        assert {issue.code for issue in caught.value.issues} == {"invalid_source"}


def test_params_container_types_are_validated():
    with pytest.raises(PlanValidationError) as caught:
        parse_plan_text(
            """
jobs:
  - id: fetch_one
    source: /api/catalog
    params:
      query: [1, 2]
      headers: [accept]
"""
        )
    messages = " ".join(issue.message for issue in caught.value.issues)
    assert "params.query must be a mapping" in messages
    assert "params.headers must be a mapping" in messages


def test_query_key_declared_directly_and_in_group_is_rejected():
    text = """
jobs:
  - id: fetch_one
    source: /api/concurrency
    params:
      delay_ms: 10
      query:
        delay_ms: 20
"""
    with pytest.raises(PlanValidationError) as caught:
        parse_plan_text(text)
    assert any(
        issue.code == "invalid_params" and "declared twice" in issue.message
        for issue in caught.value.issues
    )
