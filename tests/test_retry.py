from __future__ import annotations

import pytest

from batchsched.models import RetryPolicy
from batchsched.retry import (
    next_backoff_ms,
    raw_backoff_ms,
    should_retry,
    total_cap_reached,
)


def test_fixed_backoff_is_constant():
    policy = RetryPolicy(mode="fixed", base_delay_ms=120, max_attempts=4)
    assert raw_backoff_ms(policy, 1) == 120
    assert raw_backoff_ms(policy, 3) == 120


def test_exponential_backoff_doubles():
    policy = RetryPolicy(mode="exponential", base_delay_ms=50, max_attempts=5)
    assert [raw_backoff_ms(policy, n) for n in (1, 2, 3)] == [50, 100, 200]


def test_backoff_stops_at_attempt_limit():
    policy = RetryPolicy(mode="fixed", max_attempts=2, base_delay_ms=50)
    assert next_backoff_ms(policy, 1, 0) == 50
    assert next_backoff_ms(policy, 2, 0) is None


def test_cumulative_delay_cap_clamps_and_stops():
    policy = RetryPolicy(
        mode="fixed", max_attempts=5, base_delay_ms=1000, max_total_delay_ms=150
    )
    assert next_backoff_ms(policy, 1, 0) == 150
    assert next_backoff_ms(policy, 2, 150) is None
    assert total_cap_reached(policy, 150)


def test_default_retryable_set_and_override():
    default = RetryPolicy()
    assert default.can_retry("http_5xx")
    assert not default.can_retry("http_4xx")
    assert not default.can_retry("too_large")
    custom = RetryPolicy(retryable=frozenset({"http_4xx"}))
    assert custom.can_retry("http_4xx")
    assert not custom.can_retry("http_5xx")


def test_should_retry_respects_custom_override_and_caps():
    custom = RetryPolicy(
        retryable=frozenset({"http_4xx"}),
        max_attempts=3,
        base_delay_ms=50,
        max_total_delay_ms=50,
    )
    assert should_retry(custom, "http_4xx", 1, 0)
    assert not should_retry(custom, "http_4xx", 2, 50)
    assert not should_retry(custom, "timeout", 1, 0)


def test_invalid_attempt_number_and_mode():
    with pytest.raises(ValueError):
        raw_backoff_ms(RetryPolicy(), 0)
    with pytest.raises(ValueError):
        raw_backoff_ms(RetryPolicy(mode="mystery"), 1)
