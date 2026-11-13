"""Retry/backoff arithmetic, kept free of I/O so it is easy to test."""

from __future__ import annotations

from .models import RetryPolicy


def raw_backoff_ms(policy: RetryPolicy, failed_attempt_no: int) -> int:
    """Delay after the given 1-based failed attempt, before clamping."""

    if failed_attempt_no < 1:
        raise ValueError("failed_attempt_no must be >= 1")
    if policy.mode == "fixed":
        return policy.base_delay_ms
    if policy.mode == "exponential":
        return policy.base_delay_ms * (2 ** (failed_attempt_no - 1))
    raise ValueError(f"unknown retry mode: {policy.mode}")


def next_backoff_ms(
    policy: RetryPolicy,
    failed_attempt_no: int,
    total_backoff_ms: int,
) -> int | None:
    """Return the delay for the next attempt, or None to stop retrying.

    A policy with ``retryable`` set uses exactly that set.  Otherwise the
    built-in default set is used: transport/5xx/timeout/empty-body errors are
    retryable while 4xx, oversized responses and precondition errors are not.
    The cumulative delay can never exceed ``policy.max_total_delay_ms``.
    """

    if failed_attempt_no >= policy.max_attempts:
        return None
    remaining = policy.max_total_delay_ms - max(0, total_backoff_ms)
    if remaining <= 0:
        return None
    delay = raw_backoff_ms(policy, failed_attempt_no)
    return min(delay, remaining)


def should_retry(
    policy: RetryPolicy,
    error_class: str,
    failed_attempt_no: int,
    total_backoff_ms: int,
) -> bool:
    if not policy.can_retry(error_class):
        return False
    return next_backoff_ms(policy, failed_attempt_no, total_backoff_ms) is not None


def total_cap_reached(policy: RetryPolicy, total_backoff_ms: int) -> bool:
    return total_backoff_ms >= policy.max_total_delay_ms
