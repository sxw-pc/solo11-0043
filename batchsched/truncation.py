"""Deterministic, semantic-safe body truncation for record budgets.

A job-level ``max_record_bytes`` budget can be smaller than the fetcher's
global 2 MB cap.  A response that fits the global cap but exceeds the budget
is never sliced mid-token: every truncation routine here returns bytes that
parse as a complete, valid value of the response's media family.

Two media families are considered safe:

* JSON (``application/json``, ``+json`` variants, ``text/json``) keeps a
  structural prefix: complete container entries are dropped from the end
  until the compactly re-serialized value fits, so every retained record is
  whole and the output is always legal JSON.  Scalar JSON bodies cannot be
  shortened, so they fail closed.
* text (``text/*``) keeps a byte prefix, additionally trimming an
  incomplete trailing UTF-8 multibyte sequence so UTF-8 text remains
  decodable.  Other charsets fall back to the plain byte prefix.

Everything in this module is pure: the same inputs always produce the same
output, with no timestamps, randomness or I/O.
"""

from __future__ import annotations

from dataclasses import dataclass
import json

JSON_FAMILY = "json"
TEXT_FAMILY = "text"


@dataclass(frozen=True)
class TruncationResult:
    """Outcome of applying a budget to one response body."""

    content: bytes
    truncated: bool
    original_size: int
    family: str | None


class TruncationError(ValueError):
    """Raised when an over-budget body cannot be safely truncated."""


def media_family(content_type: str) -> str | None:
    """Classify a Content-Type header into a truncatable family.

    Matching is case-insensitive and ignores parameters such as
    ``charset=utf-8`` and JSON suffixes such as ``application/vnd.x+json``.
    """

    if not content_type:
        return None
    media_type = content_type.split(";", 1)[0].strip().lower()
    if not media_type:
        return None
    if media_type == "application/json" or media_type == "text/json":
        return JSON_FAMILY
    if media_type.endswith("+json"):
        return JSON_FAMILY
    if media_type.startswith("text/"):
        return TEXT_FAMILY
    return None


def truncate_body(content: bytes, content_type: str, budget: int) -> TruncationResult:
    """Return ``content`` (or a safe truncation of it) fitting ``budget``.

    Bodies at or below the budget pass through untouched, preserving the
    exact bytes that downstream hashing and replay expect.
    """

    if budget < 1:
        raise TruncationError("max_record_bytes must be a positive integer")
    original_size = len(content)
    if original_size <= budget:
        return TruncationResult(content, False, original_size, None)
    family = media_family(content_type)
    if family == JSON_FAMILY:
        trimmed = _truncate_json(content, budget)
    elif family == TEXT_FAMILY:
        trimmed = _truncate_text(content, content_type, budget)
    else:
        raise TruncationError(
            f"content type {content_type or '<missing>'} cannot be safely truncated"
        )
    return TruncationResult(trimmed, True, original_size, family)


def _truncate_json(content: bytes, budget: int) -> bytes:
    """Keep the largest complete structural prefix that fits the budget."""

    try:
        value = json.loads(content.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise TruncationError(
            f"JSON body cannot be parsed for truncation: {exc.__class__.__name__}"
        ) from exc
    retained = _retain(value, budget)
    encoded = _compact(retained)
    if len(encoded) > budget:
        raise TruncationError("JSON body has no structural prefix within budget")
    return encoded


def _retain(value: object, budget: int) -> object:
    """Return the largest complete structural prefix fitting ``budget``."""

    if isinstance(value, list):
        kept: list[object] = []
        for entry in value:
            trial = kept + [entry]
            if len(_compact(trial)) > budget:
                break
            kept = trial
        return kept
    if isinstance(value, dict):
        kept_map: dict[str, object] = {}
        for key, item in value.items():
            if isinstance(item, (list, dict)):
                # Remaining budget accounts for the retained prefix plus this key.
                overhead = len(_compact({**kept_map, key: None})) - len(b"null")
                candidate = _retain(item, max(0, budget - overhead))
                trial = {**kept_map, key: candidate}
            else:
                trial = {**kept_map, key: item}
            if len(_compact(trial)) > budget:
                break
            kept_map = trial
        return kept_map
    return value


def _truncate_text(content: bytes, content_type: str, budget: int) -> bytes:
    prefix = content[:budget]
    if _is_utf8_text(content_type):
        prefix = _trim_utf8_boundary(prefix)
    return prefix


def _is_utf8_text(content_type: str) -> bool:
    for parameter in content_type.split(";")[1:]:
        name, _, value = parameter.partition("=")
        if name.strip().lower() == "charset" and value.strip().lower().strip('"') in (
            "",
            "utf-8",
            "utf8",
        ):
            return True
    return "charset" not in content_type.lower()


def _trim_utf8_boundary(raw_prefix: bytes) -> bytes:
    """Drop trailing bytes of an unfinished UTF-8 multibyte sequence.

    Walks back over continuation bytes (0x80..0xBF) to the leading byte and
    checks whether the whole expected sequence is present.  Orphan
    continuation bytes (no leading byte) are also trimmed.
    """

    position = len(raw_prefix) - 1
    while position >= 0 and 0x80 <= raw_prefix[position] < 0xC0:
        position -= 1
    if position < 0:
        return b""
    if raw_prefix[position] < 0x80:
        return raw_prefix[: position + 1]
    expected = _utf8_sequence_length(raw_prefix[position])
    if expected and len(raw_prefix) - position < expected:
        return raw_prefix[:position]
    return raw_prefix


def _utf8_sequence_length(lead: int) -> int:
    for threshold, length in ((0xF0, 4), (0xE0, 3), (0xC0, 2)):
        if lead >= threshold:
            return length
    return 0


def _compact(value: object) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, separators=(",", ":"), sort_keys=False
    ).encode("utf-8")
