from __future__ import annotations

import json

import pytest

from batchsched.truncation import (
    JSON_FAMILY,
    TEXT_FAMILY,
    TruncationError,
    media_family,
    truncate_body,
)


def test_within_budget_passes_through_untouched():
    body = b'{"a": 1}'
    result = truncate_body(body, "application/json", 1000)
    assert result.truncated is False
    assert result.content == body
    assert result.original_size == len(body)
    assert result.family is None


def test_json_array_keeps_only_complete_records_and_stays_valid():
    body = json.dumps(
        {"records": [{"id": i, "label": "x" * 16} for i in range(20)]}
    ).encode("utf-8")
    result = truncate_body(body, "application/json; charset=utf-8", 220)
    assert result.truncated is True
    assert result.family == JSON_FAMILY
    assert result.original_size == len(body)
    assert len(result.content) <= 220
    parsed = json.loads(result.content)
    assert set(parsed) == {"records"}
    assert all(set(item) == {"id", "label"} for item in parsed["records"])
    assert len(parsed["records"]) < 20
    # prefix semantics: retained records are the first N
    assert [item["id"] for item in parsed["records"]] == list(
        range(len(parsed["records"]))
    )


def test_json_truncation_is_deterministic():
    body = json.dumps([{"n": i, "blob": "z" * 12} for i in range(30)]).encode()
    first = truncate_body(body, "application/json", 250)
    second = truncate_body(body, "application/json", 250)
    assert first.content == second.content
    assert json.loads(first.content) == json.loads(second.content)


def test_json_object_root_drops_trailing_whole_entries():
    body = json.dumps(
        {"k1": "a" * 20, "k2": "b" * 20, "k3": "c" * 20}
    ).encode()
    result = truncate_body(body, "application/json", 60)
    parsed = json.loads(result.content)
    assert list(parsed) == ["k1", "k2"]
    assert len(result.content) <= 60


def test_nested_array_keeps_wrapper_and_outer_keys():
    body = json.dumps({"meta": {"version": 1}, "rows": list(range(40))}).encode()
    result = truncate_body(body, "application/json", 40)
    parsed = json.loads(result.content)
    assert "meta" in parsed
    assert isinstance(parsed["rows"], list)
    assert len(result.content) <= 40


def test_tight_budget_keeps_empty_container_wrapper():
    body = json.dumps({"records": list(range(50))}).encode()
    result = truncate_body(body, "application/json", 14)
    parsed = json.loads(result.content)
    assert parsed == {"records": []}


def test_json_suffix_and_text_json_are_truncatable():
    body = json.dumps([{"id": i} for i in range(40)]).encode()
    for content_type in (
        "application/vnd.api+json; charset=utf-8",
        "text/json",
        "Application/JSON",
    ):
        result = truncate_body(body, content_type, 100)
        assert result.family == JSON_FAMILY
        json.loads(result.content)


def test_json_scalar_without_structural_prefix_fails_closed():
    body = b'"' + b"a" * 200 + b'"'
    with pytest.raises(TruncationError, match="structural prefix"):
        truncate_body(body, "application/json", 40)


def test_unparseable_json_body_fails_closed():
    with pytest.raises(TruncationError, match="cannot be parsed"):
        truncate_body(b'{"broken": [', "application/json", 10)


def test_text_body_uses_byte_prefix():
    body = ("abcdefghij " * 20).encode("ascii")
    result = truncate_body(body, "text/plain; charset=utf-8", 42)
    assert result.truncated is True
    assert result.family == TEXT_FAMILY
    assert result.content == body[:42]
    assert len(result.content) == 42


def test_text_prefix_never_splits_a_utf8_multibyte_sequence():
    body = "中" * 20  # each character is 3 bytes
    result = truncate_body(body.encode("utf-8"), "text/plain", 10)
    assert len(result.content) <= 10
    # 10 bytes cannot hold four 3-byte characters; trailing partial run dropped
    assert result.content == ("中" * 3).encode("utf-8")
    result.content.decode("utf-8")


def test_text_without_charset_defaults_to_utf8_trim():
    body = "中" * 10
    result = truncate_body(body.encode("utf-8"), "text/plain", 7)
    result.content.decode("utf-8")
    assert len(result.content) <= 7


def test_unsafe_media_type_raises():
    body = b"\x00\x01\x02" * 40
    for content_type in ("application/octet-stream", "image/png", "", "binary"):
        with pytest.raises(TruncationError):
            truncate_body(body, content_type, 50)


def test_media_family_classification():
    assert media_family("application/json") == JSON_FAMILY
    assert media_family("application/problem+json; charset=utf-8") == JSON_FAMILY
    assert media_family("text/csv; charset=latin-1") == TEXT_FAMILY
    assert media_family("text/html") == TEXT_FAMILY
    assert media_family("application/xml") is None
    assert media_family("") is None


def test_non_utf8_text_keeps_plain_byte_prefix():
    body = "åäö".encode("latin-1") * 10
    result = truncate_body(body, "text/plain; charset=latin-1", 7)
    assert result.content == body[:7]
    assert len(result.content) == 7


def test_non_positive_budget_is_rejected():
    with pytest.raises(TruncationError):
        truncate_body(b"abc", "text/plain", 0)
