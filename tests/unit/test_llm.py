"""Unit tests for llm.py helpers."""

from unittest.mock import patch

import pytest

from llm import retry_llm, strip_json_fences


def test_strip_json_fences_plain():
    assert strip_json_fences('{"a": 1}') == '{"a": 1}'


def test_strip_json_fences_markdown():
    raw = "```json\n{\"a\": 1}\n```"
    assert strip_json_fences(raw) == '{"a": 1}'


def test_retry_llm_success():
    calls = {"n": 0}

    def fn():
        calls["n"] += 1
        return "ok"

    assert retry_llm(fn) == "ok"
    assert calls["n"] == 1


def test_retry_llm_retries_on_429():
    calls = {"n": 0}

    def fn():
        calls["n"] += 1
        if calls["n"] < 2:
            raise RuntimeError("429 rate limit")
        return "ok"

    with patch("llm.time.sleep"):
        assert retry_llm(fn, max_retries=3) == "ok"
    assert calls["n"] == 2


def test_retry_llm_non_retryable_raises():
    def fn():
        raise ValueError("bad input")

    with pytest.raises(ValueError, match="bad input"):
        retry_llm(fn, max_retries=3)
