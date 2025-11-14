import pytest

from app.core.counter import (
    normalize_digits,
    stable_counter_hash,
    strip_hidden_chars,
    validate_counter,
)


def test_validate_counter_normalizes_digits() -> None:
    value = "۱۲۳۴۵۶۷۸۹"
    assert validate_counter(value) == "123456789"


def test_validate_counter_rejects_invalid_length() -> None:
    with pytest.raises(ValueError):
        validate_counter("12345")


def test_validate_counter_strips_hidden_chars_and_spaces() -> None:
    raw = "\u200c ۵۴۳۵۷۰ ۰۰۱\u200d"
    assert validate_counter(raw) == "543570001"


def test_validate_counter_rejects_non_digit_payload() -> None:
    with pytest.raises(ValueError):
        validate_counter("12345ABCD")


def test_strip_hidden_chars_removes_zero_width() -> None:
    assert strip_hidden_chars("1\u200c2\u200d3") == "123"


def test_normalize_digits_supports_arabic() -> None:
    assert normalize_digits("١٢٣") == "123"


def test_stable_counter_hash_is_deterministic() -> None:
    base = "۵۴۳۵۷۰۰۰۱"
    normalized = strip_hidden_chars(normalize_digits(base))
    assert stable_counter_hash(base) == stable_counter_hash(normalized)
