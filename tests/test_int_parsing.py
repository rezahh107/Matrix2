import pytest

from app.core.build_matrix import parse_int_safe


@pytest.mark.parametrize(
    "value, expected",
    [
        ("123", 123),
        ("+123", 123),
        ("-123", -123),
        ("0012", 12),
        ("۱۲۳", 123),
        ("١٢٣", 123),
        ("1 234", 1234),
        ("1,234", 1234),
        ("−42", -42),
    ],
)
def test_parse_int_safe_valid_inputs(value: str, expected: int) -> None:
    assert parse_int_safe(value) == expected


@pytest.mark.parametrize(
    "value",
    ["-+123", "+-123", "1,2,3", "3.14", "1-1", "abc", "", None, "  "],
)
def test_parse_int_safe_invalid_inputs(value: object) -> None:
    sentinel = object()
    assert parse_int_safe(value, default=sentinel) is sentinel


@pytest.mark.parametrize(
    "value",
    ["9" * 50, str(10**30), str(-(10**40)), "+" + str(10**25)],
)
def test_parse_int_safe_large_round_trip(value: str) -> None:
    result = parse_int_safe(value)
    assert isinstance(result, int)
    assert result == int(value)


@pytest.mark.parametrize(
    "value",
    [
        "123",
        "+123",
        "-123",
        "1,2,3",
        "3.14",
        None,
        " ",
        "−42",
        "۱۲۳",
    ],
)
def test_parse_int_safe_never_raises_value_error(value: object) -> None:
    try:
        parse_int_safe(value)
    except ValueError:  # pragma: no cover
        pytest.fail("parse_int_safe raised ValueError")
