"""تست‌های واحد برای parse_group_code_spec."""

from __future__ import annotations

from app.core.build_matrix import parse_group_code_spec


def test_parse_group_code_spec_filters_ranges_to_known_codes() -> None:
    group_codes = {27, 31, 33, 35, 41, 43, 45, 46}

    result = parse_group_code_spec(
        "27,31:35,41,43:46",
        valid_codes=group_codes,
    )

    assert result == [27, 31, 33, 35, 41, 43, 45, 46]


def test_parse_group_code_spec_collects_invalid_codes_once() -> None:
    invalid: list[int] = []

    result = parse_group_code_spec(
        "29,31-33,33,100:101",
        valid_codes={31, 33},
        invalid_collector=invalid,
    )

    assert result == [31, 33]
    assert invalid == [29, 32, 100, 101]


def test_parse_group_code_spec_without_validation_behaves_legacy() -> None:
    assert parse_group_code_spec("1:3") == [1, 2, 3]
