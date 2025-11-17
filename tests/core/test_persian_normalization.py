import pytest

from app.core.common.normalization import (
    normalize_ascii_digits,
    normalize_persian_label,
    normalize_persian_text,
)


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("كريم ياسر ۱۲۳", "کریم یاسر 123"),
        ("\u200fسجاد\u200c", "سجاد"),
        ("ى", "ی"),
        ("آزمون-1", "آزمون-1"),
    ],
)
def test_normalize_persian_text_unifies_variants(raw, expected):
    assert normalize_persian_text(raw) == expected


def test_normalize_persian_label_strips_and_normalizes():
    assert normalize_persian_label("  مدرسه‌ي نمونه  ") == "مدرسه ی نمونه"


def test_normalize_ascii_digits_removes_bidi_and_converts():
    assert normalize_ascii_digits("٠١٢۳۴۵\u200f۶۷۸۹") == "0123456789"
