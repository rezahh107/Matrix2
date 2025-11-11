import pytest

import pandas as pd

from app.core.common.columns import accepted_synonyms, resolve_aliases


@pytest.mark.parametrize(
    "source, canonical, required",
    [
        (
            "report",
            "مالی حکمت بنیاد",
            {"مالی حکمت بنیاد", "مالی_حکمت_بنیاد", "مالی حکمت بنیاد | finance", "finance"},
        ),
        (
            "report",
            "کد مدرسه",
            {"کد مدرسه", "کد_مدرسه", "کد مدرسه | school_code", "school_code", "school code"},
        ),
        (
            "report",
            "گروه آزمایشی",
            {
                "گروه آزمایشی",
                "گروه_آزمایشی",
                "گروه آزمایشی | exam_group",
                "exam_group",
                "exam group",
            },
        ),
    ],
)
def test_accepted_synonyms_includes_english_and_underscored(source, canonical, required):
    synonyms = set(accepted_synonyms(source, canonical))
    missing = required - synonyms
    assert not missing, f"expected synonyms missing: {missing}"


def test_resolve_aliases_handles_bilingual_headers():
    df = pd.DataFrame(
        [
            {
                "مالی حکمت بنیاد | finance": 1,
                "کد مدرسه | school_code": 2,
                "گروه آزمایشی | exam_group": "ریاضی",
            }
        ]
    )

    resolved = resolve_aliases(df, "report")

    assert "مالی حکمت بنیاد" in resolved.columns
    assert "کد مدرسه" in resolved.columns
    assert "گروه آزمایشی" in resolved.columns
