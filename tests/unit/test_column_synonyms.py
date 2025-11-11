import pytest

from app.core.common.columns import accepted_synonyms


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
    ],
)
def test_accepted_synonyms_includes_english_and_underscored(source, canonical, required):
    synonyms = set(accepted_synonyms(source, canonical))
    missing = required - synonyms
    assert not missing, f"expected synonyms missing: {missing}"
