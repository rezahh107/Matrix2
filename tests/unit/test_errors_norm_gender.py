import pytest

from app.core.common.domain import Gender, norm_gender
from app.core.common.errors import InvalidGenderValueError


def test_norm_gender_strict_raises_for_unknown() -> None:
    with pytest.raises(InvalidGenderValueError) as exc:
        norm_gender("??", strict=True)

    assert exc.value.column == "جنسیت"


def test_norm_gender_non_strict_defaults_to_male() -> None:
    assert norm_gender("??", strict=False) == Gender.MALE
