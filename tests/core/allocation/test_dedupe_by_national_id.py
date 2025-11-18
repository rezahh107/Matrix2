import pandas as pd

from app.core.allocation import dedupe_by_national_id


def test_student_with_history_is_allocated():
    students = pd.DataFrame({"name": ["old"], "national_code": ["0012345678"]})
    history = pd.DataFrame({"national_code": ["0012345678"]})

    allocated_df, new_df = dedupe_by_national_id(students, history)

    assert allocated_df["name"].tolist() == ["old"]
    assert new_df.empty


def test_student_without_history_is_new_candidate():
    students = pd.DataFrame({"name": ["newbie"], "national_code": ["1234567890"]})
    history = pd.DataFrame({"national_code": ["0012345678"]})

    allocated_df, new_df = dedupe_by_national_id(students, history)

    assert allocated_df.empty
    assert new_df["name"].tolist() == ["newbie"]
    assert new_df["dedupe_reason"].tolist() == ["no_history_match"]


def test_empty_history_puts_everyone_in_new_candidates():
    students = pd.DataFrame({"name": ["a", "b"], "national_code": ["1111111111", "2222222222"]})
    history = pd.DataFrame(columns=["national_code"])

    allocated_df, new_df = dedupe_by_national_id(students, history)

    assert allocated_df.empty
    assert set(new_df["name"]) == {"a", "b"}
    assert set(new_df["dedupe_reason"]) == {"no_history_match"}


def test_missing_or_invalid_national_code_treated_as_new():
    students = pd.DataFrame(
        {
            "name": ["missing", "nan", "empty", "text"],
            "national_code": [None, pd.NA, "", "ABC"],
        }
    )
    history = pd.DataFrame({"national_code": ["0012345678"]})

    allocated_df, new_df = dedupe_by_national_id(students, history)

    assert allocated_df.empty
    assert set(new_df["name"]) == {"missing", "nan", "empty", "text"}
    assert set(new_df["dedupe_reason"]) == {"missing_or_invalid_national_code"}


def test_idempotency_same_input_same_output():
    students = pd.DataFrame(
        {"name": ["old", "new"], "national_code": ["0012345678", "9999999999"]}
    )
    history = pd.DataFrame({"national_code": ["0012345678"]})

    first_allocated, first_new = dedupe_by_national_id(students, history)
    second_allocated, second_new = dedupe_by_national_id(students, history)

    assert first_allocated.equals(second_allocated)
    assert first_new.equals(second_new)
    assert set(first_allocated["national_code"]) == {"0012345678"}
    assert set(first_new["national_code"]) == {"9999999999"}
