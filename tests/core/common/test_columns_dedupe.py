import pandas as pd

from app.core.common.columns import dedupe_columns


def test_dedupe_columns_preserves_first_occurrence_and_order():
    df = pd.DataFrame([["A", "A", 1, 2]], columns=["mentor_id", "mentor_id", "c1", "c2"])

    result = dedupe_columns(df)

    assert result.columns.tolist() == ["mentor_id", "c1", "c2"]
    assert result.iloc[0].tolist() == ["A", 1, 2]
    assert result.index.equals(df.index)


def test_dedupe_columns_returns_copy_when_no_duplicates():
    df = pd.DataFrame({"a": [1], "b": [2]})

    result = dedupe_columns(df)

    assert result.columns.tolist() == ["a", "b"]
    assert result.iloc[0].tolist() == [1, 2]
    assert result is not df


def test_dedupe_columns_handles_collapsed_bilingual_headers():
    data = [["M1", "M1", 1], ["M2", "M2", 2]]
    columns = ["mentor_id", "کد کارمندی پشتیبان", "remaining_capacity"]
    df = pd.DataFrame(data, columns=columns)

    result = dedupe_columns(df)

    assert result.columns.tolist() == ["mentor_id", "کد کارمندی پشتیبان", "remaining_capacity"]
    # after canonicalization these two mentor columns would collide; dedupe preserves first
    assert result.iloc[0, 0] == "M1"
