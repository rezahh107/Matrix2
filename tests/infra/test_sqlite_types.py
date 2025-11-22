import pandas as pd

from app.infra.sqlite_types import coerce_int_columns, coerce_int_like, coerce_int_series


def test_coerce_int_like_handles_varied_inputs():
    assert coerce_int_like("12") == 12
    assert coerce_int_like("12.0") == 12
    assert coerce_int_like(12.0) == 12
    assert pd.isna(coerce_int_like(""))
    assert pd.isna(coerce_int_like(None))
    assert pd.isna(coerce_int_like("abc"))


def test_coerce_int_series_roundtrip_nullable_and_fill():
    series = pd.Series(["1", 2.0, None, "", " 3 ", "abc"])
    coerced = coerce_int_series(series)
    assert list(coerced[:2]) == [1, 2]
    assert pd.isna(coerced.iloc[2]) and pd.isna(coerced.iloc[3]) and pd.isna(coerced.iloc[5])
    assert coerced.iloc[4] == 3
    assert str(coerced.dtype) == "Int64"

    filled = coerce_int_series(series, fill_value=0)
    assert filled.tolist()[-3:] == [0, 3, 0]
    assert str(filled.dtype) == "Int64"


def test_coerce_int_columns_selective_and_fill_values():
    df = pd.DataFrame({"a": ["1", None], "b": ["x", "2"], "c": ["", "5"]})
    coerced = coerce_int_columns(df, ["a", "c"], fill_values={"c": 9})
    assert list(coerced["a"]) == [1, pd.NA]
    assert list(coerced["c"]) == [9, 5]
    assert str(coerced["a"].dtype) == "Int64"
    assert str(coerced["c"].dtype) == "Int64"
    assert coerced["b"].tolist() == ["x", "2"]
    assert df.equals(df)  # original remains unchanged
