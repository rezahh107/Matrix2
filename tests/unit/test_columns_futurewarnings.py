import warnings

import pandas as pd

from app.core.common.columns import enforce_join_key_types


def test_enforce_join_key_types_handles_bool_object_without_futurewarning() -> None:
    df = pd.DataFrame({"کد مدرسه": [True, False, "3"]})

    with warnings.catch_warnings():
        warnings.simplefilter("error", FutureWarning)
        normalized = enforce_join_key_types(df, ["کد مدرسه"])

    assert list(normalized["کد مدرسه"].astype("Int64")) == [1, 0, 3]
