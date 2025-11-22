from argparse import Namespace
from argparse import Namespace
from pathlib import Path

from argparse import Namespace
from pathlib import Path

import pandas as pd

from app.core.policy_loader import load_policy
from app.infra import cli
from app.infra.local_database import LocalDatabase
from app.infra.reference_mentors_repository import import_mentor_pool_from_excel
from app.infra.reference_students_repository import import_student_report_from_excel


def _write_excel(df: pd.DataFrame, path: Path) -> None:
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        df.to_excel(writer, index=False)


def _sample_students() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "student_id": ["S1"],
            "کدرشته": [1201],
            "گروه آزمایشی": ["تجربی"],
            "جنسیت": [1],
            "دانش آموز فارغ": [0],
            "مرکز گلستان صدرا": [1],
            "مالی حکمت بنیاد": [0],
            "کد مدرسه": [3581],
            "کد ملی": ["001"],
        }
    )


def _sample_pool() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "پشتیبان": ["الف"],
            "کد کارمندی پشتیبان": ["M1"],
            "کدرشته": [1201],
            "گروه آزمایشی": ["تجربی"],
            "جنسیت": [1],
            "دانش آموز فارغ": [0],
            "مرکز گلستان صدرا": [1],
            "مالی حکمت بنیاد": [0],
            "کد مدرسه": [3581],
            "remaining_capacity": [2],
        }
    )


def test_allocate_uses_cached_students_and_pool(tmp_path: Path, monkeypatch) -> None:
    policy = load_policy()
    db = LocalDatabase(tmp_path / "cache.sqlite")

    students_path = tmp_path / "students.xlsx"
    pool_path = tmp_path / "pool.xlsx"
    _write_excel(_sample_students(), students_path)
    _write_excel(_sample_pool(), pool_path)

    import_student_report_from_excel(students_path, db=db, policy=policy)
    import_mentor_pool_from_excel(pool_path, db=db, policy=policy)

    captured: dict[str, pd.DataFrame] = {}

    def fake_prepare(students_df, pool_df, **_kwargs):  # type: ignore[no-untyped-def]
        captured["students"] = students_df.copy()
        captured["pool"] = pool_df.copy()
        return students_df, pool_df

    monkeypatch.setattr(cli, "_prepare_allocation_frames", fake_prepare)
    monkeypatch.setattr(cli, "_allocate_and_write", lambda *_, **__: 0)
    monkeypatch.setattr(cli, "_apply_mentor_pool_overrides", lambda pool, *_: pool)

    args = Namespace(
        students=None,
        pool=None,
        output=str(tmp_path / "alloc.xlsx"),
        policy=str(tmp_path / "policy.json"),
        capacity_column=None,
        mentor_overrides=None,
        manager_overrides=None,
        _ui_overrides={},
        local_db_path=str(db.path),
        disable_local_db=False,
        academic_year=None,
        prior_roster=None,
        current_roster=None,
        export_profile="sabt",
        export_profile_path=None,
        sabt_output=None,
        sabt_config=None,
        sabt_template=None,
        audit=False,
        metrics=False,
        determinism_check=False,
        counter_duplicate_strategy="prompt",
    )

    result = cli._run_allocate(args, policy, lambda *_: None)

    assert result == 0
    assert "students" in captured and not captured["students"].empty
    assert "pool" in captured and not captured["pool"].empty


def test_build_matrix_errors_when_cache_missing(tmp_path: Path) -> None:
    policy = load_policy()
    db = LocalDatabase(tmp_path / "cache.sqlite")
    args = Namespace(
        inspactor=None,
        output=str(tmp_path / "out.xlsx"),
        schools=None,
        crosswalk=None,
        min_coverage=None,
        policy_version=None,
        manager_overrides=None,
        mentor_overrides=None,
        _ui_overrides={},
        local_db_path=str(db.path),
        disable_local_db=False,
        policy=str(tmp_path / "policy.json"),
    )

    try:
        cli._run_build_matrix(args, policy, lambda *_: None)
    except Exception as exc:  # noqa: BLE001
        assert "import-mentors" in str(exc)
    else:  # pragma: no cover
        assert False, "expected failure due to missing cache"
