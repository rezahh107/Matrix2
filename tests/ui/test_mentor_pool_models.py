import pandas as pd

from app.ui.models import MentorPoolEntry, build_mentor_entries_from_dataframe


def test_build_entries_resolves_manager_and_mentor_aliases() -> None:
    df = pd.DataFrame(
        {
            "نام مدیر": ["مدیر الف", "مدیر ب"],
            "نام پشتیبان": ["مریم", "ناصر"],
            "کد کارمندی پشتیبان": ["501", "777"],
            "مرکز گلستان صدرا": [11, 2],
            "نام مدرسه": ["مدرسه 1", "مدرسه 2"],
            "کد مدرسه": [301, 900],
            "ظرفیت": [4, 1],
        }
    )

    entries = build_mentor_entries_from_dataframe(df)

    assert [entry.mentor_id for entry in entries] == ["501", "777"]
    assert [entry.mentor_name for entry in entries] == ["مریم", "ناصر"]
    assert [entry.manager for entry in entries] == ["مدیر الف", "مدیر ب"]
    assert entries[0].center == 11
    assert entries[1].center == 2
    assert entries[0].school == "مدرسه 1"
    assert entries[1].school == "مدرسه 2"
    assert all(isinstance(entry.center, (int, str)) for entry in entries)


def test_build_entries_missing_manager_uses_placeholder() -> None:
    df = pd.DataFrame(
        {
            "نام پشتیبان": ["نگار"],
            "کد کارمندی پشتیبان": ["888"],
            "مرکز گلستان صدرا": [5],
            "کد مدرسه": [123],
            "ظرفیت": [2],
        }
    )

    entries = build_mentor_entries_from_dataframe(df)

    assert len(entries) == 1
    entry = entries[0]
    assert entry.manager == "(بدون مدیر)"
    assert entry.mentor_id == "888"
    assert entry.mentor_name == "نگار"
    assert entry.center == 5
    assert entry.school == 123
    assert isinstance(entry, MentorPoolEntry)
