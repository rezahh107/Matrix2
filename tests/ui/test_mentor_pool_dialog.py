import pandas as pd
import pytest

pytest.importorskip("PySide6")
pytest.importorskip("PySide6.QtCore", exc_type=ImportError)
pytest.importorskip("PySide6.QtWidgets", exc_type=ImportError)
from PySide6.QtCore import Qt
from PySide6.QtWidgets import QApplication

from app.ui.mentor_pool_dialog import MentorPoolDialog
from app.ui.models import MentorPoolEntry, build_mentor_entries_from_dataframe


@pytest.fixture()
def qapp() -> QApplication:
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


def _sample_entries() -> list[MentorPoolEntry]:
    return [
        MentorPoolEntry(
            mentor_id="101", mentor_name="Alpha", manager="M1", center="1", capacity=2
        ),
        MentorPoolEntry(
            mentor_id="102", mentor_name="Beta", manager="M1", center="1", capacity=3
        ),
        MentorPoolEntry(
            mentor_id="201", mentor_name="Gamma", manager="M2", center="2", capacity=1
        ),
    ]


def test_dialog_cascade_and_overrides(qapp: QApplication) -> None:
    dialog = MentorPoolDialog(_sample_entries())
    model = dialog.model

    # manager M1 should control two mentors
    manager_item = model.item(0, 0)
    assert manager_item.text() == "M1"
    manager_item.setCheckState(Qt.Unchecked)
    overrides = dialog.get_overrides()
    assert overrides["101"] is False
    assert overrides["102"] is False

    manager_item.setCheckState(Qt.Checked)
    overrides = dialog.get_overrides()
    assert overrides["101"] is True
    assert overrides["102"] is True


def test_dialog_search_and_manager_overrides(qapp: QApplication) -> None:
    dialog = MentorPoolDialog(_sample_entries())
    dialog._search.setText("Gamma")
    # Even after filtering, overrides should reflect all mentors
    overrides = dialog.get_overrides()
    assert set(overrides.keys()) == {"101", "102", "201"}

    manager_overrides = dialog.get_manager_overrides()
    assert manager_overrides["M1"] is True
    assert manager_overrides["M2"] is True

    dialog._model.set_all(False)
    manager_overrides = dialog.get_manager_overrides()
    assert all(value is False for value in manager_overrides.values())


def test_build_entries_resolves_manager_aliases() -> None:
    df = pd.DataFrame(
        {
            "مدیر": ["مدیر الف"],
            "پشتیبان": ["مریم"],
            "کد کارمندی پشتیبان": ["501"],
            "مرکز": ["مرکز بهمن"],
            "کد مدرسه": ["301"],
            "ظرفیت": [4],
        }
    )

    entries = build_mentor_entries_from_dataframe(df)

    assert len(entries) == 1
    entry = entries[0]
    assert entry.mentor_id == "501"
    assert entry.mentor_name == "مریم"
    assert entry.manager == "مدیر الف"
    assert entry.center == "مرکز بهمن"
    assert entry.school == "301"


def test_build_entries_missing_manager_uses_placeholder() -> None:
    df = pd.DataFrame(
        {
            "پشتیبان": ["ناصر"],
            "کد کارمندی پشتیبان": ["777"],
            "مرکز": ["۲"],
            "کد مدرسه": ["900"],
            "ظرفیت": [1],
        }
    )

    entries = build_mentor_entries_from_dataframe(df)

    assert len(entries) == 1
    entry = entries[0]
    assert entry.manager == "(بدون مدیر)"
    assert entry.mentor_id == "777"
    assert entry.mentor_name == "ناصر"
    assert entry.center == "۲"
    assert entry.school == "900"
