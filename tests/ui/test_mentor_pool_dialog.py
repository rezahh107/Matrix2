import pytest

try:
    from PySide6.QtCore import Qt
    from PySide6.QtWidgets import QApplication
except ImportError as exc:  # pragma: no cover
    pytest.skip(f"PySide6 unavailable: {exc}", allow_module_level=True)

from app.ui.mentor_pool_dialog import MentorPoolDialog
from app.ui.models import MentorPoolEntry


@pytest.fixture()
def qapp() -> QApplication:
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


def _sample_entries() -> list[MentorPoolEntry]:
    return [
        MentorPoolEntry(mentor_id="101", mentor_name="Alpha", manager="M1", center="1", capacity=2),
        MentorPoolEntry(mentor_id="102", mentor_name="Beta", manager="M2", center="2", capacity=3),
    ]


def test_dialog_toggles_overrides(qapp: QApplication) -> None:
    dialog = MentorPoolDialog(_sample_entries())
    model = dialog.model

    assert model.rowCount() == 2
    assert model.columnCount() >= 5

    index = model.index(1, 0)
    model.setData(index, Qt.Unchecked, Qt.CheckStateRole)

    overrides = dialog.get_overrides()
    assert overrides["101"] is True
    assert overrides["102"] is False


def test_dialog_reset_enables_all(qapp: QApplication) -> None:
    dialog = MentorPoolDialog(_sample_entries())
    model = dialog.model
    model.setData(model.index(0, 0), Qt.Unchecked, Qt.CheckStateRole)

    dialog._reset_all()

    overrides = dialog.get_overrides()
    assert all(overrides.values())
