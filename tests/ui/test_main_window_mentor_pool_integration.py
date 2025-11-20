import pytest

try:
    from PySide6.QtWidgets import QApplication, QDialog
except ImportError as exc:  # pragma: no cover
    pytest.skip(f"PySide6 unavailable: {exc}", allow_module_level=True)

from app.ui.main_window import MainWindow
from app.ui.models import MentorPoolEntry


@pytest.fixture()
def qapp() -> QApplication:
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


class _FakeMentorPoolDialog(QDialog):
    def __init__(self, entries, parent=None):
        super().__init__(parent)
        self.entries = list(entries)
        self._overrides = {item.mentor_id: item.enabled for item in self.entries}

    def set_entries(self, entries):
        self.entries = list(entries)

    def get_overrides(self):
        return self._overrides


def test_build_allocate_overrides_contains_mentor_map(qapp: QApplication, monkeypatch: pytest.MonkeyPatch) -> None:
    window = MainWindow()
    window._mentor_pool_dialog_class = _FakeMentorPoolDialog
    window._mentor_pool_entries = [
        MentorPoolEntry(mentor_id="201", mentor_name="Gamma", enabled=True),
        MentorPoolEntry(mentor_id="202", mentor_name="Delta", enabled=False),
    ]

    window._show_mentor_pool_dialog(window._mentor_pool_entries)
    window._handle_mentor_pool_finished(QDialog.Accepted, window._mentor_pool_dialog)  # type: ignore[arg-type]

    overrides = window._build_allocate_overrides()

    assert "mentor_pool_overrides" in overrides
    assert overrides["mentor_pool_overrides"]["201"] is True


def test_reset_cache_clears_overrides(qapp: QApplication) -> None:
    window = MainWindow()
    window._mentor_pool_overrides = {"301": False}
    window._reset_mentor_pool_cache()

    assert window._mentor_pool_overrides == {}


def test_toolbar_has_mentor_pool_action(qapp: QApplication, monkeypatch: pytest.MonkeyPatch) -> None:
    window = MainWindow()
    triggered: list[bool] = []
    monkeypatch.setattr(window, "_open_mentor_pool_governance", lambda: triggered.append(True))

    action = window._toolbar_actions.get("mentor_pool")

    assert action is not None
    action.trigger()

    assert triggered, "mentor pool governance action should invoke dialog handler"


def test_matrix_governance_button_and_overrides(qapp: QApplication) -> None:
    window = MainWindow()
    assert window._btn_matrix_mentor_pool is not None
    window._matrix_mentor_pool_overrides = {"401": False}
    window._matrix_manager_overrides = {"Mgr": True}

    overrides = window._build_matrix_overrides()

    assert overrides["mentor_pool_overrides"] == {"401": False}
    assert overrides["mentor_pool_manager_overrides"] == {"Mgr": True}
