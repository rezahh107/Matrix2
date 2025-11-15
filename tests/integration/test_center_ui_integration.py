import os
from unittest.mock import Mock, patch

import pandas as pd
import pytest

pytest.importorskip("PySide6.QtWidgets")
from PySide6.QtWidgets import QApplication, QMessageBox

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from app.ui.main_window import MainWindow  # noqa: E402  pylint: disable=C0413


@pytest.fixture(scope="module")
def qt_app():
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    yield app


def test_center_section_creation(qt_app):  # noqa: ARG001
    with patch("app.ui.main_window.load_policy") as mock_load_policy:
        center_management = Mock()
        center_management.enabled = True
        center_management.centers = [
            Mock(id=1, name="مرکز A", default_manager="مدیر A"),
            Mock(id=2, name="مرکز B", default_manager="مدیر B"),
            Mock(id=0, name="مرکزی", default_manager=None),
        ]
        mock_policy = Mock()
        mock_policy.center_management = center_management
        mock_load_policy.return_value = mock_policy

        window = MainWindow()
        section = window._create_center_management_section()
        assert section is not None
        assert window._center_manager_combos
        window.deleteLater()


def test_manager_list_loading_error_handling(qt_app, monkeypatch):  # noqa: ARG001
    window = MainWindow()
    monkeypatch.setattr(window, "_get_default_managers", lambda: ["پیش‌فرض"])
    window._picker_pool.setText("/path/to/missing.xlsx")

    with patch.object(QMessageBox, "warning", return_value=None):
        managers = window._load_manager_names_from_pool()

    assert managers == ["پیش‌فرض"]
    window.deleteLater()


def test_manager_list_loading_with_farsi_column(qt_app, tmp_path):  # noqa: ARG001
    window = MainWindow()
    pool_path = tmp_path / "pool.xlsx"
    pd.DataFrame({"مدیر ": ["  علی کیانی  ", None]}).to_excel(pool_path, index=False)
    window._picker_pool.setText(str(pool_path))

    managers = window._load_manager_names_from_pool()

    assert managers == ["علی کیانی"]
    window.deleteLater()
