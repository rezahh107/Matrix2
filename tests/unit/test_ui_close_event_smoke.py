from __future__ import annotations

import threading
import time

import pytest

pytest.importorskip("PySide6")
try:  # noqa: SIM105 - وابستگی سیستمی ممکن است موجود نباشد
    from PySide6.QtWidgets import QApplication  # noqa: E402  # pylint: disable=wrong-import-position
except ImportError as exc:  # pragma: no cover - در CI headless محتمل است
    pytest.skip(f"PySide6 QtWidgets not available: {exc}", allow_module_level=True)

from app.ui.main_window import MainWindow  # noqa: E402  # pylint: disable=wrong-import-position


def test_close_event_cancels_running_worker() -> None:
    app = QApplication.instance() or QApplication([])
    window = MainWindow()

    def long_task(stop_event: threading.Event, *, progress) -> None:  # type: ignore[no-untyped-def]
        progress(0, "start")
        while not stop_event.is_set():
            time.sleep(0.01)
            progress(10, "waiting")
        progress(100, "done")

    blocker = threading.Event()
    worker = window.run_task(long_task, blocker)

    for _ in range(200):
        if worker.isRunning():
            break
        app.processEvents()
        time.sleep(0.01)

    window.close()
    blocker.set()
    app.processEvents()
    worker.wait(1000)

    assert not worker.isRunning()
    window.deleteLater()
