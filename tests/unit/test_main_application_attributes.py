import importlib
import sys
import types

import pytest


@pytest.fixture(autouse=True)
def stub_pyside6_modules(monkeypatch):
    """جایگزینی ماژول‌های PySide6 برای جلوگیری از وابستگی به libGL در تست واحد."""

    qtcore = types.ModuleType("PySide6.QtCore")
    qtwidgets = types.ModuleType("PySide6.QtWidgets")

    class _ApplicationAttribute:
        AA_EnableHighDpiScaling = "AA_EnableHighDpiScaling"
        AA_UseHighDpiPixmaps = "AA_UseHighDpiPixmaps"

    class _Qt:
        ApplicationAttribute = _ApplicationAttribute

    version_box = {"value": "6.8.0"}

    def _qversion():
        return version_box["value"]

    class _QApplication:
        def __init__(self):
            self.set_calls: list[tuple[object, object]] = []

        def setAttribute(self, attr, value):
            self.set_calls.append((attr, value))

    class _QMessageBox:
        Icon = types.SimpleNamespace(Critical=1)

        def setIcon(self, *_args, **_kwargs):
            return None

        def setWindowTitle(self, *_args, **_kwargs):
            return None

        def setText(self, *_args, **_kwargs):
            return None

        def setInformativeText(self, *_args, **_kwargs):
            return None

        def setDetailedText(self, *_args, **_kwargs):
            return None

        def exec_(self):  # pragma: no cover - safety stub
            return None

    class _QSharedMemory:  # pragma: no cover - stub
        def __init__(self, *_args, **_kwargs):
            pass

    class _QTimer:  # pragma: no cover - stub
        def __init__(self, *_args, **_kwargs):
            pass

    qtcore.Qt = _Qt
    qtcore.qVersion = _qversion
    qtcore.QSharedMemory = _QSharedMemory
    qtcore.QTimer = _QTimer
    qtwidgets.QApplication = _QApplication
    qtwidgets.QMessageBox = _QMessageBox

    pyside6 = types.ModuleType("PySide6")
    sys.modules["PySide6"] = pyside6
    sys.modules["PySide6.QtCore"] = qtcore
    sys.modules["PySide6.QtWidgets"] = qtwidgets

    # refresh app.main imports to use stubs
    if "app.main" in sys.modules:
        importlib.reload(sys.modules["app.main"])
    yield version_box
    for module in ("PySide6", "PySide6.QtCore", "PySide6.QtWidgets"):
        sys.modules.pop(module, None)
    importlib.invalidate_caches()


def _load_target():
    import app.main as target

    importlib.reload(target)
    return target


@pytest.mark.parametrize(
    "attr,version,expected",
    [
        ("AA_EnableHighDpiScaling", "5.15.2", False),
        ("AA_EnableHighDpiScaling", "6.0.0", True),
        ("AA_EnableHighDpiScaling", "6.0", True),
        ("AA_UseHighDpiPixmaps", "6.7.0", False),
        ("AA_UseHighDpiPixmaps", "6.8.0", True),
        ("AA_UseHighDpiPixmaps", "7.0.0", True),
    ],
)
def test_is_deprecated_application_attribute(attr, version, expected):
    """اطمینان از بازگشت صحیح وضعیت منسوخ بودن بر اساس نسخه و نام ویژگی."""

    target = _load_target()
    value = getattr(target.Qt.ApplicationAttribute, attr)
    assert target._is_deprecated_application_attribute(value, version) is expected


def test_is_deprecated_application_attribute_unknown_attr():
    target = _load_target()
    assert target._is_deprecated_application_attribute(object(), "7.0.0") is False


def test_apply_application_attributes_respects_deprecation(stub_pyside6_modules):
    target = _load_target()
    version_box = stub_pyside6_modules
    version_box["value"] = "7.1.0"  # آینده
    app = target.QApplication()

    target._apply_application_attributes(app)

    assert app.set_calls == []


def test_apply_application_attributes_sets_allowed(stub_pyside6_modules):
    target = _load_target()
    version_box = stub_pyside6_modules
    version_box["value"] = "5.15.2"  # قبل از منسوخی
    app = target.QApplication()

    target._apply_application_attributes(app)

    expected_attrs = {
        target.Qt.ApplicationAttribute.AA_EnableHighDpiScaling,
        target.Qt.ApplicationAttribute.AA_UseHighDpiPixmaps,
    }
    assert {call[0] for call in app.set_calls} == expected_attrs
    assert all(call[1] is True for call in app.set_calls)
