import importlib
import sys
import types

import pytest


class _StubSharedMemory:
    def __init__(self, *_, **__):
        self._attached = False

    def attach(self) -> bool:
        return False

    def create(self, _size: int) -> bool:
        self._attached = True
        return True

    def error(self):
        return None

    def isAttached(self) -> bool:  # noqa: N802 - Qt naming
        return self._attached

    def detach(self) -> bool:
        self._attached = False
        return True


class _StubTimer:
    @staticmethod
    def singleShot(_delay: int, _callable):
        return None


class _StubMessageBox:
    class Icon:
        Critical = 1
        Warning = 2

    class StandardButton:
        Ok = 1

    def __init__(self, *_, **__):
        self._icon = None
        self._text = ""
        self._info = ""
        self._detailed = ""

    def setIcon(self, icon):  # noqa: N802 - Qt naming
        self._icon = icon

    def setWindowTitle(self, _title):  # noqa: N802 - Qt naming
        return None

    def setText(self, text):  # noqa: N802 - Qt naming
        self._text = text

    def setInformativeText(self, info):  # noqa: N802 - Qt naming
        self._info = info

    def setDetailedText(self, detailed):  # noqa: N802 - Qt naming
        self._detailed = detailed

    def setStandardButtons(self, _buttons):  # noqa: N802 - Qt naming
        return None

    def exec(self):
        return None


class _StubApplication:
    def __init__(self, *_, **__):
        self._attributes = []

    @staticmethod
    def instance():  # noqa: D401
        return None

    def setAttribute(self, attr, value):  # noqa: N802 - Qt naming
        self._attributes.append((attr, value))

    def setApplicationName(self, _name):  # noqa: N802 - Qt naming
        return None

    def setOrganizationName(self, _name):  # noqa: N802 - Qt naming
        return None

    def setApplicationVersion(self, _version):  # noqa: N802 - Qt naming
        return None

    def setQuitOnLastWindowClosed(self, _flag):  # noqa: N802 - Qt naming
        return None

    def exec(self):
        return 0


class _Attr:
    def __init__(self, name: str):
        self.name = name

    def __repr__(self) -> str:
        return f"<ApplicationAttribute {self.name}>"


def _install_qt_stubs(monkeypatch):
    try:
        import PySide6.QtWidgets  # type: ignore
        import PySide6.QtCore  # type: ignore
        return
    except Exception:
        pass

    qtcore = types.ModuleType("PySide6.QtCore")
    attributes = {
        "AA_EnableHighDpiScaling": _Attr("AA_EnableHighDpiScaling"),
        "AA_UseHighDpiPixmaps": _Attr("AA_UseHighDpiPixmaps"),
        "AA_Use96Dpi": _Attr("AA_Use96Dpi"),
    }
    qtcore.Qt = types.SimpleNamespace(ApplicationAttribute=types.SimpleNamespace(**attributes))
    qtcore.QSharedMemory = _StubSharedMemory
    qtcore.QTimer = _StubTimer
    qtcore.qVersion = lambda: "0.0.0"

    qtwidgets = types.ModuleType("PySide6.QtWidgets")
    qtwidgets.QApplication = _StubApplication
    qtwidgets.QMessageBox = _StubMessageBox

    qt_root = types.ModuleType("PySide6")
    qt_root.QtCore = qtcore
    qt_root.QtWidgets = qtwidgets

    monkeypatch.setitem(sys.modules, "PySide6", qt_root)
    monkeypatch.setitem(sys.modules, "PySide6.QtCore", qtcore)
    monkeypatch.setitem(sys.modules, "PySide6.QtWidgets", qtwidgets)


@pytest.fixture()
def main_module(monkeypatch):
    _install_qt_stubs(monkeypatch)
    import app.main as module

    return importlib.reload(module)


@pytest.fixture()
def qt(main_module):
    return main_module.Qt


class _FakeApp:
    def __init__(self) -> None:
        self.calls: list[tuple[object, bool]] = []

    def setAttribute(self, attr, value):  # noqa: N802 - Qt naming
        self.calls.append((attr, value))


@pytest.mark.parametrize(
    "attr_name, qt_version, expected",
    [
        ("AA_EnableHighDpiScaling", (6, 7, 9), False),
        ("AA_EnableHighDpiScaling", (6, 8, 0), True),
        ("AA_EnableHighDpiScaling", (6, 9, 1), True),
        ("AA_EnableHighDpiScaling", None, True),
        ("AA_UseHighDpiPixmaps", (6, 7, 9), False),
        ("AA_UseHighDpiPixmaps", (6, 8, 0), True),
        ("AA_UseHighDpiPixmaps", (6, 9, 1), True),
        ("AA_UseHighDpiPixmaps", None, True),
        ("AA_Use96Dpi", (6, 9, 0), False),
        ("AA_Use96Dpi", None, False),
    ],
)
def test_is_deprecated_application_attribute(main_module, qt, attr_name, qt_version, expected):
    attr = getattr(qt.ApplicationAttribute, attr_name)
    assert main_module._is_deprecated_application_attribute(attr, qt_version) is expected


@pytest.mark.parametrize(
    "version_str, is_deprecated",
    [
        ("6.7.9", False),
        ("6.8.0", True),
        ("6.9.0", True),
        ("6.8", True),
        ("dev", True),
        ("", True),
    ],
)
@pytest.mark.parametrize(
    "attr_name",
    ["AA_EnableHighDpiScaling", "AA_UseHighDpiPixmaps"],
)
def test_set_attribute_if_supported_respects_deprecation(main_module, qt, monkeypatch, version_str, is_deprecated, attr_name):
    fake_app = _FakeApp()
    monkeypatch.setattr(main_module, "qVersion", lambda: version_str)

    result = main_module._set_attribute_if_supported(fake_app, attr_name)

    if is_deprecated:
        assert result is False
        assert fake_app.calls == []
    else:
        assert result is True
        assert fake_app.calls == [
            (getattr(qt.ApplicationAttribute, attr_name), True),
        ]


@pytest.mark.parametrize(
    "version_str, expected_calls",
    [
        ("6.7.0", ["AA_EnableHighDpiScaling", "AA_UseHighDpiPixmaps"]),
        ("6.8.0", []),
    ],
)
def test_configure_high_dpi_attributes_startup_path(main_module, qt, monkeypatch, version_str, expected_calls):
    fake_app = _FakeApp()

    monkeypatch.setattr(main_module, "qVersion", lambda: version_str)
    monkeypatch.setattr(
        main_module,
        "QApplication",
        types.SimpleNamespace(instance=lambda: fake_app),
    )

    applied = main_module._configure_high_dpi_attributes(fake_app)

    applied_attr_names = [call[0].name for call in fake_app.calls]
    assert applied_attr_names == [
        getattr(qt.ApplicationAttribute, name).name for name in expected_calls
    ]
    assert applied == expected_calls

