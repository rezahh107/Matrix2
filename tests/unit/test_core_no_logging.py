from pathlib import Path


CORE_DIR = Path("app/core")


def test_core_modules_do_not_import_logging() -> None:
    offenders: list[Path] = []
    for path in CORE_DIR.rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        if "logging." in text:
            offenders.append(path)
    assert not offenders, f"logging usage found in core modules: {offenders}"
