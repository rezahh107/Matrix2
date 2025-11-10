"""Hygiene test ensuring core modules avoid hardcoded Persian column names."""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

FORBIDDEN_COLUMNS = {
    "کدرشته",
    "گروه آزمایشی",
    "جنسیت",
    "دانش آموز فارغ",
    "مرکز گلستان صدرا",
    "مالی حکمت بنیاد",
    "کد مدرسه",
}

ALLOWED_FILES = {
    Path("app/core/policy_loader.py"),
    Path("app/core/build_matrix.py"),
    Path("app/core/common/domain.py"),
}


def _string_constants_without_docstrings(source: str) -> list[str]:
    tree = ast.parse(source)
    docstring_ids: set[int] = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            if node.body and isinstance(node.body[0], ast.Expr):
                value = getattr(node.body[0], "value", None)
                if isinstance(value, ast.Constant) and isinstance(value.value, str):
                    docstring_ids.add(id(value))
    constants: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            if id(node) in docstring_ids:
                continue
            constants.append(node.value)
    return constants


def test_core_has_no_hardcoded_columns_outside_policy_loader() -> None:
    root = Path(__file__).resolve().parents[2]
    core_dir = root / "app" / "core"
    offenders: dict[Path, set[str]] = {}

    for file_path in core_dir.rglob("*.py"):
        if file_path.name == "__init__.py":
            continue
        relative = file_path.relative_to(root)
        source = file_path.read_text(encoding="utf-8")
        string_values = _string_constants_without_docstrings(source)
        hits = {literal for literal in FORBIDDEN_COLUMNS if literal in string_values}
        if hits and relative not in ALLOWED_FILES:
            offenders[relative] = hits

    if offenders:
        message_lines = [
            "Hardcoded column literals detected in core modules:",
        ]
        for path, literals in sorted(offenders.items()):
            message_lines.append(f"- {path}: {', '.join(sorted(literals))}")
        pytest.fail("\n".join(message_lines))
