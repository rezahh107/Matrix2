from __future__ import annotations

import re
from pathlib import Path

FORBIDDEN_PATTERN = re.compile(r"(read_excel|to_excel|ExcelWriter|open\()")


def test_core_has_no_io_patterns() -> None:
    core_dir = Path(__file__).resolve().parents[2] / 'app' / 'core'
    offenders: list[str] = []
    for py_file in core_dir.rglob('*.py'):
        text = py_file.read_text(encoding='utf-8')
        match = FORBIDDEN_PATTERN.search(text)
        if match:
            offenders.append(f"{py_file.relative_to(core_dir)} -> {match.group(1)}")
    assert not offenders, "Forbidden I/O patterns detected in core: " + ", ".join(offenders)
