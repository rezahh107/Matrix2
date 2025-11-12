import json
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from app.core.common.policy import load_selection_reason_policy
from app.core.policy_loader import load_policy


def test_load_selection_reason_policy_uses_config_defaults() -> None:
    config = load_policy()
    selection = load_selection_reason_policy(config)
    assert selection.trace_stage_labels == (
        "جنسیت",
        "مدرسه",
        "گروه/رشته",
        "سیاست رتبه‌بندی",
    )
    assert selection.enabled is True
    assert selection.sheet_name == "دلایل انتخاب پشتیبان"


def test_load_selection_reason_policy_missing_labels_falls_back() -> None:
    config_path = Path("config/policy.json")
    raw = json.loads(config_path.read_text("utf-8"))
    raw.setdefault("emission", {}).setdefault("selection_reasons", {}).pop(
        "trace_stage_labels",
        None,
    )
    selection = load_selection_reason_policy(raw)
    assert selection.trace_stage_labels == (
        "جنسیت",
        "مدرسه",
        "گروه/رشته",
        "سیاست رتبه‌بندی",
    )


def test_load_selection_reason_policy_version_mismatch_raises() -> None:
    config_path = Path("config/policy.json")
    raw = json.loads(config_path.read_text("utf-8"))
    raw["version"] = "0.9.0"
    with pytest.raises(ValueError):
        load_selection_reason_policy(raw, expected_version="1.0.3", on_mismatch="raise")
