"""لودر متن‌های داشبورد برای کارت‌ها و چک‌لیست."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List

from app.utils.path_utils import resource_path
from app.ui.texts import UiTranslator

__all__ = [
    "ChecklistItem",
    "DashboardTextBundle",
    "load_dashboard_texts",
]


@dataclass(frozen=True)
class ChecklistItem:
    """آیتم قابل‌نمایش در چک‌لیست داشبورد."""

    id: str
    text: str


@dataclass(frozen=True)
class DashboardTextBundle:
    """مجموعهٔ متن‌های کارت‌ها و چک‌لیست داشبورد."""

    files_title: str
    files_description: str
    checklist_title: str
    checklist_description: str
    actions_title: str
    actions_description: str
    checklist_items: List[ChecklistItem]


_DEFAULT_DATA = {
    "cards": {
        "files": {
            "title": "فایل‌های کلیدی",
            "description": "آخرین مسیرهای ذخیره‌شده"
        },
        "checklist": {
            "title": "چک‌لیست",
            "description": "مرور سریع گام‌ها"
        },
        "actions": {
            "title": "میانبرها",
            "description": "دسترسی به سناریوها"
        },
    },
    "checklist": [
        {"id": "inputs", "text": "ورودی‌ها آماده هستند"},
        {"id": "policy", "text": "سیاست صحیح انتخاب شده"},
    ],
}


def _load_json_payload(path: Path) -> dict:
    """خواندن فایل JSON با fallback به دادهٔ پیش‌فرض."""

    if not path.exists():
        return _DEFAULT_DATA
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return _DEFAULT_DATA


def _normalize_items(items: Iterable[dict]) -> List[ChecklistItem]:
    """تبدیل دادهٔ ورودی به لیست آیتم‌های معتبر."""

    normalized: List[ChecklistItem] = []
    for raw in items:
        item_id = str(raw.get("id") or "item")
        text = str(raw.get("text") or "")
        if not text:
            continue
        normalized.append(ChecklistItem(id=item_id, text=text))
    return normalized


def load_dashboard_texts(translator: UiTranslator) -> DashboardTextBundle:
    """بارگذاری متن‌های داشبورد از `config/dashboard_texts.json` با fallback ترجمه."""

    payload = _load_json_payload(resource_path("config", "dashboard_texts.json"))
    cards = payload.get("cards", {})
    files_card = cards.get("files", {})
    checklist_card = cards.get("checklist", {})
    actions_card = cards.get("actions", {})
    default_items = [
        {"id": "inputs", "text": translator.text("dashboard.checklist.item.inputs", "ورودی‌ها آماده هستند")},
        {"id": "policy", "text": translator.text("dashboard.checklist.item.policy", "سیاست صحیح انتخاب شده")},
    ]
    raw_items = payload.get("checklist", default_items)
    return DashboardTextBundle(
        files_title=str(files_card.get("title") or translator.text("dashboard.files.title", "فایل‌های کلیدی")),
        files_description=str(
            files_card.get("description") or translator.text("dashboard.files.description", "آخرین مسیرها")
        ),
        checklist_title=str(
            checklist_card.get("title") or translator.text("dashboard.checklist.title", "چک‌لیست")
        ),
        checklist_description=str(
            checklist_card.get("description")
            or translator.text("dashboard.checklist.description", "مرور سریع گام‌ها")
        ),
        actions_title=str(actions_card.get("title") or translator.text("dashboard.actions.title", "میانبرها")),
        actions_description=str(
            actions_card.get("description") or translator.text("dashboard.actions.description", "دسترسی سریع")
        ),
        checklist_items=_normalize_items(raw_items),
    )
