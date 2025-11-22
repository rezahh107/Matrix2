from __future__ import annotations

"""Forms repository for WordPress/Gravity Forms backed by SQLite."""


from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Mapping, Protocol, Sequence

import pandas as pd

from app.infra.errors import ReferenceDataMissingError
from app.infra.local_database import LocalDatabase


class WordPressFormsClient(Protocol):
    """قرارداد کلاینت WordPress/Gravity Forms برای دریافت ورودی‌ها."""

    def fetch_entries(self, *, since: datetime | None = None) -> Sequence[Mapping[str, Any]]:
        """دریافت ورودی‌های فرم از WordPress.

        پارامتر ``since`` برای فیلتر زمانی استفاده می‌شود تا دریافت افزایشی
        آسان باشد. پیاده‌سازی باید خطاهای شبکه/احراز هویت را به‌شکل خوانا
        بالا بیاورد.
        """


PrivacyHook = Callable[[pd.DataFrame], pd.DataFrame]
NormalizerFn = Callable[[Sequence[Mapping[str, Any]]], pd.DataFrame]


@dataclass(frozen=True)
class FormsSyncResult:
    """خروجی همگام‌سازی فرم شامل دیتافریم و شمارش."""

    entries: pd.DataFrame
    fetched_count: int
    persisted_count: int


class FormsRepository:
    """مخزن ورودی‌های فرم با پشتوانهٔ SQLite.

    این کلاس کاملاً در لایهٔ Infra باقی می‌ماند و Core را بدون تغییر semantics
    تغذیه می‌کند؛ دیتافریم خروجی می‌تواند مستقیماً به مصرف‌کننده‌های Core
    تزریق شود. برای انطباق با سیاست حریم خصوصی، می‌توان ``privacy_hook`` را
    به‌صورت یک تابع تزریق کرد تا ستون‌های حساس قبل از ذخیره حذف/ناشناس شوند.
    """

    def __init__(
        self,
        *,
        client: WordPressFormsClient | None,
        db: LocalDatabase,
        normalizer: NormalizerFn | None = None,
        privacy_hook: PrivacyHook | None = None,
    ) -> None:
        self._client = client
        self._db = db
        self._normalizer = normalizer or self._default_normalizer
        self._privacy_hook = privacy_hook

    def sync_from_wordpress(
        self, *, since: datetime | None = None, source: str | None = "wordpress"
    ) -> FormsSyncResult:
        """دریافت ورودی‌ها از WordPress، نرمال‌سازی و ذخیره در SQLite."""

        if self._client is None:
            raise ReferenceDataMissingError(
                table="forms_entries",
                message="کلاینت WordPress پیکربندی نشده است؛ برای sync کلاینت معتبر تزریق کنید.",
            )
        raw_entries = self._client.fetch_entries(since=since)
        normalized = self._normalize(raw_entries)
        # حریم خصوصی: در صورت نیاز می‌توان privacy_hook را برای حذف PII اعمال کرد.
        normalized = self._apply_privacy(normalized)
        normalized = self._ensure_normalized_at(normalized)
        self._db.upsert_forms_entries(normalized, source=source)
        return FormsSyncResult(
            entries=normalized,
            fetched_count=len(raw_entries),
            persisted_count=int(normalized.shape[0]),
        )

    def load_entries(self) -> pd.DataFrame:
        """بارگذاری ورودی‌های نرمال‌شده از کش SQLite بدون دانلود مجدد."""

        cached = self._db.load_forms_entries()
        return cached

    def _normalize(self, raw_entries: Sequence[Mapping[str, Any]]) -> pd.DataFrame:
        normalized = self._normalizer(raw_entries)
        return normalized

    def _apply_privacy(self, df: pd.DataFrame) -> pd.DataFrame:
        """محل اعمال فیلتر/ناشناس‌سازی PII قبل از ذخیره.

        TODO: قواعد retention (مثلاً نگهداشت N روز) و ماسک فیلدهای حساس را از
        Policy/تنظیمات بخوانید و اینجا اعمال کنید.
        """

        if self._privacy_hook is None:
            return df
        return self._privacy_hook(df.copy())

    @staticmethod
    def _ensure_normalized_at(df: pd.DataFrame) -> pd.DataFrame:
        base = df.copy()
        if "normalized_at" not in base.columns:
            base["normalized_at"] = pd.Timestamp.utcnow()
        return base

    @staticmethod
    def _default_normalizer(
        raw_entries: Sequence[Mapping[str, Any]]
    ) -> pd.DataFrame:
        """نرمال‌سازی پایهٔ ورودی‌های WordPress به DataFrame تمیز.

        * ستون‌های پایه: entry_id, form_id, received_at (datetime-aware)
        * سایر فیلدها از ``fields`` (دیکشنری) یا مقادیر سطح بالا اضافه می‌شوند.
        """

        rows: list[dict[str, Any]] = []
        for entry in raw_entries:
            entry_id = entry.get("id") or entry.get("entry_id")
            form_id = entry.get("form_id") or entry.get("formId")
            received_at = (
                entry.get("date_created")
                or entry.get("created_at")
                or entry.get("submitted_at")
            )
            base = {
                "entry_id": str(entry_id) if entry_id is not None else None,
                "form_id": str(form_id) if form_id is not None else None,
                "received_at": received_at,
            }
            fields = entry.get("fields") or entry.get("field_values") or {}
            if isinstance(fields, Mapping):
                for key, value in fields.items():
                    base[str(key)] = value
            rows.append(base)
        df = pd.DataFrame(rows)
        if df.empty:
            return pd.DataFrame(columns=["entry_id", "form_id", "received_at"])
        df = df.copy()
        df["received_at"] = pd.to_datetime(df["received_at"], errors="coerce", utc=True)
        return df


__all__ = [
    "FormsRepository",
    "FormsSyncResult",
    "NormalizerFn",
    "PrivacyHook",
    "WordPressFormsClient",
]
