from __future__ import annotations

"""بایگانی دترمینیستیک خروجی‌های ImportToSabt در SQLite."""

import hashlib
import json
from dataclasses import dataclass
from typing import Iterable, Mapping, Sequence

import pandas as pd

from app.infra.local_database import LocalDatabase


@dataclass(frozen=True)
class ExporterArchiveConfig:
    """پیکربندی بایگانی خروجی ImportToSabt."""

    enabled: bool = False
    row_limit: int = 500


@dataclass(frozen=True)
class ExporterDiff:
    """خروجی مقایسهٔ Snapshot ها شامل ردیف‌های افزوده/حذف‌شده."""

    snapshot_a: dict[str, object]
    snapshot_b: dict[str, object]
    row_hash_equal: bool
    row_count_delta: int
    added: list[dict[str, object]] | None
    removed: list[dict[str, object]] | None


class ExporterArchiveRepository:
    """مدیریت بایگانی و مقایسهٔ Snapshot های ImportToSabt در لایهٔ Infra."""

    def __init__(
        self,
        *,
        db: LocalDatabase,
        exporter_name: str = "import_to_sabt",
    ) -> None:
        self.db = db
        self.exporter_name = exporter_name

    def archive_snapshot(
        self,
        *,
        rows_df: pd.DataFrame,
        exporter_version: str | None,
        run_uuid: str | None = None,
        run_id: int | None = None,
        metadata: Mapping[str, object] | None = None,
        config: ExporterArchiveConfig | None = None,
    ) -> int:
        """نرمال‌سازی دترمینیستیک و درج Snapshot خروجی در SQLite."""

        cfg = config or ExporterArchiveConfig()
        if not cfg.enabled:
            return -1
        normalized = self._normalize_rows(rows_df)
        payload = {
            "columns": list(normalized.columns),
            "rows": normalized.to_dict(orient="records"),
        }
        row_hash = self._hash_payload(payload)
        metadata_json = json.dumps(metadata or {}, ensure_ascii=False, separators=(",", ":"))
        is_truncated = False
        rows_json: str | None = None
        row_limit = int(cfg.row_limit)
        if row_limit >= 0 and len(payload["rows"]) > row_limit:
            is_truncated = True
        else:
            rows_json = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        return self.db.insert_exporter_snapshot(
            exporter_name=self.exporter_name,
            exporter_version=exporter_version,
            run_uuid=run_uuid,
            run_id=run_id,
            rows_df=normalized,
            metadata_json=metadata_json,
            row_hash=row_hash,
            columns_json=json.dumps(payload["columns"], ensure_ascii=False, separators=(",", ":")),
            rows_json=rows_json,
            row_limit=row_limit,
            is_truncated=is_truncated,
        )

    def list_snapshots(self) -> list[dict[str, object]]:
        """بازگرداندن لیست Snapshot ها به‌صورت دیکشنری."""

        rows = self.db.list_exporter_snapshots()
        return [dict(row) for row in rows]

    def compare_snapshots(self, snapshot_a: int, snapshot_b: int) -> ExporterDiff:
        """مقایسهٔ دو Snapshot با نرمال‌سازی و کنترل truncation."""

        row_a, df_a = self.db.fetch_exporter_snapshot(snapshot_a)
        row_b, df_b = self.db.fetch_exporter_snapshot(snapshot_b)
        if row_a is None or row_b is None:
            raise ValueError("یکی از شناسه‌های Snapshot موجود نیست.")
        if row_a["is_truncated"] or row_b["is_truncated"]:
            raise ValueError("مقایسهٔ Snapshot های ناقص (truncated) پشتیبانی نمی‌شود.")
        row_hash_equal = bool(row_a["row_hash"] == row_b["row_hash"])
        row_count_delta = int(row_b["row_count"]) - int(row_a["row_count"])
        added: list[dict[str, object]] | None = None
        removed: list[dict[str, object]] | None = None
        if df_a is not None and df_b is not None:
            added, removed = self._diff_records(df_a, df_b)
        return ExporterDiff(
            snapshot_a=dict(row_a),
            snapshot_b=dict(row_b),
            row_hash_equal=row_hash_equal,
            row_count_delta=row_count_delta,
            added=added,
            removed=removed,
        )

    @staticmethod
    def _normalize_rows(df: pd.DataFrame) -> pd.DataFrame:
        normalized = df.copy()
        normalized.columns = [str(col) for col in normalized.columns]
        ordered_columns = sorted(normalized.columns, key=_natural_key)
        normalized = normalized.loc[:, ordered_columns]
        normalized = normalized.apply(lambda col: col.map(_normalize_scalar))
        if ordered_columns:
            normalized = normalized.sort_values(
                by=ordered_columns, kind="mergesort", ignore_index=True
            )
        else:
            normalized = normalized.reset_index(drop=True)
        return normalized

    @staticmethod
    def _hash_payload(payload: Mapping[str, object]) -> str:
        serialized = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
        return hashlib.sha256(serialized.encode("utf-8")).hexdigest()

    @staticmethod
    def _diff_records(
        df_a: pd.DataFrame, df_b: pd.DataFrame
    ) -> tuple[list[dict[str, object]] | None, list[dict[str, object]] | None]:
        if list(df_a.columns) != list(df_b.columns):
            return None, None
        a_records = ExporterArchiveRepository._rows_to_tuples(df_a)
        b_records = ExporterArchiveRepository._rows_to_tuples(df_b)
        added = [dict(zip(df_b.columns, row)) for row in sorted(b_records - a_records)]
        removed = [dict(zip(df_a.columns, row)) for row in sorted(a_records - b_records)]
        return added, removed

    @staticmethod
    def _rows_to_tuples(df: pd.DataFrame) -> set[tuple]:
        records: Iterable[Sequence[object]] = df.itertuples(index=False, name=None)
        return {tuple(record) for record in records}


def _natural_key(value: str) -> tuple:
    tokens = []
    num = ""
    for ch in value:
        if ch.isdigit():
            num += ch
        else:
            if num:
                tokens.append(int(num))
                num = ""
            tokens.append(ch.lower())
    if num:
        tokens.append(int(num))
    return tuple(tokens)


def _normalize_scalar(value: object) -> object:
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    if pd.isna(value):  # type: ignore[arg-type]
        return None
    if isinstance(value, (pd.Timestamp, pd.Timedelta)):
        return value.isoformat()
    if isinstance(value, (int, float, bool, str)):
        return value
    return json.loads(json.dumps(value, ensure_ascii=False))
