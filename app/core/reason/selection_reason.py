"""ساخت شیت دلایل انتخاب پشتیبان با تکیه بر Policy و داده‌های تخصیص."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Sequence

import pandas as pd

from app.core.common.columns import canonicalize_headers, ensure_series
from app.core.common.normalization import (
    fa_digitize,
    sanitize_bidi,
    safe_truncate,
    to_numlike_str,
)
from app.core.common.policy import (
    SelectionReasonLabels,
    SelectionReasonPolicy,
    load_selection_reason_policy,
)
from app.core.common.ranking import natural_key
from app.core.common.reasons import ReasonCode, reason_message
from app.core.common.reasoning import summarize_trace_steps
from app.core.policy_loader import PolicyConfig

__all__ = [
    "ReasonContext",
    "render_reason",
    "build_selection_reason_rows",
]


@dataclass(frozen=True)
class ReasonContext:
    """کانتکست تولید متن دلیل انتخاب پشتیبان."""

    gender_value: str
    school_value: str
    track_value: str
    capacity_value: str
    mentor_id: str
    mentor_name: str
    after_school_label: str
    occupancy_ratio: str
    allocations_new: str
    remaining_capacity: str
    tiebreak_text: str
    trace_summary: str | None = None
    is_after_school: bool = False


def render_reason(context: ReasonContext, policy: SelectionReasonPolicy) -> str:
    """رندر متن دلیل به‌صورت دترمینیستیک و قابل‌ردیابی."""

    labels: SelectionReasonLabels = policy.labels
    parts = [
        f"{labels.gender}: {context.gender_value}",
        f"{labels.school}: {context.school_value} ({context.after_school_label})",
        f"{labels.track}: {context.track_value}",
        f"{labels.capacity}: {context.capacity_value}",
        f"{labels.result}: {context.mentor_name} ({context.mentor_id})",
        f"{labels.tiebreak}: {context.tiebreak_text}",
    ]
    if context.trace_summary:
        parts.append(f"مراحل: {context.trace_summary}")
    template_payload = {
        # قالب جدید (segment-based)
        "gender_segment": parts[0],
        "school_segment": parts[1],
        "track_segment": parts[2],
        "capacity_segment": parts[3],
        "result_segment": parts[4],
        "tiebreak_segment": parts[5],
        "trace_segment": parts[6] if len(parts) > 6 else "",
        # کلیدهای قدیمی (label-based)
        "gender_label": context.gender_value,
        "school_name": context.school_value,
        "track_label": context.track_value,
        "capacity_label": context.capacity_value,
        "result_label": f"{context.mentor_name} ({context.mentor_id})",
        "ranking_chain": context.tiebreak_text,
        "is_after_school": str(context.is_after_school).lower(),
        "after_school_label": context.after_school_label,
        # اطلاعات مشترک
        "mentor_id": context.mentor_id,
        "mentor_name": context.mentor_name,
        "occupancy_ratio": context.occupancy_ratio,
        "allocations_new": context.allocations_new,
        "remaining_capacity": context.remaining_capacity,
        "trace_summary": context.trace_summary or "",
    }
    raw_text = policy.template.format_map(template_payload)
    normalized = sanitize_bidi(raw_text.replace("\n", " ").replace("\t", " "))
    digitized = fa_digitize(normalized)
    return safe_truncate(digitized, 512)


def _format_capacity_text(
    occupancy_ratio: str,
    allocations_new: str,
    remaining_capacity: str,
) -> str:
    segments: list[str] = []
    if occupancy_ratio:
        segments.append(f"occupancy={occupancy_ratio}")
    if allocations_new:
        segments.append(f"alloc_new={allocations_new}")
    if remaining_capacity:
        segments.append(f"remaining={remaining_capacity}")
    return "، ".join(segments)


def _format_rule_details(payload: object) -> str:
    if not isinstance(payload, Mapping):
        return ""
    segments: list[str] = []
    for key in sorted(payload.keys()):
        value = payload[key]
        if isinstance(value, Mapping):
            value_text = _format_rule_details(value)
        elif isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
            nested = [str(item).strip() for item in value if str(item).strip()]
            value_text = f"[{', '.join(nested)}]" if nested else ""
        else:
            value_text = str(value).strip()
        if value_text:
            segments.append(f"{key}={value_text}")
    return "، ".join(segments)


def _normalize_reason_payload(
    code_value: object | None, message_value: object | None
) -> tuple[str | None, str | None]:
    """نرمال‌سازی کد/پیام دلیل برای استفاده در selection_reason."""

    code_text = str(code_value).strip() if code_value not in (None, "") else None
    message_text = str(message_value).strip() if message_value not in (None, "") else None
    if code_text:
        try:
            enum_value = ReasonCode(code_text)
        except ValueError:
            return code_text, message_text
        message_text = message_text or reason_message(enum_value)
        return enum_value.value, message_text
    return code_text, message_text


def _resolve_fairness_text(log_data: Mapping[str, Any]) -> str | None:
    """ساخت متن عدالت با حفظ فرمت موجود و fallback بر اساس ReasonCode."""

    fairness_text_raw = log_data.get("fairness_reason_text")
    if fairness_text_raw not in (None, ""):
        return str(fairness_text_raw)
    code = log_data.get("fairness_reason_code")
    if not code:
        return None
    resolved_code, resolved_message = _normalize_reason_payload(code, None)
    if resolved_code and resolved_message:
        return f"[{resolved_code}] {resolved_message}".strip()
    if resolved_code:
        return f"[{resolved_code}]"
    return resolved_message


def _build_tiebreak_text(policy: PolicyConfig, labels: SelectionReasonLabels) -> str:
    phrases: list[str] = []
    order = policy.ranking_rules
    for idx, rule in enumerate(order, start=1):
        if rule.name == "min_occupancy_ratio":
            phrase = f"{idx}) نسبت اشغال کمتر"
        elif rule.name == "min_allocations_new":
            phrase = f"{idx}) تخصیص جدید کمتر"
        elif rule.name == "min_mentor_id":
            phrase = f"{idx}) شناسه پشتیبان (مرتب‌سازی طبیعی)"
        else:
            phrase = f"{idx}) {rule.name}"
        phrases.append(phrase)
    return fa_digitize(" → ".join(phrases) or labels.tiebreak)


def build_selection_reason_rows(
    allocations: pd.DataFrame,
    students: pd.DataFrame,
    mentors: pd.DataFrame,
    *,
    policy: PolicyConfig,
    logs: pd.DataFrame | None = None,
    trace: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """ساخت دیتافریم دلایل انتخاب پشتیبان بدون انجام I/O."""

    _ = logs  # رابط سازگاری؛ در این نسخه استفاده نمی‌شود.

    config = load_selection_reason_policy(
        policy,
        expected_version=policy.version,
        on_mismatch="warn",
    )
    output_columns = list(config.columns)
    if not output_columns:
        output_columns = [
            "شمارنده",
            "کدملی",
            "نام",
            "نام خانوادگی",
            "شناسه پشتیبان",
            "دلیل انتخاب پشتیبان",
        ]

    def _empty_frame() -> pd.DataFrame:
        frame = pd.DataFrame(columns=output_columns)
        frame.attrs["schema_hash"] = config.schema_hash
        return frame

    if not config.enabled or allocations is None or allocations.empty:
        return _empty_frame()

    students_fa = canonicalize_headers(students, header_mode="fa").copy()
    missing_keys = [key for key in policy.join_keys if key not in students_fa.columns]
    if missing_keys:
        raise KeyError(f"students missing join keys: {missing_keys}")
    for key in policy.join_keys:
        series = ensure_series(students_fa[key])
        normalized = series.map(to_numlike_str)
        if key == policy.columns.school_code and policy.school_code_empty_as_zero:
            normalized = normalized.replace("", "0")
        coerced = pd.to_numeric(normalized, errors="coerce")
        if coerced.isna().any():
            raise ValueError(f"students join key '{key}' contains non-integer values")
        if not coerced.empty:
            coerced_float = coerced.astype("float64")
            if not coerced_float.map(float.is_integer).all():
                raise ValueError(f"students join key '{key}' contains non-integer values")

    allocations_en = canonicalize_headers(allocations, header_mode="en")
    students_en = canonicalize_headers(students, header_mode="en")
    mentors_en = canonicalize_headers(mentors, header_mode="en")
    trace_en = canonicalize_headers(trace, header_mode="en") if trace is not None else None

    if "student_id" not in allocations_en.columns:
        return _empty_frame()

    allocations_en = allocations_en.copy()
    allocations_en["student_id"] = allocations_en["student_id"].astype("string")
    if "mentor_id" in allocations_en.columns:
        allocations_en["mentor_id"] = allocations_en["mentor_id"].astype("string")
    else:
        allocations_en["mentor_id"] = ""

    if "student_id" in students_en.columns:
        student_id_series = ensure_series(students_en["student_id"]).astype("string")
        student_index_key = student_id_series
    else:
        student_index_key = pd.Index(students_en.index.map(str))
    students_index = students_en.set_index(student_index_key, drop=False)

    student_lookup: dict[str, pd.Series] = {}
    for index_value, student_row in students_index.iterrows():
        row_copy = student_row.copy()
        primary_id = str(row_copy.get("student_id", "") or "").strip()
        fallback_id = str(index_value if index_value is not None else "").strip()
        if primary_id:
            student_lookup.setdefault(primary_id, row_copy)
        if fallback_id and fallback_id not in student_lookup:
            student_lookup[fallback_id] = row_copy

    def _alias(column: str) -> str:
        try:
            alias = canonicalize_headers(pd.DataFrame(columns=[column]), header_mode="en").columns[0]
        except Exception:
            alias = column
        return alias

    def _student_value(student_id: str, *columns: str) -> str:
        row = student_lookup.get(student_id)
        if row is None:
            return ""
        for column in columns:
            if column and column in row.index:
                value = row.get(column)
                if pd.notna(value) and str(value).strip():
                    return str(value).strip()
        return ""

    mentor_lookup: dict[str, str] = {}
    if "mentor_id" in mentors_en.columns:
        for _, row in mentors_en.iterrows():
            mentor_id = str(row.get("mentor_id", "")).strip()
            if not mentor_id:
                continue
            for column in ("mentor_name", "mentor", "پشتیبان"):
                name_value = row.get(column)
                if pd.notna(name_value) and str(name_value).strip():
                    mentor_lookup[mentor_id] = str(name_value).strip()
                    break
            mentor_lookup.setdefault(mentor_id, mentor_id)

    stage_order: Sequence[str] = policy.trace_stage_names or (
        "type",
        "group",
        "gender",
        "graduation_status",
        "center",
        "finance",
        "school",
        "capacity_gate",
    )
    stage_labels: dict[str, str] = {}
    for stage in stage_order:
        try:
            stage_labels[stage] = str(policy.stage_column(stage))
        except KeyError:
            stage_labels[stage] = stage

    trace_summary_map: dict[str, str] = {}
    if trace_en is not None and not trace_en.empty and "student_id" in trace_en.columns:
        trace_en = trace_en.copy()
        trace_en["student_id"] = trace_en["student_id"].astype("string")
        for student_id, group in trace_en.groupby("student_id", sort=False):
            summary = summarize_trace_steps(
                group,
                student_id,
                stage_order=stage_order,
                labels=stage_labels,
            )
            if summary:
                trace_summary_map[str(student_id)] = summary

    log_lookup: dict[str, dict[str, object]] = {}
    if logs is not None:
        logs_df = pd.DataFrame(logs)
        if not logs_df.empty and "student_id" in canonicalize_headers(logs_df, header_mode="en").columns:
            logs_en = canonicalize_headers(logs_df, header_mode="en")
            logs_en = logs_en.copy()
            logs_en["student_id"] = logs_en["student_id"].astype("string")
            for record in logs_en.to_dict("records"):
                student_key = str(record.get("student_id", "")).strip()
                if student_key:
                    log_lookup[student_key] = record

    gender_column = policy.stage_column("gender")
    school_column = policy.stage_column("school")
    track_column = policy.stage_column("group")
    capacity_column = policy.capacity_column
    gender_alias = _alias(gender_column)
    school_alias = _alias(school_column)
    track_alias = _alias(track_column)
    capacity_alias = _alias(capacity_column)

    tiebreak_text = _build_tiebreak_text(policy, config.labels)

    records: list[dict[str, object]] = []
    counter_candidates = ("counter", "allocation_counter", "row_number", "row_index")

    for _, row in allocations_en.iterrows():
        student_id = str(row.get("student_id", "")).strip()
        if not student_id:
            continue
        mentor_id = str(row.get("mentor_id", "")).strip()
        mentor_name = mentor_lookup.get(mentor_id, mentor_id)

        national_id = _student_value(student_id, "national_id", "کدملی", "کد ملی")
        first_name = _student_value(student_id, "first_name", "نام")
        last_name = _student_value(student_id, "last_name", "family_name", "نام خانوادگی")
        gender_value = _student_value(student_id, gender_column, gender_alias, "gender")
        school_value = _student_value(
            student_id,
            "school_name",
            "school_name_1",
            school_column,
            school_alias,
            "school",
        )
        track_value = _student_value(
            student_id,
            "exam_group",
            "group_name",
            track_column,
            track_alias,
        )
        after_school_label = "پس‌مدرسه‌ای: خیر"
        is_after_school = False
        after_school_flag = _student_value(
            student_id,
            "after_school",
            "after_school_flag",
            "پس مدرسه ای",
            "پس‌مدرسه‌ای",
        )
        if after_school_flag:
            normalized_flag = after_school_flag.strip().lower()
            if normalized_flag in {"1", "true", "بله", "yes", "y", "t"}:
                after_school_label = "پس‌مدرسه‌ای: بله"
                is_after_school = True
        counter_value: object | None = None
        student_counter = _student_value(student_id, "counter", "شمارنده")
        if student_counter:
            counter_value = student_counter

        if counter_value is None:
            for column in counter_candidates:
                if column in allocations_en.columns:
                    value = row.get(column)
                    if pd.notna(value) and str(value).strip():
                        counter_value = value
                        break
        if counter_value is None or not str(counter_value).strip():
            counter_value = student_id

        gender_label = _resolve_gender_label(gender_value, policy)
        log_data = log_lookup.get(student_id, {})
        occupancy_ratio = _format_ratio(log_data.get("occupancy_ratio"))
        if not occupancy_ratio:
            occupancy_ratio = _format_ratio(row.get("occupancy_ratio"))
        allocations_new = _format_int(
            log_data.get("allocations_new")
            if log_data.get("allocations_new") is not None
            else None
        )
        if not allocations_new:
            before = log_data.get("capacity_before")
            after = log_data.get("capacity_after")
            if before is not None and after is not None:
                try:
                    allocations_new = _format_int(float(before) - float(after))
                except (TypeError, ValueError):
                    allocations_new = ""
        if not allocations_new:
            allocations_new = _format_int(row.get("allocations_new"))
        remaining_capacity = _format_int(log_data.get("capacity_after"))
        if not remaining_capacity:
            remaining_capacity = _format_int(row.get(capacity_column) or row.get(capacity_alias))
        capacity_raw = _format_capacity_text(occupancy_ratio, allocations_new, remaining_capacity)
        if not capacity_raw:
            capacity_raw = config.labels.capacity
        capacity_value = fa_digitize(sanitize_bidi(capacity_raw))

        trace_summary = trace_summary_map.get(student_id)
        reason_text = render_reason(
            ReasonContext(
                gender_value=fa_digitize(gender_label),
                school_value=fa_digitize(sanitize_bidi(school_value)),
                track_value=fa_digitize(sanitize_bidi(track_value)),
                capacity_value=capacity_value,
                mentor_id=fa_digitize(sanitize_bidi(mentor_id)),
                mentor_name=fa_digitize(sanitize_bidi(mentor_name)),
                after_school_label=fa_digitize(sanitize_bidi(after_school_label)),
                occupancy_ratio=occupancy_ratio,
                allocations_new=allocations_new,
                remaining_capacity=remaining_capacity,
                tiebreak_text=tiebreak_text,
                trace_summary=fa_digitize(trace_summary) if trace_summary else None,
                is_after_school=is_after_school,
            ),
            config,
        )
        rule_code_raw = log_data.get("rule_reason_code")
        rule_message_raw = log_data.get("rule_reason_text")
        rule_detail_text = _format_rule_details(log_data.get("rule_reason_details"))
        rule_code, rule_message = _normalize_reason_payload(rule_code_raw, rule_message_raw)
        fairness_text = _resolve_fairness_text(log_data)
        reason_segments = [reason_text]
        if rule_code and rule_message:
            reason_segments.append(
                fa_digitize(
                    sanitize_bidi(f"دلیل Policy: [{rule_code}] {rule_message}")
                )
            )
        if rule_detail_text:
            reason_segments.append(
                fa_digitize(
                    sanitize_bidi(f"جزئیات Policy: {rule_detail_text}")
                )
            )
        if fairness_text:
            reason_segments.append(fa_digitize(sanitize_bidi(f"عدالت: {fairness_text}")))
        reason_text = " — ".join(segment for segment in reason_segments if segment)

        records.append(
            {
                "شمارنده": counter_value,
                "کدملی": sanitize_bidi(national_id),
                "نام": sanitize_bidi(first_name),
                "نام خانوادگی": sanitize_bidi(last_name),
                "شناسه پشتیبان": sanitize_bidi(mentor_id),
                "دلیل انتخاب پشتیبان": reason_text,
                "__mentor_id__": mentor_id,
            }
        )

    reason_df = pd.DataFrame.from_records(records)
    if reason_df.empty:
        return _empty_frame()

    for column in output_columns:
        if column not in reason_df.columns:
            reason_df[column] = ""

    internal_columns = list(dict.fromkeys(output_columns + ["__mentor_id__"]))
    reason_df = reason_df.reindex(columns=internal_columns)
    if "شمارنده" in reason_df.columns:
        reason_df["شمارنده"] = pd.to_numeric(reason_df["شمارنده"], errors="coerce")
    sort_columns = [
        column for column in ("شمارنده", "__mentor_id__") if column in reason_df.columns
    ]
    reason_df = reason_df.sort_values(
        sort_columns,
        kind="mergesort",
        key=lambda series: series.map(natural_key)
        if series.name == "__mentor_id__"
        else series,
    ).reset_index(drop=True)
    if "شمارنده" in reason_df.columns:
        reason_df["شمارنده"] = pd.Series(
            range(1, len(reason_df) + 1), index=reason_df.index, dtype="Int64"
        )
    for column in output_columns:
        if column == "شمارنده":
            continue
        reason_df[column] = reason_df[column].astype("string")
    reason_df = reason_df.drop(columns=["__mentor_id__"], errors="ignore")
    reason_df = reason_df.loc[:, output_columns]
    reason_df.attrs["schema_hash"] = config.schema_hash
    return reason_df


def _resolve_gender_label(value: object, policy: PolicyConfig) -> str:
    text = str(value or "").strip()
    if not text:
        return "نامشخص"
    try:
        numeric = int(float(text))
    except (TypeError, ValueError):
        normalized = text.replace("ي", "ی").replace("ك", "ک")
        if "دختر" in normalized:
            return "دختر"
        if "پسر" in normalized:
            return "پسر"
        return normalized or "نامشخص"
    female_code = policy.gender_codes.female.value
    male_code = policy.gender_codes.male.value
    if numeric == int(female_code):
        return "دختر"
    if numeric == int(male_code):
        return "پسر"
    return "نامشخص"


def _format_ratio(value: object) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return ""
    if pd.isna(number):
        return ""
    percent = number * 100
    return f"{percent:.1f}%"


def _format_int(value: object) -> str:
    try:
        number = int(float(value))
    except (TypeError, ValueError):
        return ""
    if pd.isna(number):
        return ""
    return str(number)
