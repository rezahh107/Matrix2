"""سیاست‌های کمکی برای خروجی‌های توضیحی."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, MutableSequence, Sequence
import warnings

from app.core.policy_loader import (
    DEFAULT_POLICY_VERSION,
    PolicyConfig,
    VersionMismatchMode,
    _DEFAULT_REASON_TRACE_LABELS,
    _DEFAULT_SELECTION_REASON_OPTIONS,
)
from app.core.policy.loader import compute_schema_hash, validate_policy_columns


@dataclass(frozen=True)
class SelectionReasonLabels:
    """برچسب‌های بخش‌های مختلف متن دلایل انتخاب پشتیبان."""

    gender: str
    school: str
    track: str
    capacity: str
    result: str
    tiebreak: str


@dataclass(frozen=True)
class SelectionReasonPolicy:
    """تنظیمات دترمینیستیک شیت «دلایل انتخاب پشتیبان»."""

    enabled: bool
    sheet_name: str
    template: str
    trace_stage_labels: tuple[str, ...]
    version: str
    locale: str
    labels: SelectionReasonLabels
    columns: tuple[str, ...]
    schema_hash: str


def _normalize_columns(raw: object) -> tuple[str, ...]:
    if isinstance(raw, (list, tuple)):
        candidates = [str(item).strip() for item in raw if str(item or "").strip()]
    elif isinstance(raw, str):
        cleaned = raw.strip()
        candidates = [cleaned] if cleaned else []
    else:
        candidates = []

    if not candidates:
        defaults = _DEFAULT_SELECTION_REASON_OPTIONS.get("columns", ())
        candidates = [str(item) for item in defaults]

    return validate_policy_columns(candidates)


def _normalize_labels(raw: object) -> tuple[str, ...]:
    if raw is None:
        return _DEFAULT_REASON_TRACE_LABELS
    if isinstance(raw, Mapping):
        ordered: list[str] = []
        for key in ("gender", "school", "track", "ranking"):
            value = raw.get(key)
            ordered.append(str(value) if value is not None else "")
        return tuple(_fallback_defaults(ordered))
    if isinstance(raw, Sequence) and not isinstance(raw, (str, bytes)):
        return tuple(_fallback_defaults([str(item) for item in raw]))
    return _DEFAULT_REASON_TRACE_LABELS


def _fallback_defaults(labels: MutableSequence[str]) -> tuple[str, ...]:
    padded: list[str] = list(labels[: len(_DEFAULT_REASON_TRACE_LABELS)])
    if len(padded) < len(_DEFAULT_REASON_TRACE_LABELS):
        padded.extend([""] * (len(_DEFAULT_REASON_TRACE_LABELS) - len(padded)))
    normalized: list[str] = []
    for idx, value in enumerate(padded):
        cleaned = value.strip()
        if not cleaned:
            normalized.append(_DEFAULT_REASON_TRACE_LABELS[idx])
        else:
            normalized.append(cleaned)
    return tuple(normalized)


def _extract_reason_labels(raw: object, locale: str) -> SelectionReasonLabels:
    if isinstance(raw, Mapping):
        candidate = raw.get("reason") if "reason" in raw else raw
    else:
        candidate = None
    mapping = candidate if isinstance(candidate, Mapping) else {}

    defaults = _DEFAULT_SELECTION_REASON_OPTIONS["labels"].get("reason", {})

    def pick(key: str) -> str:
        options = mapping.get(key)
        value = _pick_label_option(options, locale)
        if value:
            return value
        fallback = defaults.get(key)
        resolved = _pick_label_option(fallback, locale)
        return resolved or key

    return SelectionReasonLabels(
        gender=pick("gender"),
        school=pick("school"),
        track=pick("track"),
        capacity=pick("capacity"),
        result=pick("result"),
        tiebreak=pick("tiebreak"),
    )


def _pick_label_option(options: object, locale: str) -> str:
    if isinstance(options, Mapping):
        localized = options.get(locale)
        if localized:
            text = str(localized).strip()
            if text:
                return text
        for value in options.values():
            text = str(value or "").strip()
            if text:
                return text
        return ""
    candidates: Sequence[object]
    if isinstance(options, (list, tuple)):
        candidates = options
    elif options is None:
        candidates = ()
    else:
        candidates = (options,)
    normalized: list[str] = []
    locale_lower = (locale or "").lower()
    for item in candidates:
        text = str(item or "").strip()
        if not text:
            continue
        parts = text.split(":", 1)
        if len(parts) == 2 and parts[0].strip().lower() == locale_lower:
            candidate = parts[1].strip()
            if candidate:
                return candidate
        normalized.append(text)
    return normalized[0] if normalized else ""


def load_selection_reason_policy(
    policy: PolicyConfig | Mapping[str, object],
    *,
    expected_version: str = DEFAULT_POLICY_VERSION,
    on_mismatch: VersionMismatchMode = "warn",
) -> SelectionReasonPolicy:
    """تبدیل Policy کلی به تنظیمات مخصوص شیت دلایل."""

    schema_hash = ""

    if isinstance(policy, PolicyConfig):
        version = policy.version
        options = policy.emission.selection_reasons
        trace_labels = getattr(options, "trace_stage_labels", ())
        enabled = bool(getattr(options, "enabled", True))
        sheet_name = str(getattr(options, "sheet_name", "دلایل انتخاب پشتیبان"))
        template = str(getattr(options, "template", ""))
        locale = str(getattr(options, "locale", "fa"))
        label_mapping = getattr(options, "labels", {})
        columns = _normalize_columns(getattr(options, "columns", ()))
        schema_hash = str(getattr(options, "schema_hash", ""))
    else:
        version = str(policy.get("version", ""))
        emission = policy.get("emission") if isinstance(policy, Mapping) else None
        if not isinstance(emission, Mapping):
            emission = {}
        selection = emission.get("selection_reasons")
        if not isinstance(selection, Mapping):
            selection = {}
        enabled = bool(selection.get("enabled", True))
        sheet_name = str(selection.get("sheet_name", "دلایل انتخاب پشتیبان"))
        template = str(selection.get("template", ""))
        trace_labels = selection.get("trace_stage_labels")
        locale = str(selection.get("locale", "fa"))
        label_mapping = selection.get("labels")
        columns = _normalize_columns(selection.get("columns"))
        schema_hash = compute_schema_hash(columns)

    if version and expected_version and version != expected_version:
        message = (
            f"Policy version mismatch for selection reasons: loaded='{version}' expected='{expected_version}'"
        )
        if on_mismatch == "raise":
            raise ValueError(message)
        warnings.warn(message, RuntimeWarning, stacklevel=3)
        if on_mismatch == "migrate":
            version = expected_version

    labels = _normalize_labels(trace_labels)
    reason_labels = _extract_reason_labels(label_mapping, locale)
    if not schema_hash:
        schema_hash = compute_schema_hash(columns)
    return SelectionReasonPolicy(
        enabled=enabled,
        sheet_name=sheet_name,
        template=template,
        trace_stage_labels=labels,
        version=version or expected_version,
        locale=locale or "fa",
        labels=reason_labels,
        columns=columns,
        schema_hash=schema_hash,
    )


__all__ = [
    "SelectionReasonLabels",
    "SelectionReasonPolicy",
    "load_selection_reason_policy",
]

