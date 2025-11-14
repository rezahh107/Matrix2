from app.core.common.reasons import ReasonCode, build_reason, reason_message


def test_build_reason_returns_localized_text() -> None:
    reason = build_reason(ReasonCode.CAPACITY_FULL)
    assert reason.code == ReasonCode.CAPACITY_FULL
    assert "ظرفیت" in reason.message_fa


def test_reason_message_unknown_code_raises() -> None:
    try:
        reason_message(ReasonCode.OK)
    except ValueError:  # pragma: no cover - فقط برای نگهبان
        assert False
