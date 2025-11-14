import pytest

from app.core.common.reasons import ReasonCode, build_reason, reason_message


def test_build_reason_returns_localized_message() -> None:
    reason = build_reason(ReasonCode.CENTER_MISMATCH)
    assert reason.code is ReasonCode.CENTER_MISMATCH
    assert "مرکز" in reason.message_fa


def test_reason_message_is_defined_for_all_codes() -> None:
    for code in ReasonCode:
        message = reason_message(code)
        assert isinstance(message, str)
        assert message.strip(), f"message for {code} must not be empty"


def test_reason_message_invalid_code_raises() -> None:
    class FakeEnum:
        value = "FAKE"

    with pytest.raises(ValueError):
        reason_message(FakeEnum)  # type: ignore[arg-type]
