from pathlib import Path

FORBIDDEN = ("transform", "transition", "box-shadow", "filter", "animation")


def test_qss_has_no_unsupported_properties():
    qss_path = Path("app/ui/styles.qss")
    content = qss_path.read_text(encoding="utf-8").lower()
    for token in FORBIDDEN:
        assert token not in content
