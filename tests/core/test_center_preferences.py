from app.core.center_preferences import normalize_center_priority, parse_center_manager_config
from app.core.policy_loader import load_policy


def test_parse_center_manager_config_applies_overrides() -> None:
    policy = load_policy()
    result = parse_center_manager_config(
        policy,
        ui_overrides={1: ["UI"]},
        cli_overrides={2: ["CLI"]},
    )
    assert result.get(1) == ("UI",)
    assert result.get(2) == ("CLI",)


def test_normalize_center_priority_appends_missing_centers() -> None:
    policy = load_policy()
    normalized = normalize_center_priority(policy, [1])
    assert 1 in normalized
    assert policy.default_center_for_invalid in normalized
    assert len(normalized) >= len(policy.center_management.centers)
