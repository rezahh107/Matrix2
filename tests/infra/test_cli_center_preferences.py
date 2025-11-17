from argparse import Namespace

from app.infra.cli import _collect_cli_center_manager_overrides


def test_cli_center_manager_parsing_handles_repeatable_args() -> None:
    args = Namespace(
        center_manager=["1=الف", "2=ب"],
        center_managers=None,
        golestan_manager=None,
        sadra_manager=None,
    )
    mapping = _collect_cli_center_manager_overrides(args)
    assert mapping[1] == ("الف",)
    assert mapping[2] == ("ب",)


def test_cli_center_manager_parsing_supports_json_map() -> None:
    args = Namespace(
        center_manager=None,
        center_managers='{"3": ["ج"]}',
        golestan_manager=None,
        sadra_manager=None,
    )
    mapping = _collect_cli_center_manager_overrides(args)
    assert mapping[3] == ("ج",)
