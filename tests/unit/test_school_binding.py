import pandas as pd

from app.core.build_matrix import BuildConfig, _as_domain_config, collect_school_codes_from_row
from app.core.common.filters import filter_by_school
from app.core.policy_loader import load_policy


def test_collect_school_codes_marks_global_when_all_columns_empty() -> None:
    cfg = BuildConfig()
    domain_cfg = _as_domain_config(cfg)
    row = pd.Series({"نام مدرسه 1": "", "نام مدرسه 2": None})
    binding = collect_school_codes_from_row(
        row,
        {},
        ["نام مدرسه 1", "نام مدرسه 2"],
        domain_cfg=domain_cfg,
        binding_policy=cfg.policy.mentor_school_binding,
    )
    assert binding.codes == []
    assert not binding.has_school_constraint
    assert binding.binding_mode == cfg.policy.mentor_school_binding.global_mode


def test_collect_school_codes_marks_restricted_when_value_present() -> None:
    cfg = BuildConfig()
    domain_cfg = _as_domain_config(cfg)
    row = pd.Series({"نام مدرسه 1": "مدرسه نمونه"})
    mapping = {"مدرسه نمونه": "5001"}
    binding = collect_school_codes_from_row(
        row,
        mapping,
        ["نام مدرسه 1"],
        domain_cfg=domain_cfg,
        binding_policy=cfg.policy.mentor_school_binding,
    )
    assert binding.codes == [5001]
    assert binding.has_school_constraint
    assert binding.binding_mode == cfg.policy.mentor_school_binding.restricted_mode


def test_collect_school_codes_marks_restricted_even_without_mapping() -> None:
    cfg = BuildConfig()
    domain_cfg = _as_domain_config(cfg)
    row = pd.Series({"نام مدرسه 1": "نام ناشناس"})
    binding = collect_school_codes_from_row(
        row,
        {},
        ["نام مدرسه 1"],
        domain_cfg=domain_cfg,
        binding_policy=cfg.policy.mentor_school_binding,
    )
    assert binding.codes == []
    assert binding.has_school_constraint
    assert binding.binding_mode == cfg.policy.mentor_school_binding.restricted_mode


def test_filter_by_school_keeps_global_rows() -> None:
    policy = load_policy()
    column = policy.stage_column("school")
    pool = pd.DataFrame(
        {
            column: [0, 5001],
            "has_school_constraint": [False, True],
        }
    )
    student = {column: 5001}
    filtered = filter_by_school(pool, student, policy)
    assert filtered.shape[0] == 2
    assert set(filtered[column]) == {0, 5001}


def test_filter_by_school_drops_non_matching_restricted_rows() -> None:
    policy = load_policy()
    column = policy.stage_column("school")
    pool = pd.DataFrame(
        {
            column: [0, 7000],
            "has_school_constraint": [False, True],
        }
    )
    student = {column: 5001}
    filtered = filter_by_school(pool, student, policy)
    assert filtered.shape[0] == 1
    assert int(filtered[column].iat[0]) == 0
