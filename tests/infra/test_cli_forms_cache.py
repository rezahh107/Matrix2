from pathlib import Path

from pandas.api.types import is_integer_dtype

from app.core.policy_loader import load_policy
from app.infra import cli
from app.infra.local_database import LocalDatabase


class _FakeFormsClient:
    def __init__(self, entries: list[dict]):
        self.entries = entries

    def fetch_entries(self, *, since=None):  # type: ignore[no-untyped-def]
        return list(self.entries)


def _sample_entry():
    return {
        "id": "201",
        "form_id": "5",
        "date_created": "2024-02-01T08:00:00Z",
        "fields": {
            "student_id": "ST-1",
            "کدرشته": 1201,
            "جنسیت": 1,
            "دانش آموز فارغ": 0,
            "مرکز گلستان صدرا": 1,
            "مالی حکمت بنیاد": 0,
            "کد مدرسه": 1111,
        },
    }


def test_cli_sync_forms_and_import_students(tmp_path: Path, monkeypatch):
    db_path = tmp_path / "forms.sqlite"
    policy = load_policy()
    client = _FakeFormsClient([_sample_entry()])

    monkeypatch.setattr(cli, "_resolve_forms_client", lambda args: client)

    sync_args = [
        "sync-forms",
        "--local-db",
        str(db_path),
    ]
    assert cli.main(sync_args) == 0

    import_args = [
        "import-students",
        "--from-forms-cache",
        "--local-db",
        str(db_path),
    ]
    assert cli.main(import_args) == 0

    db = LocalDatabase(db_path)
    cached = db.load_students_cache(join_keys=policy.join_keys)
    assert not cached.empty
    assert set(policy.join_keys).issubset(set(cached.columns))
    for key in policy.join_keys:
        assert is_integer_dtype(cached[key])
    assert cached.iloc[0]["student_id"] == "ST-1"


def test_cli_sync_forms_idempotent(tmp_path: Path, monkeypatch):
    db_path = tmp_path / "forms.sqlite"
    client = _FakeFormsClient([_sample_entry()])
    monkeypatch.setattr(cli, "_resolve_forms_client", lambda args: client)

    args = ["sync-forms", "--local-db", str(db_path)]
    assert cli.main(args) == 0
    assert cli.main(args) == 0

    db = LocalDatabase(db_path)
    cached = db.load_forms_entries()
    assert len(cached) == 1


def test_cli_sync_forms_cache_only_without_client(tmp_path: Path):
    db_path = tmp_path / "forms.sqlite"
    args = ["sync-forms", "--cache-only", "--local-db", str(db_path)]
    assert cli.main(args) == 0


def test_cli_sync_forms_cache_only_with_existing_cache(tmp_path: Path, monkeypatch, capsys):
    db_path = tmp_path / "forms.sqlite"
    client = _FakeFormsClient([_sample_entry()])
    monkeypatch.setattr(cli, "_resolve_forms_client", lambda args: client)

    args = ["sync-forms", "--local-db", str(db_path)]
    assert cli.main(args) == 0

    cache_only_args = ["sync-forms", "--cache-only", "--local-db", str(db_path)]
    assert cli.main(cache_only_args) == 0
    out = capsys.readouterr().out
    assert "cached forms entries: 1 rows" in out


def test_cli_sync_forms_missing_client_error(tmp_path: Path):
    db_path = tmp_path / "forms.sqlite"
    args = ["sync-forms", "--local-db", str(db_path)]
    assert cli.main(args) == 2
