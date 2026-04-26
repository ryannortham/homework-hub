"""Tests for the CLI scaffold."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

import yaml
from click.testing import CliRunner

from homework_hub.__main__ import cli
from homework_hub.orchestrator import ChildReport, SourceResult, SyncReport


def _write_min_config(tmp_path: Path, *, sheet_id: str | None = None) -> dict[str, str]:
    """Write a minimal children.yaml + return env vars to point Settings at it."""
    config_dir = tmp_path / "config"
    tokens_dir = config_dir / "tokens"
    tokens_dir.mkdir(parents=True)
    children = {
        "children": {
            "james": {
                "display_name": "James",
                "sheet_id": sheet_id,
                "compass_user_id": 12345,
                "sources": {
                    "classroom": {"enabled": True},
                    "compass": {"enabled": True, "subdomain": "mcsc-vic"},
                    "edrolo": {"enabled": True},
                },
            },
        }
    }
    (config_dir / "children.yaml").write_text(yaml.safe_dump(children))
    return {
        "HOMEWORK_HUB_CONFIG_DIR": str(config_dir),
        "HOMEWORK_HUB_TOKENS_DIR": str(tokens_dir),
        "HOMEWORK_HUB_STATE_DB": str(tmp_path / "state.db"),
        "HOMEWORK_HUB_LOG_DIR": str(tmp_path / "logs"),
    }


def _fake_report(*, ok: bool = True) -> SyncReport:
    now = datetime.now(UTC)
    result = SourceResult(
        child="james",
        source="classroom",
        ok=ok,
        failure_kind=None if ok else "auth_expired",
        failure_message=None if ok else "token expired",
        task_count=3 if ok else 0,
    )
    child = ChildReport(
        child="james",
        source_results=[result],
        sheet_id="sheet-abc",
        rows_updated=1,
        rows_appended=2,
        rows_unchanged=0,
    )
    return SyncReport(started_at=now, finished_at=now, children=[child])


def test_cli_help_lists_subcommands():
    runner = CliRunner()
    result = runner.invoke(cli, ["--help"])
    assert result.exit_code == 0
    for cmd in ("sync", "auth", "bootstrap-sheet", "status"):
        assert cmd in result.output


def test_sync_default_all_children(tmp_path: Path):
    env = _write_min_config(tmp_path, sheet_id="sheet-abc")
    runner = CliRunner()
    with patch("homework_hub.__main__.build_orchestrator") as mock_build:
        mock_build.return_value.run.return_value = _fake_report(ok=True)
        result = runner.invoke(cli, ["sync"], env=env)
    assert result.exit_code == 0, result.output
    mock_build.return_value.run.assert_called_once_with(only_child=None)
    assert "Sync completed" in result.output


def test_sync_with_child(tmp_path: Path):
    env = _write_min_config(tmp_path, sheet_id="sheet-abc")
    runner = CliRunner()
    with patch("homework_hub.__main__.build_orchestrator") as mock_build:
        mock_build.return_value.run.return_value = _fake_report(ok=True)
        result = runner.invoke(cli, ["sync", "--child", "james"], env=env)
    assert result.exit_code == 0, result.output
    mock_build.return_value.run.assert_called_once_with(only_child="james")


def test_sync_exits_2_on_failure(tmp_path: Path):
    env = _write_min_config(tmp_path, sheet_id="sheet-abc")
    runner = CliRunner()
    with patch("homework_hub.__main__.build_orchestrator") as mock_build:
        mock_build.return_value.run.return_value = _fake_report(ok=False)
        result = runner.invoke(cli, ["sync"], env=env)
    assert result.exit_code == 2
    assert "FAIL" in result.output


def test_auth_compass_saves_token(tmp_path: Path):
    out_token = tmp_path / "compass.json"
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "auth",
            "compass",
            "--subdomain",
            "mcsc-vic",
            "--cookie",
            "ABC123",
            "--token-path",
            str(out_token),
        ],
    )
    assert result.exit_code == 0, result.output
    assert out_token.exists()
    data = json.loads(out_token.read_text())
    assert data["subdomain"] == "mcsc-vic"
    assert data["cookie"] == "ABC123"


def test_auth_compass_rejects_empty_cookie(tmp_path: Path):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "auth",
            "compass",
            "--subdomain",
            "mcsc-vic",
            "--cookie",
            "   ",
            "--token-path",
            str(tmp_path / "x.json"),
        ],
    )
    assert result.exit_code != 0
    assert "empty" in result.output.lower()


def test_auth_edrolo_invokes_headed_login(tmp_path: Path):
    out_token = tmp_path / "james-edrolo.json"
    runner = CliRunner()
    with patch("homework_hub.sources.edrolo.run_headed_login") as mock_login:
        result = runner.invoke(
            cli,
            [
                "auth",
                "edrolo",
                "--child",
                "james",
                "--token-path",
                str(out_token),
            ],
        )
    assert result.exit_code == 0, result.output
    mock_login.assert_called_once()
    # First positional arg is the output path
    call_args = mock_login.call_args
    assert call_args.args[0] == out_token
    assert "saved" in result.output.lower()


def test_auth_classroom_runs_headed_login(tmp_path: Path):
    out_token = tmp_path / "out.json"

    runner = CliRunner()
    with patch("homework_hub.sources.classroom.run_headed_login") as mock_login:
        result = runner.invoke(
            cli,
            [
                "auth",
                "classroom",
                "--child",
                "james",
                "--token-path",
                str(out_token),
            ],
        )
    assert result.exit_code == 0, result.output
    mock_login.assert_called_once()
    call_args = mock_login.call_args
    assert call_args.args[0] == out_token
    assert "saved" in result.output.lower()


def test_auth_requires_child():
    """Edrolo (and other per-child) auth commands require --child."""
    runner = CliRunner()
    result = runner.invoke(cli, ["auth", "edrolo"])
    assert result.exit_code != 0


def test_auth_compass_requires_subdomain():
    runner = CliRunner()
    result = runner.invoke(cli, ["auth", "compass"])
    assert result.exit_code != 0


def test_bootstrap_sheet_requires_child():
    runner = CliRunner()
    result = runner.invoke(cli, ["bootstrap-sheet"])
    assert result.exit_code != 0


def test_status(tmp_path: Path):
    env = _write_min_config(tmp_path, sheet_id="sheet-abc")
    runner = CliRunner()
    result = runner.invoke(cli, ["status"], env=env)
    assert result.exit_code == 0, result.output
    assert "james" in result.output
    assert "sheet-abc" in result.output
    # No syncs yet -> 'never synced' for each source.
    assert "never synced" in result.output


def test_bootstrap_sheet_writes_id_back_to_config(tmp_path: Path):
    env = _write_min_config(tmp_path, sheet_id=None)
    runner = CliRunner()
    fake_backend = type(
        "FakeBackend",
        (),
        {"create_sheet": lambda self, title, *, share_with=None: "new-sheet-id"},
    )()
    with patch("homework_hub.__main__._build_sheets_backend", return_value=fake_backend):
        result = runner.invoke(
            cli,
            ["bootstrap-sheet", "--child", "james", "--share-with", "kid@example.com"],
            env=env,
        )
    assert result.exit_code == 0, result.output
    assert "new-sheet-id" in result.output
    children_yaml = Path(env["HOMEWORK_HUB_CONFIG_DIR"]) / "children.yaml"
    assert "new-sheet-id" in children_yaml.read_text()


def test_bootstrap_sheet_refuses_when_already_set(tmp_path: Path):
    env = _write_min_config(tmp_path, sheet_id="existing-id")
    runner = CliRunner()
    result = runner.invoke(cli, ["bootstrap-sheet", "--child", "james"], env=env)
    assert result.exit_code != 0
    assert "already" in result.output.lower()


# --------------------------------------------------------------------------- #
# subjects subcommands
# --------------------------------------------------------------------------- #


def _write_subjects_yaml(config_dir: Path) -> Path:
    """Drop a tiny subjects.yaml into config_dir and return its path."""
    path = config_dir / "subjects.yaml"
    path.write_text(
        yaml.safe_dump(
            {
                "rules": [
                    {
                        "match": "exact",
                        "pattern": "9SCI2 (2026 Academic)",
                        "canonical": "Year 9 Science",
                        "short": "Sci",
                    },
                    {
                        "match": "prefix",
                        "pattern": "9SCI",
                        "canonical": "Year 9 Science",
                        "short": "Sci",
                    },
                    {
                        "match": "regex",
                        "pattern": r"^VCE Methods.*",
                        "canonical": "Year 11 Maths",
                        "short": "Maths",
                    },
                ]
            }
        )
    )
    return path


def test_subjects_list_empty(tmp_path: Path):
    env = _write_min_config(tmp_path)
    runner = CliRunner()
    result = runner.invoke(cli, ["subjects", "list"], env=env)
    assert result.exit_code == 0, result.output
    assert "No rules" in result.output


def test_subjects_seed_then_list(tmp_path: Path):
    env = _write_min_config(tmp_path)
    _write_subjects_yaml(Path(env["HOMEWORK_HUB_CONFIG_DIR"]))
    runner = CliRunner()

    seed_res = runner.invoke(cli, ["subjects", "seed"], env=env)
    assert seed_res.exit_code == 0, seed_res.output
    assert "Seeded 3" in seed_res.output

    list_res = runner.invoke(cli, ["subjects", "list"], env=env)
    assert list_res.exit_code == 0, list_res.output
    assert "9SCI2 (2026 Academic)" in list_res.output
    assert "VCE Methods" in list_res.output
    assert "3 rule(s)" in list_res.output


def test_subjects_seed_replace(tmp_path: Path):
    env = _write_min_config(tmp_path)
    _write_subjects_yaml(Path(env["HOMEWORK_HUB_CONFIG_DIR"]))
    runner = CliRunner()
    runner.invoke(cli, ["subjects", "seed"], env=env)
    # Re-seed with --replace; should still report 3 rules total.
    res = runner.invoke(cli, ["subjects", "seed", "--replace"], env=env)
    assert res.exit_code == 0, res.output
    assert "replaced" in res.output


def test_subjects_seed_missing_file(tmp_path: Path):
    env = _write_min_config(tmp_path)
    runner = CliRunner()
    res = runner.invoke(cli, ["subjects", "seed"], env=env)
    assert res.exit_code != 0
    assert "not found" in res.output.lower()


def test_subjects_test_match_and_no_match(tmp_path: Path):
    env = _write_min_config(tmp_path)
    _write_subjects_yaml(Path(env["HOMEWORK_HUB_CONFIG_DIR"]))
    runner = CliRunner()
    runner.invoke(cli, ["subjects", "seed"], env=env)

    hit = runner.invoke(cli, ["subjects", "test", "9SCI Year 9 Science"], env=env)
    assert hit.exit_code == 0, hit.output
    assert "Year 9 Science" in hit.output
    assert "Sci" in hit.output

    miss = runner.invoke(cli, ["subjects", "test", "Quidditch 101"], env=env)
    assert miss.exit_code == 1
    assert "no match" in miss.output.lower()


def test_subjects_add_and_remove(tmp_path: Path):
    env = _write_min_config(tmp_path)
    runner = CliRunner()

    add = runner.invoke(
        cli,
        [
            "subjects",
            "add",
            "--type",
            "exact",
            "--pattern",
            "11BIO3",
            "--canonical",
            "Year 11 Biology",
            "--short",
            "Bio",
        ],
        env=env,
    )
    assert add.exit_code == 0, add.output
    assert "Added rule" in add.output

    listing = runner.invoke(cli, ["subjects", "list"], env=env)
    assert "11BIO3" in listing.output

    rm = runner.invoke(
        cli,
        ["subjects", "remove", "--type", "exact", "--pattern", "11BIO3"],
        env=env,
    )
    assert rm.exit_code == 0, rm.output
    assert "Removed 1" in rm.output

    rm_again = runner.invoke(
        cli,
        ["subjects", "remove", "--type", "exact", "--pattern", "11BIO3"],
        env=env,
    )
    assert rm_again.exit_code != 0


def test_subjects_add_invalid_regex(tmp_path: Path):
    env = _write_min_config(tmp_path)
    runner = CliRunner()
    res = runner.invoke(
        cli,
        [
            "subjects",
            "add",
            "--type",
            "regex",
            "--pattern",
            "[unclosed",
            "--canonical",
            "Bad",
            "--short",
            "B",
        ],
        env=env,
    )
    assert res.exit_code != 0


# --------------------------------------------------------------------------- #
# links subcommands
# --------------------------------------------------------------------------- #


def _seed_link_pair(state_db: Path) -> None:
    """Insert a Compass↔Classroom pair for james that the detector will flag."""
    import sqlite3
    from contextlib import closing
    from datetime import UTC, datetime

    from homework_hub.state.store import StateStore

    StateStore(state_db)  # ensure schema
    due = datetime(2026, 5, 1, tzinfo=UTC).isoformat()
    now = datetime.now(UTC).isoformat()
    with closing(sqlite3.connect(state_db)) as conn, conn:
        for source, source_id, title in [
            ("compass", "C1", "WW1 Benchmark"),
            ("classroom", "K1", "WW1"),
        ]:
            conn.execute(
                "INSERT INTO silver_tasks "
                "(child, source, source_id, subject_raw, subject_canonical, "
                "subject_short, title, status, last_synced, due_at) "
                "VALUES ('james', ?, ?, '', 'Year 9 Humanities', 'Hum', "
                "?, 'not_started', ?, ?)",
                (source, source_id, title, now, due),
            )


def test_links_list_empty(tmp_path: Path):
    env = _write_min_config(tmp_path)
    runner = CliRunner()
    res = runner.invoke(cli, ["links", "list"], env=env)
    assert res.exit_code == 0, res.output
    assert "No links" in res.output


def test_links_detect_then_list(tmp_path: Path):
    env = _write_min_config(tmp_path)
    _seed_link_pair(Path(env["HOMEWORK_HUB_STATE_DB"]))
    runner = CliRunner()

    detect = runner.invoke(cli, ["links", "detect"], env=env)
    assert detect.exit_code == 0, detect.output
    assert "inserted=1" in detect.output

    listing = runner.invoke(cli, ["links", "list"], env=env)
    assert listing.exit_code == 0, listing.output
    assert "auto_high" in listing.output
    assert "compass:C1" in listing.output
    assert "classroom:K1" in listing.output


def test_links_detect_specific_child(tmp_path: Path):
    env = _write_min_config(tmp_path)
    _seed_link_pair(Path(env["HOMEWORK_HUB_STATE_DB"]))
    runner = CliRunner()
    res = runner.invoke(cli, ["links", "detect", "--child", "james"], env=env)
    assert res.exit_code == 0, res.output
    assert "james: inserted=1" in res.output
