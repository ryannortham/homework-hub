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
