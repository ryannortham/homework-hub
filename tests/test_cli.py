"""Tests for the CLI scaffold."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

from click.testing import CliRunner

from homework_hub.__main__ import cli


def test_cli_help_lists_subcommands():
    runner = CliRunner()
    result = runner.invoke(cli, ["--help"])
    assert result.exit_code == 0
    for cmd in ("sync", "auth", "bootstrap-sheet", "status"):
        assert cmd in result.output


def test_sync_default_all_children():
    runner = CliRunner()
    result = runner.invoke(cli, ["sync"])
    assert result.exit_code == 0
    assert "child=all" in result.output


def test_sync_with_child():
    runner = CliRunner()
    result = runner.invoke(cli, ["sync", "--child", "james"])
    assert result.exit_code == 0
    assert "child=james" in result.output


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


def test_auth_edrolo_stub():
    runner = CliRunner()
    result = runner.invoke(cli, ["auth", "edrolo", "--child", "james"])
    assert result.exit_code == 0
    assert "edrolo" in result.output


def test_auth_classroom_runs_oauth_flow_with_local_secret(tmp_path: Path):
    secret = tmp_path / "secret.json"
    secret.write_text(json.dumps({"installed": {"client_id": "x", "client_secret": "y"}}))
    out_token = tmp_path / "out.json"

    runner = CliRunner()
    with patch("homework_hub.sources.classroom.run_oauth_flow") as mock_flow:
        result = runner.invoke(
            cli,
            [
                "auth",
                "classroom",
                "--child",
                "james",
                "--client-secret-file",
                str(secret),
                "--token-path",
                str(out_token),
            ],
        )
    assert result.exit_code == 0, result.output
    mock_flow.assert_called_once()
    args = mock_flow.call_args[0]
    assert "installed" in args[0]
    assert args[1] == out_token


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


def test_status():
    runner = CliRunner()
    result = runner.invoke(cli, ["status"])
    assert result.exit_code == 0
