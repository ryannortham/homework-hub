"""Tests for the CLI scaffold."""

from __future__ import annotations

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


def test_auth_subgroup():
    runner = CliRunner()
    for src in ("classroom", "compass", "edrolo"):
        result = runner.invoke(cli, ["auth", src, "--child", "james"])
        assert result.exit_code == 0
        assert src in result.output


def test_auth_requires_child():
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
