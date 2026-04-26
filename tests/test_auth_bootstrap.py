"""Tests for ``homework_hub.auth_bootstrap``.

The OAuth flow itself can't be exercised without a browser, so these
tests cover everything *around* the flow:

* Cached-token-valid path returns existing credentials untouched.
* Cached-but-expired token with refresh token gets refreshed + persisted.
* Failed refresh falls through to the consent flow.
* Corrupt token file is ignored, falling through to the consent flow.
* Consent flow path invokes ``InstalledAppFlow`` correctly and persists
  the resulting token.
* ``BootstrapAuthError`` is raised when the BW note isn't valid JSON.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from homework_hub import auth_bootstrap
from homework_hub.auth_bootstrap import (
    BOOTSTRAP_TOKEN_FILENAME,
    BootstrapAuthError,
    _load_client_config,
    load_or_run_bootstrap_flow,
)


def _fake_bw_with_client_config() -> MagicMock:
    bw = MagicMock()
    bw.get_notes.return_value = json.dumps(
        {
            "installed": {
                "client_id": "x.apps.googleusercontent.com",
                "project_id": "homework-hub",
                "client_secret": "shh",
                "redirect_uris": ["http://localhost"],
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
            }
        }
    )
    return bw


# --------------------------------------------------------------------------- #
# _load_client_config
# --------------------------------------------------------------------------- #


def test_load_client_config_parses_valid_json():
    bw = _fake_bw_with_client_config()
    cfg = _load_client_config(bw)
    assert "installed" in cfg


def test_load_client_config_raises_on_invalid_json():
    bw = MagicMock()
    bw.get_notes.return_value = "not json {"
    with pytest.raises(BootstrapAuthError, match="not valid JSON"):
        _load_client_config(bw)


# --------------------------------------------------------------------------- #
# load_or_run_bootstrap_flow — cached paths
# --------------------------------------------------------------------------- #


def test_returns_cached_token_when_valid(tmp_path: Path):
    token_path = tmp_path / BOOTSTRAP_TOKEN_FILENAME
    token_path.write_text("{}")  # content irrelevant; we mock loader

    fake_creds = MagicMock()
    fake_creds.valid = True

    with patch.object(
        auth_bootstrap.Credentials,
        "from_authorized_user_file",
        return_value=fake_creds,
    ) as loader:
        result = load_or_run_bootstrap_flow(tokens_dir=tmp_path, bw=MagicMock())

    loader.assert_called_once()
    assert result.credentials is fake_creds
    assert result.token_path == token_path


def test_refreshes_expired_token_when_refresh_token_present(tmp_path: Path):
    token_path = tmp_path / BOOTSTRAP_TOKEN_FILENAME
    token_path.write_text("{}")

    fake_creds = MagicMock()
    fake_creds.valid = False
    fake_creds.expired = True
    fake_creds.refresh_token = "refresh-me"
    fake_creds.to_json.return_value = '{"refreshed": true}'

    with patch.object(
        auth_bootstrap.Credentials,
        "from_authorized_user_file",
        return_value=fake_creds,
    ):
        result = load_or_run_bootstrap_flow(tokens_dir=tmp_path, bw=MagicMock())

    fake_creds.refresh.assert_called_once()
    assert result.credentials is fake_creds
    assert token_path.read_text() == '{"refreshed": true}'


def test_falls_through_to_flow_when_refresh_fails(tmp_path: Path):
    token_path = tmp_path / BOOTSTRAP_TOKEN_FILENAME
    token_path.write_text("{}")

    expired_creds = MagicMock()
    expired_creds.valid = False
    expired_creds.expired = True
    expired_creds.refresh_token = "refresh-me"
    expired_creds.refresh.side_effect = RuntimeError("boom")

    new_creds = MagicMock()
    new_creds.to_json.return_value = '{"new": true}'

    flow = MagicMock()
    flow.run_local_server.return_value = new_creds

    bw = _fake_bw_with_client_config()

    with (
        patch.object(
            auth_bootstrap.Credentials,
            "from_authorized_user_file",
            return_value=expired_creds,
        ),
        patch.object(
            auth_bootstrap.InstalledAppFlow,
            "from_client_config",
            return_value=flow,
        ) as ff,
    ):
        result = load_or_run_bootstrap_flow(tokens_dir=tmp_path, bw=bw)

    ff.assert_called_once()
    flow.run_local_server.assert_called_once()
    assert result.credentials is new_creds
    assert token_path.read_text() == '{"new": true}'


def test_corrupt_token_is_ignored_and_flow_runs(tmp_path: Path):
    token_path = tmp_path / BOOTSTRAP_TOKEN_FILENAME
    token_path.write_text("{not json")

    new_creds = MagicMock()
    new_creds.to_json.return_value = "{}"

    flow = MagicMock()
    flow.run_local_server.return_value = new_creds

    bw = _fake_bw_with_client_config()

    # Real loader will fail with ValueError/JSONDecodeError on the corrupt
    # file — _load_cached_token swallows it and returns None.
    with patch.object(
        auth_bootstrap.InstalledAppFlow,
        "from_client_config",
        return_value=flow,
    ):
        result = load_or_run_bootstrap_flow(tokens_dir=tmp_path, bw=bw)

    assert result.credentials is new_creds
    flow.run_local_server.assert_called_once()


# --------------------------------------------------------------------------- #
# load_or_run_bootstrap_flow — fresh consent path
# --------------------------------------------------------------------------- #


def test_runs_flow_when_no_cached_token(tmp_path: Path):
    new_creds = MagicMock()
    new_creds.to_json.return_value = '{"fresh": true}'

    flow = MagicMock()
    flow.run_local_server.return_value = new_creds

    bw = _fake_bw_with_client_config()

    with patch.object(
        auth_bootstrap.InstalledAppFlow,
        "from_client_config",
        return_value=flow,
    ) as ff:
        result = load_or_run_bootstrap_flow(tokens_dir=tmp_path, bw=bw, open_browser=True)

    ff.assert_called_once()
    flow.run_local_server.assert_called_once_with(port=0, open_browser=True)
    token_path = tmp_path / BOOTSTRAP_TOKEN_FILENAME
    assert token_path.read_text() == '{"fresh": true}'
    assert result.token_path == token_path


def test_runs_flow_with_open_browser_false(tmp_path: Path):
    new_creds = MagicMock()
    new_creds.to_json.return_value = "{}"

    flow = MagicMock()
    flow.run_local_server.return_value = new_creds

    with patch.object(
        auth_bootstrap.InstalledAppFlow,
        "from_client_config",
        return_value=flow,
    ):
        load_or_run_bootstrap_flow(
            tokens_dir=tmp_path,
            bw=_fake_bw_with_client_config(),
            open_browser=False,
        )

    flow.run_local_server.assert_called_once_with(port=0, open_browser=False)


def test_creates_tokens_dir_if_missing(tmp_path: Path):
    nested = tmp_path / "deep" / "tokens"
    new_creds = MagicMock()
    new_creds.to_json.return_value = "{}"
    flow = MagicMock()
    flow.run_local_server.return_value = new_creds

    with patch.object(
        auth_bootstrap.InstalledAppFlow,
        "from_client_config",
        return_value=flow,
    ):
        load_or_run_bootstrap_flow(tokens_dir=nested, bw=_fake_bw_with_client_config())

    assert (nested / BOOTSTRAP_TOKEN_FILENAME).exists()


def test_custom_scopes_are_propagated(tmp_path: Path):
    new_creds = MagicMock()
    new_creds.to_json.return_value = "{}"
    flow = MagicMock()
    flow.run_local_server.return_value = new_creds

    custom_scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

    with patch.object(
        auth_bootstrap.InstalledAppFlow,
        "from_client_config",
        return_value=flow,
    ) as ff:
        load_or_run_bootstrap_flow(
            tokens_dir=tmp_path,
            bw=_fake_bw_with_client_config(),
            scopes=custom_scopes,
        )

    args, _ = ff.call_args
    assert args[1] == custom_scopes
