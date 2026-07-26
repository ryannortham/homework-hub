"""Tests for safe credential-file persistence."""

from __future__ import annotations

import json
from pathlib import Path

from homework_hub.token_store import safe_write_json


def test_existing_file_preserves_inode_and_permissions(tmp_path: Path):
    path = tmp_path / "token.json"
    path.write_text('{"old": true}')
    path.chmod(0o600)
    original_inode = path.stat().st_ino

    safe_write_json(path, {"new": True})

    assert path.stat().st_ino == original_inode
    assert path.stat().st_mode & 0o777 == 0o600
    assert json.loads(path.read_text()) == {"new": True}


def test_new_file_is_owner_only(tmp_path: Path):
    path = tmp_path / "token.json"

    safe_write_json(path, {"token": "secret"})

    assert path.stat().st_mode & 0o777 == 0o600
    assert json.loads(path.read_text()) == {"token": "secret"}
