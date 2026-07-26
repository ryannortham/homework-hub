"""Safe persistence helpers for authentication token files."""

from __future__ import annotations

import contextlib
import json
import os
import tempfile
from pathlib import Path
from typing import Any


def safe_write_json(path: Path, payload: dict[str, Any]) -> None:
    """Safely persist sensitive JSON while preserving dataset ACLs.

    Existing files are updated in place so ACL-backed mounts such as the
    TrueNAS token dataset retain the original secure inode and permissions.
    New files use an atomic owner-only rename.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    serialized = (json.dumps(payload, indent=2) + "\n").encode()
    if path.exists():
        original = path.read_bytes()
        try:
            _write_existing(path, serialized)
        except Exception:
            with contextlib.suppress(OSError):
                _write_existing(path, original)
            raise
        return

    fd, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=path.parent,
    )
    temporary_path = Path(temporary_name)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "wb") as handle:
            handle.write(serialized)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_path, path)
        path.chmod(0o600)
    except Exception:
        with contextlib.suppress(OSError):
            os.close(fd)
        with contextlib.suppress(OSError):
            temporary_path.unlink()
        raise


def _write_existing(path: Path, content: bytes) -> None:
    with path.open("r+b") as handle:
        handle.seek(0)
        handle.write(content)
        handle.truncate()
        handle.flush()
        os.fsync(handle.fileno())


__all__ = ["safe_write_json"]
