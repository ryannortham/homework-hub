"""Bitwarden CLI wrapper for fetching credentials from Vaultwarden at runtime.

Auth model:
    1. ``bw config server <BW_SERVER>`` — points the CLI at our Vaultwarden.
    2. ``bw login --apikey`` with ``BW_CLIENTID`` + ``BW_CLIENTSECRET`` env vars.
    3. ``bw unlock --passwordenv BW_PASSWORD`` → returns a session token.
    4. All subsequent ``bw get item ...`` calls use ``--session <token>``.

The session token is held in memory for the duration of one sync run only;
nothing is persisted to disk. The CLI binary is expected to be on PATH inside
the Docker image.

Tests inject a fake runner so no real ``bw`` invocations happen.
"""

from __future__ import annotations

import json
import os
import subprocess
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Any

# A runner takes the argv (with "bw" already prepended) and returns
# (returncode, stdout, stderr). Default runner shells out via subprocess.
Runner = Callable[[Sequence[str], dict[str, str] | None], tuple[int, str, str]]


def _default_runner(args: Sequence[str], env: dict[str, str] | None = None) -> tuple[int, str, str]:
    proc = subprocess.run(
        list(args),
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )
    return proc.returncode, proc.stdout, proc.stderr


class BitwardenError(RuntimeError):
    """Raised when a `bw` invocation fails or returns unexpected output."""


@dataclass
class BitwardenCLI:
    """Thin wrapper around the Bitwarden CLI."""

    server: str
    client_id: str
    client_secret: str
    master_password: str
    binary: str = "bw"
    runner: Runner = field(default=_default_runner)
    _session: str | None = None

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #

    def unlock(self) -> str:
        """Configure server, log in (idempotent), unlock and cache session token."""
        if self._session:
            return self._session
        self._configure_server()
        self._login_if_needed()
        self._session = self._unlock()
        return self._session

    def lock(self) -> None:
        """Drop the cached session and lock the local vault."""
        if self._session is None:
            return
        self._run([self.binary, "lock"])
        self._session = None

    def get_item(self, name: str) -> dict[str, Any]:
        """Fetch an item by name; returns the parsed JSON object."""
        session = self.unlock()
        env = self._env_with_session(session)
        rc, out, err = self.runner([self.binary, "get", "item", name], env)
        if rc != 0:
            raise BitwardenError(f"bw get item {name!r} failed: {err.strip() or out.strip()}")
        try:
            return json.loads(out)
        except json.JSONDecodeError as exc:
            raise BitwardenError(f"bw get item {name!r} returned non-JSON: {out[:200]}") from exc

    def get_password(self, name: str) -> str:
        item = self.get_item(name)
        login = item.get("login") or {}
        password = login.get("password")
        if not password:
            raise BitwardenError(f"Item {name!r} has no login.password")
        return password

    def get_username(self, name: str) -> str:
        item = self.get_item(name)
        login = item.get("login") or {}
        username = login.get("username")
        if not username:
            raise BitwardenError(f"Item {name!r} has no login.username")
        return username

    def get_notes(self, name: str) -> str:
        item = self.get_item(name)
        notes = item.get("notes")
        if not notes:
            raise BitwardenError(f"Item {name!r} has no notes")
        return notes

    def get_custom_field(self, name: str, field_name: str) -> str:
        """Read a custom field by name from a Bitwarden item."""
        item = self.get_item(name)
        for f in item.get("fields") or []:
            if f.get("name") == field_name:
                value = f.get("value")
                if value is None:
                    raise BitwardenError(f"Item {name!r} field {field_name!r} is empty")
                return value
        raise BitwardenError(f"Item {name!r} has no custom field {field_name!r}")

    # ------------------------------------------------------------------ #
    # Internals
    # ------------------------------------------------------------------ #

    def _configure_server(self) -> None:
        rc, _out, err = self.runner([self.binary, "config", "server", self.server], None)
        if rc != 0:
            raise BitwardenError(f"bw config server failed: {err.strip()}")

    def _login_if_needed(self) -> None:
        # Check status; if unauthenticated, log in via API key.
        rc, out, err = self.runner([self.binary, "status"], None)
        if rc != 0:
            raise BitwardenError(f"bw status failed: {err.strip()}")
        try:
            status = json.loads(out).get("status")
        except json.JSONDecodeError as exc:
            raise BitwardenError(f"bw status returned non-JSON: {out[:200]}") from exc
        if status == "unauthenticated":
            env = {
                **os.environ,
                "BW_CLIENTID": self.client_id,
                "BW_CLIENTSECRET": self.client_secret,
            }
            rc, _out, err = self.runner([self.binary, "login", "--apikey"], env)
            if rc != 0:
                raise BitwardenError(f"bw login --apikey failed: {err.strip()}")

    def _unlock(self) -> str:
        env = {**os.environ, "BW_PASSWORD": self.master_password}
        rc, out, err = self.runner(
            [self.binary, "unlock", "--passwordenv", "BW_PASSWORD", "--raw"], env
        )
        if rc != 0:
            raise BitwardenError(f"bw unlock failed: {err.strip()}")
        token = out.strip()
        if not token:
            raise BitwardenError("bw unlock returned empty session token")
        return token

    def _env_with_session(self, session: str) -> dict[str, str]:
        return {**os.environ, "BW_SESSION": session}

    def _run(self, args: Sequence[str]) -> tuple[int, str, str]:
        return self.runner(args, None)


def from_env() -> BitwardenCLI:
    """Construct a BitwardenCLI from process environment variables."""
    required = ("BW_SERVER", "BW_CLIENTID", "BW_CLIENTSECRET", "BW_PASSWORD")
    missing = [k for k in required if not os.environ.get(k)]
    if missing:
        raise BitwardenError(f"Missing required env vars: {', '.join(missing)}")
    return BitwardenCLI(
        server=os.environ["BW_SERVER"],
        client_id=os.environ["BW_CLIENTID"],
        client_secret=os.environ["BW_CLIENTSECRET"],
        master_password=os.environ["BW_PASSWORD"],
    )
