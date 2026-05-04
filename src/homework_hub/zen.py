"""Zen Browser launch helpers for the EP token refresh flow.

These helpers exist so ``refresh-ep`` is a single self-sufficient command:
detect whether Zen is reachable on Marionette, prompt to kill any existing
Zen instance that lacks Marionette, and spawn a fresh one with the right
flags pointed at James's profile.

Kept separate from ``sources/eduperfect.py`` because the launch concerns are
CLI-side (process management, user prompts) rather than source-side (HTTP
session, GraphQL).
"""

from __future__ import annotations

import contextlib
import os
import socket
import subprocess
import time
from pathlib import Path

ZEN_BINARY = Path("/Applications/Zen.app/Contents/MacOS/zen")
ZEN_PROFILE = Path.home() / "Library/Application Support/zen/Profiles/cvigrd5k.Default (release)"
DEFAULT_PORT = 2828
EP_DASHBOARD_URL = "https://app.educationperfect.com/learning/dashboard"
ZEN_LOG_PATH = Path("/tmp/zen-marionette.log")


def marionette_reachable(port: int = DEFAULT_PORT, timeout: float = 1.0) -> bool:
    """Return True if a TCP connection to Marionette succeeds."""
    try:
        with socket.create_connection(("localhost", port), timeout=timeout):
            return True
    except OSError:
        return False


def find_zen_processes() -> list[int]:
    """Return PIDs of running Zen Browser processes.

    Uses ``pgrep -f`` against the full Zen binary path so we don't accidentally
    match other processes that happen to mention "zen".
    """
    try:
        result = subprocess.run(
            ["pgrep", "-f", str(ZEN_BINARY)],
            capture_output=True,
            text=True,
            check=False,
        )
    except FileNotFoundError:
        return []
    if result.returncode != 0:
        return []
    return [int(line) for line in result.stdout.split() if line.strip().isdigit()]


def kill_zen_processes(pids: list[int], wait_timeout: float = 5.0) -> None:
    """Send SIGTERM to each PID and wait for them to exit.

    Polls ``find_zen_processes`` until empty or ``wait_timeout`` elapses, then
    falls back to SIGKILL for any survivors.
    """
    for pid in pids:
        with contextlib.suppress(ProcessLookupError):
            os.kill(pid, 15)  # SIGTERM

    deadline = time.monotonic() + wait_timeout
    while time.monotonic() < deadline:
        if not find_zen_processes():
            return
        time.sleep(0.2)

    # Anything still alive gets SIGKILL.
    for pid in find_zen_processes():
        with contextlib.suppress(ProcessLookupError):
            os.kill(pid, 9)
    time.sleep(0.3)


def launch_zen_with_marionette(
    *,
    port: int = DEFAULT_PORT,
    profile: Path = ZEN_PROFILE,
    binary: Path = ZEN_BINARY,
    open_url: str | None = EP_DASHBOARD_URL,
    log_path: Path = ZEN_LOG_PATH,
) -> int:
    """Spawn Zen with Marionette flags. Returns the child PID.

    The process is detached (``start_new_session=True``) so it survives this
    CLI invocation. Stdout/stderr go to ``log_path`` for post-mortem.
    """
    if not binary.exists():
        raise RuntimeError(f"Zen Browser not found at {binary}")
    if not profile.exists():
        raise RuntimeError(
            f"Zen profile not found at {profile}\n"
            "Open Zen at least once and ensure the profile path matches."
        )

    args = [
        str(binary),
        "--marionette",
        "--marionette-port",
        str(port),
        "--remote-allow-system-access",
        "--profile",
        str(profile),
    ]
    if open_url:
        args.append(open_url)

    log_handle = open(log_path, "ab")  # noqa: SIM115 — handed to Popen
    proc = subprocess.Popen(
        args,
        stdout=log_handle,
        stderr=log_handle,
        stdin=subprocess.DEVNULL,
        start_new_session=True,
    )
    return proc.pid


def wait_for_marionette(port: int = DEFAULT_PORT, timeout: float = 20.0) -> bool:
    """Poll until Marionette accepts a connection or ``timeout`` elapses."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if marionette_reachable(port):
            return True
        time.sleep(0.5)
    return False
