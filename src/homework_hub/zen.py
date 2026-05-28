"""Zen Browser launch and cookie-extraction helpers.

Three families of helpers live here:

1. **Marionette launch** — ``refresh-ep`` is a single self-sufficient command
   that detects whether Zen is reachable on Marionette, prompts to kill any
   existing Zen instance that lacks Marionette, and spawns a fresh one with
   the right flags pointed at the requested child's profile.

2. **Marionette cookie login** — ``zen_cookie_login`` opens a new tab in the
   running Zen Browser, waits for the user to complete sign-in, extracts all
   cookies via the chrome context, then closes the tab. This avoids
   Playwright's Chromium (which Google detects as a bot) by reusing the
   user's real Zen profile and fingerprint.

3. **SQLite extraction** — ``extract_cookies_from_zen_sqlite`` reads cookies
   directly from Zen's ``cookies.sqlite`` store without requiring Marionette,
   so we can harvest existing sessions while the user's normal (non-Marionette)
   Zen instance is still running. Container-aware via the ``originAttributes``
   column. Useful for child sessions that persist on disk (Edrolo
   ``sessionid``, Google ``SID``) but not for in-memory session cookies
   (EP ``access_token``, Compass ``ASP.NET_SessionId``).

Kept separate from ``sources/*.py`` because launch and extraction concerns are
CLI-side (process management, user prompts, file IO) rather than source-side
(HTTP session, GraphQL).
"""

from __future__ import annotations

import contextlib
import json
import os
import shutil
import socket
import sqlite3
import subprocess
import tempfile
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

ZEN_BINARY = Path("/Applications/Zen.app/Contents/MacOS/zen")
ZEN_PROFILE = Path.home() / "Library/Application Support/zen/Profiles/cvigrd5k.Default (release)"
DEFAULT_PORT = 2828
EP_DASHBOARD_URL = "https://app.educationperfect.com/learning/dashboard"
ZEN_LOG_PATH = Path("/tmp/zen-marionette.log")


def get_child_profile_path(child: str | None) -> Path:
    """Return the profile path for the given child, or the default profile."""
    if not child:
        return ZEN_PROFILE
    from homework_hub.config import Settings

    settings = Settings()
    path = (settings.tokens_dir / "profiles" / child).resolve()
    path.mkdir(parents=True, exist_ok=True)
    return path


def is_zen_running_with_profile(profile_path: Path) -> bool:
    """Return True if Zen is running with the specified profile path."""
    try:
        result = subprocess.run(
            ["ps", "-A", "-o", "pid,command"],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            return False

        profile_str = str(profile_path)
        for line in result.stdout.splitlines():
            if str(ZEN_BINARY) in line and profile_str in line:
                return True
    except Exception:
        pass
    return False


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


# --------------------------------------------------------------------------- #
# SQLite cookie extraction (no Marionette required)
# --------------------------------------------------------------------------- #


_SAMESITE_MAP: dict[int, str] = {0: "None", 1: "Lax", 2: "Strict"}


def _normalise_expiry(expiry: int | None) -> int:
    """Convert Firefox ``moz_cookies.expiry`` to Playwright ``expires``.

    Firefox stores ``expiry`` as a Unix timestamp in seconds for persistent
    cookies and ``0`` for session cookies. Some Firefox builds emit
    millisecond timestamps; we detect and downscale anything > 10 digits.
    Non-positive values become ``-1`` (Playwright's session-cookie marker).
    """
    if not isinstance(expiry, int) or expiry <= 0:
        return -1
    if expiry > 9_999_999_999:  # > 10 digits → milliseconds (or finer)
        return expiry // 1000
    return expiry


def extract_cookies_from_zen_sqlite(
    profile_path: Path,
    domain_suffixes: list[str],
    *,
    origin_attrs: str = "",
) -> list[dict[str, Any]]:
    """Extract cookies directly from a Zen Browser ``cookies.sqlite`` file.

    Reads cookies from ``<profile>/cookies.sqlite`` without requiring
    Marionette — Zen can be running normally (or not at all). The database
    and its WAL/SHM sidecars are copied to a temp directory first so we
    don't compete with Firefox for the file lock and so any uncommitted
    WAL writes are visible.

    *domain_suffixes* are bare hostnames (no leading dot). A cookie matches
    if its ``host`` column equals the suffix exactly, equals ``"." + suffix``,
    or ends with ``"." + suffix`` (catching sub-domain cookies like
    ``app.edrolo.com`` for suffix ``edrolo.com``).

    *origin_attrs* filters by Firefox's container/partition key. Use ``""``
    (default) for the parent container, ``"^userContextId=1"`` for the first
    Multi-Account Container, etc. The match is an exact string comparison
    against ``moz_cookies.originAttributes``.

    Returns Playwright ``storage_state`` cookie dicts.
    """
    cookies_db = profile_path / "cookies.sqlite"
    if not cookies_db.exists():
        raise RuntimeError(f"Zen cookies DB not found at {cookies_db}")

    with tempfile.TemporaryDirectory(prefix="zen-cookies-") as td:
        tmp_dir = Path(td)
        tmp_db = tmp_dir / "cookies.sqlite"
        shutil.copy2(cookies_db, tmp_db)
        # Copy WAL/SHM siblings if present so SQLite reconstructs the full
        # state. Missing sidecars are non-fatal (no in-flight transactions).
        for suffix in ("-wal", "-shm"):
            sidecar = profile_path / f"cookies.sqlite{suffix}"
            if sidecar.exists():
                shutil.copy2(sidecar, tmp_dir / f"cookies.sqlite{suffix}")

        conn = sqlite3.connect(str(tmp_db))
        try:
            conn.row_factory = sqlite3.Row
            # Build the WHERE clause: originAttributes match + at least one
            # domain-suffix match.
            domain_clauses = []
            params: list[Any] = [origin_attrs]
            for suffix in domain_suffixes:
                domain_clauses.append("(host = ? OR host = ? OR host LIKE ?)")
                params.extend([suffix, f".{suffix}", f"%.{suffix}"])
            where = "originAttributes = ?"
            if domain_clauses:
                where += " AND (" + " OR ".join(domain_clauses) + ")"
            sql = (
                "SELECT name, value, host, path, expiry, "
                "isSecure, isHttpOnly, sameSite "
                f"FROM moz_cookies WHERE {where}"
            )
            rows = conn.execute(sql, params).fetchall()
        finally:
            conn.close()

    cookies: list[dict[str, Any]] = []
    for row in rows:
        cookies.append(
            {
                "name": row["name"] or "",
                "value": row["value"] or "",
                "domain": row["host"] or "",
                "path": row["path"] or "/",
                "expires": _normalise_expiry(row["expiry"]),
                "httpOnly": bool(row["isHttpOnly"]),
                "secure": bool(row["isSecure"]),
                "sameSite": _SAMESITE_MAP.get(int(row["sameSite"] or 0), "None"),
            }
        )
    return cookies


# --------------------------------------------------------------------------- #
# Marionette cookie-based login helper (Classroom / Edrolo)
# --------------------------------------------------------------------------- #

_MSG_ID: list[int] = [0]


def _send(s: socket.socket, cmd: str, params: dict[str, Any]) -> None:
    mid = _MSG_ID[0]
    _MSG_ID[0] += 1
    msg = json.dumps([0, mid, cmd, params])
    s.sendall(f"{len(msg)}:{msg}".encode())


class MarionetteConnectionLost(RuntimeError):  # noqa: N818
    """Raised when the Marionette socket is closed or reset by the peer.

    The "Lost" suffix reads as a state, not a generic error indicator, so
    we deliberately don't follow the ``*Error`` convention here.
    """


def _recv(s: socket.socket, timeout: float = 30.0) -> Any:
    """Read one length-prefixed Marionette frame.

    Raises :class:`MarionetteConnectionLost` if the peer closes the socket
    (empty ``recv``) or resets the connection (``ConnectionResetError``).
    Callers can use this to detect Zen Browser exiting mid-flow and surface
    a helpful error rather than spinning or crashing.
    """
    buf = b""
    s.settimeout(timeout)
    while True:
        try:
            chunk = s.recv(8192)
        except ConnectionResetError as exc:
            raise MarionetteConnectionLost(
                "Marionette connection reset by Zen Browser — "
                "the browser likely closed or crashed."
            ) from exc
        if not chunk:
            raise MarionetteConnectionLost(
                "Marionette socket closed by Zen Browser — " "the browser likely closed or crashed."
            )
        buf += chunk
        try:
            colon = buf.index(b":")
            length = int(buf[:colon])
            rest = buf[colon + 1 :]
            if len(rest) >= length:
                return json.loads(rest[:length])
        except (ValueError, json.JSONDecodeError):
            continue


def zen_cookie_login(
    url: str,
    is_logged_in: Callable[[str], bool],
    *,
    port: int = DEFAULT_PORT,
    login_timeout: float = 300.0,
    poll_interval: float = 1.0,
    warmup_urls: list[str] | None = None,
    extra_cookie_hosts: list[str] | None = None,
) -> list[dict[str, Any]]:
    """Open a new tab in Zen Browser, navigate to *url*, wait for login, return cookies.

    *is_logged_in* is called with the current URL on each poll tick; return
    ``True`` when the user has successfully authenticated.

    *warmup_urls*, if provided, is a list of additional URLs the tab is
    navigated to AFTER login is detected and BEFORE cookies are collected.
    This forces the browser to complete SSO handshakes for other Google
    properties (e.g. ``docs.google.com``) so their session cookies are
    materialised in the cookie store. Each warmup URL is loaded and given a
    short settle window; non-fatal on individual failure.

    *extra_cookie_hosts*, if provided, expands the set of cookie domains
    returned beyond the host of *url*. Used together with *warmup_urls* so we
    can keep cookies for docs.google.com / forms.gle when warming those.

    Returns a list of cookie dicts in Playwright ``storage_state`` format::

        [{"name": ..., "value": ..., "domain": ..., "path": ...,
          "expires": ..., "httpOnly": ..., "secure": ..., "sameSite": ...}]

    Raises ``RuntimeError`` if Marionette is unreachable, the login times out,
    or the tab cannot be managed.
    """
    _MSG_ID[0] = 0

    try:
        s = socket.socket()
        s.settimeout(5)
        s.connect(("localhost", port))
        s.recv(1024)  # greeting
    except OSError as exc:
        raise RuntimeError(
            f"Cannot connect to Zen Browser Marionette on port {port}. "
            "Ensure Zen is running with --marionette."
        ) from exc

    _send(s, "WebDriver:NewSession", {})
    _recv(s)

    # Open a new tab and switch to it.
    _send(s, "WebDriver:NewWindow", {"type": "tab"})
    resp = _recv(s)
    try:
        handle = resp[3]["handle"]
    except (IndexError, KeyError, TypeError) as exc:
        raise RuntimeError(f"Unexpected NewWindow response: {resp}") from exc

    _send(s, "WebDriver:SwitchToWindow", {"handle": handle, "focus": True})
    _recv(s)

    # Navigate to the login page. Marionette returns after page load.
    _send(s, "WebDriver:Navigate", {"url": url})
    _recv(s)

    # Poll until logged in.
    deadline = time.monotonic() + login_timeout
    while time.monotonic() < deadline:
        _send(s, "WebDriver:GetCurrentURL", {})
        url_resp = _recv(s)
        try:
            current = url_resp[3]["value"]
        except (IndexError, KeyError, TypeError):
            current = ""
        if is_logged_in(current):
            break
        time.sleep(poll_interval)
    else:
        _send(s, "WebDriver:CloseWindow", {})
        _recv(s)
        raise RuntimeError(
            f"Login timed out after {login_timeout:.0f}s — "
            "complete sign-in in the Zen Browser window."
        )

    # Brief settle so all cookies flush.
    time.sleep(2.0)

    # Optional warmup: navigate to additional Google properties so SSO
    # handshakes complete and their session cookies materialise. Workspace
    # accounts often only complete SSO on first visit per origin — without
    # this, ``.google.com`` parent cookies (SID/SAPISID) authenticate
    # Classroom but Drive/Forms still bounce to accounts.google.com.
    for warmup in warmup_urls or []:
        _send(s, "WebDriver:Navigate", {"url": warmup})
        _recv(s)
        # Wait for redirects to settle. A warmup failure is non-fatal; we
        # still want to collect whatever cookies were issued.
        time.sleep(3.0)

    # Collect cookies via the chrome context so we get all cookies from the
    # full Firefox cookie store — including httpOnly and parent-domain cookies
    # (e.g. SID on .google.com) that WebDriver:GetCookies misses because it
    # only returns cookies accessible from the current page's exact URL.
    _send(s, "Marionette:SetContext", {"value": "chrome"})
    _recv(s)
    cookie_script = """
    const out = [];
    for (const c of Services.cookies.cookies) {
        out.push({
            name: c.name,
            value: c.value,
            domain: (c.isDomain ? "." : "") + c.rawHost,
            path: c.path,
            expiry: c.isSession ? -1 : c.expiry,
            httpOnly: c.isHttpOnly,
            secure: c.isSecure,
            sameSite: (["None", "Lax", "Strict"])[c.sameSite] || "None",
        });
    }
    return JSON.stringify(out);
    """
    _send(s, "WebDriver:ExecuteScript", {"script": cookie_script, "args": []})
    cookie_resp = _recv(s)

    # Switch back to content context before closing.
    _send(s, "Marionette:SetContext", {"value": "content"})
    _recv(s)

    # Close the tab.
    _send(s, "WebDriver:CloseWindow", {})
    _recv(s)
    s.close()

    try:
        raw_json = cookie_resp[3]["value"]
        raw_cookies: list[dict[str, Any]] = json.loads(raw_json)
    except (IndexError, KeyError, TypeError, json.JSONDecodeError):
        raw_cookies = []

    # Filter to domains relevant to the login URL (and any extra hosts the
    # caller has warmed up) so we don't return the entire browser cookie jar.
    import urllib.parse

    login_host = urllib.parse.urlparse(url).hostname or ""
    allowed_hosts = [login_host, *(extra_cookie_hosts or [])]

    def _matches(domain: str) -> bool:
        d = domain.lstrip(".")
        return any(host == d or host.endswith("." + d) for host in allowed_hosts if host)

    # Normalise to Playwright storage_state cookie format.
    cookies: list[dict[str, Any]] = []
    for c in raw_cookies:
        if _matches(c.get("domain", "")):
            expiry = c.get("expiry", -1)
            # Playwright requires expires == -1 (session) or a positive unix timestamp.
            # Expired/zero values from the cookie store must be clamped to -1.
            if not isinstance(expiry, int) or expiry <= 0:
                expiry = -1
            elif expiry > 9999999999:  # > 10 digits implies milliseconds (or microseconds)
                expiry = expiry // 1000
            # Modern browsers (and Playwright) silently drop cookies whose
            # ``SameSite=None`` is paired with ``Secure=False`` because that
            # combination is forbidden by the cookie spec. Google's auth
            # cookies (SID/HSID/APISID) come through the Firefox cookie
            # store as ``secure=False; sameSite=None`` for legacy reasons
            # but are only ever sent over HTTPS in practice. Force
            # ``Secure=True`` for these so Playwright actually replays them.
            same_site = c.get("sameSite", "None")
            secure = c.get("secure", False)
            if same_site == "None" and not secure:
                secure = True
            cookies.append(
                {
                    "name": c.get("name", ""),
                    "value": c.get("value", ""),
                    "domain": c.get("domain", ""),
                    "path": c.get("path", "/"),
                    "expires": expiry,
                    "httpOnly": c.get("httpOnly", False),
                    "secure": secure,
                    "sameSite": same_site,
                }
            )
    return cookies
