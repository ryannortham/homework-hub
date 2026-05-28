"""Unit tests for the Zen Browser launch helpers."""

from __future__ import annotations

import socket
import sqlite3
import threading
from pathlib import Path
from unittest.mock import MagicMock, patch

from homework_hub import zen


def test_marionette_reachable_true_when_listener_up():
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    sock.listen(1)
    port = sock.getsockname()[1]

    def _accept_silently():
        try:
            conn, _ = sock.accept()
            conn.close()
        except OSError:
            pass

    accept_thread = threading.Thread(target=_accept_silently, daemon=True)
    accept_thread.start()
    try:
        assert zen.marionette_reachable(port=port) is True
    finally:
        accept_thread.join(timeout=1.0)
        sock.close()


def test_marionette_reachable_false_when_nothing_listening():
    # Port 1 is reserved/unused on Darwin. Bind a socket only to grab a free
    # port number then close it.
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    free_port = sock.getsockname()[1]
    sock.close()
    assert zen.marionette_reachable(port=free_port, timeout=0.2) is False


def test_find_zen_processes_returns_pids():
    fake = MagicMock()
    fake.returncode = 0
    fake.stdout = "1234\n5678\n"
    with patch("subprocess.run", return_value=fake):
        assert zen.find_zen_processes() == [1234, 5678]


def test_find_zen_processes_empty_when_no_match():
    fake = MagicMock()
    fake.returncode = 1  # pgrep exits 1 when no matches
    fake.stdout = ""
    with patch("subprocess.run", return_value=fake):
        assert zen.find_zen_processes() == []


def test_kill_zen_processes_sigterms_then_returns_when_gone():
    pids = [42, 43]
    with (
        patch("os.kill") as mock_kill,
        patch("homework_hub.zen.find_zen_processes", return_value=[]),
        patch("time.sleep"),
    ):
        zen.kill_zen_processes(pids, wait_timeout=1.0)
    # SIGTERM (15) sent to each pid
    sigterm_calls = [c for c in mock_kill.call_args_list if c.args[1] == 15]
    assert {c.args[0] for c in sigterm_calls} == {42, 43}


def test_wait_for_marionette_returns_true_when_eventually_reachable():
    calls = iter([False, False, True])
    with (
        patch(
            "homework_hub.zen.marionette_reachable",
            side_effect=lambda *a, **kw: next(calls),
        ),
        patch("time.sleep"),
    ):
        assert zen.wait_for_marionette(timeout=5.0) is True


def test_wait_for_marionette_returns_false_on_timeout():
    with (
        patch("homework_hub.zen.marionette_reachable", return_value=False),
        patch("time.sleep"),
    ):
        assert zen.wait_for_marionette(timeout=0.05) is False


def test_zen_cookie_login_normalises_cookies():
    import json

    raw_cookies = [
        {
            "name": "SID",
            "value": "sid_val",
            "domain": ".google.com",
            "path": "/",
            "expiry": 1812427241518,  # Milliseconds
            "httpOnly": False,
            "secure": False,
            "sameSite": "None",
        },
        {
            "name": "SSID",
            "value": "ssid_val",
            "domain": ".google.com",
            "path": "/",
            "expiry": 1812427241,  # Seconds
            "httpOnly": True,
            "secure": True,
            "sameSite": "Lax",
        },
        {
            "name": "SESSION_COOKIE",
            "value": "session_val",
            "domain": ".google.com",
            "path": "/",
            "expiry": -1,  # Session
            "httpOnly": True,
            "secure": True,
            "sameSite": "Strict",
        },
        {
            "name": "ZERO_COOKIE",
            "value": "zero_val",
            "domain": ".google.com",
            "path": "/",
            "expiry": 0,  # Should clamp to -1
            "httpOnly": False,
            "secure": False,
            "sameSite": "None",
        },
    ]

    def _frame(data):
        msg = json.dumps(data)
        return f"{len(msg)}:{msg}".encode()

    responses = [
        b"greeting",
        _frame([1, 0, None, {}]),
        _frame([1, 1, None, {"handle": "tab1"}]),
        _frame([1, 2, None, {}]),
        _frame([1, 3, None, {}]),
        _frame([1, 4, None, {"value": "https://classroom.google.com/h"}]),
        _frame([1, 5, None, {}]),
        _frame([1, 6, None, {"value": json.dumps(raw_cookies)}]),
        _frame([1, 7, None, {}]),
        _frame([1, 8, None, {}]),
    ]

    mock_socket_inst = MagicMock()
    mock_socket_inst.recv.side_effect = responses

    with patch("socket.socket", return_value=mock_socket_inst), patch("time.sleep"):
        cookies = zen.zen_cookie_login(
            url="https://classroom.google.com",
            is_logged_in=lambda url: "h" in url,
        )

    assert len(cookies) == 4

    # Check millisecond conversion (1812427241518 -> 1812427241)
    sid_cookie = next(c for c in cookies if c["name"] == "SID")
    assert sid_cookie["expires"] == 1812427241

    # Check standard seconds expiry remains untouched
    ssid_cookie = next(c for c in cookies if c["name"] == "SSID")
    assert ssid_cookie["expires"] == 1812427241

    # Check session cookie remains -1
    session_cookie = next(c for c in cookies if c["name"] == "SESSION_COOKIE")
    assert session_cookie["expires"] == -1

    # Check zero cookie clamps to -1
    zero_cookie = next(c for c in cookies if c["name"] == "ZERO_COOKIE")
    assert zero_cookie["expires"] == -1


# --------------------------------------------------------------------------- #
# extract_cookies_from_zen_sqlite
# --------------------------------------------------------------------------- #


def _make_zen_cookies_db(path: Path, rows: list[dict]) -> None:
    """Create a moz_cookies-shaped SQLite DB and populate it."""
    conn = sqlite3.connect(str(path))
    conn.execute(
        "CREATE TABLE moz_cookies ("
        "id INTEGER PRIMARY KEY, "
        "originAttributes TEXT NOT NULL DEFAULT '', "
        "name TEXT, value TEXT, host TEXT, path TEXT, "
        "expiry INTEGER, isSecure INTEGER, isHttpOnly INTEGER, "
        "sameSite INTEGER DEFAULT 0)"
    )
    for r in rows:
        conn.execute(
            "INSERT INTO moz_cookies "
            "(originAttributes, name, value, host, path, expiry, "
            "isSecure, isHttpOnly, sameSite) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                r.get("originAttributes", ""),
                r["name"],
                r["value"],
                r["host"],
                r.get("path", "/"),
                r.get("expiry", 0),
                int(r.get("isSecure", 0)),
                int(r.get("isHttpOnly", 0)),
                int(r.get("sameSite", 0)),
            ),
        )
    conn.commit()
    conn.close()


def test_extract_cookies_domain_and_container_filtering(tmp_path: Path):
    profile = tmp_path / "profile"
    profile.mkdir()
    _make_zen_cookies_db(
        profile / "cookies.sqlite",
        [
            # Container 1 — Edrolo (should match)
            {
                "originAttributes": "^userContextId=1",
                "name": "sessionid",
                "value": "tahlia_session",
                "host": "app.edrolo.com",
                "expiry": 1812427241,
            },
            # Container 1 — Google (different domain, should NOT match edrolo query)
            {
                "originAttributes": "^userContextId=1",
                "name": "SID",
                "value": "google_sid",
                "host": ".google.com",
                "expiry": 1812427241,
            },
            # Default container — Edrolo (wrong container, should NOT match)
            {
                "originAttributes": "",
                "name": "sessionid",
                "value": "default_session",
                "host": "app.edrolo.com",
                "expiry": 1812427241,
            },
            # Container 2 — Edrolo (wrong container, should NOT match)
            {
                "originAttributes": "^userContextId=2",
                "name": "sessionid",
                "value": "ryan_work_session",
                "host": "app.edrolo.com",
                "expiry": 1812427241,
            },
        ],
    )

    cookies = zen.extract_cookies_from_zen_sqlite(
        profile, ["edrolo.com"], origin_attrs="^userContextId=1"
    )

    assert len(cookies) == 1
    assert cookies[0]["name"] == "sessionid"
    assert cookies[0]["value"] == "tahlia_session"
    assert cookies[0]["domain"] == "app.edrolo.com"


def test_extract_cookies_session_cookie_gives_minus_one(tmp_path: Path):
    profile = tmp_path / "profile"
    profile.mkdir()
    _make_zen_cookies_db(
        profile / "cookies.sqlite",
        [
            {
                "originAttributes": "",
                "name": "session_only",
                "value": "v",
                "host": "example.com",
                "expiry": 0,  # session cookie in Firefox
            },
        ],
    )

    cookies = zen.extract_cookies_from_zen_sqlite(profile, ["example.com"])
    assert len(cookies) == 1
    assert cookies[0]["expires"] == -1


def test_extract_cookies_millisecond_expiry_downscaled(tmp_path: Path):
    profile = tmp_path / "profile"
    profile.mkdir()
    _make_zen_cookies_db(
        profile / "cookies.sqlite",
        [
            {
                "originAttributes": "",
                "name": "ms_cookie",
                "value": "v",
                "host": "example.com",
                "expiry": 1812427241518,  # 13-digit ms timestamp
            },
        ],
    )

    cookies = zen.extract_cookies_from_zen_sqlite(profile, ["example.com"])
    assert cookies[0]["expires"] == 1812427241


def test_extract_cookies_samesite_mapping(tmp_path: Path):
    profile = tmp_path / "profile"
    profile.mkdir()
    _make_zen_cookies_db(
        profile / "cookies.sqlite",
        [
            {
                "originAttributes": "",
                "name": "none_cookie",
                "value": "v",
                "host": "example.com",
                "expiry": 1812427241,
                "sameSite": 0,
            },
            {
                "originAttributes": "",
                "name": "lax_cookie",
                "value": "v",
                "host": "example.com",
                "expiry": 1812427241,
                "sameSite": 1,
            },
            {
                "originAttributes": "",
                "name": "strict_cookie",
                "value": "v",
                "host": "example.com",
                "expiry": 1812427241,
                "sameSite": 2,
            },
            {
                "originAttributes": "",
                "name": "unknown_cookie",
                "value": "v",
                "host": "example.com",
                "expiry": 1812427241,
                "sameSite": 99,
            },
        ],
    )

    cookies = zen.extract_cookies_from_zen_sqlite(profile, ["example.com"])
    by_name = {c["name"]: c["sameSite"] for c in cookies}
    assert by_name["none_cookie"] == "None"
    assert by_name["lax_cookie"] == "Lax"
    assert by_name["strict_cookie"] == "Strict"
    assert by_name["unknown_cookie"] == "None"


def test_extract_cookies_domain_suffix_matches_exact_dot_and_subdomain(tmp_path: Path):
    profile = tmp_path / "profile"
    profile.mkdir()
    _make_zen_cookies_db(
        profile / "cookies.sqlite",
        [
            # exact host match
            {
                "originAttributes": "",
                "name": "exact",
                "value": "v",
                "host": "edrolo.com",
                "expiry": 1812427241,
            },
            # dot-prefixed (domain cookie)
            {
                "originAttributes": "",
                "name": "dot",
                "value": "v",
                "host": ".edrolo.com",
                "expiry": 1812427241,
            },
            # subdomain
            {
                "originAttributes": "",
                "name": "sub",
                "value": "v",
                "host": "app.edrolo.com",
                "expiry": 1812427241,
            },
            # unrelated
            {
                "originAttributes": "",
                "name": "other",
                "value": "v",
                "host": "google.com",
                "expiry": 1812427241,
            },
        ],
    )

    cookies = zen.extract_cookies_from_zen_sqlite(profile, ["edrolo.com"])
    names = sorted(c["name"] for c in cookies)
    assert names == ["dot", "exact", "sub"]


def test_extract_cookies_raises_when_db_missing(tmp_path: Path):
    profile = tmp_path / "profile"
    profile.mkdir()
    import pytest

    with pytest.raises(RuntimeError, match=r"cookies\.sqlite"):
        zen.extract_cookies_from_zen_sqlite(profile, ["example.com"])
