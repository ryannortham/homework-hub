"""Unit tests for the Zen Browser launch helpers."""

from __future__ import annotations

import socket
import threading
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
