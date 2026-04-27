"""Tests for the Vaultwarden CLI wrapper.

Uses an injected runner so no real `bw` binary is invoked.
"""

from __future__ import annotations

import json
from collections.abc import Sequence

import pytest

from homework_hub.secrets import VaultwardenCLI, VaultwardenError, from_env


class FakeRunner:
    """Scriptable runner: queues responses keyed by argv prefix."""

    def __init__(self) -> None:
        # Each entry: (predicate, returncode, stdout, stderr).
        # Predicate takes argv and returns True if it matches.
        self.script: list[tuple] = []
        self.calls: list[tuple[list[str], dict[str, str] | None]] = []

    def expect(self, match_argv: list[str], rc: int = 0, stdout: str = "", stderr: str = ""):
        def matches(argv: Sequence[str]) -> bool:
            return list(argv)[: len(match_argv)] == match_argv

        self.script.append((matches, rc, stdout, stderr))

    def __call__(
        self, argv: Sequence[str], env: dict[str, str] | None = None
    ) -> tuple[int, str, str]:
        self.calls.append((list(argv), env))
        for i, (matches, rc, out, err) in enumerate(self.script):
            if matches(argv):
                # Consume the entry so subsequent identical calls match later expectations.
                self.script.pop(i)
                return rc, out, err
        raise AssertionError(f"Unexpected bw invocation: {list(argv)}")


def _cli(runner: FakeRunner) -> VaultwardenCLI:
    return VaultwardenCLI(
        server="https://vaultwarden.test",
        client_id="user.fake",
        client_secret="secret",
        master_password="pw",
        runner=runner,
    )


class TestUnlock:
    def _expect_server_already_configured(self, r: FakeRunner) -> None:
        """Queue the no-arg config server check returning the correct URL."""
        r.expect(["bw", "config", "server"], 0, "https://vaultwarden.test\n")

    def _expect_server_needs_setting(self, r: FakeRunner) -> None:
        """Queue the no-arg check returning a different URL, then the set call."""
        r.expect(["bw", "config", "server"], 0, "https://other.server\n")
        r.expect(["bw", "config", "server", "https://vaultwarden.test"], 0)

    def test_unlock_when_unauthenticated_logs_in_first(self):
        r = FakeRunner()
        self._expect_server_needs_setting(r)
        r.expect(["bw", "status"], 0, json.dumps({"status": "unauthenticated"}))
        r.expect(["bw", "login", "--apikey"], 0)
        r.expect(["bw", "unlock", "--passwordenv"], 0, "session-token-abc\n")
        cli = _cli(r)
        token = cli.unlock()
        assert token == "session-token-abc"
        called = [c[0][1] for c in r.calls]
        assert called == ["config", "config", "status", "login", "unlock"]

    def test_unlock_skips_login_if_already_authenticated(self):
        r = FakeRunner()
        self._expect_server_already_configured(r)
        r.expect(["bw", "status"], 0, json.dumps({"status": "locked"}))
        r.expect(["bw", "unlock", "--passwordenv"], 0, "tok\n")
        cli = _cli(r)
        cli.unlock()
        called = [c[0][1] for c in r.calls]
        assert "login" not in called

    def test_unlock_skips_server_set_when_already_configured(self):
        # The key fix: if the server URL already matches, no set call is made.
        # This prevents "Logout required before server config update." on restart.
        r = FakeRunner()
        self._expect_server_already_configured(r)
        r.expect(["bw", "status"], 0, json.dumps({"status": "locked"}))
        r.expect(["bw", "unlock", "--passwordenv"], 0, "tok\n")
        cli = _cli(r)
        cli.unlock()
        set_calls = [c for c in r.calls if c[0][:2] == ["bw", "config"] and len(c[0]) == 4]
        assert set_calls == [], "bw config server <url> should not be called when already set"

    def test_unlock_sets_server_when_url_differs(self):
        r = FakeRunner()
        self._expect_server_needs_setting(r)
        r.expect(["bw", "status"], 0, json.dumps({"status": "unauthenticated"}))
        r.expect(["bw", "login", "--apikey"], 0)
        r.expect(["bw", "unlock", "--passwordenv"], 0, "tok\n")
        cli = _cli(r)
        cli.unlock()
        set_calls = [c for c in r.calls if c[0][:3] == ["bw", "config", "server"] and len(c[0]) == 4]
        assert len(set_calls) == 1

    def test_unlock_caches_session(self):
        r = FakeRunner()
        self._expect_server_already_configured(r)
        r.expect(["bw", "status"], 0, json.dumps({"status": "locked"}))
        r.expect(["bw", "unlock", "--passwordenv"], 0, "tok\n")
        cli = _cli(r)
        cli.unlock()
        first_call_count = len(r.calls)
        cli.unlock()  # second call should be a no-op
        assert len(r.calls) == first_call_count

    def test_unlock_failure_raises(self):
        r = FakeRunner()
        self._expect_server_already_configured(r)
        r.expect(["bw", "status"], 0, json.dumps({"status": "locked"}))
        r.expect(["bw", "unlock", "--passwordenv"], 1, "", "bad password")
        cli = _cli(r)
        with pytest.raises(VaultwardenError, match="bad password"):
            cli.unlock()

    def test_config_server_read_failure_proceeds_to_set(self):
        # Non-zero from the no-arg check (e.g. fresh install) — fall through
        # to the set call, which succeeds.
        r = FakeRunner()
        r.expect(["bw", "config", "server"], 1, "", "no config")
        r.expect(["bw", "config", "server", "https://vaultwarden.test"], 0)
        r.expect(["bw", "status"], 0, json.dumps({"status": "locked"}))
        r.expect(["bw", "unlock", "--passwordenv"], 0, "tok\n")
        cli = _cli(r)
        cli.unlock()  # should not raise

    def test_login_retries_after_logout_on_failure(self):
        # First login attempt fails (broken data.json) — should logout then retry.
        r = FakeRunner()
        self._expect_server_already_configured(r)
        r.expect(["bw", "status"], 0, json.dumps({"status": "unauthenticated"}))
        r.expect(["bw", "login", "--apikey"], 1, "", "Account does not exist")
        r.expect(["bw", "logout"], 0)
        r.expect(["bw", "login", "--apikey"], 0)
        r.expect(["bw", "unlock", "--passwordenv"], 0, "tok\n")
        cli = _cli(r)
        token = cli.unlock()
        assert token == "tok"
        called = [c[0][1] for c in r.calls]
        assert called == ["config", "status", "login", "logout", "login", "unlock"]

    def test_login_raises_if_retry_also_fails(self):
        r = FakeRunner()
        self._expect_server_already_configured(r)
        r.expect(["bw", "status"], 0, json.dumps({"status": "unauthenticated"}))
        r.expect(["bw", "login", "--apikey"], 1, "", "Account does not exist")
        r.expect(["bw", "logout"], 0)
        r.expect(["bw", "login", "--apikey"], 1, "", "still broken")
        cli = _cli(r)
        with pytest.raises(VaultwardenError, match="still broken"):
            cli.unlock()
        r = FakeRunner()
        r.expect(["bw", "config", "server"], 0, "https://other.server\n")
        r.expect(["bw", "config", "server", "https://vaultwarden.test"], 1, "", "no network")
        cli = _cli(r)
        with pytest.raises(VaultwardenError, match="no network"):
            cli.unlock()

    def test_status_returning_garbage_raises(self):
        r = FakeRunner()
        self._expect_server_already_configured(r)
        r.expect(["bw", "status"], 0, "not json")
        cli = _cli(r)
        with pytest.raises(VaultwardenError, match="non-JSON"):
            cli.unlock()


class TestGetItem:
    def _unlocked(self):
        r = FakeRunner()
        r.expect(["bw", "config", "server"], 0, "https://vaultwarden.test\n")
        r.expect(["bw", "status"], 0, json.dumps({"status": "locked"}))
        r.expect(["bw", "unlock", "--passwordenv"], 0, "tok\n")
        return r, _cli(r)

    def test_get_item_returns_parsed_json(self):
        r, cli = self._unlocked()
        item = {
            "name": "Compass MCSC",
            "login": {"username": "parent@example.com", "password": "hunter2"},
        }
        r.expect(["bw", "get", "item", "Compass MCSC"], 0, json.dumps(item))
        got = cli.get_item("Compass MCSC")
        assert got == item

    def test_get_item_uses_session_env(self):
        r, cli = self._unlocked()
        r.expect(["bw", "get", "item", "X"], 0, json.dumps({"name": "X"}))
        cli.get_item("X")
        get_call = next(c for c in r.calls if c[0][:3] == ["bw", "get", "item"])
        assert get_call[1] is not None
        assert get_call[1].get("BW_SESSION") == "tok"

    def test_get_item_failure_raises(self):
        r, cli = self._unlocked()
        r.expect(["bw", "get", "item", "Missing"], 1, "", "Not found.")
        with pytest.raises(VaultwardenError, match="Not found"):
            cli.get_item("Missing")

    def test_get_password(self):
        r, cli = self._unlocked()
        item = {"login": {"username": "u", "password": "p"}}
        r.expect(["bw", "get", "item", "X"], 0, json.dumps(item))
        assert cli.get_password("X") == "p"

    def test_get_password_missing_raises(self):
        r, cli = self._unlocked()
        item = {"login": {"username": "u"}}
        r.expect(["bw", "get", "item", "X"], 0, json.dumps(item))
        with pytest.raises(VaultwardenError, match=r"login\.password"):
            cli.get_password("X")

    def test_get_username(self):
        r, cli = self._unlocked()
        item = {"login": {"username": "u", "password": "p"}}
        r.expect(["bw", "get", "item", "X"], 0, json.dumps(item))
        assert cli.get_username("X") == "u"

    def test_get_notes(self):
        r, cli = self._unlocked()
        item = {"notes": "secret note"}
        r.expect(["bw", "get", "item", "X"], 0, json.dumps(item))
        assert cli.get_notes("X") == "secret note"

    def test_get_custom_field(self):
        r, cli = self._unlocked()
        item = {
            "fields": [
                {"name": "subdomain", "value": "mcsc-vic"},
                {"name": "userId", "value": "12345"},
            ]
        }
        r.expect(["bw", "get", "item", "X"], 0, json.dumps(item))
        assert cli.get_custom_field("X", "subdomain") == "mcsc-vic"
        # Re-fetch caches nothing — call again with new expectation.
        r.expect(["bw", "get", "item", "X"], 0, json.dumps(item))
        assert cli.get_custom_field("X", "userId") == "12345"

    def test_get_custom_field_missing_raises(self):
        r, cli = self._unlocked()
        item = {"fields": [{"name": "other", "value": "x"}]}
        r.expect(["bw", "get", "item", "X"], 0, json.dumps(item))
        with pytest.raises(VaultwardenError, match="custom field 'subdomain'"):
            cli.get_custom_field("X", "subdomain")


class TestLock:
    def test_lock_clears_session(self):
        r = FakeRunner()
        r.expect(["bw", "config", "server"], 0, "https://vaultwarden.test\n")
        r.expect(["bw", "status"], 0, json.dumps({"status": "locked"}))
        r.expect(["bw", "unlock", "--passwordenv"], 0, "tok\n")
        r.expect(["bw", "lock"], 0)
        cli = _cli(r)
        cli.unlock()
        cli.lock()
        # Re-unlock should hit the bw chain again.
        r.expect(["bw", "config", "server"], 0, "https://vaultwarden.test\n")
        r.expect(["bw", "status"], 0, json.dumps({"status": "locked"}))
        r.expect(["bw", "unlock", "--passwordenv"], 0, "tok2\n")
        assert cli.unlock() == "tok2"

    def test_lock_when_never_unlocked_is_noop(self):
        r = FakeRunner()
        cli = _cli(r)
        cli.lock()
        assert r.calls == []


class TestFromEnv:
    def test_from_env_reads_required(self, monkeypatch):
        monkeypatch.setenv("BW_SERVER", "https://vaultwarden.test")
        monkeypatch.setenv("BW_CLIENTID", "user.x")
        monkeypatch.setenv("BW_CLIENTSECRET", "shh")
        monkeypatch.setenv("BW_PASSWORD", "pw")
        cli = from_env()
        assert cli.server == "https://vaultwarden.test"
        assert cli.client_id == "user.x"

    def test_from_env_missing_raises(self, monkeypatch):
        for k in ("BW_SERVER", "BW_CLIENTID", "BW_CLIENTSECRET", "BW_PASSWORD"):
            monkeypatch.delenv(k, raising=False)
        with pytest.raises(VaultwardenError, match="Missing required env vars"):
            from_env()
