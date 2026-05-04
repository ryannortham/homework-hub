"""Education Perfect (app.educationperfect.com) source.

Education Perfect is an Australian school learning platform. Auth uses FusionAuth
IAM (``iam.educationperfect.com``) with a school Google SSO.

Auth architecture
-----------------
EP's GraphQL gateway authenticates via a **cookie** named ``access_token`` on the
``.educationperfect.com`` domain — not an ``Authorization: Bearer`` header.
The token is a JWT issued by FusionAuth and set as a ``SameSite=None; Secure``
cookie by ``authentication.educationperfect.com/oauth-redirect`` after the OAuth
code exchange.

Token capture requires a real browser with the user's active session.
We use Firefox (Zen Browser) with Marionette enabled and the
``--remote-allow-system-access`` flag to register an ``nsIObserverService``
HTTP observer in the privileged chrome context. This captures the ``Cookie``
header on outgoing requests to ``graphql-gateway.educationperfect.com``,
extracting the ``access_token`` value.

Token lifetime: ~30 minutes (JWT ``exp`` claim). Refresh is performed
headlessly: navigate to the EP dashboard via Playwright Firefox (seeding the
``fusionauth.sso`` cookie from the token file), which triggers a silent
``/oauth-redirect`` → ``/refresh-token`` cycle that sets a new ``access_token``
cookie. We capture this via the same HTTP observer pattern.

Wait — in practice the headless refresh is unreliable because FusionAuth binds
the SSO cookie to the originating client fingerprint. Instead, the auth command
must be re-run via Zen when the token expires.  The token expiry is embedded in
the JWT; the daemon checks it before each sync and raises ``AuthExpiredError``
if expired, which surfaces as a Discord alert.

API
---
Query: ``assignedClasswork(schoolId: $schoolId, status: TO_DO|DONE)``

Returns ``AssignedClasswork`` objects with:
  ``id``, ``name``, ``source``, ``progressStatus``, ``startDate``, ``dueDate``,
  ``finalSubmissionDate``, ``subjectId``, ``classes[].name``

Both ``TO_DO`` and ``DONE`` statuses are fetched and merged.

Status mapping:
  ``NOT_STARTED`` → ``Status.NOT_STARTED``
  ``IN_PROGRESS``  → ``Status.IN_PROGRESS``
  ``COMPLETE``     → ``Status.SUBMITTED``

School ID is resolved once via ``user(id: $userId).memberships[0].school.id``.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx

from homework_hub.models import Source as SourceEnum
from homework_hub.models import Status, Task
from homework_hub.pipeline.ingest import RawRecord
from homework_hub.sources.base import (
    AuthExpiredError,
    SchemaBreakError,
    Source,
    TransientError,
)

GRAPHQL_URL = "https://graphql-gateway.educationperfect.com/graphql"
APP_BASE_URL = "https://app.educationperfect.com"
IAM_HOST = "iam.educationperfect.com"

# Buffer before expiry at which we consider the token stale.
_EXPIRY_BUFFER = timedelta(minutes=5)

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:138.0) "
    "Gecko/20100101 Firefox/138.0"
)

_ASSIGNED_CLASSWORK_QUERY = """
query GetAssignedClasswork($schoolId: UUID!, $status: AssignedClassworkStatus) {
  assignedClasswork(schoolId: $schoolId, status: $status) {
    result {
      id
      name
      source
      progressStatus
      startDate
      dueDate
      finalSubmissionDate
      subjectId
      classes { id name }
    }
    total
  }
}
"""

_USER_SCHOOL_QUERY = """
query GetUserSchool($userId: UUID!) {
  user(id: $userId) {
    id
    memberships {
      school { id name }
    }
  }
}
"""

_STATUS_MAP: dict[str, Status] = {
    "NOT_STARTED": Status.NOT_STARTED,
    "IN_PROGRESS": Status.IN_PROGRESS,
    "COMPLETE": Status.SUBMITTED,
}


# --------------------------------------------------------------------------- #
# Pure mapping
# --------------------------------------------------------------------------- #


def map_ep_classwork_to_task(
    *,
    child: str,
    classwork: dict[str, Any],
) -> Task:
    """Translate one ``AssignedClasswork`` GraphQL dict into a canonical Task."""
    task_id = classwork.get("id")
    name = classwork.get("name")
    if not task_id or not name:
        raise SchemaBreakError(
            f"EP classwork missing id/name: keys={list(classwork.keys())}"
        )

    classes = classwork.get("classes") or []
    subject = classes[0].get("name", "Education Perfect") if classes else "Education Perfect"

    assigned_at = _parse_ep_dt(classwork.get("startDate"))
    due_at = _parse_ep_dt(classwork.get("dueDate"))

    progress = (classwork.get("progressStatus") or "NOT_STARTED").upper()
    status = _STATUS_MAP.get(progress, Status.NOT_STARTED)

    source_type = (classwork.get("source") or "").upper()
    description = {
        "TEACHER": "EP teacher-assigned task",
        "SYSTEM_RECOMMENDATION": "EP recommended task",
    }.get(source_type, "")

    url = f"{APP_BASE_URL}/learning/tasks/{task_id}"

    return Task(
        source=SourceEnum.EDUPERFECT,
        source_id=str(task_id),
        child=child,
        subject=subject,
        title=name,
        description=description,
        assigned_at=assigned_at,
        due_at=due_at,
        status=status,
        status_raw=progress.lower(),
        url=url,
    )


def _parse_ep_dt(value: Any) -> datetime | None:
    if not value or not isinstance(value, str):
        return None
    cleaned = value.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(cleaned)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


# --------------------------------------------------------------------------- #
# Token file
# --------------------------------------------------------------------------- #


class EduPerfectTokenFile:
    """Wrapper around ``<child>-eduperfect.json``.

    Schema::

        {
          "access_token": "<JWT>",
          "expires_at":   "<ISO-8601 UTC>",
          "school_id":    "<UUID>",          # cached on first resolve
          "storage_state": { <cookies dict> } # for future headless refresh
        }
    """

    def __init__(self, raw: dict[str, Any], path: Path | None = None):
        self.raw = raw
        self.path = path

    @classmethod
    def load(cls, path: Path) -> EduPerfectTokenFile:
        if not path.exists():
            raise AuthExpiredError(
                f"No Education Perfect token at {path} — "
                "run `homework-hub auth eduperfect --child <name>`"
            )
        try:
            raw = json.loads(path.read_text())
        except json.JSONDecodeError as exc:
            raise AuthExpiredError(
                f"Education Perfect token at {path} is not valid JSON"
            ) from exc
        for key in ("access_token", "expires_at"):
            if key not in raw:
                raise AuthExpiredError(
                    f"Education Perfect token at {path} missing '{key}' — "
                    "re-run `homework-hub auth eduperfect`"
                )
        return cls(raw, path=path)

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(self.raw, indent=2))
        self.path = path

    @property
    def access_token(self) -> str:
        return str(self.raw["access_token"])

    @property
    def expires_at(self) -> datetime:
        return datetime.fromisoformat(self.raw["expires_at"]).astimezone(UTC)

    @property
    def school_id(self) -> str | None:
        return self.raw.get("school_id")

    @property
    def storage_state(self) -> dict[str, Any]:
        return dict(self.raw.get("storage_state", {"cookies": [], "origins": []}))

    def is_expired(self, now: datetime | None = None) -> bool:
        ref = now or datetime.now(UTC)
        return self.expires_at - ref < _EXPIRY_BUFFER

    def with_school_id(self, school_id: str) -> EduPerfectTokenFile:
        updated = EduPerfectTokenFile({**self.raw, "school_id": school_id}, path=self.path)
        if self.path:
            updated.save(self.path)
        return updated


# --------------------------------------------------------------------------- #
# Token capture via Zen Browser Marionette
# --------------------------------------------------------------------------- #


def run_headed_login(out_path: Path, *, base_url: str = APP_BASE_URL) -> None:
    """Capture the EP access_token cookie from a running Zen Browser session.

    Requires Zen Browser to be running with Marionette enabled::

        /Applications/Zen.app/Contents/MacOS/zen \\
          --marionette --marionette-port 2828 \\
          --remote-allow-system-access \\
          --profile <profile-path>

    The ``auth eduperfect`` CLI command handles launching Zen with the correct
    flags before calling this function.

    How it works:
    1. Connects to Zen's Marionette protocol on port 2828.
    2. Registers an nsIObserverService HTTP observer in the privileged chrome
       context — the only mechanism that can see the ``Cookie`` header on requests
       made by the mfe-proxy cross-origin iframe.
    3. Navigates to the EP dashboard in the content context (uses James's live
       session from the existing Zen profile).
    4. Reads the captured ``access_token`` value from the observer.
    5. Decodes the JWT ``exp`` claim and saves the token file.
    """
    import socket

    out_path.parent.mkdir(parents=True, exist_ok=True)

    MSG_ID = [0]

    def _send(s: socket.socket, cmd: str, params: dict) -> None:
        mid = MSG_ID[0]; MSG_ID[0] += 1
        msg = json.dumps([0, mid, cmd, params])
        s.sendall(f"{len(msg)}:{msg}".encode())

    def _recv(s: socket.socket) -> Any:
        buf = b""
        s.settimeout(30)
        while True:
            buf += s.recv(8192)
            try:
                colon = buf.index(b":")
                length = int(buf[:colon])
                rest = buf[colon + 1:]
                if len(rest) >= length:
                    return json.loads(rest[:length])
            except (ValueError, json.JSONDecodeError):
                continue

    def _exec(s: socket.socket, script: str) -> Any:
        _send(s, "WebDriver:ExecuteScript", {"script": script, "args": []})
        r = _recv(s)
        if isinstance(r, list) and len(r) >= 4:
            if r[2]:
                return None
            val = r[3]
            return val.get("value") if isinstance(val, dict) else val
        return None

    try:
        s = socket.socket()
        s.settimeout(5)
        s.connect(("localhost", 2828))
        s.recv(1024)  # hello
    except OSError as exc:
        raise RuntimeError(
            "Cannot connect to Zen Browser Marionette on port 2828.\n"
            "Launch Zen with:\n"
            "  /Applications/Zen.app/Contents/MacOS/zen \\\n"
            "    --marionette --marionette-port 2828 \\\n"
            "    --remote-allow-system-access \\\n"
            f"    --profile \"$HOME/Library/Application Support/zen/Profiles/<profile>\"\n"
            f"  Then re-run this command.\nOriginal error: {exc}"
        ) from exc

    _send(s, "WebDriver:NewSession", {})
    _recv(s)

    # Switch to chrome context and install HTTP observer
    _send(s, "Marionette:SetContext", {"value": "chrome"})
    _recv(s)
    observer_script = """
    if (window.__ep_cookie_obs__) {
        try { Services.obs.removeObserver(window.__ep_cookie_obs__, 'http-on-modify-request'); } catch(e) {}
    }
    const obs = {
        token: null,
        observe: function(subject, topic) {
            if (this.token) return;
            try {
                const ch = subject.QueryInterface(Components.interfaces.nsIHttpChannel);
                if (!ch.URI.spec.includes('graphql-gateway')) return;
                let cookie = '';
                try { cookie = ch.getRequestHeader('Cookie'); } catch(e) {}
                const match = cookie.match(/access_token=([^;]+)/);
                if (match) this.token = match[1];
            } catch(e) {}
        },
        QueryInterface: ChromeUtils.generateQI(['nsIObserver'])
    };
    Services.obs.addObserver(obs, 'http-on-modify-request');
    window.__ep_cookie_obs__ = obs;
    return 'ok';
    """
    _exec(s, observer_script)

    # Navigate to EP in content context
    _send(s, "Marionette:SetContext", {"value": "content"})
    _recv(s)
    _send(s, "WebDriver:Navigate", {"url": f"{base_url}/learning/dashboard"})
    _recv(s)

    # Wait up to 20s for a token to appear
    import time
    deadline = time.monotonic() + 20.0
    token = None
    while time.monotonic() < deadline:
        _send(s, "Marionette:SetContext", {"value": "chrome"})
        _recv(s)
        token = _exec(s, "return window.__ep_cookie_obs__ ? window.__ep_cookie_obs__.token : null;")
        if token:
            break
        _send(s, "Marionette:SetContext", {"value": "content"})
        _recv(s)
        time.sleep(0.5)

    if not token:
        # Try navigating to assigned-work to trigger fresh requests
        _send(s, "Marionette:SetContext", {"value": "content"})
        _recv(s)
        _send(s, "WebDriver:Navigate", {"url": f"{base_url}/learning/assigned-work"})
        _recv(s)
        deadline2 = time.monotonic() + 15.0
        while time.monotonic() < deadline2:
            _send(s, "Marionette:SetContext", {"value": "chrome"})
            _recv(s)
            token = _exec(s, "return window.__ep_cookie_obs__ ? window.__ep_cookie_obs__.token : null;")
            if token:
                break
            _send(s, "Marionette:SetContext", {"value": "content"})
            _recv(s)
            time.sleep(0.5)

    s.close()

    if not token:
        raise RuntimeError(
            "EP dashboard loaded but no access_token cookie was captured.\n"
            "Ensure James is logged into app.educationperfect.com in Zen Browser,\n"
            "then re-run this command."
        )

    expires_at = _decode_jwt_exp(token)
    EduPerfectTokenFile(
        {
            "access_token": token,
            "expires_at": expires_at.isoformat(),
            "storage_state": {"cookies": [], "origins": []},
        }
    ).save(out_path)


def _decode_jwt_exp(token: str) -> datetime:
    """Decode the ``exp`` claim from a JWT without verifying the signature."""
    import base64

    try:
        parts = token.split(".")
        if len(parts) != 3:
            raise ValueError("not a JWT")
        payload = json.loads(base64.urlsafe_b64decode(parts[1] + "=="))
        return datetime.fromtimestamp(int(payload["exp"]), tz=UTC)
    except Exception:
        return datetime.now(UTC) + timedelta(hours=1)


# --------------------------------------------------------------------------- #
# GraphQL client (cookie-based auth)
# --------------------------------------------------------------------------- #


class EduPerfectClient:
    """httpx wrapper for the EP GraphQL gateway.

    Auth via ``access_token`` cookie on ``.educationperfect.com`` — not a
    Bearer header.
    """

    def __init__(
        self,
        access_token: str,
        *,
        graphql_url: str = GRAPHQL_URL,
        user_agent: str = DEFAULT_USER_AGENT,
        client: httpx.Client | None = None,
        timeout: float = 30.0,
    ):
        self._token = access_token
        self._url = graphql_url
        self._user_agent = user_agent
        self._owns_client = client is None
        self._client = client or httpx.Client(timeout=timeout, follow_redirects=False)

    def __enter__(self) -> EduPerfectClient:
        return self

    def __exit__(self, *_exc: Any) -> None:
        self.close()

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def get_school_id(self, user_id: str) -> str:
        """Resolve the student's school UUID from the user record."""
        result = self._query(_USER_SCHOOL_QUERY, {"userId": user_id})
        user = (result.get("user") or {})
        memberships = user.get("memberships") or []
        for m in memberships:
            school = m.get("school") or {}
            sid = school.get("id")
            if sid:
                return sid
        raise SchemaBreakError("EP: could not resolve school_id from user memberships")

    def get_assigned_classwork(self, school_id: str) -> list[dict[str, Any]]:
        """Fetch all assigned classwork (both TO_DO and DONE) for the student."""
        all_items: list[dict[str, Any]] = []
        for status in ("TO_DO", "DONE"):
            result = self._query(
                _ASSIGNED_CLASSWORK_QUERY,
                {"schoolId": school_id, "status": status},
            )
            payload = (result.get("assignedClasswork") or {})
            items = payload.get("result") or []
            if not isinstance(items, list):
                raise SchemaBreakError(
                    f"EP assignedClasswork {status} missing list payload: {str(result)[:200]}"
                )
            all_items.extend(items)
        return all_items

    # ------------------------------------------------------------------ #

    def _headers(self) -> dict[str, str]:
        return {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": self._user_agent,
            "Origin": APP_BASE_URL,
            "Referer": f"{APP_BASE_URL}/",
        }

    def _cookies(self) -> dict[str, str]:
        return {"access_token": self._token}

    def _query(self, query: str, variables: dict[str, Any] | None = None) -> dict[str, Any]:
        body: dict[str, Any] = {"query": query}
        if variables:
            body["variables"] = variables

        try:
            resp = self._client.post(
                self._url, json=body, headers=self._headers(), cookies=self._cookies()
            )
        except httpx.TimeoutException as exc:
            raise TransientError(f"EP GraphQL timeout: {exc}") from exc
        except httpx.HTTPError as exc:
            raise TransientError(f"EP GraphQL network error: {exc}") from exc

        if resp.status_code in (401, 403):
            raise AuthExpiredError(
                f"EP GraphQL returned {resp.status_code} — token expired. "
                "Re-run `homework-hub auth eduperfect --child <name>`."
            )
        if 500 <= resp.status_code < 600:
            raise TransientError(f"EP GraphQL {resp.status_code}: {resp.text[:200]}")
        if resp.status_code != 200:
            raise SchemaBreakError(
                f"Unexpected EP GraphQL status {resp.status_code}: {resp.text[:200]}"
            )

        try:
            data = resp.json()
        except json.JSONDecodeError as exc:
            raise SchemaBreakError(
                f"Non-JSON EP GraphQL response: {resp.text[:200]}"
            ) from exc

        errors = data.get("errors")
        if errors:
            first = errors[0]
            code = (first.get("extensions") or {}).get("code", "")
            if "NOT_AUTHORIZED" in code or "NOT_AUTHENTICATED" in code:
                raise AuthExpiredError(
                    f"EP GraphQL: {code} — token expired or insufficient permissions."
                )
            raise SchemaBreakError(f"EP GraphQL error: {first.get('message')}")

        return data.get("data") or {}


# --------------------------------------------------------------------------- #
# Source implementation
# --------------------------------------------------------------------------- #


class EduPerfectSource(Source):
    """Education Perfect source — per-child token file on disk."""

    name = "eduperfect"

    def __init__(
        self,
        token_path_for_child: dict[str, Path],
        *,
        client_factory: Any = None,
    ):
        self.token_path_for_child = token_path_for_child
        self._client_factory = client_factory or (
            lambda token: EduPerfectClient(token)
        )

    def fetch(self, child: str) -> list[Task]:
        """Not used — EP runs entirely through the medallion fetch_raw path."""
        raise NotImplementedError("EduPerfectSource only supports fetch_raw")

    def fetch_raw(self, child: str) -> list[RawRecord]:
        """Fetch EP assigned classwork as raw payloads for the bronze layer."""
        if child not in self.token_path_for_child:
            raise SchemaBreakError(
                f"No Education Perfect token path configured for {child}."
            )
        path = self.token_path_for_child[child]
        token_file = EduPerfectTokenFile.load(path)

        if token_file.is_expired():
            raise AuthExpiredError(
                f"Education Perfect token expired at {token_file.expires_at.isoformat()}. "
                "Re-run `homework-hub auth eduperfect --child <name>`."
            )

        import base64
        jwt_payload = json.loads(
            base64.urlsafe_b64decode(token_file.access_token.split(".")[1] + "==")
        )
        user_id = jwt_payload.get("userId") or jwt_payload.get("sub")
        if not user_id:
            raise SchemaBreakError("EP token JWT missing userId claim")

        with self._client_factory(token_file.access_token) as client:
            # Resolve school_id on first run and cache it in the token file.
            school_id = token_file.school_id
            if not school_id:
                school_id = client.get_school_id(user_id)
                token_file = token_file.with_school_id(school_id)

            classwork_items = client.get_assigned_classwork(school_id)

        records: list[RawRecord] = []
        for item in classwork_items:
            task_id = item.get("id")
            if not task_id:
                raise SchemaBreakError(
                    f"EP classwork missing id: keys={sorted(item.keys())}"
                )
            records.append(
                RawRecord(
                    child=child,
                    source=SourceEnum.EDUPERFECT.value,
                    source_id=str(task_id),
                    payload={"classwork": item},
                )
            )
        return records
