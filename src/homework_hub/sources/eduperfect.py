"""Education Perfect (app.educationperfect.com) source.

Education Perfect is an Australian school learning platform with a clean,
publicly-introspectable GraphQL API at ``graphql-gateway.educationperfect.com``.
Auth uses the school Google account via FusionAuth IAM (``iam.educationperfect.com``).

Auth architecture
-----------------
EP's IAM is a confidential OAuth2 client — token refresh requires a
``client_secret`` that EP holds server-side. We therefore cannot refresh headlessly
via httpx alone. Instead:

1. **One-time auth (Mac, headed):** ``auth eduperfect --child <name>`` runs a headed
   Playwright session. The user completes Google SSO, and we intercept the first
   request to ``graphql-gateway.educationperfect.com`` to capture the Bearer token
   and expiry. Token + Playwright ``storage_state.json`` (which contains the long-lived
   ``fusionauth.sso`` cookie) are saved to ``<tokens_dir>/<child>-eduperfect.json``.

2. **Per-sync refresh (server, headless):** On each sync we check the token expiry.
   If it has expired we replay ``storage_state.json`` in headless Playwright — the
   ``fusionauth.sso`` cookie drives a silent re-auth through FusionAuth → EP's auth
   proxy → app dashboard, and we intercept the fresh Bearer token from the first
   GraphQL request. Playwright is then torn down and the remaining API call is made
   via plain ``httpx``.

3. **Auth expiry:** If Playwright navigation redirects to the FusionAuth login page
   (``iam.educationperfect.com``) the school Google session has expired and the user
   must re-run ``auth eduperfect``. This raises ``AuthExpiredError`` which the
   orchestrator surfaces as a Discord alert.

API
---
Single query: ``assignedWorkForUser`` returns every piece of assigned work
(with status, dates, class IDs). A batch ``classes`` query resolves class IDs
to human-readable names for the Subject column.

Confirmed via GraphQL introspection (2026-05-04):
- ``graphql-gateway.educationperfect.com/graphql`` — 162 query fields, 1248 types.
- ``AssignedWork``: ``id``, ``title``, ``status`` (``UPCOMING`` / ``IN_PROGRESS`` /
  ``PAST_DUE`` / ``COMPLETED``), ``assignedWorkSettings.{startDate, endDate}``,
  ``assignedActivityType`` (``LESSON`` / ``QUIZ`` / ``EXAM_REVISION``),
  ``assignedVia.classIds``.
"""

from __future__ import annotations

import json
import time
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

# Buffer before expiry at which we proactively refresh the token.
_EXPIRY_BUFFER = timedelta(minutes=5)

# Browser-like UA.
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)

_ASSIGNED_WORK_QUERY = """
query GetAssignedWork($input: AssignedWorkInput) {
  assignedWorkForUser(input: $input) {
    assignedWork {
      id
      title
      status
      assignedWorkSettings { startDate endDate }
      assignedActivityType
      assignedVia { classIds directlyAssigned }
    }
  }
}
"""

_CLASSES_QUERY = """
query GetClasses($ids: [UUID!]!) {
  classes(ids: $ids) { id name }
}
"""

# GraphQL AssignedWorkStatus → canonical Status.
_STATUS_MAP: dict[str, Status] = {
    "UPCOMING": Status.NOT_STARTED,
    "IN_PROGRESS": Status.IN_PROGRESS,
    "PAST_DUE": Status.OVERDUE,
    "COMPLETED": Status.SUBMITTED,
}


# --------------------------------------------------------------------------- #
# Pure mapping
# --------------------------------------------------------------------------- #


def map_ep_task_to_task(
    *,
    child: str,
    assigned_work: dict[str, Any],
    class_names: dict[str, str] | None = None,
) -> Task:
    """Translate one ``AssignedWork`` GraphQL dict into a canonical Task.

    ``class_names`` maps stringified class UUID → human-readable name.
    Tasks whose class IDs don't resolve fall back to ``"Education Perfect"``.
    """
    task_id = assigned_work.get("id")
    title = assigned_work.get("title")
    if not task_id or not title:
        raise SchemaBreakError(
            f"EP task missing id/title: keys={list(assigned_work.keys())}"
        )

    class_names = class_names or {}
    class_ids = (assigned_work.get("assignedVia") or {}).get("classIds") or []
    resolved = [class_names[cid] for cid in class_ids if cid in class_names]
    subject = resolved[0] if resolved else "Education Perfect"

    settings = assigned_work.get("assignedWorkSettings") or {}
    assigned_at = _parse_ep_dt(settings.get("startDate"))
    due_at = _parse_ep_dt(settings.get("endDate"))

    status_raw = (assigned_work.get("status") or "UPCOMING").upper()
    status = _STATUS_MAP.get(status_raw, Status.NOT_STARTED)

    activity_type = assigned_work.get("assignedActivityType") or ""
    description = _activity_type_description(activity_type)

    url = f"{APP_BASE_URL}/learning/tasks/{task_id}"

    return Task(
        source=SourceEnum.EDUPERFECT,
        source_id=str(task_id),
        child=child,
        subject=subject,
        title=title,
        description=description,
        assigned_at=assigned_at,
        due_at=due_at,
        status=status,
        status_raw=status_raw.lower(),
        url=url,
    )


def _activity_type_description(activity_type: str) -> str:
    return {
        "LESSON": "EP lesson",
        "QUIZ": "EP quiz",
        "EXAM_REVISION": "EP exam revision",
    }.get(activity_type.upper(), "")


def _parse_ep_dt(value: Any) -> datetime | None:
    """EP timestamps are ISO-8601 strings from the GraphQL DateTime scalar."""
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
          "storage_state": { <Playwright storage_state dict> }
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
        for key in ("access_token", "expires_at", "storage_state"):
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
    def storage_state(self) -> dict[str, Any]:
        return dict(self.raw["storage_state"])

    def is_expired(self, now: datetime | None = None) -> bool:
        """Return True if the token is within the expiry buffer or past it."""
        ref = now or datetime.now(UTC)
        return self.expires_at - ref < _EXPIRY_BUFFER


# --------------------------------------------------------------------------- #
# Playwright headed login (Mac-only, one-time)
# --------------------------------------------------------------------------- #


def run_headed_login(out_path: Path, *, base_url: str = APP_BASE_URL) -> None:
    """Open a headed Chromium, let the user complete Google SSO, capture token.

    Intercepts the first authenticated request to ``graphql-gateway`` to
    extract the Bearer JWT. Saves ``access_token``, ``expires_at`` (decoded
    from the JWT ``exp`` claim), and ``storage_state`` to ``out_path``.

    Imported lazily so the server runtime never needs Playwright browser
    binaries. Only meant to be run on the Mac.
    """
    from playwright.sync_api import sync_playwright

    out_path.parent.mkdir(parents=True, exist_ok=True)

    captured: dict[str, str] = {}

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(user_agent=DEFAULT_USER_AGENT)
        page = context.new_page()

        def _on_request(request: Any) -> None:
            if GRAPHQL_URL in request.url and "Authorization" in request.headers:
                auth = request.headers.get("Authorization", "")
                if auth.startswith("Bearer ") and not captured.get("token"):
                    captured["token"] = auth[len("Bearer "):]

        page.on("request", _on_request)

        page.goto(f"{base_url}/app/login")
        page.wait_for_url(
            lambda url: IAM_HOST not in url and "accounts.google.com" not in url,
            timeout=300_000,  # 5 min for the human
        )

        # Wait until the SPA fires a GraphQL request carrying a Bearer token.
        deadline = time.monotonic() + 60.0
        while time.monotonic() < deadline and not captured.get("token"):
            page.wait_for_timeout(500)

        if not captured.get("token"):
            browser.close()
            raise RuntimeError(
                "Logged in but no Bearer token was captured within 60s. "
                "Ensure the Education Perfect dashboard fully loads before "
                "the browser closes."
            )

        state = context.storage_state()
        browser.close()

    token = captured["token"]
    expires_at = _decode_jwt_exp(token)

    token_file = EduPerfectTokenFile(
        {
            "access_token": token,
            "expires_at": expires_at.isoformat(),
            "storage_state": state,
        }
    )
    token_file.save(out_path)


def _decode_jwt_exp(token: str) -> datetime:
    """Decode the ``exp`` claim from a JWT without verifying the signature.

    Returns a UTC datetime. Falls back to 1 hour from now if decoding fails
    (the token will simply be refreshed on the next sync).
    """
    import base64

    try:
        parts = token.split(".")
        if len(parts) != 3:
            raise ValueError("not a JWT")
        # Add padding so base64 doesn't choke.
        payload_b64 = parts[1] + "=="
        payload = json.loads(base64.urlsafe_b64decode(payload_b64))
        exp = int(payload["exp"])
        return datetime.fromtimestamp(exp, tz=UTC)
    except Exception:
        return datetime.now(UTC) + timedelta(hours=1)


# --------------------------------------------------------------------------- #
# Headless token refresh (server, per-sync)
# --------------------------------------------------------------------------- #


def refresh_token_headless(
    token_file: EduPerfectTokenFile,
    *,
    base_url: str = APP_BASE_URL,
) -> EduPerfectTokenFile:
    """Replay storage_state in headless Playwright to obtain a fresh token.

    The ``fusionauth.sso`` cookie in ``storage_state`` is long-lived and drives
    a silent re-auth through FusionAuth → EP dashboard without user interaction.
    Raises ``AuthExpiredError`` if FusionAuth shows the login page (school Google
    session expired).
    """
    from playwright.sync_api import sync_playwright

    captured: dict[str, str] = {}

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            storage_state=token_file.storage_state,
            user_agent=DEFAULT_USER_AGENT,
        )
        page = context.new_page()

        def _on_request(request: Any) -> None:
            if GRAPHQL_URL in request.url and "Authorization" in request.headers:
                auth = request.headers.get("Authorization", "")
                if auth.startswith("Bearer ") and not captured.get("token"):
                    captured["token"] = auth[len("Bearer "):]

        page.on("request", _on_request)

        page.goto(f"{base_url}/learning/dashboard")

        # Check we didn't land on the FusionAuth login page.
        if IAM_HOST in page.url or "accounts.google.com" in page.url:
            browser.close()
            raise AuthExpiredError(
                "Education Perfect session expired — school Google session needs renewal. "
                "Re-run `homework-hub auth eduperfect --child <name>`."
            )

        # Wait for a GraphQL request with a Bearer token.
        deadline = time.monotonic() + 45.0
        while time.monotonic() < deadline and not captured.get("token"):
            page.wait_for_timeout(500)

        new_state = context.storage_state()
        browser.close()

    if not captured.get("token"):
        raise TransientError(
            "Education Perfect: headless token refresh completed but no "
            "Bearer token was intercepted. Will retry next sync."
        )

    new_token = captured["token"]
    new_expires = _decode_jwt_exp(new_token)

    updated = EduPerfectTokenFile(
        {
            "access_token": new_token,
            "expires_at": new_expires.isoformat(),
            "storage_state": new_state,
        },
        path=token_file.path,
    )
    if token_file.path:
        updated.save(token_file.path)
    return updated


# --------------------------------------------------------------------------- #
# GraphQL client
# --------------------------------------------------------------------------- #


class EduPerfectClient:
    """Thin httpx wrapper around the EP GraphQL gateway."""

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

    def get_assigned_work(
        self,
        *,
        ends_after: datetime | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch all assigned work for the logged-in student."""
        variables: dict[str, Any] = {}
        if ends_after:
            variables["input"] = {
                "filters": {"endsAfterDate": ends_after.isoformat()}
            }
        result = self._query(_ASSIGNED_WORK_QUERY, variables)
        payload = (result.get("assignedWorkForUser") or {}).get("assignedWork")
        if not isinstance(payload, list):
            raise SchemaBreakError(
                f"EP assignedWorkForUser missing list payload: {str(result)[:200]}"
            )
        return payload

    def get_class_names(self, class_ids: list[str]) -> dict[str, str]:
        """Batch-resolve class UUIDs → human-readable names."""
        if not class_ids:
            return {}
        result = self._query(_CLASSES_QUERY, {"ids": class_ids})
        classes = result.get("classes") or []
        return {
            c["id"]: c.get("name", "")
            for c in classes
            if isinstance(c, dict) and "id" in c
        }

    # ------------------------------------------------------------------ #
    # Internals
    # ------------------------------------------------------------------ #

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self._token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": self._user_agent,
            "Origin": APP_BASE_URL,
            "Referer": f"{APP_BASE_URL}/",
        }

    def _query(self, query: str, variables: dict[str, Any] | None = None) -> dict[str, Any]:
        body = {"query": query}
        if variables:
            body["variables"] = variables

        try:
            resp = self._client.post(self._url, json=body, headers=self._headers())
        except httpx.TimeoutException as exc:
            raise TransientError(f"EP GraphQL timeout: {exc}") from exc
        except httpx.HTTPError as exc:
            raise TransientError(f"EP GraphQL network error: {exc}") from exc

        if resp.status_code in (401, 403):
            raise AuthExpiredError(
                f"EP GraphQL returned {resp.status_code} — token expired. "
                "Will attempt refresh on next sync."
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
            if code == "AUTH_NOT_AUTHENTICATED":
                raise AuthExpiredError(
                    "EP GraphQL: AUTH_NOT_AUTHENTICATED — token expired or invalid."
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
        refresh_fn: Any = None,
    ):
        self.token_path_for_child = token_path_for_child
        # Allow injection in tests.
        self._client_factory = client_factory or (
            lambda token: EduPerfectClient(token)
        )
        self._refresh_fn = refresh_fn or refresh_token_headless

    def fetch(self, child: str) -> list[Task]:
        """Not used — EP runs entirely through the medallion fetch_raw path."""
        raise NotImplementedError("EduPerfectSource only supports fetch_raw")

    def fetch_raw(self, child: str) -> list[RawRecord]:
        """Fetch EP assigned work as raw payloads for the bronze layer."""
        if child not in self.token_path_for_child:
            raise SchemaBreakError(
                f"No Education Perfect token path configured for {child}."
            )
        path = self.token_path_for_child[child]
        token_file = EduPerfectTokenFile.load(path)

        if token_file.is_expired():
            token_file = self._refresh_fn(token_file)

        with self._client_factory(token_file.access_token) as client:
            # Fetch work assigned in the last 12 months — wide enough to catch
            # any outstanding or recently completed tasks.
            ends_after = datetime.now(UTC) - timedelta(days=365)
            raw_work = client.get_assigned_work(ends_after=ends_after)

            # Collect all unique class IDs and resolve them in one batch call.
            all_class_ids: list[str] = []
            for w in raw_work:
                ids = (w.get("assignedVia") or {}).get("classIds") or []
                all_class_ids.extend(ids)
            class_names = client.get_class_names(list(dict.fromkeys(all_class_ids)))

        records: list[RawRecord] = []
        for w in raw_work:
            task_id = w.get("id")
            if not task_id:
                raise SchemaBreakError(
                    f"EP assigned work missing id: keys={sorted(w.keys())}"
                )
            records.append(
                RawRecord(
                    child=child,
                    source=SourceEnum.EDUPERFECT.value,
                    source_id=str(task_id),
                    payload={"assigned_work": w, "class_names": class_names},
                )
            )
        return records
