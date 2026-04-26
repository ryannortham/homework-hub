"""Edrolo (edrolo.com.au) source.

Edrolo is a paid Australian VCE/HSC platform with no public API. Auth uses
Google SSO, and headless Chromium is reliably detected by Google's anti-bot
systems — so login is performed *headed*, on the user's Mac, via Playwright.
The resulting ``storage_state.json`` (cookies + localStorage) is copied to
the server. Runtime sync just replays the cookies with httpx; no Playwright
on the server.

Split architecture (mirrors Compass):

- ``map_edrolo_task_to_task``: pure dict → Task. Fully unit-tested.
- ``EdroloStorageState``: load/save the Playwright ``storage_state.json``;
  exposes the cookie jar Edrolo's API needs (``sessionid``, ``csrftoken``).
- ``EdroloClient``: thin httpx wrapper around the suspected REST API.
  Endpoint paths and response envelope are best-guess based on Edrolo's
  Django stack (see DEVTOOLS_NEEDED below) — verify on first headed login
  by running the browser with DevTools and capturing requests, then update
  ``API_TASKS_PATH`` and the mapper to match reality.
- ``EdroloSource``: per-child Source.fetch implementation.

DEVTOOLS_NEEDED:
    On first headed login, open DevTools → Network → XHR, browse to the
    student dashboard / tasks page, and capture:
      1. The exact URL of the tasks/assignments listing endpoint
      2. The full response JSON shape (including any envelope like {results: [...]})
      3. The fields used for: id, title, due date, status, subject/course, url
    Then update ``API_TASKS_PATH``, ``_extract_tasks_payload``, and
    ``map_edrolo_task_to_task`` to match. The current values are defensive
    guesses based on standard Django REST Framework conventions.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx

from homework_hub.models import Source as SourceEnum
from homework_hub.models import Status, Task
from homework_hub.sources.base import (
    AuthExpiredError,
    SchemaBreakError,
    Source,
    TransientError,
)

# Edrolo serves its app under the apex domain.
DEFAULT_BASE_URL = "https://edrolo.com.au"

# Best-guess API path. Common DRF patterns: /api/student/tasks/,
# /api/v2/students/me/tasks/, /api/studyplanner/tasks/. Verify and update.
API_TASKS_PATH = "/api/student/tasks/"

# Browser-like UA so we don't stand out from a logged-in session.
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)

# Edrolo task status — best guess. DRF apps commonly use string enums or
# booleans like ``submitted``/``completed_at``. Map liberally; unknown values
# fall back to NOT_STARTED so we err toward "still to do".
_STATUS_MAP: dict[str, Status] = {
    "not_started": Status.NOT_STARTED,
    "pending": Status.NOT_STARTED,
    "open": Status.NOT_STARTED,
    "in_progress": Status.IN_PROGRESS,
    "started": Status.IN_PROGRESS,
    "submitted": Status.SUBMITTED,
    "completed": Status.SUBMITTED,
    "complete": Status.SUBMITTED,
    "graded": Status.GRADED,
    "marked": Status.GRADED,
    "returned": Status.GRADED,
}


# --------------------------------------------------------------------------- #
# Pure mapping
# --------------------------------------------------------------------------- #


def map_edrolo_task_to_task(*, child: str, edrolo_task: dict[str, Any]) -> Task:
    """Translate one Edrolo task dict into a canonical Task.

    Field names are best-guess; verify against captured DevTools responses.
    Synonyms are accepted for resilience across API revisions.
    """
    task_id = edrolo_task.get("id") or edrolo_task.get("uuid") or edrolo_task.get("pk")
    title = edrolo_task.get("title") or edrolo_task.get("name") or edrolo_task.get("display_name")
    if task_id is None or not title:
        raise SchemaBreakError(f"Edrolo task missing id/title: keys={list(edrolo_task.keys())}")

    subject = (
        edrolo_task.get("course_name")
        or edrolo_task.get("subject_name")
        or edrolo_task.get("subject")
        or _nested_str(edrolo_task.get("course"), "name")
        or _nested_str(edrolo_task.get("subject_obj"), "name")
        or ""
    )

    description = edrolo_task.get("description") or edrolo_task.get("instructions") or ""

    assigned_at = _parse_dt(
        edrolo_task.get("assigned_at")
        or edrolo_task.get("created_at")
        or edrolo_task.get("set_at")
        or edrolo_task.get("start_date")
    )
    due_at = _parse_dt(
        edrolo_task.get("due_at") or edrolo_task.get("due_date") or edrolo_task.get("deadline")
    )

    status_raw = _resolve_status_raw(edrolo_task)
    status = _STATUS_MAP.get(status_raw.lower(), Status.NOT_STARTED)

    url = edrolo_task.get("url") or edrolo_task.get("absolute_url") or _build_default_url(task_id)

    return Task(
        source=SourceEnum.EDROLO,
        source_id=str(task_id),
        child=child,
        subject=subject,
        title=title,
        description=description,
        assigned_at=assigned_at,
        due_at=due_at,
        status=status,
        status_raw=status_raw,
        url=url,
    )


def _resolve_status_raw(t: dict[str, Any]) -> str:
    """Edrolo may report status as a string, a bool, or via timestamps."""
    explicit = t.get("status") or t.get("state")
    if isinstance(explicit, str) and explicit:
        return explicit
    # Common DRF idiom: completed_at / submitted_at non-null implies done.
    if t.get("graded_at") or t.get("marked_at"):
        return "graded"
    if t.get("submitted_at") or t.get("completed_at"):
        return "submitted"
    if t.get("started_at"):
        return "in_progress"
    if isinstance(t.get("is_complete"), bool) and t["is_complete"]:
        return "submitted"
    return "not_started"


def _nested_str(obj: Any, key: str) -> str:
    if isinstance(obj, dict):
        v = obj.get(key)
        if isinstance(v, str):
            return v
    return ""


def _build_default_url(task_id: Any) -> str:
    return f"{DEFAULT_BASE_URL}/student/tasks/{task_id}/"


def _parse_dt(value: Any) -> datetime | None:
    """Edrolo timestamps are ISO-8601 strings (Django default)."""
    if not value:
        return None
    if isinstance(value, int | float):
        return datetime.fromtimestamp(float(value), tz=UTC)
    if not isinstance(value, str):
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
# Storage state (Playwright cookie/localStorage dump)
# --------------------------------------------------------------------------- #


class EdroloStorageState:
    """Wrapper around the JSON file Playwright produces via ``context.storage_state()``.

    The file's shape is:
        {
          "cookies": [{"name": ..., "value": ..., "domain": ..., ...}, ...],
          "origins": [{"origin": "...", "localStorage": [...]}, ...]
        }

    We only use the cookies; localStorage isn't needed for API auth.
    """

    REQUIRED_COOKIES = ("sessionid",)  # csrftoken needed only for unsafe methods

    def __init__(self, raw: dict[str, Any], path: Path | None = None):
        self.raw = raw
        self.path = path

    @classmethod
    def load(cls, path: Path) -> EdroloStorageState:
        if not path.exists():
            raise AuthExpiredError(
                f"No Edrolo storage state at {path} — run `homework-hub auth edrolo`"
            )
        try:
            raw = json.loads(path.read_text())
        except json.JSONDecodeError as exc:
            raise AuthExpiredError(f"Edrolo storage state at {path} is not valid JSON") from exc
        state = cls(raw, path=path)
        state.validate()
        return state

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(self.raw, indent=2))
        self.path = path

    def validate(self) -> None:
        cookies = self.cookies_for_domain("edrolo.com.au")
        for required in self.REQUIRED_COOKIES:
            if required not in cookies:
                raise AuthExpiredError(
                    f"Edrolo storage state missing '{required}' cookie — re-run "
                    "`homework-hub auth edrolo`."
                )

    def cookies_for_domain(self, domain: str) -> dict[str, str]:
        """Return ``{name: value}`` for cookies scoped to *domain* or its parents."""
        out: dict[str, str] = {}
        for c in self.raw.get("cookies", []):
            cd = (c.get("domain") or "").lstrip(".")
            if cd and (domain == cd or domain.endswith("." + cd) or cd.endswith("." + domain)):
                out[c["name"]] = c["value"]
        return out

    def cookie_header(self, domain: str = "edrolo.com.au") -> str:
        """Render the matching cookies as a single Cookie header value."""
        return "; ".join(f"{k}={v}" for k, v in self.cookies_for_domain(domain).items())


# --------------------------------------------------------------------------- #
# Playwright headed login (Mac-only)
# --------------------------------------------------------------------------- #


def run_headed_login(out_path: Path, *, base_url: str = DEFAULT_BASE_URL) -> None:
    """Open a headed Chromium, let the user complete Google SSO, dump storage state.

    Imported lazily so the server runtime (which never calls this) doesn't
    need the Playwright browser binaries. Only meant to be run on the Mac.
    """
    from playwright.sync_api import sync_playwright

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(user_agent=DEFAULT_USER_AGENT)
        page = context.new_page()
        page.goto(f"{base_url}/account/login/")
        # Wait for the user to complete Google SSO and land on /account or a
        # subpage. We poll the URL rather than rely on a single selector
        # because Edrolo redirects through several pages post-login.
        page.wait_for_url(
            lambda url: "/account/login" not in url and "accounts.google.com" not in url,
            timeout=300_000,  # 5 min for the human to finish 2FA
        )
        # Give SPA hydration a moment so cookies are set.
        page.wait_for_timeout(3_000)
        state = context.storage_state()
        EdroloStorageState(state).save(out_path)
        browser.close()


# --------------------------------------------------------------------------- #
# HTTP client
# --------------------------------------------------------------------------- #


class EdroloClient:
    """Replays the headed-login session to call Edrolo's REST API."""

    def __init__(
        self,
        storage: EdroloStorageState,
        *,
        base_url: str = DEFAULT_BASE_URL,
        user_agent: str = DEFAULT_USER_AGENT,
        client: httpx.Client | None = None,
        timeout: float = 30.0,
    ):
        self.storage = storage
        self.base_url = base_url.rstrip("/")
        self.user_agent = user_agent
        self._owns_client = client is None
        self._client = client or httpx.Client(timeout=timeout, follow_redirects=False)

    def __enter__(self) -> EdroloClient:
        return self

    def __exit__(self, *_exc: Any) -> None:
        self.close()

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def get_tasks(self) -> list[dict[str, Any]]:
        """Fetch all tasks for the logged-in student."""
        return self._get_json(API_TASKS_PATH)

    # ------------------------------------------------------------------ #
    # Internals
    # ------------------------------------------------------------------ #

    def _get_json(self, path: str) -> list[dict[str, Any]]:
        url = f"{self.base_url}{path}"
        headers = {
            "User-Agent": self.user_agent,
            "Accept": "application/json",
            "Cookie": self.storage.cookie_header(),
            # Edrolo's frontend sets X-Requested-With for XHR; cheap signal.
            "X-Requested-With": "XMLHttpRequest",
            "Referer": f"{self.base_url}/",
        }
        try:
            resp = self._client.get(url, headers=headers)
        except httpx.TimeoutException as exc:
            raise TransientError(f"Edrolo timeout: {exc}") from exc
        except httpx.HTTPError as exc:
            raise TransientError(f"Edrolo network error: {exc}") from exc

        # Django-style auth: 302 to /account/login on expired session.
        if resp.status_code in (302, 401, 403):
            raise AuthExpiredError(
                f"Edrolo returned {resp.status_code} — session expired. "
                "Refresh via `homework-hub auth edrolo`."
            )
        if 500 <= resp.status_code < 600:
            raise TransientError(f"Edrolo {resp.status_code} on {url}")
        if resp.status_code != 200:
            raise SchemaBreakError(
                f"Unexpected Edrolo status {resp.status_code} on {url}: {resp.text[:200]}"
            )

        try:
            payload = resp.json()
        except json.JSONDecodeError as exc:
            raise SchemaBreakError(f"Non-JSON Edrolo response: {resp.text[:200]}") from exc

        return _extract_tasks_payload(payload)


def _extract_tasks_payload(payload: Any) -> list[dict[str, Any]]:
    """Unwrap common DRF envelopes around the task list.

    Accepts:
        [...]                       — bare list
        {"results": [...]}          — DRF ListAPIView (paginated)
        {"tasks": [...]}            — custom wrapper
        {"data": [...]}             — JSON:API-ish
    """
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ("results", "tasks", "data", "items"):
            v = payload.get(key)
            if isinstance(v, list):
                return v
    raise SchemaBreakError(f"Edrolo response missing list payload: type={type(payload).__name__}")


# --------------------------------------------------------------------------- #
# Source implementation
# --------------------------------------------------------------------------- #


class EdroloSource(Source):
    """Edrolo source — per-child storage_state.json on disk."""

    name = "edrolo"

    def __init__(
        self,
        storage_path_for_child: dict[str, Path],
        *,
        client_factory: Any = None,
    ):
        self.storage_path_for_child = storage_path_for_child
        self._client_factory = client_factory or (lambda storage: EdroloClient(storage))

    def fetch(self, child: str) -> list[Task]:
        if child not in self.storage_path_for_child:
            raise SchemaBreakError(f"No Edrolo storage state path configured for {child}.")
        path = self.storage_path_for_child[child]
        storage = EdroloStorageState.load(path)
        with self._client_factory(storage) as client:
            raw_tasks = client.get_tasks()
        return [map_edrolo_task_to_task(child=child, edrolo_task=t) for t in raw_tasks]
