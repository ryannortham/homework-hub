"""Edrolo (app.edrolo.com) source.

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
- ``EdroloClient``: thin httpx wrapper around the confirmed REST API.
- ``EdroloSource``: per-child Source.fetch implementation.

API shape confirmed via DevTools sniffing (see ``scripts/sniff_edrolo_api.py``):

- ``GET /api/v1/student-tasks/`` — bare endpoint returns a flat list of all
  tasks for the logged-in student (both ``created`` and ``spaced_retrieval``
  types, all stages including ARCHIVED). The SPA's paginated variants apply
  filters that we deliberately don't reproduce; we filter client-side.
- ``GET /api/v1/my-courses/`` — flat list of the student's enrolled courses.
  Used to translate ``course_ids: ["66921"]`` → ``"VCE Biology Units 3&4"``.

Task fields we use::

    id, title, start_datetime, due_datetime, type ('created'|'spaced_retrieval'),
    resolved_stage ('ARCHIVED'|'OPEN'|...), soft_deleted (bool),
    course_ids (list of str), task_assignments[0].completion_status

We pass *all* tasks through to the sheet — including ARCHIVED, CLOSED, and
COMPLETED ones — mapping them to ``Status.SUBMITTED``. The sheet's ``Today``
and ``Tasks`` views already exclude submitted/graded entries from at-a-glance
displays, while the ``Raw`` tab keeps the full history. This mirrors how
Classroom and Compass behave. ``is_active_edrolo_task`` is retained as a
helper but no longer applied at fetch time.
"""

from __future__ import annotations

import json
import time
from datetime import UTC, datetime
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

# Edrolo serves its app under the apex domain.
DEFAULT_BASE_URL = "https://app.edrolo.com"

# Confirmed via DevTools sniffing on app.edrolo.com (2026-04). The bare
# endpoint returns a flat list of *all* tasks (no envelope, no pagination).
# We filter client-side rather than try to encode Edrolo's many overlapping
# query-param conventions.
API_TASKS_PATH = "/api/v1/student-tasks/"
API_COURSES_PATH = "/api/v1/my-courses/"

# Browser-like UA so we don't stand out from a logged-in session.
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)

# Edrolo's task_assignments[].completion_status enum.
_COMPLETION_STATUS_MAP: dict[str, Status] = {
    "NOT_STARTED": Status.NOT_STARTED,
    "IN_PROGRESS": Status.IN_PROGRESS,
    "COMPLETED": Status.SUBMITTED,
}


# --------------------------------------------------------------------------- #
# Pure mapping
# --------------------------------------------------------------------------- #


def map_edrolo_task_to_task(
    *,
    child: str,
    edrolo_task: dict[str, Any],
    course_titles: dict[str, str] | None = None,
) -> Task:
    """Translate one Edrolo task dict into a canonical Task.

    ``course_titles`` maps stringified ``course_id`` → human-readable course
    title (e.g. ``"66921" -> "VCE Biology Units 3&4 [2026]"``). Tasks may
    reference course IDs from previous years that aren't in the current
    enrolment list — those fall back to ``"Edrolo"``.
    """
    task_id = edrolo_task.get("id")
    title = edrolo_task.get("title")
    if task_id is None or not title:
        raise SchemaBreakError(f"Edrolo task missing id/title: keys={list(edrolo_task.keys())}")

    course_titles = course_titles or {}
    course_ids = edrolo_task.get("course_ids") or []
    subject_titles = [course_titles[str(cid)] for cid in course_ids if str(cid) in course_titles]
    subject = subject_titles[0] if subject_titles else "Edrolo"

    assigned_at = _parse_dt(edrolo_task.get("start_datetime"))
    due_at = _parse_dt(edrolo_task.get("due_datetime"))

    status_raw, status = _resolve_status(edrolo_task)

    description = ""
    task_type = edrolo_task.get("type")
    if task_type == "spaced_retrieval":
        description = "Edrolo revision (spaced retrieval)"
    elif task_type == "created":
        description = "Edrolo task (teacher-set)"

    url = _build_default_url(task_id)

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


def _resolve_status(t: dict[str, Any]) -> tuple[str, Status]:
    """Derive (raw, canonical) status from Edrolo's nested assignment record."""
    assignments = t.get("task_assignments") or []
    completion = ""
    if assignments and isinstance(assignments[0], dict):
        completion = (assignments[0].get("completion_status") or "").upper()

    resolved = (t.get("resolved_stage") or "").upper()

    # Archived/closed always means the work is no longer outstanding. We map
    # to SUBMITTED for canonical bucketing; the upstream filter in
    # ``EdroloSource`` drops these before they ever get here in the normal
    # path, but be safe in case the filter is ever loosened.
    if resolved in ("ARCHIVED", "CLOSED"):
        return resolved.lower(), Status.SUBMITTED

    if completion in _COMPLETION_STATUS_MAP:
        return completion.lower(), _COMPLETION_STATUS_MAP[completion]

    return "not_started", Status.NOT_STARTED


def is_active_edrolo_task(t: dict[str, Any]) -> bool:
    """Return True if the task is outstanding (not archived/done/deleted)."""
    if t.get("soft_deleted"):
        return False
    if (t.get("resolved_stage") or "").upper() in ("ARCHIVED", "CLOSED"):
        return False
    assignments = t.get("task_assignments") or []
    return not (
        assignments
        and isinstance(assignments[0], dict)
        and (assignments[0].get("completion_status") or "").upper() == "COMPLETED"
    )


def _build_default_url(task_id: Any) -> str:
    # The SPA presents tasks under the studyplanner namespace; this URL form
    # opens the task detail view in a logged-in browser.
    return f"{DEFAULT_BASE_URL}/studyplanner/tasks/{task_id}/"


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
        cookies = self.cookies_for_domain("app.edrolo.com")
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

    def cookie_header(self, domain: str = "app.edrolo.com") -> str:
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
        # Poll until the SPA has set the ``sessionid`` cookie. Edrolo's hydration
        # can be slow, especially on first login, so a fixed sleep isn't safe.
        deadline = time.monotonic() + 60.0
        while time.monotonic() < deadline:
            cookies = {c["name"]: c.get("value") for c in context.cookies()}
            if cookies.get("sessionid"):
                break
            page.wait_for_timeout(500)
        else:
            browser.close()
            raise RuntimeError(
                "Edrolo headed login finished but no 'sessionid' cookie was set "
                "within 60s. Try again and ensure the dashboard fully loads "
                "before closing the browser."
            )
        # Tiny extra settle so any sibling cookies (csrftoken, etc.) land too.
        page.wait_for_timeout(500)
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
        """Fetch all tasks for the logged-in student (flat list, no filters)."""
        return self._get_json(API_TASKS_PATH)

    def get_courses(self) -> list[dict[str, Any]]:
        """Fetch the student's enrolled courses (used for course_id → title)."""
        return self._get_json(API_COURSES_PATH)

    # ------------------------------------------------------------------ #
    # Internals
    # ------------------------------------------------------------------ #

    def _get_json(
        self,
        path: str,
        *,
        params: tuple[tuple[str, str], ...] | None = None,
    ) -> list[dict[str, Any]]:
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
            resp = self._client.get(url, headers=headers, params=params)
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
            raw_courses = client.get_courses()

        course_titles = {
            str(c["id"]): c.get("title", "")
            for c in raw_courses
            if isinstance(c, dict) and "id" in c
        }
        # Pass everything through — completed/archived tasks land in the sheet
        # mapped to Status.SUBMITTED so they appear greyed-out in the views.
        # Mirrors Classroom + Compass behaviour. The ``is_active_edrolo_task``
        # helper is retained for reference / future use but no longer applied
        # at fetch time.
        return [
            map_edrolo_task_to_task(child=child, edrolo_task=t, course_titles=course_titles)
            for t in raw_tasks
        ]

    def fetch_raw(self, child: str) -> list[RawRecord]:
        """Fetch Edrolo student-tasks as raw payloads for the bronze layer.

        Course titles are resolved once and embedded in each task's payload
        so the silver mapper doesn't need a second API call. ``soft_deleted``
        tasks are filtered out at fetch time — they're upstream tombstones,
        not study items, and we never showed them in the sheet.
        """
        if child not in self.storage_path_for_child:
            raise SchemaBreakError(f"No Edrolo storage state path configured for {child}.")
        path = self.storage_path_for_child[child]
        storage = EdroloStorageState.load(path)
        with self._client_factory(storage) as client:
            raw_tasks = client.get_tasks()
            raw_courses = client.get_courses()

        course_titles = {
            str(c["id"]): c.get("title", "")
            for c in raw_courses
            if isinstance(c, dict) and "id" in c
        }
        records: list[RawRecord] = []
        for t in raw_tasks:
            if t.get("soft_deleted"):
                continue
            task_id = t.get("id")
            if task_id is None:
                raise SchemaBreakError(f"Edrolo task missing id: keys={sorted(t.keys())}")
            records.append(
                RawRecord(
                    child=child,
                    source=SourceEnum.EDROLO.value,
                    source_id=str(task_id),
                    payload={"task": t, "course_titles": course_titles},
                )
            )
        return records
