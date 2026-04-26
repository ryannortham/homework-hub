"""Compass (Compass Education AU) source.

Parent-login model:

A single parent ASP.NET_SessionId cookie covers all children. The Compass web
UI exposes Learning Tasks per-student via ``userId`` parameter; ``children.yaml``
records the school's numeric userId for each kid.

Auth: cookie cannot be obtained automatically (school requires SMS OTP). The
``homework-hub auth compass`` CLI prompts for a paste of ``ASP.NET_SessionId``
copied from a logged-in browser.

Split architecture:

- ``map_learning_task_to_task``: pure function mapping a LearningTask dict
  to our canonical Task. Fully unit-tested.
- ``CompassClient``: thin HTTP wrapper around requests.Session. Handles
  cookie loading, 401/302 → AuthExpiredError translation. Not unit-tested
  here; smoke-tested with a captured fixture once we have one.
- ``CompassSource``: implements ``Source.fetch(child)`` by reading the
  child's ``compass_user_id`` from config and calling the client.
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

# Compass uses an iOS app User-Agent that the unofficial Compasspy library
# pins; non-browser UAs get bot-blocked. Keep this in sync with Compasspy.
DEFAULT_USER_AGENT = "Compass/3.6.16 (iPhone; iOS 16.6; Scale/3.00)"

# Compass status enum values observed on Learning Tasks. Mapping below is
# pessimistic — anything not explicitly known maps to NOT_STARTED so we err on
# "still to do" rather than silently marking submitted.
_STATUS_MAP: dict[int, Status] = {
    0: Status.NOT_STARTED,  # Pending
    1: Status.SUBMITTED,  # Submitted (on time)
    2: Status.SUBMITTED,  # Submitted late
    3: Status.GRADED,  # Marked / Returned
}


# --------------------------------------------------------------------------- #
# Pure mapping
# --------------------------------------------------------------------------- #


def map_learning_task_to_task(*, child: str, learning_task: dict[str, Any], subdomain: str) -> Task:
    """Translate one Compass Learning Task into a canonical Task.

    Compass Learning Tasks come back from
    ``Services/LearningTasks.svc/GetAllLearningTasksByUserId``. The shape is
    semi-documented; key fields we rely on:

        id              int   — stable Learning Task ID
        name            str   — task title (we map to title)
        subjectName     str   — e.g. "9MATH"
        description     str   — HTML/plaintext
        dueDateTimestamp str  — ISO-8601, sometimes with milliseconds
        status          int   — 0..3, see _STATUS_MAP

    Per-student submission status comes back via ``students[]`` (when the
    parent endpoint returns task across cohort) OR ``submissionStatus`` (when
    queried per userId). We accept either.
    """
    task_id = learning_task.get("id")
    title = learning_task.get("name") or learning_task.get("title")
    if task_id is None or not title:
        raise SchemaBreakError(
            f"Compass LearningTask missing id/name: keys={list(learning_task.keys())}"
        )

    subject = learning_task.get("subjectName") or learning_task.get("subject") or ""
    description = _strip_html(learning_task.get("description") or "")

    assigned_at = _parse_compass_dt(learning_task.get("activityStart")) or _parse_compass_dt(
        learning_task.get("createdTimestamp")
    )
    due_at = _parse_compass_dt(learning_task.get("dueDateTimestamp")) or _parse_compass_dt(
        learning_task.get("dueDate")
    )

    status_raw_int = _resolve_student_status(learning_task)
    status = _STATUS_MAP.get(status_raw_int, Status.NOT_STARTED)

    url = f"https://{subdomain}.compass.education/Communicate/LearningTasksStudentDetails.aspx?taskId={task_id}"

    return Task(
        source=SourceEnum.COMPASS,
        source_id=str(task_id),
        child=child,
        subject=subject,
        title=title,
        description=description,
        assigned_at=assigned_at,
        due_at=due_at,
        status=status,
        status_raw=str(status_raw_int),
        url=url,
    )


def _resolve_student_status(lt: dict[str, Any]) -> int:
    """Compass returns either an int ``status`` or per-student ``students[].status``.

    Prefer the explicit per-student value when available, fall back to the
    top-level status.
    """
    students = lt.get("students")
    if isinstance(students, list) and students:
        for s in students:
            if isinstance(s, dict) and "status" in s:
                return int(s["status"])
    return int(lt.get("status", 0))


def _parse_compass_dt(value: Any) -> datetime | None:
    """Compass timestamps are typically ISO-8601 strings, sometimes Date(epoch_ms)."""
    if not value:
        return None
    if isinstance(value, int | float):
        return datetime.fromtimestamp(value / 1000, tz=UTC)
    if not isinstance(value, str):
        return None
    # Common ".NET" date form: /Date(1714521600000)/
    if value.startswith("/Date(") and value.endswith(")/"):
        try:
            ms = int(value[6:-2].split("+")[0].split("-")[0])
            return datetime.fromtimestamp(ms / 1000, tz=UTC)
        except ValueError:
            return None
    # ISO-8601
    cleaned = value.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(cleaned)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def _strip_html(text: str) -> str:
    """Best-effort HTML strip for Compass description fields."""
    if "<" not in text:
        return text.strip()
    # Tiny tag stripper — sufficient for Compass's modest HTML.
    out: list[str] = []
    inside = False
    for ch in text:
        if ch == "<":
            inside = True
        elif ch == ">":
            inside = False
        elif not inside:
            out.append(ch)
    return "".join(out).strip()


# --------------------------------------------------------------------------- #
# Token store (cookie persistence)
# --------------------------------------------------------------------------- #


class CompassToken:
    """Persisted parent Compass cookie."""

    def __init__(self, *, subdomain: str, cookie: str, captured_at: datetime | None = None):
        self.subdomain = subdomain
        self.cookie = cookie
        self.captured_at = captured_at or datetime.now(UTC)

    def to_dict(self) -> dict[str, Any]:
        return {
            "subdomain": self.subdomain,
            "cookie": self.cookie,
            "captured_at": self.captured_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> CompassToken:
        captured = data.get("captured_at")
        captured_dt = datetime.fromisoformat(captured) if captured else datetime.now(UTC)
        return cls(
            subdomain=data["subdomain"],
            cookie=data["cookie"],
            captured_at=captured_dt,
        )

    @classmethod
    def load(cls, path: Path) -> CompassToken:
        if not path.exists():
            raise AuthExpiredError(f"No Compass token at {path} — run `homework-hub auth compass`")
        data = json.loads(path.read_text())
        return cls.from_dict(data)

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(self.to_dict(), indent=2))


# --------------------------------------------------------------------------- #
# HTTP client
# --------------------------------------------------------------------------- #


class CompassClient:
    """Thin wrapper around the Compass mobile services API."""

    def __init__(
        self,
        token: CompassToken,
        *,
        user_agent: str = DEFAULT_USER_AGENT,
        client: httpx.Client | None = None,
        timeout: float = 30.0,
    ):
        self.token = token
        self.user_agent = user_agent
        self._owns_client = client is None
        self._client = client or httpx.Client(timeout=timeout, follow_redirects=False)

    def __enter__(self) -> CompassClient:
        return self

    def __exit__(self, *_exc: Any) -> None:
        self.close()

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def get_learning_tasks(self, user_id: int) -> list[dict[str, Any]]:
        """Fetch all Learning Tasks for a single student userId."""
        url = (
            f"https://{self.token.subdomain}.compass.education"
            "/Services/LearningTasks.svc/GetAllLearningTasksByUserId"
        )
        body = {"userId": int(user_id), "page": 1, "start": 0, "limit": 200}
        return self._post(url, body)

    # ------------------------------------------------------------------ #
    # Internals
    # ------------------------------------------------------------------ #

    def _post(self, url: str, body: dict[str, Any]) -> list[dict[str, Any]]:
        headers = {
            "Content-Type": "application/json",
            "User-Agent": self.user_agent,
            "Accept": "application/json",
            "Cookie": f"ASP.NET_SessionId={self.token.cookie}",
        }
        try:
            resp = self._client.post(url, json=body, headers=headers)
        except httpx.TimeoutException as exc:
            raise TransientError(f"Compass timeout: {exc}") from exc
        except httpx.HTTPError as exc:
            raise TransientError(f"Compass network error: {exc}") from exc

        # Compass redirects unauthenticated requests to the login page (302),
        # and returns 401 for expired session cookies.
        if resp.status_code in (302, 401, 403):
            raise AuthExpiredError(
                f"Compass returned {resp.status_code} — session cookie expired. "
                "Refresh via `homework-hub auth compass`."
            )
        if 500 <= resp.status_code < 600:
            raise TransientError(f"Compass {resp.status_code} on {url}")
        if resp.status_code != 200:
            raise SchemaBreakError(
                f"Unexpected Compass status {resp.status_code} on {url}: {resp.text[:200]}"
            )

        try:
            payload = resp.json()
        except json.JSONDecodeError as exc:
            raise SchemaBreakError(f"Non-JSON Compass response: {resp.text[:200]}") from exc

        # Compass wraps results in {d: {data: [...], h: ..., ...}} or {d: [...]}.
        d = payload.get("d", payload)
        data = (d.get("data") or d.get("Data") or []) if isinstance(d, dict) else d
        if not isinstance(data, list):
            raise SchemaBreakError(f"Compass response missing list payload: {type(data).__name__}")
        return data


# --------------------------------------------------------------------------- #
# Source implementation
# --------------------------------------------------------------------------- #


class CompassSource(Source):
    """Compass source — shared parent token, per-child userId."""

    name = "compass"

    def __init__(
        self,
        token_path: Path,
        *,
        user_id_for_child: dict[str, int],
        client_factory: Any = None,
    ):
        self.token_path = token_path
        self.user_id_for_child = user_id_for_child
        # client_factory is overridable for tests; defaults to CompassClient.
        self._client_factory = client_factory or (lambda token: CompassClient(token))

    def fetch(self, child: str) -> list[Task]:
        if child not in self.user_id_for_child:
            raise SchemaBreakError(
                f"No compass_user_id configured for {child}. Add it to children.yaml."
            )
        user_id = self.user_id_for_child[child]
        token = CompassToken.load(self.token_path)
        with self._client_factory(token) as client:
            raw_tasks = client.get_learning_tasks(user_id)
        return [
            map_learning_task_to_task(child=child, learning_task=lt, subdomain=token.subdomain)
            for lt in raw_tasks
        ]
