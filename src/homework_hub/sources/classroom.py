"""Google Classroom homework source.

Architecture:

- ``map_coursework_to_task`` and ``_extract_due_at`` are **pure functions** that
  consume Classroom API JSON shapes and produce ``Task`` instances. These are
  unit-tested with captured fixture JSON.
- ``ClassroomSource`` is a **thin shim** that calls the API via
  ``google-api-python-client``, threads the per-child OAuth token, and feeds
  the JSON into the pure mappers.

Required scopes (read-only):
    classroom.courses.readonly
    classroom.coursework.me.readonly
    classroom.student-submissions.me.readonly
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from homework_hub.models import Source as SourceEnum
from homework_hub.models import Status, Task
from homework_hub.sources.base import (
    AuthExpiredError,
    SchemaBreakError,
    Source,
    TransientError,
)

OAUTH_SCOPES: list[str] = [
    "https://www.googleapis.com/auth/classroom.courses.readonly",
    "https://www.googleapis.com/auth/classroom.coursework.me.readonly",
    "https://www.googleapis.com/auth/classroom.student-submissions.me.readonly",
]

# Map Classroom submission state strings to our canonical Status.
# https://developers.google.com/workspace/classroom/reference/rest/v1/courses.courseWork.studentSubmissions
_STATE_MAP: dict[str, Status] = {
    "NEW": Status.NOT_STARTED,
    "CREATED": Status.IN_PROGRESS,
    "TURNED_IN": Status.SUBMITTED,
    "RETURNED": Status.GRADED,
    "RECLAIMED_BY_STUDENT": Status.IN_PROGRESS,
    "SUBMISSION_STATE_UNSPECIFIED": Status.NOT_STARTED,
}


# --------------------------------------------------------------------------- #
# Pure mapping
# --------------------------------------------------------------------------- #


def map_coursework_to_task(
    *,
    child: str,
    course: dict[str, Any],
    coursework: dict[str, Any],
    submission: dict[str, Any] | None,
) -> Task:
    """Translate a (course, coursework, submission) triple into a Task.

    Raises ``SchemaBreakError`` if mandatory fields are missing.
    """
    cw_id = coursework.get("id")
    title = coursework.get("title")
    if not cw_id or not title:
        raise SchemaBreakError(f"coursework missing id/title: {list(coursework.keys())}")

    course_id = course.get("id") or coursework.get("courseId") or ""
    subject = course.get("name") or ""
    description = coursework.get("description") or ""
    url = coursework.get("alternateLink") or ""

    assigned_at = _parse_rfc3339(coursework.get("creationTime"))
    due_at = _extract_due_at(coursework)

    state_raw = (submission or {}).get("state") or "NEW"
    status = _STATE_MAP.get(state_raw, Status.NOT_STARTED)
    if (submission or {}).get("late") and status not in (Status.SUBMITTED, Status.GRADED):
        status = Status.OVERDUE

    return Task(
        source=SourceEnum.CLASSROOM,
        source_id=f"{course_id}:{cw_id}",
        child=child,
        subject=subject,
        title=title,
        description=description,
        assigned_at=assigned_at,
        due_at=due_at,
        status=status,
        status_raw=state_raw,
        url=url,
    )


def _extract_due_at(coursework: dict[str, Any]) -> datetime | None:
    """Combine Classroom's split ``dueDate`` + ``dueTime`` into a UTC datetime."""
    due_date = coursework.get("dueDate")
    if not due_date:
        return None
    year = due_date.get("year")
    month = due_date.get("month")
    day = due_date.get("day")
    if not (year and month and day):
        return None
    due_time = coursework.get("dueTime") or {}
    hour = due_time.get("hours", 23)
    minute = due_time.get("minutes", 59)
    second = due_time.get("seconds", 0)
    # Classroom stores dueTime in UTC.
    return datetime(year, month, day, hour, minute, second, tzinfo=UTC)


def _parse_rfc3339(value: str | None) -> datetime | None:
    if not value:
        return None
    # Classroom timestamps are RFC3339 with trailing 'Z'.
    cleaned = value.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(cleaned).astimezone(UTC)
    except ValueError:
        return None


# --------------------------------------------------------------------------- #
# Classroom API client
# --------------------------------------------------------------------------- #


class ClassroomSource(Source):
    """Live Classroom client — wraps google-api-python-client.

    The constructor takes a path to a token JSON file produced by the
    ``auth classroom`` CLI subcommand. Fetch raises ``AuthExpiredError`` if the
    refresh fails (typically: revoked consent or password change).
    """

    name = "classroom"

    def __init__(self, token_path: Path):
        self.token_path = token_path

    def fetch(self, child: str) -> list[Task]:
        # Imports are lazy so unit tests don't pull in google libs.
        from google.auth.exceptions import RefreshError
        from google.auth.transport.requests import Request
        from google.oauth2.credentials import Credentials
        from googleapiclient.discovery import build
        from googleapiclient.errors import HttpError

        if not self.token_path.exists():
            raise AuthExpiredError(
                f"No Classroom token at {self.token_path} — run "
                f"`homework-hub auth classroom --child {child}`"
            )

        creds = Credentials.from_authorized_user_file(str(self.token_path), OAUTH_SCOPES)
        try:
            if creds.expired and creds.refresh_token:
                creds.refresh(Request())
                self.token_path.write_text(creds.to_json())
        except RefreshError as exc:
            raise AuthExpiredError(f"Classroom token refresh failed: {exc}") from exc

        if not creds.valid:
            raise AuthExpiredError(f"Classroom token invalid for {child} — re-run auth")

        service = build("classroom", "v1", credentials=creds, cache_discovery=False)
        try:
            return list(_collect_tasks(service, child))
        except HttpError as exc:
            if exc.resp.status in (401, 403):
                raise AuthExpiredError(f"Classroom API auth error: {exc}") from exc
            if 500 <= exc.resp.status < 600:
                raise TransientError(f"Classroom API {exc.resp.status}: {exc}") from exc
            raise


def _collect_tasks(service: Any, child: str):
    """Page through courses → coursework → submissions, yielding Tasks."""
    courses_resp = service.courses().list(courseStates=["ACTIVE"], pageSize=100).execute()
    for course in courses_resp.get("courses", []):
        course_id = course["id"]
        cw_resp = service.courses().courseWork().list(courseId=course_id, pageSize=100).execute()
        for cw in cw_resp.get("courseWork", []):
            sub_resp = (
                service.courses()
                .courseWork()
                .studentSubmissions()
                .list(courseId=course_id, courseWorkId=cw["id"], userId="me")
                .execute()
            )
            submissions = sub_resp.get("studentSubmissions") or [None]
            # There's typically exactly one submission per (courseWork, user).
            yield map_coursework_to_task(
                child=child,
                course=course,
                coursework=cw,
                submission=submissions[0],
            )


# --------------------------------------------------------------------------- #
# OAuth bootstrap (run interactively on the Mac)
# --------------------------------------------------------------------------- #


def run_oauth_flow(client_secret: dict[str, Any], token_path: Path) -> None:
    """Run the Desktop-app OAuth flow and write the resulting token file.

    Pulled out so the CLI just calls this with a parsed dict from Vaultwarden.
    Tests mock this away.
    """
    from google_auth_oauthlib.flow import InstalledAppFlow

    flow = InstalledAppFlow.from_client_config(client_secret, OAUTH_SCOPES)
    creds = flow.run_local_server(port=0)
    token_path.parent.mkdir(parents=True, exist_ok=True)
    token_path.write_text(creds.to_json())


def load_client_secret(raw: str) -> dict[str, Any]:
    """Parse the Google OAuth client_secret JSON blob.

    Accepts either the full ``{"installed": {...}}`` wrapper or the inner dict.
    """
    parsed = json.loads(raw)
    if "installed" in parsed or "web" in parsed:
        return parsed
    # Bare inner shape — wrap so InstalledAppFlow accepts it.
    return {"installed": parsed}
