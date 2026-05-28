"""Canonical task schema and helpers shared across all sources and sinks."""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Self

from pydantic import BaseModel, Field, field_validator


class Source(StrEnum):
    """The platform a task originated from."""

    CLASSROOM = "classroom"
    COMPASS = "compass"
    EDUPERFECT = "eduperfect"
    EDROLO = "edrolo"


class Status(StrEnum):
    """Normalised completion status across all sources."""

    NOT_STARTED = "not_started"
    IN_PROGRESS = "in_progress"
    SUBMITTED = "submitted"
    GRADED = "graded"
    OVERDUE = "overdue"
    ARCHIVED = "archived"


class TaskType(StrEnum):
    """Task category as reported by the upstream LMS.

    Compass exposes three teacher-assigned badge types. All other sources
    default to ``homework`` — they are assignment/practice platforms with no
    equivalent type taxonomy.
    """

    ASSESSMENT = "assessment"
    HOMEWORK = "homework"
    GENERAL = "general"


class Task(BaseModel):
    """The canonical homework task. Every source maps to this shape."""

    source: Source
    source_id: str = Field(min_length=1)
    child: str = Field(min_length=1)
    subject: str = ""
    title: str = Field(min_length=1)
    description: str = ""
    assigned_at: datetime | None = None
    due_at: datetime | None = None
    submitted_at: datetime | None = None
    status_raw: str = ""
    status: Status = Status.NOT_STARTED
    task_type: TaskType = TaskType.HOMEWORK
    # Ordered list of sub-task checkpoints derived from Compass gradingItems
    # where measureUniqueId == "Checkpoints". Empty for all other sources and
    # for Compass tasks that have no checkpoint breakdown.
    checkpoints: list[dict[str, Any]] = Field(default_factory=list)
    url: str = ""
    last_synced: datetime = Field(default_factory=lambda: datetime.now(UTC))
    # Operational/archival metadata populated by the silver layer. Not set by
    # source mappers; surfaced in publish for partitioning + diagnostics.
    first_seen_at: datetime | None = None
    last_seen_at: datetime | None = None
    archived_at: datetime | None = None
    archived_reason: str | None = None

    @field_validator(
        "assigned_at",
        "due_at",
        "submitted_at",
        "last_synced",
        "first_seen_at",
        "last_seen_at",
        "archived_at",
    )
    @classmethod
    def ensure_tz_aware(cls, value: datetime | None) -> datetime | None:
        """All datetimes stored in UTC. Naive values are assumed UTC."""
        if value is None:
            return None
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)

    @property
    def dedup_key(self) -> tuple[str, str, str]:
        """Stable upsert key — (child, source, source_id)."""
        return (self.child, self.source.value, self.source_id)

    def with_overdue_check(self, now: datetime | None = None) -> Self:
        """Return a copy with status flipped to OVERDUE if past due and not submitted."""
        if self.due_at is None or self.status in (Status.SUBMITTED, Status.GRADED, Status.ARCHIVED):
            return self
        ref = now or datetime.now(UTC)
        if self.due_at < ref:
            return self.model_copy(update={"status": Status.OVERDUE})
        return self


def merge_tasks(existing: list[Task], incoming: list[Task]) -> list[Task]:
    """Merge incoming tasks into existing, replacing on dedup_key match.

    Ordering of the returned list is: incoming task order first (in their order),
    followed by any existing tasks that were not replaced (in their order).
    """
    incoming_keys = {t.dedup_key for t in incoming}
    kept = [t for t in existing if t.dedup_key not in incoming_keys]
    return [*incoming, *kept]
