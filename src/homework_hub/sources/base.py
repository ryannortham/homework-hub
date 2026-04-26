"""Source ABC and shared exceptions for all homework-fetching connectors."""

from __future__ import annotations

from abc import ABC, abstractmethod

from homework_hub.models import Task


class SourceError(Exception):
    """Base class for source-level failures."""


class AuthExpiredError(SourceError):
    """Raised when the source's session/cookie/token is no longer valid.

    Triggers an `auth_expired` Discord alert in the orchestrator.
    """


class TransientError(SourceError):
    """Raised on network timeouts, 5xx responses, etc. Auto-retried."""


class SchemaBreakError(SourceError):
    """Raised when the upstream response doesn't match expected shape.

    Indicates the third-party site changed its API; needs a code fix.
    """


class Source(ABC):
    """Interface every homework source implements."""

    name: str = ""

    @abstractmethod
    def fetch(self, child: str) -> list[Task]:
        """Return the current set of homework tasks for a single child."""
