"""Source ABC and shared exceptions for all homework-fetching connectors."""

from __future__ import annotations

from abc import ABC, abstractmethod

from homework_hub.models import Task
from homework_hub.pipeline.ingest import RawRecord


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

    # Set to True on sources whose auth tokens are structurally short-lived
    # (e.g. EP ~30 min JWTs). When True, the orchestrator silently skips
    # subsequent syncs after the first auth_expired failure until a successful
    # ingest resets the clock — avoiding hourly noise for an expected condition.
    silence_repeated_auth_expired: bool = False

    @abstractmethod
    def fetch(self, child: str) -> list[Task]:
        """Return the current set of homework tasks for a single child."""

    def fetch_raw(self, child: str) -> list[RawRecord]:
        """Return raw upstream payloads for the bronze layer.

        Default implementation raises ``NotImplementedError`` so legacy
        sources keep working until they migrate. Once the medallion
        pipeline is the only path, this becomes ``@abstractmethod``.
        """
        raise NotImplementedError(f"{type(self).__name__}.fetch_raw not implemented")
