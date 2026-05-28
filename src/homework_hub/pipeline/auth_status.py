"""Per-source auth status — surfaces last-sync + token-expiry for the Settings tab.

For each source enabled in ``children.yaml`` for a given child, this module
reads:

* the most-recent ``auth_status`` row from ``state.db`` (populated by the
  medallion orchestrator on every ingest), and
* the token file on disk (cookie expiry for Classroom/Edrolo, the
  ``expires_at`` JWT field for EduPerfect, ``captured_at`` for Compass).

The two pieces are combined into a small :class:`SourceAuthRow` dataclass
which the publisher renders into the Settings tab.

Token-file reads are best-effort: a missing or unparseable file yields a
``token_expires_at=None`` and a ``status='missing'`` rather than raising.
The Settings tab is informational and must never crash a sync.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from homework_hub.config import ChildConfig, Settings
from homework_hub.state.store import StateStore

log = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Public types
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class SourceAuthRow:
    """One row of per-source auth/sync status."""

    source: str  # canonical key: 'classroom' / 'compass' / 'edrolo' / 'eduperfect'
    display_name: str  # 'Classroom' / 'Compass' / 'Edrolo' / 'EduPerfect'
    last_success_at: datetime | None  # most recent successful ingest
    last_failure_at: datetime | None
    last_failure_kind: str | None
    token_expires_at: datetime | None  # None = unknown / session cookie
    token_present: bool  # token file exists on disk
    status: str  # 'ok' | 'expired' | 'expiring' | 'missing' | 'never_synced' | 'unknown'


# Mapping from canonical source key to display label. Mirrors
# publish._SOURCE_DISPLAY but kept here so we don't import from publish.
_DISPLAY: dict[str, str] = {
    "classroom": "Classroom",
    "compass": "Compass",
    "edrolo": "Edrolo",
    "eduperfect": "EduPerfect",
}


# --------------------------------------------------------------------------- #
# Token expiry readers
# --------------------------------------------------------------------------- #


def _read_classroom_expiry(path: Path) -> datetime | None:
    """Read SID cookie expiry from a Classroom storage_state.json."""
    try:
        raw = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        log.debug("classroom token read failed: %s", exc)
        return None
    for c in raw.get("cookies", []):
        if c.get("name") != "SID":
            continue
        cd = (c.get("domain") or "").lstrip(".")
        if cd != "google.com" and not cd.endswith(".google.com"):
            continue
        exp = c.get("expires")
        if not isinstance(exp, (int, float)) or exp <= 0:
            return None
        try:
            return datetime.fromtimestamp(float(exp), tz=UTC)
        except (OSError, ValueError, OverflowError):
            return None
    return None


def _read_edrolo_expiry(path: Path) -> datetime | None:
    """Read sessionid cookie expiry from an Edrolo storage_state.json.

    Edrolo sessions are usually browser-session cookies (``expires == -1``)
    so this typically returns ``None``.
    """
    try:
        raw = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        log.debug("edrolo token read failed: %s", exc)
        return None
    for c in raw.get("cookies", []):
        if c.get("name") != "sessionid":
            continue
        cd = (c.get("domain") or "").lstrip(".")
        if cd != "app.edrolo.com" and not cd.endswith(".edrolo.com"):
            continue
        exp = c.get("expires")
        if not isinstance(exp, (int, float)) or exp <= 0:
            return None
        try:
            return datetime.fromtimestamp(float(exp), tz=UTC)
        except (OSError, ValueError, OverflowError):
            return None
    return None


def _read_eduperfect_expiry(path: Path) -> datetime | None:
    """Read the explicit ``expires_at`` JWT field from an EP token file."""
    try:
        raw = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        log.debug("eduperfect token read failed: %s", exc)
        return None
    iso = raw.get("expires_at")
    if not isinstance(iso, str):
        return None
    try:
        return datetime.fromisoformat(iso).astimezone(UTC)
    except ValueError:
        return None


def _read_compass_captured_at(path: Path) -> datetime | None:
    """Read ``captured_at`` from the shared parent Compass token file.

    Compass does not publish a session lifetime so we return the capture
    time as the closest meaningful timestamp. Stale Compass cookies are
    only detected reactively (HTTP 302/401/403).
    """
    try:
        raw = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        log.debug("compass token read failed: %s", exc)
        return None
    iso = raw.get("captured_at")
    if not isinstance(iso, str):
        return None
    try:
        return datetime.fromisoformat(iso).astimezone(UTC)
    except ValueError:
        return None


# --------------------------------------------------------------------------- #
# Status classification
# --------------------------------------------------------------------------- #

# Treat tokens expiring within this window as "expiring soon" so the
# Settings tab nudges the user to re-auth before the next sync tick fails.
_EXPIRING_SOON = 24 * 60 * 60  # seconds == 24 hours


def _classify_status(
    *,
    source: str,
    token_present: bool,
    token_expires_at: datetime | None,
    last_success_at: datetime | None,
    last_failure_at: datetime | None,
    now: datetime,
) -> str:
    """Distill the auth/expiry signals into a single short label.

    ``source == 'compass'`` skips the expiry check because the
    ``token_expires_at`` slot holds ``captured_at`` for Compass (the
    cookie has no published lifetime), so an "old" capture should not be
    reported as expired.
    """
    if not token_present:
        return "missing"
    if source != "compass" and token_expires_at is not None:
        delta = (token_expires_at - now).total_seconds()
        if delta <= 0:
            return "expired"
        if delta <= _EXPIRING_SOON:
            return "expiring"
    if last_success_at is None and last_failure_at is None:
        return "never_synced"
    if last_failure_at is not None and (
        last_success_at is None or last_failure_at > last_success_at
    ):
        return "failing"
    if last_success_at is not None:
        return "ok"
    return "unknown"


# --------------------------------------------------------------------------- #
# Public assembly
# --------------------------------------------------------------------------- #


def _enabled_sources(child_cfg: ChildConfig) -> list[str]:
    """Return canonical source keys enabled for this child, in display order."""
    s = child_cfg.sources
    out: list[str] = []
    if s.classroom.enabled:
        out.append("classroom")
    if s.compass.enabled:
        out.append("compass")
    if s.edrolo.enabled:
        out.append("edrolo")
    if s.eduperfect.enabled:
        out.append("eduperfect")
    return out


def _token_path_for(child: str, source: str, settings: Settings) -> Path:
    """Return the on-disk token path for ``(child, source)``.

    Compass is special: a single shared parent token covers all children.
    """
    if source == "compass":
        return settings.tokens_dir / "compass-parent.json"
    return settings.child_token_path(child, source)


def _read_expiry(source: str, path: Path) -> datetime | None:
    if source == "classroom":
        return _read_classroom_expiry(path)
    if source == "edrolo":
        return _read_edrolo_expiry(path)
    if source == "eduperfect":
        return _read_eduperfect_expiry(path)
    if source == "compass":
        # Compass has no expiry; surface captured_at instead so the kid can
        # see roughly when the cookie was last refreshed.
        return _read_compass_captured_at(path)
    return None


def build_source_auth_rows(
    *,
    child: str,
    child_cfg: ChildConfig,
    settings: Settings,
    state: StateStore,
    now: datetime | None = None,
) -> list[SourceAuthRow]:
    """Assemble one :class:`SourceAuthRow` per source enabled for the child."""
    ref_now = now or datetime.now(UTC)
    rows: list[SourceAuthRow] = []
    for source in _enabled_sources(child_cfg):
        token_path = _token_path_for(child, source, settings)
        token_present = token_path.exists()
        expires_at = _read_expiry(source, token_path) if token_present else None

        auth = state.get_auth(child, source)
        last_success = auth.last_success_at if auth else None
        last_failure = auth.last_failure_at if auth else None
        last_failure_kind = auth.last_failure_kind if auth else None

        status = _classify_status(
            source=source,
            token_present=token_present,
            token_expires_at=expires_at,
            last_success_at=last_success,
            last_failure_at=last_failure,
            now=ref_now,
        )

        rows.append(
            SourceAuthRow(
                source=source,
                display_name=_DISPLAY.get(source, source.title()),
                last_success_at=last_success,
                last_failure_at=last_failure,
                last_failure_kind=last_failure_kind,
                token_expires_at=expires_at,
                token_present=token_present,
                status=status,
            )
        )
    return rows


__all__ = [
    "SourceAuthRow",
    "build_source_auth_rows",
]
