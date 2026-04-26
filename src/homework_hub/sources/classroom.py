"""Google Classroom homework source — Playwright DOM scrape.

The official Classroom REST API requires OAuth, which Mordialloc's Workspace
admin has blocked domain-wide (``admin_policy_enforced``). We work around
this by replaying the kid's authenticated browser session against the public
classroom.google.com web app and parsing the rendered DOM.

Architecture mirrors :mod:`homework_hub.sources.edrolo`:

- ``map_classroom_card_to_task``: pure dict → Task. Fully unit-tested.
- ``parse_due_text``: pure date-string parser, also unit-tested.
- ``ClassroomStorageState``: load/save Playwright ``storage_state.json`` and
  validate that the Google session cookies are present.
- ``run_headed_login``: opens a headed Chromium on the Mac so the kid can
  finish Google SSO; dumps cookies + localStorage on success.
- ``ClassroomScraper``: replays the cookies in headless Chromium at sync
  time, visits the three "to-do" tabs, and extracts assignment cards via
  ``page.eval_on_selector_all``.
- ``ClassroomSource``: per-child ``Source.fetch`` implementation.

DOM contract verified 2026-04 against classroom.google.com; see
``tests/fixtures/classroom_card_*.json`` for captured samples. If Google
changes the markup, the selectors in ``CARD_SELECTORS`` are the only thing
that should need updating — the parser is otherwise data-driven.
"""

from __future__ import annotations

import contextlib
import json
import re
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from homework_hub.models import Source as SourceEnum
from homework_hub.models import Status, Task
from homework_hub.pipeline.ingest import RawRecord
from homework_hub.sources.base import (
    AuthExpiredError,
    SchemaBreakError,
    Source,
    TransientError,
)

DEFAULT_BASE_URL = "https://classroom.google.com"

# Three views we scrape. URL paths are stable across Classroom releases.
VIEW_PATHS: dict[str, str] = {
    "assigned": "/u/0/a/not-turned-in/all",
    "missing": "/u/0/a/missing/all",
    "done": "/u/0/a/turned-in/all",
}

# Australian school timezone — Classroom renders due dates in the user's
# local zone without timezone markers, so we must assume one.
DEFAULT_TZ = ZoneInfo("Australia/Melbourne")

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)

# CSS selectors used inside the assignment-card <a> element. Verified live
# against classroom.google.com on 2026-04-26. Update here if Google ships a
# UI change.
CARD_SELECTORS: dict[str, str] = {
    "card_link": "a.nUg0Te.Ixt5L",
    "card_meta": ".QRiHXd",  # holds data-course-id / data-stream-item-id
    "title": "p.VjRxGc.oDLUVd",
    "subject": "p.tWeh6",
    "due_or_status": "p.tGZ0W.pOf0gc",
    "icon": "i.google-symbols",
}

# JS run inside the page to extract structured cards. Returning a dict per
# card is dramatically cheaper than serialising HTML and re-parsing it Python-
# side, and it keeps the brittle DOM coupling in exactly one place.
_EXTRACT_CARDS_JS = """
() => {
  const out = [];
  for (const a of document.querySelectorAll('a.nUg0Te.Ixt5L')) {
    const meta = a.querySelector('.QRiHXd');
    const titleEl = a.querySelector('p.VjRxGc.oDLUVd');
    const subjEl = a.querySelector('p.tWeh6');
    const dueEl = a.querySelector('p.tGZ0W.pOf0gc');
    const iconEl = a.querySelector('i.google-symbols');
    out.push({
      href: a.getAttribute('href') || '',
      course_id: meta ? meta.getAttribute('data-course-id') : null,
      stream_item_id: meta ? meta.getAttribute('data-stream-item-id') : null,
      stream_item_type: meta ? meta.getAttribute('data-stream-item-type') : null,
      title: titleEl ? titleEl.innerText.trim() : '',
      subject: subjEl ? subjEl.innerText.trim() : '',
      due_or_status: dueEl ? dueEl.innerText.trim() : '',
      icon: iconEl ? iconEl.innerText.trim() : '',
    });
  }
  return out;
}
"""


# --------------------------------------------------------------------------- #
# Pure mappers
# --------------------------------------------------------------------------- #


def map_classroom_card_to_task(
    *,
    child: str,
    view: str,
    card: dict[str, Any],
    base_url: str = DEFAULT_BASE_URL,
    tz: ZoneInfo = DEFAULT_TZ,
    today: date | None = None,
) -> Task:
    """Translate one extracted card dict into a canonical Task.

    ``view`` is one of ``"assigned" | "missing" | "done"`` and primes the
    status mapping when the card text alone is ambiguous.
    """
    course_id = card.get("course_id") or ""
    stream_item_id = card.get("stream_item_id") or ""
    title = card.get("title") or ""
    if not course_id or not stream_item_id or not title:
        raise SchemaBreakError(f"Classroom card missing id/title fields: {sorted(card.keys())}")

    href = card.get("href") or ""
    url = f"{base_url}{href}" if href.startswith("/") else href

    due_or_status = card.get("due_or_status") or ""
    due_at = parse_due_text(due_or_status, tz=tz, today=today)
    status_raw, status = _resolve_status(view, due_or_status, due_at)

    return Task(
        source=SourceEnum.CLASSROOM,
        source_id=f"{course_id}:{stream_item_id}",
        child=child,
        subject=card.get("subject") or "",
        title=title,
        description="",  # not present on list view; would need per-card fetch
        # Classroom's list-view DOM (cards on /u/0/a/{not-turned-in,missing,
        # turned-in}/all) does not expose a Posted timestamp at all. It's
        # only rendered on the per-card detail page (/u/0/c/{course}/m/{id}
        # /details). Investigated under M7 (2026-04-26): an extra HTTP
        # round-trip per card would add ~1-2 min per sync and a fresh
        # rate-limit / flake surface, for a field that's purely
        # informational on the kid sheet. Documented as upstream-
        # unavailable; assigned_at stays None for Classroom rows.
        assigned_at=None,
        due_at=due_at,
        status=status,
        status_raw=status_raw,
        url=url,
    )


# --------------------------------------------------------------------------- #
# Status resolution
# --------------------------------------------------------------------------- #


# Strings observed on cards. Order matters: negative ("not handed in") and
# more-specific ("done late") matchers must run before the bare positive
# patterns or "Not handed in" would match the "handed in" rule.
_STATUS_PATTERNS: list[tuple[re.Pattern[str], Status]] = [
    (re.compile(r"\bnot handed in\b", re.I), Status.OVERDUE),
    (re.compile(r"\bnot turned in\b", re.I), Status.OVERDUE),
    (re.compile(r"\bdone late\b", re.I), Status.SUBMITTED),
    (re.compile(r"\bhanded in\b", re.I), Status.SUBMITTED),
    (re.compile(r"\bturned in\b", re.I), Status.SUBMITTED),
    (re.compile(r"\breturned\b", re.I), Status.GRADED),
    (re.compile(r"\bgraded\b", re.I), Status.GRADED),
]


def _resolve_status(view: str, raw: str, due_at: datetime | None) -> tuple[str, Status]:
    """Combine view-tab + card text + due date into a Status."""
    cleaned = _strip_artifacts(raw)
    for pattern, status in _STATUS_PATTERNS:
        if pattern.search(cleaned):
            return cleaned, status
    if view == "done":
        # Fallback: anything reaching the Done tab without an explicit
        # marker is treated as submitted.
        return cleaned or "done", Status.SUBMITTED
    if view == "missing":
        return cleaned or "missing", Status.OVERDUE
    # Assigned tab: derive overdue from the due date.
    if due_at is not None and due_at < datetime.now(UTC):
        return cleaned or "overdue", Status.OVERDUE
    return cleaned or "assigned", Status.NOT_STARTED


def _strip_artifacts(text: str) -> str:
    """Remove Material translation labels and collapse whitespace."""
    # "Estigfend" is the Material translate-button label that occasionally
    # leaks into innerText. Drop it and any pure whitespace.
    cleaned = re.sub(r"\bEstigfend\b", "", text or "", flags=re.I)
    return re.sub(r"\s+", " ", cleaned).strip()


# --------------------------------------------------------------------------- #
# Date parsing — Classroom shows multiple formats with no year/timezone
# --------------------------------------------------------------------------- #


_WEEKDAYS = {
    "monday": 0,
    "tuesday": 1,
    "wednesday": 2,
    "thursday": 3,
    "friday": 4,
    "saturday": 5,
    "sunday": 6,
}
_MONTHS = {
    "jan": 1,
    "january": 1,
    "feb": 2,
    "february": 2,
    "mar": 3,
    "march": 3,
    "apr": 4,
    "april": 4,
    "may": 5,
    "jun": 6,
    "june": 6,
    "jul": 7,
    "july": 7,
    "aug": 8,
    "august": 8,
    "sep": 9,
    "sept": 9,
    "september": 9,
    "oct": 10,
    "october": 10,
    "nov": 11,
    "november": 11,
    "dec": 12,
    "december": 12,
}

# Patterns we accept (all observed live):
#   "Wednesday, 23:59"                   — this week, time only
#   "Friday, 23:59"
#   "Tuesday, 2 Dec 2025"                — full date with year
#   "Wednesday 6 May"                    — date without year (next year)
_RE_TIME_ONLY = re.compile(r"^(?P<dow>[A-Za-z]+)[\s,]+(?P<h>\d{1,2}):(?P<m>\d{2})$")
_RE_FULL_DATE = re.compile(
    r"^(?P<dow>[A-Za-z]+)[\s,]+(?P<d>\d{1,2})\s+(?P<mon>[A-Za-z]+)\s+(?P<y>\d{4})"
    r"(?:[\s,]+(?P<h>\d{1,2}):(?P<m>\d{2}))?$"
)
_RE_DATE_NO_YEAR = re.compile(
    r"^(?P<dow>[A-Za-z]+)[\s,]+(?P<d>\d{1,2})\s+(?P<mon>[A-Za-z]+)"
    r"(?:[\s,]+(?P<h>\d{1,2}):(?P<m>\d{2}))?$"
)


def parse_due_text(
    text: str,
    *,
    tz: ZoneInfo = DEFAULT_TZ,
    today: date | None = None,
) -> datetime | None:
    """Parse a Classroom due-date string into a UTC datetime.

    Returns ``None`` for status-only strings (e.g. "Handed in") or unparseable
    input. ``today`` is injectable so unit tests can lock the calendar.
    """
    if not text:
        return None
    cleaned = _strip_artifacts(text)
    if not cleaned:
        return None

    today = today or datetime.now(tz).date()

    # Time-only: "Wednesday, 23:59" — resolve weekday to next 0..6 days out.
    m = _RE_TIME_ONLY.match(cleaned)
    if m:
        dow = _WEEKDAYS.get(m.group("dow").lower())
        if dow is None:
            return None
        target = today + timedelta(days=(dow - today.weekday()) % 7)
        return _make_dt(target, int(m.group("h")), int(m.group("m")), tz)

    # Full date with explicit year: "Tuesday, 2 Dec 2025" (optional time).
    m = _RE_FULL_DATE.match(cleaned)
    if m:
        month = _MONTHS.get(m.group("mon").lower())
        if month is None:
            return None
        try:
            target = date(int(m.group("y")), month, int(m.group("d")))
        except ValueError:
            return None
        hh = int(m.group("h")) if m.group("h") else 23
        mm = int(m.group("m")) if m.group("m") else 59
        return _make_dt(target, hh, mm, tz)

    # Date without year: "Wednesday 6 May" — assume the next occurrence.
    m = _RE_DATE_NO_YEAR.match(cleaned)
    if m:
        month = _MONTHS.get(m.group("mon").lower())
        if month is None:
            return None
        day = int(m.group("d"))
        try:
            candidate = date(today.year, month, day)
        except ValueError:
            return None
        if candidate < today:
            try:
                candidate = candidate.replace(year=today.year + 1)
            except ValueError:
                return None
        hh = int(m.group("h")) if m.group("h") else 23
        mm = int(m.group("m")) if m.group("m") else 59
        return _make_dt(candidate, hh, mm, tz)

    return None


def _make_dt(d: date, hour: int, minute: int, tz: ZoneInfo) -> datetime:
    return datetime(d.year, d.month, d.day, hour, minute, tzinfo=tz).astimezone(UTC)


# --------------------------------------------------------------------------- #
# Storage state
# --------------------------------------------------------------------------- #


class ClassroomStorageState:
    """Wrapper around a Playwright ``storage_state.json`` for classroom.google.com.

    Google's auth lives across ``.google.com`` cookies (``SID``, ``HSID``,
    ``SSID``, ``APISID``, ``SAPISID`` and the SameSite-flavoured variants).
    A valid session has at least ``SID`` and one of the SAPISID cookies.
    """

    REQUIRED_COOKIES = ("SID",)
    SECURE_COOKIE_GROUP = ("SAPISID", "__Secure-1PAPISID", "__Secure-3PAPISID")

    def __init__(self, raw: dict[str, Any], path: Path | None = None):
        self.raw = raw
        self.path = path

    @classmethod
    def load(cls, path: Path) -> ClassroomStorageState:
        if not path.exists():
            raise AuthExpiredError(
                f"No Classroom storage state at {path} — run "
                "`homework-hub auth classroom --child <name>`"
            )
        try:
            raw = json.loads(path.read_text())
        except json.JSONDecodeError as exc:
            raise AuthExpiredError(f"Classroom storage state at {path} is not valid JSON") from exc
        state = cls(raw, path=path)
        state.validate()
        return state

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(self.raw, indent=2))
        self.path = path

    def validate(self) -> None:
        cookies = self.cookies_for_domain("google.com")
        for required in self.REQUIRED_COOKIES:
            if required not in cookies:
                raise AuthExpiredError(
                    f"Classroom storage state missing '{required}' cookie — "
                    "re-run `homework-hub auth classroom`."
                )
        if not any(name in cookies for name in self.SECURE_COOKIE_GROUP):
            raise AuthExpiredError(
                "Classroom storage state missing SAPISID-family cookie — "
                "re-run `homework-hub auth classroom`."
            )

    def cookies_for_domain(self, domain: str) -> dict[str, str]:
        """Return ``{name: value}`` for cookies scoped to *domain* or its parents."""
        out: dict[str, str] = {}
        for c in self.raw.get("cookies", []):
            cd = (c.get("domain") or "").lstrip(".")
            if cd and (domain == cd or domain.endswith("." + cd) or cd.endswith("." + domain)):
                out[c["name"]] = c["value"]
        return out


# --------------------------------------------------------------------------- #
# Headed login (Mac-only)
# --------------------------------------------------------------------------- #


def run_headed_login(out_path: Path, *, base_url: str = DEFAULT_BASE_URL) -> None:
    """Open a headed Chromium so the kid can complete Google SSO.

    Lazily imports playwright so the server runtime — which only does
    headless replay — doesn't pay the cost.
    """
    from playwright.sync_api import sync_playwright

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(user_agent=DEFAULT_USER_AGENT)
        page = context.new_page()
        page.goto(f"{base_url}/u/0/a/not-turned-in/all")
        # Wait until we land on an authenticated Classroom page (i.e. we've
        # left accounts.google.com and the URL contains /a/ or /h).
        page.wait_for_url(
            lambda url: "accounts.google.com" not in url and "classroom.google.com" in url,
            timeout=300_000,
        )
        # Allow SPA hydration so cookies are fully flushed.
        page.wait_for_timeout(3_000)
        ClassroomStorageState(context.storage_state()).save(out_path)
        browser.close()


# --------------------------------------------------------------------------- #
# Headless scrape (server)
# --------------------------------------------------------------------------- #


@dataclass
class ScrapeResult:
    view: str
    cards: list[dict[str, Any]]


class ClassroomScraper:
    """Headless Playwright wrapper that replays storage_state and harvests cards.

    Designed to be context-managed so the browser is closed on every exit
    path — this matters because the daemon process is long-lived.
    """

    def __init__(
        self,
        storage: ClassroomStorageState,
        *,
        base_url: str = DEFAULT_BASE_URL,
        user_agent: str = DEFAULT_USER_AGENT,
        page_timeout_ms: int = 30_000,
        post_load_settle_ms: int = 1_500,
    ):
        self.storage = storage
        self.base_url = base_url.rstrip("/")
        self.user_agent = user_agent
        self.page_timeout_ms = page_timeout_ms
        self.post_load_settle_ms = post_load_settle_ms
        self._pw: Any = None
        self._browser: Any = None
        self._context: Any = None

    def __enter__(self) -> ClassroomScraper:
        from playwright.sync_api import sync_playwright

        self._pw = sync_playwright().start()
        self._browser = self._pw.chromium.launch(headless=True)
        self._context = self._browser.new_context(
            storage_state=self.storage.raw, user_agent=self.user_agent
        )
        return self

    def __exit__(self, *_exc: Any) -> None:
        for closer in (self._context, self._browser):
            if closer is not None:
                with contextlib.suppress(Exception):  # pragma: no cover - best-effort teardown
                    closer.close()
        if self._pw is not None:
            with contextlib.suppress(Exception):  # pragma: no cover
                self._pw.stop()

    def fetch_view(self, view: str) -> ScrapeResult:
        from playwright.sync_api import Error as PlaywrightError
        from playwright.sync_api import TimeoutError as PlaywrightTimeoutError

        if view not in VIEW_PATHS:
            raise SchemaBreakError(f"Unknown Classroom view: {view}")
        if self._context is None:
            raise RuntimeError("ClassroomScraper used outside of context manager")

        url = f"{self.base_url}{VIEW_PATHS[view]}"
        page = self._context.new_page()
        try:
            try:
                page.goto(url, timeout=self.page_timeout_ms, wait_until="domcontentloaded")
            except PlaywrightTimeoutError as exc:
                raise TransientError(f"Classroom timeout loading {view}: {exc}") from exc

            # Detect auth bounce: Google redirects to accounts.google.com when
            # the session cookies are stale.
            if "accounts.google.com" in page.url:
                raise AuthExpiredError(
                    f"Classroom redirected to login for {view} — re-run "
                    "`homework-hub auth classroom`."
                )

            # Wait for either the card list or the "no work" empty state.
            # Empty pages legitimately have neither marker; fall through and
            # return zero cards rather than failing the whole sync.
            with contextlib.suppress(PlaywrightTimeoutError):
                page.wait_for_function(
                    "() => document.querySelectorAll('a.nUg0Te.Ixt5L').length > 0 "
                    "|| document.body.innerText.includes('All done')",
                    timeout=self.page_timeout_ms,
                )

            page.wait_for_timeout(self.post_load_settle_ms)

            try:
                cards = page.evaluate(_EXTRACT_CARDS_JS)
            except PlaywrightError as exc:
                raise SchemaBreakError(f"Classroom DOM extract failed: {exc}") from exc

            if not isinstance(cards, list):
                raise SchemaBreakError(
                    f"Classroom extractor returned non-list: {type(cards).__name__}"
                )
            return ScrapeResult(view=view, cards=cards)
        finally:
            page.close()


# --------------------------------------------------------------------------- #
# Source implementation
# --------------------------------------------------------------------------- #


class ClassroomSource(Source):
    """Per-child Classroom source — one storage_state.json on disk."""

    name = "classroom"

    def __init__(
        self,
        storage_path_for_child: dict[str, Path],
        *,
        scraper_factory: Any = None,
        tz: ZoneInfo = DEFAULT_TZ,
        base_url: str = DEFAULT_BASE_URL,
    ):
        self.storage_path_for_child = storage_path_for_child
        self.tz = tz
        self.base_url = base_url
        self._scraper_factory = scraper_factory or (
            lambda storage: ClassroomScraper(storage, base_url=base_url)
        )

    def fetch(self, child: str) -> list[Task]:
        if child not in self.storage_path_for_child:
            raise SchemaBreakError(f"No Classroom storage state path configured for {child}.")
        storage = ClassroomStorageState.load(self.storage_path_for_child[child])
        tasks: list[Task] = []
        with self._scraper_factory(storage) as scraper:
            for view in VIEW_PATHS:
                result = scraper.fetch_view(view)
                for card in result.cards:
                    tasks.append(
                        map_classroom_card_to_task(
                            child=child,
                            view=view,
                            card=card,
                            base_url=self.base_url,
                            tz=self.tz,
                        )
                    )
        return _dedupe_by_source_id(tasks)

    def fetch_raw(self, child: str) -> list[RawRecord]:
        """Scrape Classroom views and emit raw card payloads for bronze.

        One ``RawRecord`` per (course_id, stream_item_id, view). The view is
        embedded in the payload so the silver mapper can resolve status
        without re-scraping. Cards that appear in multiple views (e.g.
        ``/missing`` and ``/not-turned-in``) generate one bronze row per
        view; silver-layer dedup picks the last-write-wins.
        """
        if child not in self.storage_path_for_child:
            raise SchemaBreakError(f"No Classroom storage state path configured for {child}.")
        storage = ClassroomStorageState.load(self.storage_path_for_child[child])
        records: list[RawRecord] = []
        with self._scraper_factory(storage) as scraper:
            for view in VIEW_PATHS:
                result = scraper.fetch_view(view)
                for card in result.cards:
                    course_id = card.get("course_id") or ""
                    stream_item_id = card.get("stream_item_id") or ""
                    if not course_id or not stream_item_id:
                        raise SchemaBreakError(
                            f"Classroom card missing id fields: {sorted(card.keys())}"
                        )
                    source_id = f"{course_id}:{stream_item_id}"
                    records.append(
                        RawRecord(
                            child=child,
                            source=SourceEnum.CLASSROOM.value,
                            source_id=source_id,
                            payload={
                                "card": card,
                                "view": view,
                                "base_url": self.base_url,
                            },
                        )
                    )
        return records


def _dedupe_by_source_id(tasks: list[Task]) -> list[Task]:
    """A task can appear in both /missing and /not-turned-in. Last-write wins,
    which means /missing (later in our iteration order is /done) overrides
    /assigned — appropriate because /missing is a stricter signal."""
    by_id: dict[str, Task] = {}
    for t in tasks:
        by_id[t.source_id] = t
    return list(by_id.values())
