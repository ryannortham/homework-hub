"""Workplan ingestion — Student Workplan Tracker materials from Google Classroom.

This is a deliberately self-contained module so the entire workplan feature
can be removed by deleting this file, ``school_calendar.yaml``, the ``workplan``
block in ``children.yaml`` and the two seam call-sites in ``wiring.py`` and
``medallion_orchestrator.py``.

Design:

* Reuses the existing ``ClassroomScraper`` browser context to harvest topic
  materials from the Classwork page (``/u/0/w/<b64>/t/all``).
* Each material has a Google Form attachment whose questions (one per chapter
  section) become individual ``Task`` rows.
* Tasks land directly in ``silver_tasks`` via ``SilverWriter.upsert_many``
  with ``bronze_id=None`` and ``Source.CLASSROOM`` plus a ``workplan:``
  ``source_id`` prefix so they coexist with real Classroom assignments.
* Full-replace per child: any existing ``workplan:%`` row not in the current
  fetch is deleted (handles Form deletion / topic edits).
* Term/semester dates come from a static YAML committed at build time.

The fetch is wrapped at the orchestrator call-site in ``try/except`` so any
failure here cannot poison the regular sync.
"""

from __future__ import annotations

import json
import logging
import re
import sqlite3
import unicodedata
from base64 import b64encode
from contextlib import closing, suppress
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import yaml
from pydantic import BaseModel

from homework_hub.models import Source, Status, Task, TaskType
from homework_hub.pipeline.transform import SilverWriter
from homework_hub.sources.base import AuthExpiredError, SchemaBreakError, TransientError
from homework_hub.sources.classroom import ClassroomScraper, ClassroomStorageState
from homework_hub.state.store import StateStore

log = logging.getLogger(__name__)

MELBOURNE = ZoneInfo("Australia/Melbourne")

# Form ``FB_PUBLIC_LOAD_DATA_`` regex. The variable embeds a JSON array that
# describes the entire form. We extract the array literal between ``= `` and
# the trailing ``;`` immediately before ``</script>``.
_FB_DATA_RE = re.compile(
    r"var\s+FB_PUBLIC_LOAD_DATA_\s*=\s*(\[.*?\])\s*;\s*</script>",
    re.DOTALL,
)

# Form question types in FB_PUBLIC_LOAD_DATA_. We accept text-ish questions
# and skip identity / agreement / page-break types so each workplan section
# yields exactly one task.
#
#   0 = short answer
#   1 = paragraph
#   2 = multiple choice
#   3 = dropdown
#   4 = checkboxes
#   5 = linear scale
#   6 = title/section header (not a question — used as page break)
#   7 = grid
#   8 = page break
#   9 = date
#  10 = time
#  11 = image
#  13 = video
#
# Workplan sections in practice use type 2 (multiple choice: "Not started" /
# "In progress" / "Complete") but we accept any of 0-4 to be forgiving.
_TASK_QUESTION_TYPES = frozenset({0, 1, 2, 3, 4})
# Common identity / housekeeping fields that prefix workplan forms. Matched
# case-insensitively against the question title.
_SKIP_TITLE_PATTERNS = (
    "name",
    "student name",
    "class",
    "email",
    "date",
    "agreement",
    "i understand",
    "acknowledge",
)


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #


class WorkplanChildConfig(BaseModel):
    """Per-child workplan block in ``children.yaml``.

    Example::

        tahlia:
          workplan:
            enabled: true
            course_id: "829769119654"
            topic: "Student Workplan Tracker"
            subject: "Mathematics Methods"  # raw subject for silver
    """

    enabled: bool = False
    course_id: str = ""
    topic: str = "Student Workplan Tracker"
    subject: str = ""  # raw subject string, fed to dim_subjects resolver


def parse_workplan_child_config(raw: dict[str, Any] | None) -> WorkplanChildConfig | None:
    """Parse a child's ``workplan`` block. Returns ``None`` if disabled/absent."""
    if not raw:
        return None
    cfg = WorkplanChildConfig.model_validate(raw)
    if not cfg.enabled:
        return None
    if not cfg.course_id:
        log.warning("workplan: missing course_id, disabling")
        return None
    return cfg


# --------------------------------------------------------------------------- #
# School calendar
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class Semester:
    label: str  # "S1" / "S2"
    start: date
    end: date


@dataclass
class SchoolCalendar:
    """VIC school term/semester dates loaded from ``school_calendar.yaml``.

    Structure::

        years:
          2026:
            semesters:
              S1: {start: 2026-01-28, end: 2026-06-26}
              S2: {start: 2026-07-13, end: 2026-12-18}
    """

    semesters_by_year: dict[int, list[Semester]] = field(default_factory=dict)

    @classmethod
    def load(cls, path: Path) -> SchoolCalendar:
        if not path.exists():
            raise FileNotFoundError(f"school_calendar.yaml not found at {path}")
        raw = yaml.safe_load(path.read_text()) or {}
        by_year: dict[int, list[Semester]] = {}
        for year_key, year_block in (raw.get("years") or {}).items():
            year = int(year_key)
            sems: list[Semester] = []
            for label, span in (year_block.get("semesters") or {}).items():
                sems.append(
                    Semester(
                        label=str(label),
                        start=_as_date(span["start"]),
                        end=_as_date(span["end"]),
                    )
                )
            sems.sort(key=lambda s: s.start)
            by_year[year] = sems
        return cls(semesters_by_year=by_year)

    def current_semester_end(self, ref: date) -> datetime | None:
        """Return the end-of-day UTC for the semester containing ``ref``.

        If ``ref`` falls outside any defined semester (e.g. a holiday window),
        returns the next semester's end. Returns ``None`` if no calendar data
        for the relevant year is available.
        """
        candidates: list[Semester] = []
        for year in (ref.year, ref.year + 1):
            candidates.extend(self.semesters_by_year.get(year, []))
        candidates.sort(key=lambda s: s.start)
        for sem in candidates:
            if sem.start <= ref <= sem.end:
                return _eod_melbourne_utc(sem.end)
        # Holiday window: pick the next semester ending after ref.
        for sem in candidates:
            if sem.end >= ref:
                return _eod_melbourne_utc(sem.end)
        return None


def _as_date(value: Any) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    return date.fromisoformat(str(value))


def _eod_melbourne_utc(d: date) -> datetime:
    """End of the given Melbourne local day, expressed in UTC."""
    return datetime.combine(d, time(23, 59), tzinfo=MELBOURNE).astimezone(UTC)


# --------------------------------------------------------------------------- #
# Data shapes
# --------------------------------------------------------------------------- #


@dataclass
class MaterialPost:
    """A material card under the workplan topic."""

    stream_item_id: str
    title: str  # e.g. "Chapter 7 Workplan"
    form_url: str  # resolved viewform URL


@dataclass
class FormQuestion:
    title: str
    qtype: int


# --------------------------------------------------------------------------- #
# Form parser
# --------------------------------------------------------------------------- #


def parse_form_questions(html: str) -> list[FormQuestion]:
    """Extract questions from a viewform HTML page.

    ``FB_PUBLIC_LOAD_DATA_`` is a JSON array; ``data[1][1]`` is a list of
    question records where each record's ``[1]`` is the title and ``[3]`` is
    the question type.

    Raises ``SchemaBreakError`` if the variable isn't present or the JSON
    isn't shaped as expected. Caller is responsible for catching.
    """
    m = _FB_DATA_RE.search(html)
    if m is None:
        raise SchemaBreakError("FB_PUBLIC_LOAD_DATA_ not found in form HTML")
    try:
        data = json.loads(m.group(1))
    except json.JSONDecodeError as exc:
        raise SchemaBreakError(f"FB_PUBLIC_LOAD_DATA_ JSON decode failed: {exc}") from exc

    try:
        raw_questions = data[1][1]
    except (IndexError, TypeError) as exc:
        raise SchemaBreakError(f"FB_PUBLIC_LOAD_DATA_ shape mismatch: {exc}") from exc

    out: list[FormQuestion] = []
    if not isinstance(raw_questions, list):
        raise SchemaBreakError(
            f"FB_PUBLIC_LOAD_DATA_[1][1] is not a list: {type(raw_questions).__name__}"
        )
    for q in raw_questions:
        if not isinstance(q, list) or len(q) < 4:
            continue
        title = q[1]
        qtype = q[3]
        if not isinstance(title, str) or not isinstance(qtype, int):
            continue
        out.append(FormQuestion(title=title.strip(), qtype=qtype))
    return out


def filter_section_questions(questions: list[FormQuestion]) -> list[FormQuestion]:
    """Drop identity / housekeeping questions; keep one row per chapter section."""
    keep: list[FormQuestion] = []
    for q in questions:
        if q.qtype not in _TASK_QUESTION_TYPES:
            continue
        if not q.title:
            continue
        lowered = q.title.lower().strip()
        if any(pat in lowered for pat in _SKIP_TITLE_PATTERNS):
            continue
        keep.append(q)
    return keep


# --------------------------------------------------------------------------- #
# Classroom DOM extraction
# --------------------------------------------------------------------------- #


def course_id_to_b64(course_id: str) -> str:
    """Classroom's URL slug is the numeric course_id base64-encoded (no pad)."""
    return b64encode(course_id.encode()).decode().rstrip("=")


# Extract every workplan topic's materials. Args: topic_text (str).
# Returns a list of {stream_item_id, title} for materials under the matching
# topic. Form URL resolution happens separately because it requires per-card
# interaction.
_EXTRACT_MATERIALS_JS = r"""
(topicText) => {
    const headers = Array.from(document.querySelectorAll('h2.Vu2fZd'));
    const match = headers.find(h => (h.textContent || '').trim() === topicText);
    if (!match) return {found: false, materials: []};
    // Walk up to find the topic container that holds the LIs.
    let container = match;
    for (let i = 0; i < 5 && container; i++) {
        if (container.querySelector('li[data-stream-item-id][data-stream-item-type="5"]')) {
            break;
        }
        container = container.parentElement;
    }
    if (!container) return {found: true, materials: []};
    const lis = Array.from(
        container.querySelectorAll('li[data-stream-item-id][data-stream-item-type="5"]')
    );
    return {
        found: true,
        materials: lis.map(li => {
            const titleEl = li.querySelector('span.Vu2fZd.Cx437e');
            return {
                stream_item_id: li.getAttribute('data-stream-item-id'),
                title: titleEl ? (titleEl.textContent || '').trim() : '',
            };
        }).filter(m => m.stream_item_id && m.title),
    };
}
"""


# --------------------------------------------------------------------------- #
# Fetcher
# --------------------------------------------------------------------------- #


@dataclass
class WorkplanResult:
    child: str
    tasks: list[Task]
    materials_seen: int
    questions_extracted: int


class WorkplanFetcher:
    """Top-level entry point — one fetch per child per sync run.

    Reuses the per-child Classroom ``storage_state.json`` (refreshed via the
    extended ``auth classroom`` flow so it now contains ``docs.google.com``
    cookies).
    """

    def __init__(
        self,
        *,
        store: StateStore,
        silver: SilverWriter,
        calendar: SchoolCalendar,
        per_child: dict[str, tuple[Path, WorkplanChildConfig]],
        page_timeout_ms: int = 30_000,
        post_load_settle_ms: int = 2_000,
    ):
        self.store = store
        self.silver = silver
        self.calendar = calendar
        self.per_child = per_child
        self.page_timeout_ms = page_timeout_ms
        self.post_load_settle_ms = post_load_settle_ms

    def fetch_all(self, *, now: datetime | None = None) -> list[WorkplanResult]:
        """Run the workplan fetch for every configured child.

        Per-child errors are logged and swallowed so one bad child can't
        block another. The outer orchestrator hook also wraps this call in
        ``try/except`` as a belt-and-braces guard.
        """
        results: list[WorkplanResult] = []
        for child in self.per_child:
            result = self.fetch_one_child(child, now=now)
            if result is not None:
                results.append(result)
        return results

    def fetch_one_child(
        self, child: str, *, now: datetime | None = None
    ) -> WorkplanResult | None:
        """Run the workplan fetch for a single child. Returns ``None`` if the
        child has no workplan config or the fetch raised a handled error.
        """
        entry = self.per_child.get(child)
        if entry is None:
            return None
        storage_path, cfg = entry
        try:
            return self._fetch_one(child, storage_path, cfg, now=now)
        except (AuthExpiredError, TransientError, SchemaBreakError) as exc:
            log.warning("workplan[%s]: %s: %s", child, type(exc).__name__, exc)
            return None
        except Exception:
            log.exception("workplan[%s]: unexpected failure", child)
            return None

    def _fetch_one(
        self,
        child: str,
        storage_path: Path,
        cfg: WorkplanChildConfig,
        *,
        now: datetime | None,
    ) -> WorkplanResult:
        storage = ClassroomStorageState.load(storage_path)
        with ClassroomScraper(
            storage,
            page_timeout_ms=self.page_timeout_ms,
            post_load_settle_ms=self.post_load_settle_ms,
        ) as scraper:
            materials = self._scrape_materials(scraper, cfg)
            log.info("workplan[%s]: %d materials under '%s'", child, len(materials), cfg.topic)

            tasks: list[Task] = []
            questions_total = 0
            ref = (now or datetime.now(UTC)).astimezone(MELBOURNE).date()
            due_at = self.calendar.current_semester_end(ref)
            if due_at is None:
                log.warning("workplan[%s]: no semester end for %s, due_at left blank", child, ref)

            for material in materials:
                try:
                    questions = self._scrape_form_questions(scraper, material.form_url)
                except (TransientError, SchemaBreakError) as exc:
                    log.warning(
                        "workplan[%s]: skipping material %r: %s",
                        child,
                        material.title,
                        exc,
                    )
                    continue
                sections = filter_section_questions(questions)
                questions_total += len(sections)
                for q in sections:
                    tasks.append(
                        _build_task(
                            child=child,
                            cfg=cfg,
                            material=material,
                            question=q,
                            due_at=due_at,
                        )
                    )

        self._persist(child, tasks)
        return WorkplanResult(
            child=child,
            tasks=tasks,
            materials_seen=len(materials),
            questions_extracted=questions_total,
        )

    def _scrape_materials(
        self,
        scraper: ClassroomScraper,
        cfg: WorkplanChildConfig,
    ) -> list[MaterialPost]:
        from playwright.sync_api import Error as PlaywrightError
        from playwright.sync_api import TimeoutError as PlaywrightTimeoutError

        b64 = course_id_to_b64(cfg.course_id)
        url = f"https://classroom.google.com/u/0/w/{b64}/t/all"

        page = scraper._context.new_page()
        try:
            try:
                page.goto(url, timeout=self.page_timeout_ms, wait_until="domcontentloaded")
            except PlaywrightTimeoutError as exc:
                raise TransientError(f"workplan: timeout loading classwork: {exc}") from exc

            if "accounts.google.com" in page.url:
                raise AuthExpiredError("workplan: classroom redirected to login")

            page.wait_for_timeout(self.post_load_settle_ms)

            try:
                payload = page.evaluate(_EXTRACT_MATERIALS_JS, cfg.topic)
            except PlaywrightError as exc:
                raise SchemaBreakError(f"workplan: materials JS failed: {exc}") from exc

            if not isinstance(payload, dict) or not payload.get("found"):
                log.warning("workplan: topic %r not found on classwork page", cfg.topic)
                return []

            materials: list[MaterialPost] = []
            for entry in payload.get("materials", []):
                stream_id = entry.get("stream_item_id")
                title = entry.get("title") or ""
                if not stream_id:
                    continue
                form_url = self._resolve_form_url(page, stream_id)
                if not form_url:
                    log.debug("workplan: no form URL for material %r (%s)", title, stream_id)
                    continue
                materials.append(
                    MaterialPost(stream_item_id=stream_id, title=title, form_url=form_url)
                )
            return materials
        finally:
            page.close()

    def _resolve_form_url(self, page: Any, stream_item_id: str) -> str | None:
        """Click into a material card and read the Form URL from the attachment.

        The materials list renders Form attachments lazily — the URL is only
        present in the DOM after the card header is clicked. We click, wait
        for the form link to render, capture, then click again to collapse.
        """
        from playwright.sync_api import Error as PlaywrightError

        selector = (
            f'li[data-stream-item-id="{stream_item_id}"] [jsname="tdoU3e"]'
        )
        try:
            page.locator(selector).first.click(timeout=5_000)
        except PlaywrightError as exc:
            log.debug("workplan: click expand failed for %s: %s", stream_item_id, exc)
            return None
        page.wait_for_timeout(2_000)

        link_selector = (
            f'li[data-stream-item-id="{stream_item_id}"] a[href*="docs.google.com/forms"]'
        )
        try:
            href = page.locator(link_selector).first.get_attribute("href", timeout=3_000)
        except PlaywrightError:
            href = None

        # Collapse again to keep DOM tidy for the next card.
        with suppress(PlaywrightError):
            page.locator(selector).first.click(timeout=2_000)
        return href

    def _scrape_form_questions(
        self, scraper: ClassroomScraper, form_url: str
    ) -> list[FormQuestion]:
        from playwright.sync_api import Error as PlaywrightError
        from playwright.sync_api import TimeoutError as PlaywrightTimeoutError

        # Normalise to the public viewform URL — teachers sometimes share
        # ``usp=publish-editor`` links which require edit auth.
        url = form_url.replace("usp=publish-editor", "usp=sharing")
        page = scraper._context.new_page()
        try:
            try:
                page.goto(url, timeout=self.page_timeout_ms, wait_until="domcontentloaded")
            except PlaywrightTimeoutError as exc:
                raise TransientError(f"workplan: timeout loading form: {exc}") from exc

            if "accounts.google.com" in page.url:
                raise AuthExpiredError("workplan: form redirected to login")

            html = page.content()
            if "Page Not Found" in html or "form is no longer accepting" in html.lower():
                raise SchemaBreakError("workplan: form unavailable (404 / closed)")

            try:
                return parse_form_questions(html)
            except PlaywrightError as exc:  # pragma: no cover - defensive
                raise SchemaBreakError(f"workplan: form parse failed: {exc}") from exc
        finally:
            page.close()

    def _persist(self, child: str, tasks: list[Task]) -> None:
        """Upsert ``tasks`` and delete any stale workplan rows for ``child``.

        Stale = existing silver rows with ``source='classroom'`` and
        ``source_id LIKE 'workplan:%'`` whose ``source_id`` is not in the
        current batch. This is the throwaway-friendly "full replace" model.
        """
        if tasks:
            self.silver.upsert_many([(t, None) for t in tasks])

        current_ids = {t.source_id for t in tasks}
        with closing(sqlite3.connect(self.store.db_path)) as conn, conn:
            conn.row_factory = sqlite3.Row
            existing = conn.execute(
                "SELECT source_id FROM silver_tasks "
                "WHERE child = ? AND source = 'classroom' "
                "AND source_id LIKE 'workplan:%'",
                (child,),
            ).fetchall()
            stale = [row["source_id"] for row in existing if row["source_id"] not in current_ids]
            if stale:
                placeholders = ",".join("?" * len(stale))
                conn.execute(
                    "DELETE FROM silver_tasks "
                    f"WHERE child = ? AND source = 'classroom' AND source_id IN ({placeholders})",
                    (child, *stale),
                )
                log.info("workplan[%s]: removed %d stale rows", child, len(stale))


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


def _slug(value: str) -> str:
    """Stable, URL-safe slug for ``source_id``.

    Strips diacritics, lower-cases, collapses non-alphanumerics to ``-``.
    """
    norm = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode()
    norm = norm.lower().strip()
    norm = re.sub(r"[^a-z0-9]+", "-", norm).strip("-")
    return norm or "section"


def _build_task(
    *,
    child: str,
    cfg: WorkplanChildConfig,
    material: MaterialPost,
    question: FormQuestion,
    due_at: datetime | None,
) -> Task:
    title = question.title
    source_id = f"workplan:{cfg.course_id}:{material.stream_item_id}:{_slug(title)}"
    return Task(
        source=Source.CLASSROOM,
        source_id=source_id,
        child=child,
        subject=cfg.subject,
        title=title,
        description=material.title,
        due_at=due_at,
        status_raw="workplan",
        status=Status.NOT_STARTED,
        task_type=TaskType.HOMEWORK,
        url=material.form_url,
    )
