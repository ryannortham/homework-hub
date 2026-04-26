"""Subject canonicalisation — ``dim_subjects`` resolver and CRUD.

Resolution precedence (highest to lowest):

1. ``exact`` match (case-insensitive) at priority 100
2. ``prefix`` match (case-insensitive starts-with) at priority 50
3. ``regex`` match (full-string ``re.fullmatch`` semantics) at priority 10

Within a tier, the rule with the highest ``priority`` wins; ties are broken
by row id (earlier-inserted wins). Lookups return both ``canonical`` (the
human label, e.g. ``"Year 9 Science"``) and ``short`` (the kid-facing
column value, e.g. ``"Sci"``).

Rules live in ``dim_subjects`` and are seeded from ``config/subjects.yaml``
on demand via ``SubjectResolver.seed_from_yaml`` or the ``subjects seed``
CLI command. The YAML format is intentionally minimal:

```yaml
rules:
  - {match: exact,  pattern: "9SCI2 (2026 Academic)", canonical: "Year 9 Science", short: "Sci"}
  - {match: prefix, pattern: "9SCI",                  canonical: "Year 9 Science", short: "Sci"}
  - {match: regex,  pattern: "^9SCI.*",               canonical: "Year 9 Science", short: "Sci"}
```
"""

from __future__ import annotations

import re
import sqlite3
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path

import yaml

from homework_hub.state.store import StateStore

_DEFAULT_PRIORITY = {"exact": 100, "prefix": 50, "regex": 10}


@dataclass(frozen=True)
class SubjectRule:
    id: int
    match_type: str
    pattern: str
    canonical: str
    short: str
    priority: int


@dataclass(frozen=True)
class SubjectMatch:
    canonical: str
    short: str
    rule_id: int
    match_type: str


class SubjectResolver:
    """Resolves raw subject strings against ``dim_subjects``.

    The resolver caches the rule list per instance for cheap repeated
    lookups during a single sync. Call ``refresh()`` after mutating rules
    via the CLI; in normal operation a fresh instance is built per run.
    """

    def __init__(self, store: StateStore):
        self.store = store
        self._rules: list[SubjectRule] = []
        self.refresh()

    # ------------------------------------------------------------------ #
    # Read path
    # ------------------------------------------------------------------ #

    def refresh(self) -> None:
        with closing(_connect(self.store)) as conn:
            rows = conn.execute(
                "SELECT id, match_type, pattern, canonical, short, priority "
                "FROM dim_subjects ORDER BY priority DESC, id ASC"
            ).fetchall()
        self._rules = [
            SubjectRule(
                id=int(r["id"]),
                match_type=r["match_type"],
                pattern=r["pattern"],
                canonical=r["canonical"],
                short=r["short"],
                priority=int(r["priority"]),
            )
            for r in rows
        ]

    @property
    def rules(self) -> list[SubjectRule]:
        return list(self._rules)

    def resolve(self, raw: str) -> SubjectMatch | None:
        """Return the best-match rule for ``raw``, or None if no rule fires.

        Empty/whitespace input always returns None — the caller should
        keep the empty string rather than fabricating a canonical label.
        """
        if not raw or not raw.strip():
            return None
        target = raw.strip()
        target_lower = target.lower()
        for rule in self._rules:
            if _matches(rule, target, target_lower):
                return SubjectMatch(
                    canonical=rule.canonical,
                    short=rule.short,
                    rule_id=rule.id,
                    match_type=rule.match_type,
                )
        return None

    # ------------------------------------------------------------------ #
    # Write path
    # ------------------------------------------------------------------ #

    def add_rule(
        self,
        *,
        match_type: str,
        pattern: str,
        canonical: str,
        short: str,
        priority: int | None = None,
    ) -> int:
        """Insert a rule. Returns the new row id. Raises if duplicate
        (same ``match_type`` + ``pattern``)."""
        if match_type not in _DEFAULT_PRIORITY:
            raise ValueError(f"match_type must be one of exact/prefix/regex, got {match_type!r}")
        if match_type == "regex":
            re.compile(pattern)  # validate up front
        prio = priority if priority is not None else _DEFAULT_PRIORITY[match_type]
        with closing(_connect(self.store)) as conn, conn:
            cur = conn.execute(
                "INSERT INTO dim_subjects "
                "(match_type, pattern, canonical, short, priority) "
                "VALUES (?, ?, ?, ?, ?)",
                (match_type, pattern, canonical, short, prio),
            )
            new_id = int(cur.lastrowid or 0)
        self.refresh()
        return new_id

    def remove_rule(self, *, match_type: str, pattern: str) -> int:
        """Delete a rule. Returns the number of rows removed (0 or 1)."""
        with closing(_connect(self.store)) as conn, conn:
            cur = conn.execute(
                "DELETE FROM dim_subjects WHERE match_type = ? AND pattern = ?",
                (match_type, pattern),
            )
            removed = cur.rowcount
        self.refresh()
        return removed

    def clear(self) -> None:
        with closing(_connect(self.store)) as conn, conn:
            conn.execute("DELETE FROM dim_subjects")
        self.refresh()

    def seed_from_yaml(self, yaml_path: Path, *, replace: bool = False) -> int:
        """Load rules from a YAML file. Returns count of rules upserted.

        With ``replace=True`` the table is wiped first; otherwise existing
        rules with the same (match_type, pattern) are left as-is so manual
        CLI tweaks survive a re-seed.
        """
        if not yaml_path.exists():
            raise FileNotFoundError(yaml_path)
        data = yaml.safe_load(yaml_path.read_text()) or {}
        rules = data.get("rules") or []
        if not isinstance(rules, list):
            raise ValueError(f"{yaml_path}: 'rules' must be a list")

        if replace:
            self.clear()

        seeded = 0
        with closing(_connect(self.store)) as conn, conn:
            for raw in rules:
                if not isinstance(raw, dict):
                    raise ValueError(f"Rule must be a mapping, got: {raw!r}")
                mt = raw.get("match")
                pattern = raw.get("pattern")
                canonical = raw.get("canonical")
                short = raw.get("short")
                priority = raw.get("priority")
                if not (mt and pattern and canonical and short):
                    raise ValueError(
                        f"Rule missing required fields (match/pattern/canonical/short): {raw!r}"
                    )
                if mt not in _DEFAULT_PRIORITY:
                    raise ValueError(f"Invalid match type {mt!r} in {raw!r}")
                if mt == "regex":
                    re.compile(pattern)
                prio = priority if priority is not None else _DEFAULT_PRIORITY[mt]
                conn.execute(
                    "INSERT OR IGNORE INTO dim_subjects "
                    "(match_type, pattern, canonical, short, priority) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (mt, pattern, canonical, short, prio),
                )
                seeded += 1
        self.refresh()
        return seeded


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


def _matches(rule: SubjectRule, target: str, target_lower: str) -> bool:
    if rule.match_type == "exact":
        return rule.pattern.lower() == target_lower
    if rule.match_type == "prefix":
        return target_lower.startswith(rule.pattern.lower())
    if rule.match_type == "regex":
        try:
            return re.fullmatch(rule.pattern, target) is not None
        except re.error:
            return False
    return False


def _connect(store: StateStore) -> sqlite3.Connection:
    conn = sqlite3.connect(store.db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn
