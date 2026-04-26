"""Tests for the subject resolver + dim_subjects CRUD (M4)."""

from __future__ import annotations

from pathlib import Path

import pytest

from homework_hub.pipeline.subjects import SubjectResolver
from homework_hub.state.store import StateStore


@pytest.fixture
def store(tmp_path: Path) -> StateStore:
    return StateStore(tmp_path / "state.db")


@pytest.fixture
def resolver(store: StateStore) -> SubjectResolver:
    return SubjectResolver(store)


# --------------------------------------------------------------------------- #
# Empty-store behaviour
# --------------------------------------------------------------------------- #


class TestEmptyStore:
    def test_no_rules_resolves_none(self, resolver: SubjectResolver):
        assert resolver.resolve("9MATH") is None

    def test_empty_input_resolves_none(self, resolver: SubjectResolver):
        assert resolver.resolve("") is None
        assert resolver.resolve("   ") is None

    def test_rules_property_empty(self, resolver: SubjectResolver):
        assert resolver.rules == []


# --------------------------------------------------------------------------- #
# add_rule + resolve
# --------------------------------------------------------------------------- #


class TestAddRule:
    def test_exact_match(self, resolver: SubjectResolver):
        resolver.add_rule(
            match_type="exact",
            pattern="9MATH",
            canonical="Year 9 Maths",
            short="Maths",
        )
        match = resolver.resolve("9MATH")
        assert match is not None
        assert match.canonical == "Year 9 Maths"
        assert match.short == "Maths"
        assert match.match_type == "exact"

    def test_exact_is_case_insensitive(self, resolver: SubjectResolver):
        resolver.add_rule(
            match_type="exact",
            pattern="9MATH",
            canonical="Year 9 Maths",
            short="Maths",
        )
        assert resolver.resolve("9math") is not None
        assert resolver.resolve("9Math") is not None

    def test_prefix_match(self, resolver: SubjectResolver):
        resolver.add_rule(
            match_type="prefix",
            pattern="9SCI",
            canonical="Year 9 Science",
            short="Sci",
        )
        match = resolver.resolve("9SCI2 (2026 Academic)")
        assert match is not None
        assert match.short == "Sci"

    def test_prefix_does_not_match_substring_in_middle(self, resolver: SubjectResolver):
        resolver.add_rule(
            match_type="prefix",
            pattern="9SCI",
            canonical="Year 9 Science",
            short="Sci",
        )
        # "X9SCI" would only match if we accidentally allowed contains semantics
        assert resolver.resolve("X9SCI") is None

    def test_regex_match(self, resolver: SubjectResolver):
        resolver.add_rule(
            match_type="regex",
            pattern=r"VCE Biology.*",
            canonical="Year 11 Biology",
            short="Bio",
        )
        match = resolver.resolve("VCE Biology Units 3&4 [2026]")
        assert match is not None
        assert match.short == "Bio"

    def test_regex_uses_fullmatch(self, resolver: SubjectResolver):
        # Pattern intentionally narrow — partial overlap should not fire.
        resolver.add_rule(
            match_type="regex",
            pattern=r"VCE Biology",
            canonical="Bio",
            short="Bio",
        )
        assert resolver.resolve("VCE Biology Units 3&4") is None

    def test_invalid_match_type_raises(self, resolver: SubjectResolver):
        with pytest.raises(ValueError):
            resolver.add_rule(
                match_type="fuzzy",
                pattern="x",
                canonical="y",
                short="z",
            )

    def test_invalid_regex_raises(self, resolver: SubjectResolver):
        import re as _re

        with pytest.raises(_re.error):
            resolver.add_rule(
                match_type="regex",
                pattern="[unclosed",
                canonical="y",
                short="z",
            )

    def test_returns_new_id(self, resolver: SubjectResolver):
        rid = resolver.add_rule(
            match_type="exact",
            pattern="9MATH",
            canonical="Year 9 Maths",
            short="Maths",
        )
        assert rid > 0


# --------------------------------------------------------------------------- #
# Precedence
# --------------------------------------------------------------------------- #


class TestPrecedence:
    def test_exact_beats_prefix(self, resolver: SubjectResolver):
        # Both rules would fire on "9SCI"; the exact rule's higher
        # default priority (100 vs 50) must win.
        resolver.add_rule(
            match_type="prefix",
            pattern="9SCI",
            canonical="Year 9 Science (prefix)",
            short="Sci-P",
        )
        resolver.add_rule(
            match_type="exact",
            pattern="9SCI",
            canonical="Year 9 Science (exact)",
            short="Sci-E",
        )
        match = resolver.resolve("9SCI")
        assert match is not None
        assert match.short == "Sci-E"

    def test_prefix_beats_regex(self, resolver: SubjectResolver):
        resolver.add_rule(
            match_type="regex",
            pattern=r".*SCI.*",
            canonical="any sci",
            short="Sci-R",
        )
        resolver.add_rule(
            match_type="prefix",
            pattern="9SCI",
            canonical="year 9 sci",
            short="Sci-P",
        )
        match = resolver.resolve("9SCI Science Y9")
        assert match is not None
        assert match.short == "Sci-P"

    def test_explicit_priority_overrides_default(self, resolver: SubjectResolver):
        resolver.add_rule(
            match_type="prefix",
            pattern="9SCI",
            canonical="cheap",
            short="Cheap",
            priority=5,
        )
        resolver.add_rule(
            match_type="regex",
            pattern=r".*SCI.*",
            canonical="winner",
            short="Win",
            priority=999,
        )
        match = resolver.resolve("9SCI")
        assert match is not None
        assert match.short == "Win"


# --------------------------------------------------------------------------- #
# remove_rule + clear + refresh
# --------------------------------------------------------------------------- #


class TestRemoveAndClear:
    def test_remove_existing_rule(self, resolver: SubjectResolver):
        resolver.add_rule(
            match_type="prefix",
            pattern="9SCI",
            canonical="Year 9 Science",
            short="Sci",
        )
        assert resolver.remove_rule(match_type="prefix", pattern="9SCI") == 1
        assert resolver.resolve("9SCI") is None

    def test_remove_missing_returns_zero(self, resolver: SubjectResolver):
        assert resolver.remove_rule(match_type="prefix", pattern="ghost") == 0

    def test_clear(self, resolver: SubjectResolver):
        resolver.add_rule(
            match_type="prefix",
            pattern="9SCI",
            canonical="x",
            short="x",
        )
        resolver.clear()
        assert resolver.rules == []

    def test_duplicate_match_type_pattern_raises(self, resolver: SubjectResolver):
        import sqlite3

        resolver.add_rule(
            match_type="prefix",
            pattern="9SCI",
            canonical="a",
            short="A",
        )
        with pytest.raises(sqlite3.IntegrityError):
            resolver.add_rule(
                match_type="prefix",
                pattern="9SCI",
                canonical="b",
                short="B",
            )


# --------------------------------------------------------------------------- #
# seed_from_yaml
# --------------------------------------------------------------------------- #


class TestSeedFromYaml:
    def test_loads_rules(self, resolver: SubjectResolver, tmp_path: Path):
        yaml_path = tmp_path / "subjects.yaml"
        yaml_path.write_text(
            "rules:\n"
            "  - {match: prefix, pattern: '9SCI', canonical: 'Year 9 Science', short: 'Sci'}\n"
            "  - {match: exact,  pattern: '9MATH', canonical: 'Year 9 Maths', short: 'Maths'}\n"
        )
        count = resolver.seed_from_yaml(yaml_path)
        assert count == 2
        assert resolver.resolve("9SCI Science Y9") is not None
        assert resolver.resolve("9MATH") is not None

    def test_idempotent_without_replace(self, resolver: SubjectResolver, tmp_path: Path):
        yaml_path = tmp_path / "subjects.yaml"
        yaml_path.write_text(
            "rules:\n"
            "  - {match: prefix, pattern: '9SCI', canonical: 'Year 9 Science', short: 'Sci'}\n"
        )
        resolver.seed_from_yaml(yaml_path)
        resolver.seed_from_yaml(yaml_path)  # second call must not raise
        assert len(resolver.rules) == 1

    def test_replace_wipes_first(self, resolver: SubjectResolver, tmp_path: Path):
        resolver.add_rule(
            match_type="exact",
            pattern="manual",
            canonical="Manual",
            short="M",
        )
        yaml_path = tmp_path / "subjects.yaml"
        yaml_path.write_text(
            "rules:\n"
            "  - {match: prefix, pattern: '9SCI', canonical: 'Year 9 Science', short: 'Sci'}\n"
        )
        resolver.seed_from_yaml(yaml_path, replace=True)
        patterns = {r.pattern for r in resolver.rules}
        assert patterns == {"9SCI"}

    def test_missing_file_raises(self, resolver: SubjectResolver, tmp_path: Path):
        with pytest.raises(FileNotFoundError):
            resolver.seed_from_yaml(tmp_path / "ghost.yaml")

    def test_missing_required_field_raises(self, resolver: SubjectResolver, tmp_path: Path):
        yaml_path = tmp_path / "subjects.yaml"
        yaml_path.write_text("rules:\n  - {match: prefix, pattern: '9SCI'}\n")
        with pytest.raises(ValueError, match="missing required fields"):
            resolver.seed_from_yaml(yaml_path)


# --------------------------------------------------------------------------- #
# Bundled config/subjects.yaml is valid + complete
# --------------------------------------------------------------------------- #


class TestBundledSeed:
    """Sanity checks against the committed seed file."""

    @pytest.fixture
    def seed_path(self) -> Path:
        return Path(__file__).resolve().parent.parent / "config" / "subjects.yaml"

    def test_seed_file_exists(self, seed_path: Path):
        assert seed_path.exists(), f"missing {seed_path}"

    def test_seed_loads_cleanly(self, resolver: SubjectResolver, seed_path: Path):
        count = resolver.seed_from_yaml(seed_path)
        assert count > 20  # We have well over 20 rules in the seed.

    def test_seed_resolves_observed_subjects(self, resolver: SubjectResolver, seed_path: Path):
        resolver.seed_from_yaml(seed_path)
        cases = {
            # Compass-style
            "9MATH": ("Year 9 Maths", "Maths"),
            "9SCI Science Y9": ("Year 9 Science", "Sci"),
            "11CHEM 3": ("Year 11 Chemistry", "Chem"),
            "11BIO 3": ("Year 11 Biology", "Bio"),
            "11ENG": ("Year 11 English", "Eng"),
            # Edrolo VCE titles
            "VCE Biology Units 3&4 [2026]": ("Year 11 Biology", "Bio"),
            "VCE Methods Units 1&2": ("Year 11 Methods", "Maths"),
            # Classroom-style with suffix
            "9SCI2 (2026 Academic)": ("Year 9 Science", "Sci"),
            "Year 11 English": ("Year 11 English", "Eng"),
        }
        for raw, (canonical, short) in cases.items():
            match = resolver.resolve(raw)
            assert match is not None, f"no rule fired for {raw!r}"
            assert match.canonical == canonical, f"{raw!r} -> {match.canonical!r}"
            assert match.short == short, f"{raw!r} -> short={match.short!r}"
