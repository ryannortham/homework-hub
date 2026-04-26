"""Tests for the composition-root wiring."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml

from homework_hub import wiring
from homework_hub.config import ChildrenConfig, Settings
from homework_hub.wiring import (
    _build_sources,
    build_medallion_orchestrator,
    write_sheet_id_to_config,
)


def _settings(tmp_path: Path) -> tuple[Settings, Path]:
    config_dir = tmp_path / "config"
    tokens_dir = config_dir / "tokens"
    tokens_dir.mkdir(parents=True)
    settings = Settings(
        config_dir=config_dir,
        tokens_dir=tokens_dir,
        state_db=tmp_path / "state.db",
        log_dir=tmp_path / "logs",
    )
    return settings, config_dir / "children.yaml"


def test_write_sheet_id_updates_existing_key(tmp_path: Path):
    yaml_path = tmp_path / "children.yaml"
    yaml_path.write_text(
        "children:\n"
        "  james:\n"
        "    display_name: James\n"
        "    sheet_id:\n"  # empty, awaiting bootstrap
        "    compass_user_id: 12345\n"
        "  tahlia:\n"
        "    display_name: Tahlia\n"
        "    sheet_id: keep-me\n"
    )
    write_sheet_id_to_config(yaml_path, "james", "new-id-123")
    text = yaml_path.read_text()
    assert "sheet_id: new-id-123" in text
    # Tahlia's id is untouched.
    assert "sheet_id: keep-me" in text
    # Comments / structure preserved (no full re-serialise).
    assert "compass_user_id: 12345" in text


def test_write_sheet_id_appends_when_missing(tmp_path: Path):
    yaml_path = tmp_path / "children.yaml"
    yaml_path.write_text("children:\n  james:\n    display_name: James\n")
    write_sheet_id_to_config(yaml_path, "james", "abc")
    text = yaml_path.read_text()
    assert "sheet_id: abc" in text
    # Round-trips through pydantic cleanly.
    cfg = ChildrenConfig.load(yaml_path)
    assert cfg.children["james"].sheet_id == "abc"


def test_write_sheet_id_unknown_child_raises(tmp_path: Path):
    yaml_path = tmp_path / "children.yaml"
    yaml_path.write_text("children:\n  james:\n    display_name: James\n")
    with pytest.raises(KeyError):
        write_sheet_id_to_config(yaml_path, "tahlia", "x")


def test_build_sources_respects_disabled_flags(tmp_path: Path):
    settings, yaml_path = _settings(tmp_path)
    yaml_path.write_text(
        yaml.safe_dump(
            {
                "children": {
                    "james": {
                        "display_name": "James",
                        "compass_user_id": 1,
                        "sources": {
                            "classroom": {"enabled": True},
                            "compass": {"enabled": False, "subdomain": "mcsc-vic"},
                            "edrolo": {"enabled": False},
                        },
                    },
                }
            }
        )
    )
    cfg = ChildrenConfig.load(yaml_path)
    sources = _build_sources(settings, cfg)
    names = [s.name for s in sources["james"]]
    assert names == ["classroom"]


def test_build_sources_shares_compass_session(tmp_path: Path):
    """One CompassSource instance covers both children — shared parent token."""
    settings, yaml_path = _settings(tmp_path)
    yaml_path.write_text(
        yaml.safe_dump(
            {
                "children": {
                    "james": {
                        "display_name": "James",
                        "compass_user_id": 1,
                        "sources": {
                            "classroom": {"enabled": False},
                            "compass": {"enabled": True, "subdomain": "mcsc-vic"},
                            "edrolo": {"enabled": False},
                        },
                    },
                    "tahlia": {
                        "display_name": "Tahlia",
                        "compass_user_id": 2,
                        "sources": {
                            "classroom": {"enabled": False},
                            "compass": {"enabled": True, "subdomain": "mcsc-vic"},
                            "edrolo": {"enabled": False},
                        },
                    },
                }
            }
        )
    )
    cfg = ChildrenConfig.load(yaml_path)
    sources = _build_sources(settings, cfg)
    assert sources["james"][0] is sources["tahlia"][0]


# --------------------------------------------------------------------------- #
# build_medallion_orchestrator + _try_build_gold_sink
# --------------------------------------------------------------------------- #


def _minimal_children_yaml(yaml_path: Path) -> None:
    yaml_path.write_text(
        yaml.safe_dump(
            {
                "children": {
                    "james": {
                        "display_name": "James",
                        "sheet_id": "sheet-james",
                        "compass_user_id": 1,
                        "sources": {
                            "classroom": {"enabled": False},
                            "compass": {"enabled": False, "subdomain": "mcsc-vic"},
                            "edrolo": {"enabled": False},
                        },
                    },
                }
            }
        )
    )


def test_build_medallion_orchestrator_uses_provided_sink(tmp_path: Path):
    settings, yaml_path = _settings(tmp_path)
    _minimal_children_yaml(yaml_path)
    fake_sink = MagicMock()
    orch = build_medallion_orchestrator(settings, sink=fake_sink)
    assert orch.sink is fake_sink


def test_build_medallion_orchestrator_builds_sink_from_bw(tmp_path: Path):
    settings, yaml_path = _settings(tmp_path)
    _minimal_children_yaml(yaml_path)
    fake_sink = MagicMock()
    with patch.object(wiring, "_try_build_gold_sink", return_value=fake_sink) as helper:
        orch = build_medallion_orchestrator(settings)
    helper.assert_called_once()
    assert orch.sink is fake_sink


def test_build_medallion_orchestrator_tolerates_missing_credentials(tmp_path: Path):
    settings, yaml_path = _settings(tmp_path)
    _minimal_children_yaml(yaml_path)
    with patch.object(wiring, "_try_build_gold_sink", return_value=None):
        orch = build_medallion_orchestrator(settings)
    assert orch.sink is None


def test_try_build_gold_sink_returns_none_when_bw_unavailable():
    bw = MagicMock()
    bw.get_notes.side_effect = RuntimeError("BW down")
    assert wiring._try_build_gold_sink(bw) is None


def test_try_build_gold_sink_returns_sink_when_creds_load(monkeypatch):
    bw = MagicMock()
    bw.get_notes.return_value = '{"client_email": "sa@x.iam.gserviceaccount.com"}'
    fake_creds = MagicMock()
    monkeypatch.setattr(wiring, "load_service_account_credentials", lambda raw: fake_creds)
    sink = wiring._try_build_gold_sink(bw)
    assert sink is not None
