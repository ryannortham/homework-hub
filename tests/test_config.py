"""Tests for config loading."""

from __future__ import annotations

from pathlib import Path

import pytest

from homework_hub.config import ChildrenConfig, Settings


def test_load_children_yaml(tmp_path: Path):
    yaml_content = """
children:
  james:
    display_name: James
    sources:
      classroom: {enabled: true}
      compass: {enabled: true, subdomain: mcsc-vic}
      edrolo: {enabled: false}
    sheet_id: abc123
  tahlia:
    display_name: Tahlia
    sources:
      classroom: {enabled: true}
      compass: {enabled: true, subdomain: mcsc-vic}
      edrolo: {enabled: true}
"""
    p = tmp_path / "children.yaml"
    p.write_text(yaml_content)
    cfg = ChildrenConfig.load(p)
    assert set(cfg.children) == {"james", "tahlia"}
    james = cfg.children["james"]
    assert james.display_name == "James"
    assert james.sources.compass.subdomain == "mcsc-vic"
    assert james.sources.edrolo.enabled is False
    assert james.sheet_id == "abc123"
    assert cfg.children["tahlia"].sheet_id is None


def test_load_missing_file_raises(tmp_path: Path):
    with pytest.raises(FileNotFoundError):
        ChildrenConfig.load(tmp_path / "nope.yaml")


def test_load_empty_file_returns_empty(tmp_path: Path):
    p = tmp_path / "empty.yaml"
    p.write_text("")
    cfg = ChildrenConfig.load(p)
    assert cfg.children == {}


def test_settings_defaults():
    s = Settings(_env_file=None)
    assert s.health_port == 30062
    assert s.children_yaml == Path("/config/children.yaml")
    assert s.child_token_path("james", "compass") == Path("/config/tokens/james-compass.json")


def test_settings_env_overrides(monkeypatch):
    monkeypatch.setenv("HOMEWORK_HUB_CONFIG_DIR", "/tmp/cfg")
    monkeypatch.setenv("HOMEWORK_HUB_HEALTH_PORT", "31999")
    s = Settings(_env_file=None)
    assert s.config_dir == Path("/tmp/cfg")
    assert s.health_port == 31999
    assert s.children_yaml == Path("/tmp/cfg/children.yaml")
