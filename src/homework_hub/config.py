"""Configuration loading — env vars and children.yaml."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Self

import yaml
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class CompassConfig(BaseModel):
    enabled: bool = True
    subdomain: str = ""


class SimpleSourceConfig(BaseModel):
    enabled: bool = True


class ChildSources(BaseModel):
    classroom: SimpleSourceConfig = SimpleSourceConfig()
    compass: CompassConfig = CompassConfig()
    eduperfect: SimpleSourceConfig = SimpleSourceConfig(enabled=False)
    edrolo: SimpleSourceConfig = SimpleSourceConfig(enabled=False)


class ChildConfig(BaseModel):
    display_name: str
    sources: ChildSources = ChildSources()
    sheet_id: str | None = None
    # Compass uses a single parent session covering all children. Per-child
    # numeric userId is captured during onboarding and recorded here.
    compass_user_id: int | None = None
    # Optional Student Workplan Tracker block. Parsed by
    # ``homework_hub.sources.workplan.parse_workplan_child_config`` so config
    # stays decoupled from the workplan module.
    workplan: dict[str, Any] | None = None


class ChildrenConfig(BaseModel):
    children: dict[str, ChildConfig] = Field(default_factory=dict)

    @classmethod
    def load(cls, path: Path) -> Self:
        if not path.exists():
            raise FileNotFoundError(f"children.yaml not found at {path}")
        data = yaml.safe_load(path.read_text()) or {}
        return cls.model_validate(data)


class Settings(BaseSettings):
    """Process-level settings sourced from environment variables.

    Defaults match the in-container layout; can be overridden for local dev.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="HOMEWORK_HUB_",
        extra="ignore",
    )

    config_dir: Path = Path("/config")
    tokens_dir: Path = Path("/config/tokens")
    state_db: Path = Path("/config/state.db")
    log_dir: Path = Path("/logs")
    sync_cron: str = "7 * * * *"
    health_port: int = 30062
    history_cutoff_days: int = 30
    # Days past due (or past first_seen for date-less tasks) before a non-terminal
    # task is auto-archived. Catches stale Overdue rows and forgotten date-less
    # zombies.
    active_cutoff_days: int = 60
    # Consecutive successful syncs a task must be missing from upstream before
    # being auto-archived with reason ``upstream_removed``.
    stale_grace_syncs: int = 2
    # Any parsed due_at more than this many days in the future is treated as
    # corrupt (parser drift) and blanked on the gold sheet.
    future_date_cap_days: int = 365

    @property
    def children_yaml(self) -> Path:
        return self.config_dir / "children.yaml"

    @property
    def school_calendar_yaml(self) -> Path:
        return self.config_dir / "school_calendar.yaml"

    def child_token_path(self, child: str, source: str) -> Path:
        return self.tokens_dir / f"{child}-{source}.json"
