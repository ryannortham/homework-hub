"""Configuration loading — env vars and children.yaml."""

from __future__ import annotations

from pathlib import Path
from typing import Self

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
    edrolo: SimpleSourceConfig = SimpleSourceConfig()


class ChildConfig(BaseModel):
    display_name: str
    sources: ChildSources = ChildSources()
    sheet_id: str | None = None


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

    @property
    def children_yaml(self) -> Path:
        return self.config_dir / "children.yaml"

    def child_token_path(self, child: str, source: str) -> Path:
        return self.tokens_dir / f"{child}-{source}.json"
