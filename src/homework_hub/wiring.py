"""Composition root — builds the orchestrator from runtime config.

The CLI and (later) the daemon both call ``build_orchestrator`` with a
``Settings`` to get a wired-up orchestrator. Tests bypass this module and
construct components directly with fakes.

Kept deliberately simple: read ``children.yaml``, instantiate one Source
per (child, source) pair from config, load tokens from the ``tokens_dir``,
and pass the collection plus a real ``SheetsClient`` to ``Orchestrator``.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from homework_hub.config import ChildrenConfig, Settings
from homework_hub.orchestrator import Orchestrator
from homework_hub.secrets import BitwardenCLI, from_env
from homework_hub.sinks.sheets_client import (
    SheetsBackend,
    SheetsClient,
    load_service_account_credentials,
)
from homework_hub.sources.base import Source
from homework_hub.state.store import StateStore

log = logging.getLogger(__name__)

SERVICE_ACCOUNT_BW_NAME = "Homework Hub - Sheets Service Account"


def build_orchestrator(
    settings: Settings,
    *,
    sheets_backend: SheetsBackend | None = None,
    bw: BitwardenCLI | None = None,
) -> Orchestrator:
    """Construct an Orchestrator wired from config + tokens on disk.

    ``sheets_backend`` and ``bw`` are overrideable for tests / dry-runs.
    """
    children_config = ChildrenConfig.load(settings.children_yaml)
    sources_for_child = _build_sources(settings, children_config)
    state = StateStore(settings.state_db)
    backend = sheets_backend or _build_sheets_backend(bw)
    return Orchestrator(
        children_config=children_config,
        sources_for_child=sources_for_child,
        sheets=backend,
        state=state,
    )


def _build_sources(settings: Settings, cfg: ChildrenConfig) -> dict[str, list[Source]]:
    """Instantiate one Source per enabled (child, source) pair.

    Tokens that are missing on disk are tolerated: the source is still
    constructed, and the AuthExpiredError it raises during ``fetch`` is
    caught + recorded by the orchestrator. This keeps a single missing
    token from breaking startup.
    """
    out: dict[str, list[Source]] = {}
    compass_user_ids: dict[str, int] = {}
    edrolo_paths: dict[str, Path] = {}
    classroom_paths: dict[str, Path] = {}

    # First pass: collect per-child config knobs that the shared sources need.
    for child, child_cfg in cfg.children.items():
        if child_cfg.sources.compass.enabled and child_cfg.compass_user_id is not None:
            compass_user_ids[child] = child_cfg.compass_user_id
        if child_cfg.sources.edrolo.enabled:
            edrolo_paths[child] = settings.child_token_path(child, "edrolo")
        if child_cfg.sources.classroom.enabled:
            classroom_paths[child] = settings.child_token_path(child, "classroom")

    # Build the shared Compass source once (single parent token covers all).
    compass_source = None
    compass_token_path = settings.tokens_dir / "compass-parent.json"
    if compass_user_ids:
        from homework_hub.sources.compass import CompassSource

        compass_source = CompassSource(compass_token_path, user_id_for_child=compass_user_ids)

    # Build the shared Edrolo source once (per-child storage_state on disk).
    edrolo_source = None
    if edrolo_paths:
        from homework_hub.sources.edrolo import EdroloSource

        edrolo_source = EdroloSource(edrolo_paths)

    # Build the shared Classroom source once (per-child storage_state on disk).
    classroom_source = None
    if classroom_paths:
        from homework_hub.sources.classroom import ClassroomSource

        classroom_source = ClassroomSource(classroom_paths)

    # Second pass: assemble the per-child source list in stable order
    # (classroom, compass, edrolo) so the report ordering is predictable.
    for child, child_cfg in cfg.children.items():
        sources: list[Source] = []
        if child_cfg.sources.classroom.enabled and classroom_source is not None:
            sources.append(classroom_source)
        if (
            child_cfg.sources.compass.enabled
            and child in compass_user_ids
            and compass_source is not None
        ):
            sources.append(compass_source)
        if child_cfg.sources.edrolo.enabled and edrolo_source is not None:
            sources.append(edrolo_source)
        out[child] = sources
    return out


def _build_sheets_backend(bw: BitwardenCLI | None = None) -> SheetsBackend:
    bw = bw or from_env()
    raw = bw.get_notes(SERVICE_ACCOUNT_BW_NAME)
    creds = load_service_account_credentials(raw)
    return SheetsClient(creds)


# --------------------------------------------------------------------------- #
# children.yaml mutation (used by bootstrap-sheet)
# --------------------------------------------------------------------------- #


def write_sheet_id_to_config(children_yaml: Path, child: str, sheet_id: str) -> None:
    """Persist the freshly-bootstrapped sheet ID back to children.yaml.

    Done as a string-level edit rather than a full re-serialise so the
    user's hand-written comments and ordering survive round-trips.
    """
    import re

    text = children_yaml.read_text()
    pattern = rf"(^[ \t]*{re.escape(child)}:[\s\S]*?sheet_id:)[ \t]*(\S*)"
    new_text, count = re.subn(pattern, rf"\g<1> {sheet_id}", text, count=1, flags=re.MULTILINE)
    if count == 0:
        # No existing key — append under the child block.
        # Find the child block and insert sheet_id at the end of its mapping.
        block_pattern = rf"(^[ \t]*{re.escape(child)}:[ \t]*\n)"
        replacement = rf"\g<1>    sheet_id: {sheet_id}\n"
        new_text, count = re.subn(block_pattern, replacement, text, count=1, flags=re.MULTILINE)
        if count == 0:
            raise KeyError(f"Could not locate child '{child}' in {children_yaml}")
    children_yaml.write_text(new_text)


# Re-export for convenience.
__all__: list[str] = [
    "build_orchestrator",
    "write_sheet_id_to_config",
]


def _ignore_unused(*_args: Any) -> None:  # pragma: no cover - keep linters happy
    pass
