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
from homework_hub.medallion_orchestrator import MedallionOrchestrator
from homework_hub.orchestrator import Orchestrator
from homework_hub.pipeline.publish import GoldSink
from homework_hub.secrets import BitwardenCLI, from_env
from homework_hub.sinks.gold_sink import GspreadGoldSink
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
    """Construct a (legacy) Orchestrator wired from config + tokens on disk.

    Kept for the bootstrap-sheet path (it still uses ``SheetsBackend``)
    and for callers that have not been migrated to the medallion flow.
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


def build_medallion_orchestrator(
    settings: Settings,
    *,
    sink: GoldSink | None = None,
    bw: BitwardenCLI | None = None,
) -> MedallionOrchestrator:
    """Construct a MedallionOrchestrator wired from config + tokens on disk.

    If ``sink`` is not provided, attempts to build a live
    :class:`GspreadGoldSink` from the service-account credentials in
    Bitwarden. If that fails (e.g. running tests without ``bw``), publish
    skips with a clear ``sync_runs`` row instead of crashing the run.
    """
    children_config = ChildrenConfig.load(settings.children_yaml)
    sources_for_child = _build_sources(settings, children_config)
    state = StateStore(settings.state_db)
    if sink is None:
        sink = _try_build_gold_sink(bw)
    return MedallionOrchestrator(
        children_config=children_config,
        sources_for_child=sources_for_child,
        state=state,
        sink=sink,
    )


def _try_build_gold_sink(bw: BitwardenCLI | None) -> GoldSink | None:
    """Best-effort construction of the live gold sink.

    Returns ``None`` (and logs a warning) when service-account credentials
    aren't available, so the daemon and CLI keep working in environments
    without Bitwarden access (CI, local dev with no creds).
    """
    try:
        bw = bw or from_env()
        raw = bw.get_notes(SERVICE_ACCOUNT_BW_NAME)
        creds = load_service_account_credentials(raw)
        return GspreadGoldSink(creds)
    except Exception as exc:
        log.warning("GoldSink unavailable, publish will skip: %s", exc)
        return None


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


def build_bootstrap_sheets_backend(
    settings: Settings, *, bw: BitwardenCLI | None = None
) -> tuple[SheetsBackend, str]:
    """Build a :class:`SheetsClient` authed as the human bootstrap user.

    Returns ``(backend, sa_email)``. Callers feed ``sa_email`` into
    ``backend.create_sheet(..., share_with=[..., sa_email])`` so the
    daemon's service account inherits writer access on the new sheet.
    """
    import json as _json

    from homework_hub.auth_bootstrap import load_or_run_bootstrap_flow

    bw = bw or from_env()
    auth = load_or_run_bootstrap_flow(tokens_dir=settings.tokens_dir, bw=bw)
    sa_raw = bw.get_notes(SERVICE_ACCOUNT_BW_NAME)
    sa_email = _json.loads(sa_raw)["client_email"]
    return SheetsClient(auth.credentials), sa_email


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
    "build_bootstrap_sheets_backend",
    "build_medallion_orchestrator",
    "build_orchestrator",
    "write_sheet_id_to_config",
]


def _ignore_unused(*_args: Any) -> None:  # pragma: no cover - keep linters happy
    pass
