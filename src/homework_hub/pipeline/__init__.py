"""Pipeline layer — bronze (ingest) → silver (transform) → gold (publish).

Each submodule owns one stage of the medallion architecture; the orchestrator
composes them. See ``HomeworkHub.md → Data Architecture (Medallion)`` in the
Vault for the full design.
"""

from __future__ import annotations
