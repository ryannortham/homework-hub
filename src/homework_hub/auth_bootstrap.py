"""OAuth bootstrap-token helpers for the Sheets bootstrap flow (M5c).

The bootstrap step (creating + templating a fresh kid spreadsheet) runs as
``ryan.northam@gmail.com`` rather than the service account so the resulting
sheet lives in the user's personal Drive and is naturally shared with kids
without quota concerns. The service account is then added as a writer so
the daemon's routine publishes work.

Two artefacts are involved:

* **Client secret** — the GCP-issued OAuth Client ID JSON. Stored as a
  Vaultwarden secure note named in :data:`OAUTH_CLIENT_BW_NAME`. Looked up
  via the existing :class:`~homework_hub.secrets.VaultwardenCLI` helper.
* **User token** — the per-user refresh token cache. Saved at
  ``settings.tokens_dir / "ryan-bootstrap.json"`` after the first
  authorise + reused on subsequent runs until revoked.

The flow uses ``InstalledAppFlow.run_local_server()``: opens the system
browser, redirects to ``http://localhost:<port>``. Suitable for running
on the Mac during onboarding; the daemon never needs to call this.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow

from homework_hub.secrets import VaultwardenCLI, from_env

log = logging.getLogger(__name__)

OAUTH_CLIENT_BW_NAME = "Homework Hub - Google OAuth Client"
BOOTSTRAP_TOKEN_FILENAME = "ryan-bootstrap.json"

# Same scopes as the service account: spreadsheets (full) + drive (for
# share()). Granting drive at user level is fine — the user owns the sheet.
DEFAULT_SCOPES: list[str] = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


class BootstrapAuthError(Exception):
    """Raised when the bootstrap OAuth flow can't produce valid credentials."""


@dataclass(frozen=True)
class BootstrapAuth:
    """A loaded ``Credentials`` plus the path it was cached at.

    Returned by :func:`load_or_run_bootstrap_flow` so callers can both use
    the credentials immediately and know where the refreshed token landed
    if they want to log it.
    """

    credentials: Credentials
    token_path: Path


def _load_client_config(bw: VaultwardenCLI) -> dict[str, Any]:
    """Fetch the OAuth client JSON from Vaultwarden and parse it.

    Accepts either the ``installed`` or ``web`` client-type JSON shapes —
    ``InstalledAppFlow.from_client_config`` autodetects.
    """
    raw = bw.get_notes(OAUTH_CLIENT_BW_NAME)
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise BootstrapAuthError(
            f"Vaultwarden note '{OAUTH_CLIENT_BW_NAME}' is not valid JSON: {exc}"
        ) from exc


def _load_cached_token(token_path: Path) -> Credentials | None:
    """Return cached credentials from disk, or None if missing/unreadable."""
    if not token_path.exists():
        return None
    try:
        return Credentials.from_authorized_user_file(str(token_path), DEFAULT_SCOPES)
    except (ValueError, json.JSONDecodeError) as exc:
        log.warning("ignoring corrupt bootstrap token %s: %s", token_path, exc)
        return None


def _save_token(token_path: Path, creds: Credentials) -> None:
    token_path.parent.mkdir(parents=True, exist_ok=True)
    token_path.write_text(creds.to_json())


def load_or_run_bootstrap_flow(
    *,
    tokens_dir: Path,
    bw: VaultwardenCLI | None = None,
    scopes: list[str] | None = None,
    open_browser: bool = True,
) -> BootstrapAuth:
    """Return valid bootstrap credentials, prompting the user if needed.

    Behaviour:

    1. Load ``tokens_dir / "ryan-bootstrap.json"`` if it exists.
    2. If valid, return as-is. If expired-but-refreshable, refresh + persist.
    3. Otherwise run :class:`InstalledAppFlow` against the Vaultwarden-stored
       client secret, opening a browser for user consent. Persist the new token.

    ``open_browser=False`` switches the flow to printing the URL instead
    (handy for SSH sessions or tests). Tests typically inject a fake
    ``VaultwardenCLI`` and short-circuit by writing a token file directly.
    """
    bw = bw or from_env()
    scopes = scopes or DEFAULT_SCOPES
    token_path = tokens_dir / BOOTSTRAP_TOKEN_FILENAME

    creds = _load_cached_token(token_path)

    if creds and creds.valid:
        return BootstrapAuth(credentials=creds, token_path=token_path)

    if creds and creds.expired and creds.refresh_token:
        try:
            creds.refresh(Request())
            _save_token(token_path, creds)
            return BootstrapAuth(credentials=creds, token_path=token_path)
        except Exception as exc:
            log.warning("bootstrap token refresh failed (%s); reauthorising", exc)

    # Fresh consent flow.
    client_config = _load_client_config(bw)
    flow = InstalledAppFlow.from_client_config(client_config, scopes)
    if open_browser:
        creds = flow.run_local_server(port=0, open_browser=True)
    else:
        creds = flow.run_local_server(port=0, open_browser=False)
    _save_token(token_path, creds)
    return BootstrapAuth(credentials=creds, token_path=token_path)


__all__ = [
    "BOOTSTRAP_TOKEN_FILENAME",
    "DEFAULT_SCOPES",
    "OAUTH_CLIENT_BW_NAME",
    "BootstrapAuth",
    "BootstrapAuthError",
    "load_or_run_bootstrap_flow",
]
