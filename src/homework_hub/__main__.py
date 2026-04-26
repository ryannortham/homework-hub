"""CLI entrypoint. Subcommands are wired in as phases land.

Usage:
    python -m homework_hub                         # run daemon (default)
    python -m homework_hub sync [--child <name>]
    python -m homework_hub auth (classroom|compass|edrolo) --child <name>
    python -m homework_hub bootstrap-sheet --child <name>
    python -m homework_hub status
"""

from __future__ import annotations

import logging
from pathlib import Path

import click

from homework_hub.config import Settings
from homework_hub.daemon import run_daemon
from homework_hub.orchestrator import summarise_for_humans
from homework_hub.wiring import (
    _build_sheets_backend,
    build_orchestrator,
    write_sheet_id_to_config,
)


@click.group(invoke_without_command=True)
@click.pass_context
def cli(ctx: click.Context) -> None:
    """Homework Hub — aggregate homework from Classroom, Compass and Edrolo."""
    if ctx.invoked_subcommand is None:
        # Default action: start the long-running daemon (cron + /health).
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        )
        run_daemon(Settings())


@cli.command()
@click.option("--child", default=None, help="Child name; omit to sync all.")
def sync(child: str | None) -> None:
    """Run a one-shot sync."""
    settings = Settings()
    orchestrator = build_orchestrator(settings)
    report = orchestrator.run(only_child=child)
    click.echo(summarise_for_humans(report))
    if report.any_failures:
        # Non-zero exit so a CI/cron wrapper can detect issues.
        raise SystemExit(2)


@cli.group()
def auth() -> None:
    """Per-source authentication helpers."""


@auth.command("classroom")
@click.option("--child", required=True)
@click.option(
    "--client-secret-file",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    default=None,
    help="Google OAuth client secret JSON. Defaults to fetching from Vaultwarden.",
)
@click.option(
    "--token-path",
    type=click.Path(dir_okay=False, path_type=Path),
    default=None,
    help="Override the output token path. Defaults to <tokens_dir>/<child>-classroom.json.",
)
def auth_classroom(child: str, client_secret_file: Path | None, token_path: Path | None) -> None:
    """Run the Google Classroom OAuth flow on the local machine.

    Opens a browser window for the kid to grant consent. Token is written to
    ``<tokens_dir>/<child>-classroom.json``. The same token is used by every
    subsequent sync; refresh is automatic.
    """
    from homework_hub.secrets import from_env
    from homework_hub.sources.classroom import load_client_secret, run_oauth_flow

    settings = Settings()
    out_path = token_path or settings.child_token_path(child, "classroom")

    if client_secret_file:
        client_secret = load_client_secret(client_secret_file.read_text())
    else:
        bw = from_env()
        raw = bw.get_notes("Homework Hub - Google OAuth Client")
        client_secret = load_client_secret(raw)

    click.echo(f"Opening browser for {child} (Google Classroom)…")
    run_oauth_flow(client_secret, out_path)
    click.echo(f"Token saved → {out_path}")


@auth.command("compass")
@click.option(
    "--subdomain",
    required=True,
    help="Compass school subdomain, e.g. mcsc-vic.",
)
@click.option(
    "--cookie",
    default=None,
    help="ASP.NET_SessionId value. If omitted, prompts interactively.",
)
@click.option(
    "--token-path",
    type=click.Path(dir_okay=False, path_type=Path),
    default=None,
    help="Override token output path. Defaults to <tokens_dir>/compass-parent.json.",
)
def auth_compass(subdomain: str, cookie: str | None, token_path: Path | None) -> None:
    """Persist the parent Compass ASP.NET_SessionId cookie.

    The Compass school portal requires SMS-OTP login that we cannot automate.
    Log into Compass on Chrome, F12 → Application → Cookies → copy the value
    of ``ASP.NET_SessionId`` and pass it to this command (or paste when
    prompted).

    One token covers all children — no --child flag needed.
    """
    from homework_hub.sources.compass import CompassToken

    settings = Settings()
    out_path = token_path or settings.tokens_dir / "compass-parent.json"

    if not cookie:
        cookie = click.prompt("Paste ASP.NET_SessionId", hide_input=True)
    cookie = (cookie or "").strip()
    if not cookie:
        raise click.ClickException("Cookie is empty.")

    CompassToken(subdomain=subdomain, cookie=cookie).save(out_path)
    click.echo(f"Compass parent token saved → {out_path}")


@auth.command("edrolo")
@click.option("--child", required=True)
@click.option(
    "--token-path",
    type=click.Path(dir_okay=False, path_type=Path),
    default=None,
    help="Override storage-state output path. Defaults to <tokens_dir>/<child>-edrolo.json.",
)
@click.option(
    "--base-url",
    default="https://edrolo.com.au",
    help="Override Edrolo base URL (rarely needed).",
)
def auth_edrolo(child: str, token_path: Path | None, base_url: str) -> None:
    """Run a headed Playwright login for Edrolo and dump storage_state.json.

    Opens a real Chromium window so Google SSO (including 2FA) works without
    being detected as a bot. The kid signs in once on the Mac; the resulting
    cookies are copied to the server. Re-run when Discord alerts on expiry.
    """
    from homework_hub.sources.edrolo import run_headed_login

    settings = Settings()
    out_path = token_path or settings.child_token_path(child, "edrolo")

    click.echo(f"Opening headed Chromium for {child} (Edrolo)…")
    click.echo("Complete the Google sign-in in the browser; window closes automatically.")
    run_headed_login(out_path, base_url=base_url)
    click.echo(f"Edrolo storage state saved → {out_path}")


@cli.command("bootstrap-sheet")
@click.option("--child", required=True)
@click.option(
    "--title",
    default=None,
    help="Sheet title. Defaults to 'Homework — <Child Display Name>'.",
)
@click.option(
    "--share-with",
    multiple=True,
    help="Email(s) to share the new sheet with as Editor. May be repeated.",
)
def bootstrap_sheet(child: str, title: str | None, share_with: tuple[str, ...]) -> None:
    """Create a new Google Sheet for a child and apply the homework-hub template.

    Saves the spreadsheet ID back to children.yaml so subsequent syncs
    target the correct sheet. Service-account credentials are pulled from
    Vaultwarden.
    """
    from homework_hub.config import ChildrenConfig

    settings = Settings()
    cfg = ChildrenConfig.load(settings.children_yaml)
    if child not in cfg.children:
        raise click.ClickException(f"Unknown child '{child}' in children.yaml")
    if cfg.children[child].sheet_id:
        raise click.ClickException(
            f"{child} already has sheet_id={cfg.children[child].sheet_id}. "
            "Delete it from children.yaml first if you really want to re-bootstrap."
        )

    sheet_title = title or f"Homework — {cfg.children[child].display_name}"
    backend = _build_sheets_backend()
    click.echo(f"Creating sheet '{sheet_title}' …")
    sheet_id = backend.create_sheet(sheet_title, share_with=list(share_with) or None)
    write_sheet_id_to_config(settings.children_yaml, child, sheet_id)
    click.echo(f"Created sheet {sheet_id} and saved to children.yaml")
    if share_with:
        click.echo(f"Shared with: {', '.join(share_with)}")


@cli.command()
def status() -> None:
    """Print the most recent success/failure per child + source."""
    from homework_hub.config import ChildrenConfig
    from homework_hub.state.store import StateStore

    settings = Settings()
    cfg = ChildrenConfig.load(settings.children_yaml)
    state = StateStore(settings.state_db)
    records = {(r.child, r.source): r for r in state.all_auth()}

    for child_name, child_cfg in cfg.children.items():
        click.echo(f"{child_name} ({child_cfg.display_name})")
        click.echo(f"  sheet_id: {child_cfg.sheet_id or '— not bootstrapped —'}")
        for src in ("classroom", "compass", "edrolo"):
            rec = records.get((child_name, src))
            if rec is None:
                click.echo(f"  {src:9s}  — never synced —")
                continue
            success = rec.last_success_at.isoformat() if rec.last_success_at else "never"
            failure = ""
            if rec.last_failure_at:
                failure = (
                    f"  last_failure: {rec.last_failure_at.isoformat()} "
                    f"({rec.last_failure_kind}: {rec.last_failure_message})"
                )
            click.echo(f"  {src:9s}  last_success: {success}{failure}")


if __name__ == "__main__":
    cli()
