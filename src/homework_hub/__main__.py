"""CLI entrypoint. Subcommands are wired in as phases land.

Usage:
    python -m homework_hub sync [--child <name>]
    python -m homework_hub auth (classroom|compass|edrolo) --child <name>
    python -m homework_hub bootstrap-sheet --child <name>
    python -m homework_hub status
"""

from __future__ import annotations

from pathlib import Path

import click

from homework_hub.config import Settings


@click.group()
def cli() -> None:
    """Homework Hub — aggregate homework from Classroom, Compass and Edrolo."""


@cli.command()
@click.option("--child", default=None, help="Child name; omit to sync all.")
def sync(child: str | None) -> None:
    """Run a one-shot sync."""
    click.echo(f"sync stub (child={child or 'all'}) — wired up in phase 9")


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
@click.option("--child", required=True)
def auth_compass(child: str) -> None:
    click.echo(f"compass auth stub (child={child}) — wired up in phase 6")


@auth.command("edrolo")
@click.option("--child", required=True)
def auth_edrolo(child: str) -> None:
    click.echo(f"edrolo auth stub (child={child}) — wired up in phase 7")


@cli.command("bootstrap-sheet")
@click.option("--child", required=True)
def bootstrap_sheet(child: str) -> None:
    click.echo(f"bootstrap-sheet stub (child={child}) — wired up in phase 3")


@cli.command()
def status() -> None:
    click.echo("status stub — wired up in phase 9")


if __name__ == "__main__":
    cli()
