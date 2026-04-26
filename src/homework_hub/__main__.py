"""CLI entrypoint. Subcommands are wired in as phases land.

Usage:
    python -m homework_hub sync [--child <name>]
    python -m homework_hub auth (classroom|compass|edrolo) --child <name>
    python -m homework_hub bootstrap-sheet --child <name>
    python -m homework_hub status
"""

from __future__ import annotations

import click


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
def auth_classroom(child: str) -> None:
    click.echo(f"classroom auth stub (child={child}) — wired up in phase 5")


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
