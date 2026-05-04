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
from homework_hub.medallion_orchestrator import (
    replay_silver_from_bronze,
    summarise_medallion,
)
from homework_hub.wiring import (
    build_medallion_orchestrator,
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
    """Run a one-shot full medallion sync (ingest \u2192 transform \u2192 detect \u2192 publish)."""
    settings = Settings()
    orchestrator = build_medallion_orchestrator(settings)
    report = orchestrator.run(only_child=child)
    click.echo(summarise_medallion(report))
    if report.any_failures:
        raise SystemExit(2)


@cli.command()
@click.option("--child", default=None, help="Child name; omit to ingest for all.")
def ingest(child: str | None) -> None:
    """Run only the ingest stage (sources \u2192 bronze)."""
    settings = Settings()
    orchestrator = build_medallion_orchestrator(settings)
    report = orchestrator.ingest_only(only_child=child)
    click.echo(summarise_medallion(report))
    if report.any_failures:
        raise SystemExit(2)


@cli.command()
@click.option("--child", default=None, help="Child name; omit to transform for all.")
def transform(child: str | None) -> None:
    """Run only the transform stage (bronze \u2192 silver)."""
    settings = Settings()
    orchestrator = build_medallion_orchestrator(settings)
    report = orchestrator.transform_only(only_child=child)
    click.echo(summarise_medallion(report))
    if report.any_failures:
        raise SystemExit(2)


@cli.command()
@click.option("--child", default=None, help="Child name; omit to publish for all.")
def publish(child: str | None) -> None:
    """Run only the detect + publish stages."""
    settings = Settings()
    orchestrator = build_medallion_orchestrator(settings)
    report = orchestrator.publish_only(only_child=child)
    click.echo(summarise_medallion(report))
    if report.any_failures:
        raise SystemExit(2)


@cli.command()
@click.option(
    "--child",
    default=None,
    help="Child name; omit to replay for every child currently in bronze.",
)
def replay(child: str | None) -> None:
    """Re-run transform against existing bronze (no source fetches).

    Useful after editing subject rules or transform code: rebuilds
    ``silver_tasks`` from the ``bronze_records`` already on disk.
    """
    from homework_hub.state.store import StateStore

    settings = Settings()
    state = StateStore(settings.state_db)
    results = replay_silver_from_bronze(state, only_child=child)
    if not results:
        click.echo("No bronze rows found \u2014 run `homework-hub ingest` first.")
        return
    failed = False
    for c, r in results.items():
        if r.ok:
            click.echo(f"{c}: +{r.inserted} new, ~{r.updated} changed, ={r.unchanged} unchanged")
        else:
            failed = True
            click.echo(f"{c}: FAILED \u2014 {r.error}")
    if failed:
        raise SystemExit(2)


@cli.group()
def auth() -> None:
    """Per-source authentication helpers."""


@auth.command("classroom")
@click.option("--child", required=True)
@click.option(
    "--token-path",
    type=click.Path(dir_okay=False, path_type=Path),
    default=None,
    help="Override storage-state output path. Defaults to <tokens_dir>/<child>-classroom.json.",
)
@click.option(
    "--base-url",
    default="https://classroom.google.com",
    help="Override Classroom base URL (rarely needed).",
)
def auth_classroom(child: str, token_path: Path | None, base_url: str) -> None:
    """Run a headed Playwright login for Classroom and dump storage_state.json.

    Mordialloc's Workspace admin blocks third-party OAuth apps, so we replay
    the kid's authenticated browser session instead. Opens a real Chromium
    window so Google SSO (including 2FA) works without being detected as a
    bot. The kid signs in once on the Mac; the resulting cookies are copied
    to the server. Re-run when Discord alerts on expiry.
    """
    from homework_hub.sources.classroom import run_headed_login

    settings = Settings()
    out_path = token_path or settings.child_token_path(child, "classroom")

    click.echo(f"Opening headed Chromium for {child} (Google Classroom)…")
    click.echo("Complete the Google sign-in in the browser; window closes automatically.")
    run_headed_login(out_path, base_url=base_url)
    click.echo(f"Classroom storage state saved → {out_path}")


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


@auth.command("eduperfect")
@click.option("--child", required=True)
@click.option(
    "--token-path",
    type=click.Path(dir_okay=False, path_type=Path),
    default=None,
    help="Override token output path. Defaults to <tokens_dir>/<child>-eduperfect.json.",
)
def auth_eduperfect(child: str, token_path: Path | None) -> None:
    """Capture the EP access_token from a running Zen Browser session.

    Zen Browser must be running with Marionette enabled. Launch it once with::

        /Applications/Zen.app/Contents/MacOS/zen \\
          --marionette --marionette-port 2828 \\
          --remote-allow-system-access \\
          --profile "$HOME/Library/Application Support/zen/Profiles/<profile>"

    Ensure James is logged into app.educationperfect.com in that Zen window,
    then run this command. The token is captured by observing the HTTP traffic
    from the existing session.
    """
    from homework_hub.sources.eduperfect import run_headed_login

    settings = Settings()
    out_path = token_path or settings.child_token_path(child, "eduperfect")

    click.echo(f"Connecting to Zen Browser Marionette for {child} (Education Perfect)…")
    click.echo("Ensure Zen is running with --marionette and James is logged into EP.")
    run_headed_login(out_path)
    click.echo(f"Education Perfect token saved → {out_path}")


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
    default="https://app.edrolo.com",
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


@cli.command("refresh-ep")
@click.option("--child", default="james", show_default=True, help="Child name.")
@click.option(
    "--token-path",
    type=click.Path(dir_okay=False, path_type=Path),
    default=None,
    help="Override local token output path.",
)
@click.option(
    "--host",
    default="root@192.168.1.100",
    show_default=True,
    help="SSH destination to copy token to.",
)
@click.option(
    "--dest",
    default="/mnt/tank/Apps/HomeworkHub/Config/tokens/",
    show_default=True,
    help="Remote directory for the token file.",
)
@click.option(
    "--trigger-sync/--no-trigger-sync",
    default=True,
    help="SSH into host and trigger a sync after copying (default: yes).",
)
@click.option(
    "--force",
    "-f",
    is_flag=True,
    default=False,
    help="Skip the confirm prompt before killing an existing Zen instance.",
)
def refresh_ep(
    child: str,
    token_path: Path | None,
    host: str,
    dest: str,
    trigger_sync: bool,
    force: bool,
) -> None:
    """Capture a fresh EP token, copy to TrueNAS, and trigger a sync.

    One-command refresh. If Zen Browser is not running with Marionette, this
    command launches it (prompting first if a non-Marionette Zen is already
    running — pass --force to skip the prompt).

    Prerequisite: James must have logged into app.educationperfect.com in Zen
    at least once on this Mac. The FusionAuth SSO cookie persists across
    reboots, so subsequent refreshes are silent.
    """
    import subprocess

    from homework_hub.sources.eduperfect import run_headed_login
    from homework_hub.zen import (
        DEFAULT_PORT,
        find_zen_processes,
        kill_zen_processes,
        launch_zen_with_marionette,
        marionette_reachable,
        wait_for_marionette,
    )

    settings = Settings()
    out_path = token_path or settings.child_token_path(child, "eduperfect")

    # 1. Ensure Marionette is reachable, launching Zen if needed.
    if not marionette_reachable(DEFAULT_PORT):
        existing = find_zen_processes()
        if existing:
            click.echo(
                f"Zen is running (PIDs: {existing}) but Marionette is not "
                f"reachable on port {DEFAULT_PORT}.\n"
                "Marionette must be enabled at launch — it cannot be hot-attached."
            )
            if not force and not click.confirm(
                "Kill the running Zen instance and relaunch with Marionette?",
                default=False,
            ):
                raise click.ClickException("Aborted. Quit Zen yourself and re-run, or use --force.")
            click.echo("Stopping existing Zen…")
            kill_zen_processes(existing)

        click.echo("Launching Zen with Marionette…")
        try:
            launch_zen_with_marionette()
        except RuntimeError as exc:
            raise click.ClickException(str(exc)) from exc

        click.echo("Waiting for Marionette to come up…")
        if not wait_for_marionette(DEFAULT_PORT, timeout=20.0):
            raise click.ClickException(
                f"Marionette did not become available on port {DEFAULT_PORT} "
                "within 20 seconds. Check /tmp/zen-marionette.log for details."
            )
        # Brief pause for the EP dashboard to start loading before we navigate.
        import time

        time.sleep(3.0)
        click.echo("  Marionette ready.")

    # 2. Capture token from Zen Marionette.
    click.echo(f"Capturing EP token for {child}…")
    try:
        run_headed_login(out_path)
    except RuntimeError as exc:
        msg = str(exc)
        if "no access_token cookie was captured" in msg:
            raise click.ClickException(
                "EP token capture failed — James may be logged out of "
                "Education Perfect.\n"
                "  1. Switch to the Zen window already open on your desktop.\n"
                "  2. Navigate to https://app.educationperfect.com and log in.\n"
                "  3. Re-run: uv run homework-hub refresh-ep"
            ) from exc
        raise click.ClickException(msg) from exc

    # Show token freshness.
    try:
        from homework_hub.sources.eduperfect import EduPerfectTokenFile

        tf = EduPerfectTokenFile.load(out_path)
        from datetime import UTC, datetime

        remaining = tf.expires_at - datetime.now(UTC)
        mins = max(0, int(remaining.total_seconds() // 60))
        click.echo(f"  Token saved → {out_path} (expires in ~{mins}m)")
    except Exception:
        click.echo(f"  Token saved → {out_path}")

    # 3. Copy to remote host.
    remote = f"{host}:{dest}{out_path.name}"
    click.echo(f"Copying token to {remote}…")
    result = subprocess.run(["scp", str(out_path), remote], capture_output=True, text=True)
    if result.returncode != 0:
        raise click.ClickException(
            f"scp failed (exit {result.returncode}): {result.stderr.strip()}"
        )
    click.echo("  Token copied.")

    # 4. Trigger sync on remote host with live output.
    if trigger_sync:
        click.echo(f"Triggering sync for {child} on {host}…")
        result = subprocess.run(
            ["ssh", host, f"docker exec homework-hub homework-hub sync --child {child}"],
            text=True,
        )
        if result.returncode not in (0, 2):  # 2 = sync ran with non-fatal failures
            raise click.ClickException(f"Sync command exited with code {result.returncode}")


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

    Authenticates as the human bootstrap user (``ryan.northam@gmail.com``)
    via OAuth so the sheet is owned by a real account; auto-shares it
    with the daemon's service account as Editor so subsequent syncs can
    publish. Saves the spreadsheet ID back to children.yaml.
    """
    from homework_hub.config import ChildrenConfig
    from homework_hub.wiring import build_bootstrap_sheets_backend

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
    click.echo("Authenticating as bootstrap user (browser may open) …")
    backend, sa_email = build_bootstrap_sheets_backend(settings)
    share_targets = [*share_with, sa_email]
    click.echo(f"Creating sheet '{sheet_title}' …")
    sheet_id = backend.create_sheet(sheet_title, share_with=share_targets)
    write_sheet_id_to_config(settings.children_yaml, child, sheet_id)
    click.echo(f"Created sheet {sheet_id} and saved to children.yaml")
    click.echo(f"Shared with service account {sa_email} (writer)")
    if share_with:
        click.echo(f"Also shared with: {', '.join(share_with)}")


@cli.group()
def subjects() -> None:
    """Manage the ``dim_subjects`` canonicalisation rule table."""


def _build_resolver() -> tuple[Settings, object]:
    """Construct ``(settings, SubjectResolver)`` for CLI commands.

    Imported lazily so ``homework_hub --help`` stays cheap.
    """
    from homework_hub.pipeline.subjects import SubjectResolver
    from homework_hub.state.store import StateStore

    settings = Settings()
    store = StateStore(settings.state_db)
    return settings, SubjectResolver(store)


@subjects.command("list")
def subjects_list() -> None:
    """List all subject rules in priority order."""
    _, resolver = _build_resolver()
    rules = resolver.rules
    if not rules:
        click.echo("No rules. Run `homework_hub subjects seed` to load defaults.")
        return
    click.echo(f"{'id':>4} {'type':<7} {'prio':>4}  {'pattern':<40} → canonical (short)")
    for r in rules:
        click.echo(
            f"{r.id:>4} {r.match_type:<7} {r.priority:>4}  "
            f"{r.pattern:<40} → {r.canonical} ({r.short})"
        )
    click.echo(f"\n{len(rules)} rule(s).")


@subjects.command("seed")
@click.option(
    "--from",
    "from_path",
    type=click.Path(dir_okay=False, path_type=Path),
    default=None,
    help="YAML file to seed from. Defaults to <config_dir>/subjects.yaml.",
)
@click.option(
    "--replace",
    is_flag=True,
    default=False,
    help="Wipe existing rules before seeding (otherwise INSERT OR IGNORE).",
)
def subjects_seed(from_path: Path | None, replace: bool) -> None:
    """Seed ``dim_subjects`` from a YAML file."""
    settings, resolver = _build_resolver()
    yaml_path = from_path or settings.config_dir / "subjects.yaml"
    if not yaml_path.exists():
        raise click.ClickException(f"Seed file not found: {yaml_path}")
    count = resolver.seed_from_yaml(yaml_path, replace=replace)
    verb = "replaced" if replace else "merged"
    click.echo(f"Seeded {count} rule(s) from {yaml_path} ({verb}).")


@subjects.command("test")
@click.argument("raw")
def subjects_test(raw: str) -> None:
    """Test how a raw subject string resolves."""
    _, resolver = _build_resolver()
    match = resolver.resolve(raw)
    if match is None:
        click.echo(f"{raw!r} → no match (would fall back to raw value)")
        raise SystemExit(1)
    click.echo(
        f"{raw!r} → {match.canonical} ({match.short})  "
        f"[rule #{match.rule_id}, {match.match_type}]"
    )


@subjects.command("add")
@click.option(
    "--type",
    "match_type",
    type=click.Choice(["exact", "prefix", "regex"]),
    required=True,
)
@click.option("--pattern", required=True)
@click.option("--canonical", required=True, help="Human label, e.g. 'Year 9 Science'.")
@click.option("--short", required=True, help="Kid-facing short, e.g. 'Sci'.")
@click.option(
    "--priority",
    type=int,
    default=None,
    help="Override default priority (exact=100, prefix=50, regex=10).",
)
def subjects_add(
    match_type: str,
    pattern: str,
    canonical: str,
    short: str,
    priority: int | None,
) -> None:
    """Add a new rule."""
    _, resolver = _build_resolver()
    try:
        new_id = resolver.add_rule(
            match_type=match_type,
            pattern=pattern,
            canonical=canonical,
            short=short,
            priority=priority,
        )
    except ValueError as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(f"Added rule #{new_id}: {match_type} {pattern!r} → {canonical} ({short})")


@subjects.command("remove")
@click.option(
    "--type",
    "match_type",
    type=click.Choice(["exact", "prefix", "regex"]),
    required=True,
)
@click.option("--pattern", required=True)
def subjects_remove(match_type: str, pattern: str) -> None:
    """Remove a rule by (type, pattern)."""
    _, resolver = _build_resolver()
    removed = resolver.remove_rule(match_type=match_type, pattern=pattern)
    if removed == 0:
        raise click.ClickException(f"No rule matched {match_type} {pattern!r}.")
    click.echo(f"Removed {removed} rule(s).")


@cli.group()
def links() -> None:
    """Inspect or re-run cross-source duplicate detection.

    Confirmation/dismissal of pending links is done by kids via the
    Possible Duplicates sheet checkboxes — there is no CLI verb for it.
    """


def _build_link_detector() -> tuple[Settings, object]:
    from homework_hub.pipeline.link_detector import LinkDetector
    from homework_hub.state.store import StateStore

    settings = Settings()
    store = StateStore(settings.state_db)
    return settings, LinkDetector(store)


@links.command("list")
@click.option(
    "--child",
    default=None,
    help="Child name; omit to list links for every child in children.yaml.",
)
def links_list(child: str | None) -> None:
    """Print every silver_task_link row, grouped by child."""
    from homework_hub.config import ChildrenConfig

    settings, detector = _build_link_detector()
    cfg = ChildrenConfig.load(settings.children_yaml)
    targets = [child] if child else list(cfg.children.keys())

    any_rows = False
    for name in targets:
        rows = detector.list_for_child(name)
        if not rows:
            continue
        any_rows = True
        click.echo(f"\n== {name} ({len(rows)} link(s)) ==")
        for r in rows:
            click.echo(
                f"  #{r['id']:<4} {r['confidence']:<11} {r['state']:<10} "
                f"{r['primary_source']}:{r['primary_source_id']} ↔ "
                f"{r['secondary_source']}:{r['secondary_source_id']} "
                f"(Δdays={r['score_due']}, title={r['score_title']:.2f})"
            )
    if not any_rows:
        click.echo("No links found.")


@links.command("detect")
@click.option(
    "--child",
    default=None,
    help="Child name; omit to re-detect for every child in children.yaml.",
)
def links_detect(child: str | None) -> None:
    """Re-run the duplicate detector against current silver_tasks."""
    from homework_hub.config import ChildrenConfig

    settings, detector = _build_link_detector()
    cfg = ChildrenConfig.load(settings.children_yaml)
    targets = [child] if child else list(cfg.children.keys())

    for name in targets:
        result = detector.detect(name)
        click.echo(
            f"{name}: inserted={result.inserted} updated={result.updated} "
            f"unchanged={result.unchanged}"
        )


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
        for src in ("classroom", "compass", "eduperfect", "edrolo"):
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
