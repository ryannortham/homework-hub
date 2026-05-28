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
    """Run a one-shot full medallion sync (ingest \u2192 transform \u2192 publish)."""
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
    """Run only the publish stage."""
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


def _ensure_zen_marionette(child: str | None = None, force: bool = False) -> None:
    """Ensure Zen Browser is running with Marionette and the correct child profile; prompt to relaunch if not.

    Mirrors the Zen-launch logic in ``refresh-ep`` so all auth commands share
    the same behaviour.
    """
    from homework_hub.zen import (
        DEFAULT_PORT,
        find_zen_processes,
        get_child_profile_path,
        is_zen_running_with_profile,
        kill_zen_processes,
        launch_zen_with_marionette,
        marionette_reachable,
        wait_for_marionette,
    )

    profile_path = get_child_profile_path(child)
    is_correct_profile = is_zen_running_with_profile(profile_path)

    if marionette_reachable(DEFAULT_PORT) and is_correct_profile:
        return

    existing = find_zen_processes()
    if existing:
        if not is_correct_profile:
            click.echo(
                f"Zen is running (PIDs: {existing}) but not with the correct profile for "
                f"{child or 'default'}.\n"
                f"Target profile: {profile_path}"
            )
        else:
            click.echo(
                f"Zen is running (PIDs: {existing}) but Marionette is not reachable "
                f"on port {DEFAULT_PORT}.\n"
                "Marionette must be enabled at launch — it cannot be hot-attached."
            )

        if not force and not click.confirm(
            "Kill the running Zen instance and relaunch with the correct profile and Marionette?",
            default=False,
        ):
            raise click.ClickException("Aborted. Quit Zen yourself and re-run, or use --force.")
        click.echo("Stopping existing Zen…")
        kill_zen_processes(existing)

    click.echo(f"Launching Zen with Marionette for {child or 'default'}…")
    try:
        launch_zen_with_marionette(profile=profile_path)
    except RuntimeError as exc:
        raise click.ClickException(str(exc)) from exc

    click.echo("Waiting for Marionette to come up…")
    if not wait_for_marionette(DEFAULT_PORT, timeout=20.0):
        raise click.ClickException(
            f"Marionette did not become available on port {DEFAULT_PORT} within 20s. "
            "Check /tmp/zen-marionette.log for details."
        )
    import time

    time.sleep(2.0)
    click.echo("  Marionette ready.")


def _push_token(out_path: Path, host: str, dest: str, child: str, trigger_sync: bool) -> None:
    """Copy a token file to the remote host and optionally trigger a sync."""
    import subprocess

    remote = f"{host}:{dest}{out_path.name}"
    click.echo(f"Copying token to {remote}…")
    result = subprocess.run(["scp", str(out_path), remote], capture_output=True, text=True)
    if result.returncode != 0:
        raise click.ClickException(
            f"scp failed (exit {result.returncode}): {result.stderr.strip()}"
        )
    click.echo("  Token copied.")

    if trigger_sync:
        click.echo(f"Triggering sync for {child} on {host}…")
        result = subprocess.run(
            ["ssh", host, f"docker exec homework-hub homework-hub sync --child {child}"],
            text=True,
        )
        if result.returncode not in (0, 2):
            raise click.ClickException(f"Sync command exited with code {result.returncode}")


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
    help="Trigger a sync on the remote host after copying (default: yes).",
)
@click.option(
    "--force",
    "-f",
    is_flag=True,
    default=False,
    help="Skip the confirm prompt before killing an existing Zen instance.",
)
def auth_classroom(
    child: str,
    token_path: Path | None,
    base_url: str,
    host: str,
    dest: str,
    trigger_sync: bool,
    force: bool,
) -> None:
    """Login to Classroom via Zen Browser and save the session cookies.

    Opens a new tab in Zen Browser (launching it with Marionette if needed)
    so Google SSO works with a real Firefox fingerprint. Complete sign-in in
    the Zen window; the tab closes automatically once authenticated.
    The resulting token is copied to TrueNAS and a sync is triggered.
    Re-run when Discord alerts on auth expiry.
    """
    from homework_hub.sources.classroom import run_headed_login

    _ensure_zen_marionette(child=child, force=force)

    settings = Settings()
    out_path = token_path or settings.child_token_path(child, "classroom")

    click.echo(f"Opening new Zen tab for {child} (Google Classroom)…")
    click.echo("Complete the Google sign-in in the Zen window; the tab closes automatically.")
    run_headed_login(out_path, base_url=base_url)
    click.echo(f"Classroom storage state saved → {out_path}")
    _push_token(out_path, host, dest, child, trigger_sync)


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
    help="Trigger a sync on the remote host after copying (default: yes).",
)
@click.option(
    "--force",
    "-f",
    is_flag=True,
    default=False,
    help="Skip the confirm prompt before killing an existing Zen instance.",
)
def auth_edrolo(
    child: str,
    token_path: Path | None,
    base_url: str,
    host: str,
    dest: str,
    trigger_sync: bool,
    force: bool,
) -> None:
    """Login to Edrolo via Zen Browser and save the session cookies.

    Opens a new tab in Zen Browser (launching it with Marionette if needed)
    so Google SSO works with a real Firefox fingerprint. Complete sign-in in
    the Zen window; the tab closes automatically once authenticated.
    The resulting token is copied to TrueNAS and a sync is triggered.
    Re-run when Discord alerts on auth expiry.
    """
    from homework_hub.sources.edrolo import run_headed_login

    _ensure_zen_marionette(child=child, force=force)

    settings = Settings()
    out_path = token_path or settings.child_token_path(child, "edrolo")

    click.echo(f"Opening new Zen tab for {child} (Edrolo)…")
    click.echo("Complete the Google sign-in in the Zen window; the tab closes automatically.")
    run_headed_login(out_path, base_url=base_url)
    click.echo(f"Edrolo storage state saved → {out_path}")
    _push_token(out_path, host, dest, child, trigger_sync)


@auth.command("extract-from-zen")
@click.option(
    "--child",
    default=None,
    help="Restrict extraction to one child (james|tahlia). Omit to extract all.",
)
@click.option(
    "--profile",
    type=click.Path(file_okay=False, path_type=Path),
    default=None,
    help="Override Zen profile path. Defaults to the configured release profile.",
)
@click.option(
    "--host",
    default="root@192.168.1.100",
    show_default=True,
    help="SSH destination to copy tokens to.",
)
@click.option(
    "--dest",
    default="/mnt/tank/Apps/HomeworkHub/Config/tokens/",
    show_default=True,
    help="Remote directory for token files.",
)
@click.option(
    "--trigger-sync/--no-trigger-sync",
    default=True,
    help="Trigger a sync per child on the remote host after copying (default: yes).",
)
def auth_extract_from_zen(
    child: str | None,
    profile: Path | None,
    host: str,
    dest: str,
    trigger_sync: bool,
) -> None:
    """Extract persistent cookies from a running Zen Browser's SQLite store.

    Reads ``cookies.sqlite`` directly — no Marionette, no browser restart.
    Use this when Zen is already open in its normal session and you just
    want to harvest the existing logins.

    Container map (Firefox Multi-Account Containers):

    - default (``''``)           — Ryan personal (no homework data)
    - ``^userContextId=1``       — Tahlia/James school + Edrolo + Compass
    - ``^userContextId=2``       — Ryan work (no homework data)

    Per-source extraction:

    - **Tahlia Edrolo** (Container 1): ``sessionid`` on ``app.edrolo.com``
    - **James Classroom** (Container 1): Google ``SID`` on ``.google.com``

    EP ``access_token`` and Compass ``ASP.NET_SessionId`` are session
    cookies (in-memory only) and cannot be extracted via SQLite — use
    ``refresh-ep`` and ``auth compass`` respectively for those.
    """
    from homework_hub.sources.classroom import ClassroomStorageState
    from homework_hub.sources.edrolo import EdroloStorageState
    from homework_hub.zen import ZEN_PROFILE, extract_cookies_from_zen_sqlite

    settings = Settings()
    profile_path = profile or ZEN_PROFILE
    if not profile_path.exists():
        raise click.ClickException(f"Zen profile not found: {profile_path}")

    container1 = "^userContextId=1"
    successes: list[tuple[str, str, Path]] = []  # (child, source, token_path)
    skipped: list[str] = []

    # --- Tahlia Edrolo --------------------------------------------------- #
    if child in (None, "tahlia"):
        click.echo("Extracting Tahlia Edrolo (Container 1) …")
        edrolo_cookies = extract_cookies_from_zen_sqlite(
            profile_path, ["edrolo.com"], origin_attrs=container1
        )
        names = {c["name"] for c in edrolo_cookies}
        if "sessionid" in names:
            out_path = settings.child_token_path("tahlia", "edrolo")
            state = EdroloStorageState({"cookies": edrolo_cookies, "origins": []})
            state.save(out_path)
            click.echo(f"  saved {len(edrolo_cookies)} cookie(s) → {out_path}")
            successes.append(("tahlia", "edrolo", out_path))
        else:
            skipped.append("tahlia edrolo (no sessionid in Container 1)")

    # --- James Classroom ------------------------------------------------- #
    if child in (None, "james"):
        click.echo("Extracting James Classroom (Container 1, .google.com) …")
        google_cookies = extract_cookies_from_zen_sqlite(
            profile_path, ["google.com", "google.com.au"], origin_attrs=container1
        )
        names = {c["name"] for c in google_cookies}
        if "SID" in names:
            out_path = settings.child_token_path("james", "classroom")
            state = ClassroomStorageState({"cookies": google_cookies, "origins": []})
            state.save(out_path)
            click.echo(f"  saved {len(google_cookies)} cookie(s) → {out_path}")
            successes.append(("james", "classroom", out_path))
        else:
            skipped.append("james classroom (no Google SID in Container 1)")

    if not successes:
        click.echo("\nNo tokens extracted.")
        for s in skipped:
            click.echo(f"  skipped: {s}")
        raise click.ClickException("Nothing to push.")

    # --- Push + sync ----------------------------------------------------- #
    click.echo("")
    pushed_children: set[str] = set()
    for c, src, path in successes:
        click.echo(f"Pushing {c} {src} …")
        _push_token(path, host, dest, c, trigger_sync=False)
        pushed_children.add(c)

    for s in skipped:
        click.echo(f"skipped: {s}")

    if trigger_sync:
        import subprocess

        for c in sorted(pushed_children):
            click.echo(f"\nTriggering sync for {c} …")
            result = subprocess.run(
                ["ssh", host, f"docker exec homework-hub homework-hub sync --child {c}"],
                text=True,
            )
            if result.returncode not in (0, 2):
                raise click.ClickException(f"Sync for {c} exited with code {result.returncode}")


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
    from homework_hub.sources.eduperfect import run_headed_login

    settings = Settings()
    out_path = token_path or settings.child_token_path(child, "eduperfect")

    # 1. Ensure Marionette is reachable, launching Zen if needed.
    _ensure_zen_marionette(child=child, force=force)
    # EP dashboard needs a moment to start loading before we navigate.
    import time

    time.sleep(1.0)

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

    # 3. Copy to remote host and optionally trigger sync.
    _push_token(out_path, host, dest, child, trigger_sync)


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


@cli.command("reapply-template")
@click.option(
    "--child",
    default=None,
    help="Reapply to a single child's sheet. Defaults to every child with a sheet_id.",
)
def reapply_template(child: str | None) -> None:
    """Push schema-driven Dashboard layout + dropdown rules to EXISTING sheets
    without recreating them. Use after the schema changes (e.g. a renamed
    landing tab, new formulas, new dropdown values like ``"Archived"``)
    so already-bootstrapped sheets pick up the new shape.

    Uses the daemon's service-account credentials (which already have writer
    access to every kid's sheet), so no human OAuth flow is needed.
    """
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError

    from homework_hub.config import ChildrenConfig
    from homework_hub.schema import DASHBOARD_TABLE_IDS
    from homework_hub.secrets import from_env
    from homework_hub.sheet_template import refresh_layout_requests
    from homework_hub.sinks.sheets_client import (
        SheetsAPIError,
        load_service_account_credentials,
    )
    from homework_hub.wiring import SERVICE_ACCOUNT_BW_NAME, build_medallion_orchestrator

    settings = Settings()
    cfg = ChildrenConfig.load(settings.children_yaml)

    if child is not None and child not in cfg.children:
        raise click.ClickException(f"Unknown child '{child}' in children.yaml")

    targets = (
        [(child, cfg.children[child])]
        if child is not None
        else [(name, c) for name, c in cfg.children.items() if c.sheet_id]
    )
    if not targets:
        click.echo("No children with sheet_id set; nothing to do.")
        return

    bw = from_env()
    raw = bw.get_notes(SERVICE_ACCOUNT_BW_NAME)
    creds = load_service_account_credentials(raw)
    service = build("sheets", "v4", credentials=creds, cache_discovery=False)

    click.echo(f"Reapplying template to {len(targets)} sheet(s)…")
    for name, ch in targets:
        if not ch.sheet_id:
            click.echo(f"  {name}: no sheet_id; skipped")
            continue
        try:
            # Fetch live tab→sheetId mapping + stateful artefacts on the
            # Dashboard tab (charts, bandings, merges, conditional rules)
            # so the refresh can tear them down before re-adding — otherwise
            # they'd stack up on every reapply.
            meta = (
                service.spreadsheets()
                .get(
                    spreadsheetId=ch.sheet_id,
                    fields=(
                        "sheets.properties(title,sheetId,index,gridProperties),"
                        "sheets.charts.chartId,"
                        "sheets.bandedRanges(bandedRangeId,range),"
                        "sheets.merges,"
                        "sheets.conditionalFormats,"
                        "sheets.tables(tableId)"
                    ),
                )
                .execute()
            )
            sheets = sorted(meta.get("sheets", []), key=lambda s: s["properties"].get("index", 0))
            overrides = {s["properties"]["title"]: s["properties"]["sheetId"] for s in sheets}
            existing_tab_names = [s["properties"]["title"] for s in sheets]
            # Dashboard is always the first tab (index 0 in the sorted list);
            # collect its stateful artefacts.
            dash = sheets[0] if sheets else {}
            dash_sid = dash.get("properties", {}).get("sheetId")
            chart_ids = [c["chartId"] for c in dash.get("charts", []) if "chartId" in c]
            banded_ids = [
                b["bandedRangeId"] for b in dash.get("bandedRanges", []) if "bandedRangeId" in b
            ]
            # Merges fetched under ``sheets[i].merges`` omit ``sheetId``
            # in the API response (it's implicit from the parent sheet),
            # but ``unmergeCells`` requires the GridRange to carry it.
            # Inject the parent sheet's id so the teardown can address
            # the merges by their explicit range.
            merges = [{**m, "sheetId": dash_sid} for m in dash.get("merges", [])]
            cf_rules = dash.get("conditionalFormats", [])
            # v5.0 Dashboard Tables — torn down before frame re-emit so
            # the publish step that follows can recreate them sized to
            # the current data.
            dash_table_ids = [
                t["tableId"]
                for t in dash.get("tables", [])
                if t.get("tableId") in DASHBOARD_TABLE_IDS
            ]
            requests = refresh_layout_requests(
                sheet_id_overrides=overrides,
                existing_tab_names=existing_tab_names,
                existing_chart_ids=chart_ids,
                existing_banded_range_ids=banded_ids,
                existing_merge_ranges=merges,
                existing_conditional_format_rule_count=len(cf_rules),
                existing_dashboard_table_ids=dash_table_ids,
            )
            service.spreadsheets().batchUpdate(
                spreadsheetId=ch.sheet_id,
                body={"requests": requests},
            ).execute()
            click.echo(f"  {name}: frame OK ({ch.sheet_id}, {len(requests)} requests)")
        except HttpError as exc:
            raise SheetsAPIError(
                f"Failed to reapply template to {name} ({ch.sheet_id}): {exc}"
            ) from exc

    # After every per-child frame reapply succeeds, run publish_only so
    # the Dashboard's three task-list Tables materialise sized to the
    # current data. Frame reapply alone leaves the lists region empty;
    # publish populates it. Reported per-child so a single failure
    # doesn't hide the others.
    settings = Settings()
    orchestrator = build_medallion_orchestrator(settings)
    failures = 0
    for name, _ch in targets:
        click.echo(f"Publishing {name} so Dashboard tables materialise…")
        try:
            report = orchestrator.publish_only(only_child=name)
            click.echo(summarise_medallion(report))
            if report.any_failures:
                failures += 1
        except Exception as exc:
            failures += 1
            click.echo(f"  {name}: publish failed: {exc}", err=True)
    if failures:
        raise SystemExit(2)


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
            # Check if source is enabled in config
            source_cfg = getattr(child_cfg.sources, src, None)
            enabled = source_cfg is not None and source_cfg.enabled

            rec = records.get((child_name, src))
            if rec is None:
                if not enabled:
                    continue
                click.echo(f"  {src:9s}  — never synced —")
                continue

            status_suffix = "" if enabled else " (disabled)"
            success = rec.last_success_at.isoformat() if rec.last_success_at else "never"
            failure = ""
            if rec.last_failure_at:
                failure = (
                    f"  last_failure: {rec.last_failure_at.isoformat()} "
                    f"({rec.last_failure_kind}: {rec.last_failure_message})"
                )
            click.echo(f"  {src:9s}{status_suffix}  last_success: {success}{failure}")


# --------------------------------------------------------------------------- #
# Archive / un-archive — manual silver-row lifecycle controls
# --------------------------------------------------------------------------- #


@cli.command("archive")
@click.option("--child", required=True, help="Child key as defined in children.yaml.")
@click.option("--uid", required=True, help="task_uid in the form '<source>:<source_id>'.")
@click.option(
    "--reason",
    type=click.Choice(["manual", "upstream_removed", "age_cap"]),
    default="manual",
    show_default=True,
)
def archive_cmd(child: str, uid: str, reason: str) -> None:
    """Manually archive a silver task. Routes the row to the History tab."""
    from homework_hub.state.store import StateStore

    settings = Settings()
    state = StateStore(settings.state_db)
    source, _, source_id = uid.partition(":")
    if not source or not source_id:
        raise click.ClickException(f"Invalid uid {uid!r}; expected '<source>:<source_id>'.")
    ok = state.mark_archived(child=child, source=source, source_id=source_id, reason=reason)
    if not ok:
        raise click.ClickException(f"No silver row for child={child} uid={uid}.")
    click.echo(f"Archived {uid} for {child} (reason={reason}).")


@cli.command("unarchive")
@click.option("--child", required=True)
@click.option("--uid", required=True, help="task_uid in the form '<source>:<source_id>'.")
def unarchive_cmd(child: str, uid: str) -> None:
    """Clear the archive flags on a silver task. Status reverts to ``not_started``
    so the source mapper (or kid's UserEdit) can take over on next sync."""
    from homework_hub.state.store import StateStore

    settings = Settings()
    state = StateStore(settings.state_db)
    source, _, source_id = uid.partition(":")
    if not source or not source_id:
        raise click.ClickException(f"Invalid uid {uid!r}; expected '<source>:<source_id>'.")
    ok = state.clear_archive(child=child, source=source, source_id=source_id)
    if not ok:
        raise click.ClickException(f"No silver row for child={child} uid={uid}.")
    click.echo(f"Un-archived {uid} for {child}.")


@cli.command("list-archived")
@click.option("--child", required=True)
@click.option(
    "--reason",
    type=click.Choice(["manual", "upstream_removed", "age_cap"]),
    default=None,
    help="Filter to a single archive reason.",
)
def list_archived_cmd(child: str, reason: str | None) -> None:
    """List archived silver tasks for a child, newest first."""
    from homework_hub.state.store import StateStore

    settings = Settings()
    state = StateStore(settings.state_db)
    rows = state.list_archived(child=child, reason=reason)
    if not rows:
        click.echo(f"No archived tasks for {child}" + (f" (reason={reason})." if reason else "."))
        return
    for r in rows:
        uid = f"{r['source']}:{r['source_id']}"
        due = r["due_at"] or "—"
        click.echo(
            f"{r['archived_at']}  {r['archived_reason']:17s}  {uid:50s}  "
            f"due={due}  {r['subject_raw']} — {r['title']}"
        )


if __name__ == "__main__":
    cli()
