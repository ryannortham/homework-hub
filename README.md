# Homework Hub

Aggregates homework tasks from Google Classroom, Compass and Edrolo for each
child and writes a tidy per-child Google Sheet so they have one place to plan
their study and parents have visibility.

- One Google Sheet per child (Today / Tasks / Possible Duplicates / Settings tabs)
- Hourly sync, hosted on the homelab as a Portainer stack
- Discord notifications on new homework and on auth expiry
- Secrets fetched at runtime from Vaultwarden via the `bw` CLI

See `🏠 Personal/🖥️ Homelab/HomeworkHub.md` in the vault for full
architecture, runbook and onboarding instructions.

## Quick Start (Development)

```bash
# Install deps
uv sync

# Run the test suite
make test

# Lint / format
make lint
make format
```

## CLI

```bash
# One-time auth flows (run on a machine with a browser)
python -m homework_hub auth classroom --child james
python -m homework_hub auth compass --child james   # paste session cookie
python -m homework_hub auth edrolo --child james    # headed Playwright login

# Bootstrap a new child's Google Sheet
python -m homework_hub bootstrap-sheet --child james

# One-shot sync
python -m homework_hub sync --child james

# Sync all children (default scheduled action)
python -m homework_hub sync

# Status
python -m homework_hub status

# Run the daemon (default action when no subcommand is given) — APScheduler
# hourly cron + FastAPI /health on port 30062. This is what the container
# runs as its CMD.
python -m homework_hub
```

## Deployment (TrueNAS + Portainer)

Custom-built image; the Dockerfile lives in this repo. Deployed as a
Portainer GitOps stack pulling `docker-compose.yml` directly from this
repo on `main`. The image itself is built manually on the TrueNAS host
(Portainer's container has no buildx) and tagged `homework-hub:latest`
so the compose stack can reference it without a `build:` block.

Layout on TrueNAS:

```
/mnt/tank/Apps/HomeworkHub/
├── Config/                ← persisted: children.yaml, tokens/, state.db
└── Logs/                  ← rotating logs
```

(No `Build/` directory or `.env` on host — Portainer clones the repo
into its own working directory and stores the four `BW_*` secrets in
its encrypted DB.)

### Initial deploy

1. Create the persistent dirs once:
   ```bash
   ssh root@192.168.1.100 'mkdir -p /mnt/tank/Apps/HomeworkHub/{Config/tokens,Logs} && chown -R 568:568 /mnt/tank/Apps/HomeworkHub'
   ```
2. Build the image on TrueNAS:
   ```bash
   ssh root@192.168.1.100 'cd /tmp && rm -rf homework-hub-build && \
     git clone https://github.com/ryannortham/homework-hub homework-hub-build && \
     cd homework-hub-build && docker build -t homework-hub:latest .'
   ```
3. https://portainer.homelab → **Stacks** → **Add stack** → name `homework-hub`.
4. Build method: **Repository** → URL `https://github.com/ryannortham/homework-hub`, ref `refs/heads/main`, compose path `docker-compose.yml`.
5. Environment variables (Advanced mode): `BW_SERVER`, `BW_CLIENTID`, `BW_CLIENTSECRET`, `BW_PASSWORD`.
6. Enable **GitOps updates** (5 min polling) so `git push` auto-redeploys the compose changes.
7. **Deploy the stack**.

### Updating after a code change

- **Compose-only change** (env vars, volumes, etc.): `git push` and Portainer auto-redeploys within 5 min.
- **Code/Dockerfile change**: rerun step 2 above (`docker build` on TrueNAS), then trigger a redeploy in Portainer (Stacks → homework-hub → "Update the stack").

`/health` is polled by Uptime Kuma at port 30062 — there is no Caddy
entry and no UI; ops surface only.

## Layout

```
src/homework_hub/
├── __main__.py           # CLI entrypoint
├── config.py             # children.yaml + env settings
├── secrets.py            # Vaultwarden CLI wrapper
├── models.py             # canonical Task + Status schema
├── schema.py             # Gold tab/column specs (single source of truth)
├── medallion_orchestrator.py  # per-child ingest → transform → publish pipeline
├── sources/              # classroom.py, compass.py, edrolo.py
├── pipeline/
│   ├── publish.py        # silver → gold projection + UserEdits merge
│   └── transform.py      # bronze → silver upsert (SilverWriter)
├── sinks/
│   ├── gold_sink.py      # GspreadGoldSink — live Sheets reads/writes
│   └── sheets_client.py  # bootstrap-sheet creation + SA auth
├── state/
│   └── store.py          # SQLite schema init + StateStore
└── sheet_template.py     # bootstrap batchUpdate requests (tabs, tables, formatting)
```

## Licence

Private — personal use only.
