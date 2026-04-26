# Homework Hub

Aggregates homework tasks from Google Classroom, Compass and Edrolo for each
child and writes a tidy per-child Google Sheet so they have one place to plan
their study and parents have visibility.

- One Google Sheet per child (Today / Tasks / By Subject / Settings tabs)
- Hourly sync, hosted on the homelab as a Portainer stack
- Discord notifications on new homework and on auth expiry
- Secrets fetched from Vaultwarden via the Bitwarden CLI

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

Custom-built image; the Dockerfile lives in this repo and the compose
stack is checked in as `docker-compose.yml`. Convention follows
`HomeworkHub.md` in the vault:

```
/mnt/tank/Apps/HomeworkHub/
├── Build/                 ← `git clone` of this repo (Dockerfile lives here)
├── Config/                ← persisted: children.yaml, tokens/, state.db
└── Logs/                  ← rotating logs
```

Build + deploy:

```bash
ssh root@192.168.1.100
mkdir -p /mnt/tank/Apps/HomeworkHub/{Config/tokens,Logs}
cd /mnt/tank/Apps/HomeworkHub
git clone git@github.com:ryannortham/homework-hub.git Build
cp Build/docker-compose.yml ./docker-compose.yml
cp Build/.env.example ./.env   # then edit BW_* secrets
chown -R 568:568 Config Logs

# Stand up via Portainer (Stacks → Add → Repository or upload the compose file)
docker compose up -d --build
```

Updates: `git -C Build pull && docker compose up -d --build` (or use the
Portainer "Pull and redeploy" button).

`/health` is polled by Uptime Kuma at port 30062 — there is no Caddy
entry and no UI; ops surface only.

## Layout

```
src/homework_hub/
├── __main__.py           # CLI entrypoint
├── config.py             # children.yaml + env settings
├── secrets.py            # Bitwarden CLI wrapper
├── models.py             # canonical Task schema
├── orchestrator.py       # per-child collect → merge → write → notify
├── sources/              # classroom, compass, edrolo
├── sinks/                # sheets, discord
├── state/                # token store, seen-task SQLite
└── sheet_template.py     # bootstraps tabs/formulas/conditional formatting
```

## Licence

Private — personal use only.
