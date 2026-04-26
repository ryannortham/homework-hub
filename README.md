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
```

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
