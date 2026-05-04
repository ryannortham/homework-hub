"""Headed Playwright sniffer for Education Perfect.

Run this once on the Mac to capture James's EP token and storage state.
The output file is then copied to the server for headless replay.

Usage:
    uv run python scripts/sniff_eduperfect_api.py --child james

What it does:
1. Opens a headed Chromium at app.educationperfect.com/app/login.
2. Waits for the user to complete Google SSO (up to 5 minutes).
3. Intercepts the first authenticated request to graphql-gateway.educationperfect.com
   and extracts the Bearer JWT.
4. Saves storage_state.json + access_token + expires_at to
   config/tokens/<child>-eduperfect.json.

Once the token is captured, you can copy it to the server:
    scp config/tokens/<child>-eduperfect.json root@192.168.1.100:/mnt/tank/Apps/HomeworkHub/Config/tokens/

The per-sync headless refresh replays storage_state.json automatically when
the token expires — typically driven by the school Google session lifetime
(long-lived for Mordialloc school accounts, same as Edrolo).
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Ensure the project src is on the path when run directly.
_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT / "src"))


def main() -> None:
    parser = argparse.ArgumentParser(description="Capture EP token via headed Playwright")
    parser.add_argument("--child", required=True, help="Child name (e.g. james)")
    parser.add_argument(
        "--out",
        default=None,
        help="Output path (default: config/tokens/<child>-eduperfect.json)",
    )
    args = parser.parse_args()

    out_path = Path(args.out) if args.out else _ROOT / "config" / "tokens" / f"{args.child}-eduperfect.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    from homework_hub.sources.eduperfect import run_headed_login

    print(f"Opening headed Chromium for {args.child} (Education Perfect)…")
    print("Complete the Google sign-in in the browser.")
    print("The browser will close automatically once the EP dashboard loads and the token is captured.")
    print()

    try:
        run_headed_login(out_path)
    except RuntimeError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)

    print(f"\nToken saved → {out_path}")
    print(f"\nCopy to server:")
    print(f"  scp {out_path} root@192.168.1.100:/mnt/tank/Apps/HomeworkHub/Config/tokens/")


if __name__ == "__main__":
    main()
