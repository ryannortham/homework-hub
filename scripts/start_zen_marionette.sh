#!/usr/bin/env bash
# Launch Zen Browser with Marionette enabled for EP token capture.
#
# Run this once before `homework-hub refresh-ep --child james`.
# Wait ~10 seconds after this script for the EP dashboard to fully load
# before running the refresh command.
#
# Usage:
#   bash ~/Code/homework-hub/scripts/start_zen_marionette.sh

set -euo pipefail

ZEN="/Applications/Zen.app/Contents/MacOS/zen"
PROFILE="$HOME/Library/Application Support/zen/Profiles/cvigrd5k.Default (release)"
PORT=2828

if ! [ -x "$ZEN" ]; then
    echo "Error: Zen Browser not found at $ZEN" >&2
    exit 1
fi

# Check if Marionette is already running
if nc -z localhost "$PORT" 2>/dev/null; then
    echo "Marionette already running on port $PORT — no need to relaunch."
    exit 0
fi

echo "Launching Zen Browser with Marionette on port $PORT…"
"$ZEN" \
    --marionette \
    --marionette-port "$PORT" \
    --remote-allow-system-access \
    --profile "$PROFILE" \
    "https://app.educationperfect.com/learning/dashboard" \
    > /tmp/zen-marionette.log 2>&1 &

echo "Waiting for Marionette to become available…"
for i in $(seq 1 20); do
    if nc -z localhost "$PORT" 2>/dev/null; then
        echo "Marionette ready on port $PORT."
        echo "Wait ~10 more seconds for the EP dashboard to fully load, then run:"
        echo "  uv run homework-hub refresh-ep --child james"
        exit 0
    fi
    sleep 1
done

echo "Error: Marionette did not become available within 20 seconds." >&2
echo "Check /tmp/zen-marionette.log for details." >&2
exit 1
