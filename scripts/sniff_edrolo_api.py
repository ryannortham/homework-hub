"""One-off: sniff the real Edrolo API endpoints by replaying Tahlia's session.

Loads ``config/tokens/tahlia-edrolo.json`` into a Chromium context, navigates
the SPA, and prints every JSON-ish XHR/fetch URL it sees plus a short
preview of the response body so we can pick the tasks endpoint.

Run from repo root::

    uv run python scripts/sniff_edrolo_api.py

Headed by default so you can drive the SPA into the right view (studyplanner /
upcoming tasks, etc.) for a minute or two before closing the window.
"""

from __future__ import annotations

import json
from pathlib import Path

from playwright.sync_api import sync_playwright

TOKEN_PATH = Path("config/tokens/tahlia-edrolo.json")
START_URL = "https://app.edrolo.com/"
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)


def main() -> None:
    state = json.loads(TOKEN_PATH.read_text())
    seen: set[str] = set()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(user_agent=USER_AGENT, storage_state=state)
        page = context.new_page()

        def on_response(resp):  # noqa: ANN001
            url = resp.url
            ct = (resp.headers.get("content-type") or "").lower()
            if "json" not in ct:
                return
            if url in seen:
                return
            seen.add(url)
            try:
                body = resp.text()
            except Exception as exc:  # noqa: BLE001
                body = f"<could not read body: {exc}>"
            preview = body[:200].replace("\n", " ")
            print(f"[{resp.status}] {url}\n   ↳ {preview}\n")

        page.on("response", on_response)

        page.goto(START_URL)
        print("Navigate around the SPA — studyplanner, tasks, upcoming, etc.")
        print("Close the browser window when done. JSON URLs will print here.\n")

        # Block until the user closes the window manually.
        try:
            page.wait_for_event("close", timeout=0)
        except Exception:
            pass

        browser.close()


if __name__ == "__main__":
    main()
