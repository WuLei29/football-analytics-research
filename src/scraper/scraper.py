"""
Playwright-based scraper for Opta match event files.

Navigates scoresway.com, discovers matchdays and matches, intercepts
api.performfeeds.com responses, and saves raw JSONP event files to disk.
"""

import asyncio
import json
import random
import re
from pathlib import Path

from playwright.async_api import async_playwright, Page, Response, BrowserContext

PROJECT_ROOT = Path(__file__).resolve().parents[2]

# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

async def run_scraper(*, headless: bool = False) -> None:
    print("\n╔══════════════════════════════════════╗")
    print("║   Football Event Scraper             ║")
    print("╚══════════════════════════════════════╝\n")

    results_url = input("Competition results URL: ").strip()
    league_code = input("League code (e.g. PRD): ").strip()
    season_code = input("Season code (e.g. 2025-2026): ").strip()

    output_dir = PROJECT_ROOT / "data" / "raw" / league_code / season_code / "matches"
    output_dir.mkdir(parents=True, exist_ok=True)

    existing_files = {f.name for f in output_dir.iterdir() if f.is_file()}
    print(f"\nOutput directory : {output_dir}")
    print(f"Existing files   : {len(existing_files)}")

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=headless,
            channel="chrome",
        )
        context = await browser.new_context(
            viewport={"width": 1280, "height": 900},
        )
        page = await context.new_page()

        try:
            await _interactive_flow(page, context, results_url, output_dir, existing_files)
        finally:
            await browser.close()


# ---------------------------------------------------------------------------
# Interactive flow
# ---------------------------------------------------------------------------

async def _interactive_flow(
    page: Page,
    context: BrowserContext,
    results_url: str,
    output_dir: Path,
    existing_files: set[str],
) -> None:
    print(f"\nNavigating to results page...")
    await page.goto(results_url, wait_until="networkidle", timeout=30_000)
    await page.wait_for_timeout(2000)

    # --- Matchday discovery ---
    matchdays = await _discover_matchdays(page)

    if matchdays:
        print(f"\nFound {len(matchdays)} matchdays:")
        for i, md in enumerate(matchdays, 1):
            print(f"  {i:>3}. {md['label']}")

        selection = input(
            "\nWhich matchdays to scrape? (e.g. '1-5', '3,7,12', 'all'): "
        ).strip()
        selected_indices = _parse_selection(selection, len(matchdays))

        if not selected_indices:
            print("No matchdays selected. Exiting.")
            return

        all_matches = await _collect_matches_from_matchdays(
            page, matchdays, selected_indices, existing_files
        )
    else:
        print(
            "\nCould not auto-detect matchday selector."
            "\nThe browser is open — navigate to the matchday you want,"
            " then come back here and press Enter."
        )
        input("\n[Press Enter when the page shows the matches you want] ")
        all_matches = await _scrape_match_links(page, existing_files)

    if not all_matches:
        print("\nNo new matches to download. Done.")
        return

    # --- Confirmation ---
    print(f"\n{'─' * 50}")
    print(f"  {len(all_matches)} new match(es) to download:")
    for m in all_matches:
        print(f"    • {m['label']}")
    print(f"{'─' * 50}")

    confirm = input("\nProceed with download? (y/n): ").strip().lower()
    if confirm != "y":
        print("Cancelled.")
        return

    # --- Download loop ---
    downloaded, failed = 0, 0
    for i, match in enumerate(all_matches, 1):
        print(f"\n[{i}/{len(all_matches)}] {match['label']}")
        try:
            saved_path = await _download_match(page, match, output_dir)
            if saved_path:
                print(f"  ✓ Saved: {saved_path.name}")
                downloaded += 1
            else:
                print(f"  ✗ No event data intercepted")
                failed += 1
        except Exception as exc:
            print(f"  ✗ Error: {exc}")
            failed += 1

        if i < len(all_matches):
            delay = round(random.uniform(3.0, 6.0), 1)
            print(f"  … waiting {delay}s")
            await asyncio.sleep(delay)

    print(f"\n{'═' * 50}")
    print(f"  Done — {downloaded} downloaded, {failed} failed")
    print(f"{'═' * 50}")


# ---------------------------------------------------------------------------
# Matchday discovery  (opta-widget lives in shadow DOM)
# ---------------------------------------------------------------------------

_JS_FIND_MATCHDAYS = """
() => {
    // The opta-widget renders a shadow DOM with its own <select> for matchdays
    const widget = document.querySelector('opta-widget[widget="fixtures"]');
    if (!widget) return null;

    const root = widget.shadowRoot || widget;

    // Find all <select> elements inside the widget
    const selects = root.querySelectorAll('select');
    for (const sel of selects) {
        const opts = Array.from(sel.options);
        if (opts.length < 2) continue;

        // Skip season selectors (labels like "2025/2026")
        const isSeasonSelect = opts.every(o => /^\\d{4}\\/\\d{4}$/.test(o.text.trim()));
        if (isSeasonSelect) continue;

        return opts.map(o => ({ label: o.text.trim(), value: o.value }));
    }
    return null;
}
"""

_JS_SELECT_MATCHDAY = """
(value) => {
    const widget = document.querySelector('opta-widget[widget="fixtures"]');
    if (!widget) return false;
    const root = widget.shadowRoot || widget;

    const selects = root.querySelectorAll('select');
    for (const sel of selects) {
        const opts = Array.from(sel.options);
        if (opts.every(o => /^\\d{4}\\/\\d{4}$/.test(o.text.trim()))) continue;

        sel.value = value;
        sel.dispatchEvent(new Event('change', { bubbles: true }));
        return true;
    }
    return false;
}
"""

_JS_FIND_MATCH_LINKS = """
() => {
    function harvest(root) {
        const links = root.querySelectorAll('a[href*="/match/"]');
        const results = [];
        for (const a of links) {
            const href = a.getAttribute('href') || '';
            const m = href.match(/\\/match\\/view\\/([^\\/]+)/);
            if (!m) continue;
            results.push({
                match_id: m[1],
                href: a.href,
                label: a.textContent.replace(/\\s+/g, ' ').trim(),
            });
        }
        return results;
    }

    // Try shadow DOM first
    const widget = document.querySelector('opta-widget[widget="fixtures"]');
    if (widget && widget.shadowRoot) {
        const links = harvest(widget.shadowRoot);
        if (links.length) return links;
    }

    // Fallback: light DOM
    return harvest(document);
}
"""


async def _discover_matchdays(page: Page) -> list[dict]:
    """Find the matchday dropdown inside the opta-widget shadow DOM."""
    # Wait for the widget to render
    for _ in range(10):
        result = await page.evaluate(_JS_FIND_MATCHDAYS)
        if result:
            return result
        await page.wait_for_timeout(1000)
    return []


async def _select_matchday(page: Page, matchday: dict) -> None:
    """Select a matchday in the opta-widget dropdown."""
    await page.evaluate(_JS_SELECT_MATCHDAY, matchday["value"])
    await page.wait_for_timeout(2000)


# ---------------------------------------------------------------------------
# Match discovery
# ---------------------------------------------------------------------------

async def _collect_matches_from_matchdays(
    page: Page,
    matchdays: list[dict],
    selected_indices: list[int],
    existing_files: set[str],
) -> list[dict]:
    """Iterate over selected matchdays and collect all new match links."""
    all_matches = []

    for idx in selected_indices:
        md = matchdays[idx]
        print(f"\nMatchday: {md['label']}")
        await _select_matchday(page, md)
        matches = await _scrape_match_links(page, existing_files)
        all_matches.extend(matches)

    return all_matches


async def _scrape_match_links(page: Page, existing_files: set[str]) -> list[dict]:
    """Extract match links (piercing shadow DOM), filtering already-downloaded."""
    raw_links = await page.evaluate(_JS_FIND_MATCH_LINKS)

    matches = []
    seen_ids: set[str] = set()

    for entry in raw_links:
        match_id = entry["match_id"]
        if match_id in seen_ids:
            continue
        seen_ids.add(match_id)

        label = entry["label"] or match_id
        href = entry["href"]

        if match_id in existing_files:
            print(f"  SKIP (exists): {label}")
            continue

        if not href.startswith("http"):
            href = f"https://www.scoresway.com{href}"

        matches.append({
            "match_id": match_id,
            "label": label,
            "url": href,
        })

    new_count = len(matches)
    skip_count = len(seen_ids) - new_count
    if skip_count > 0:
        print(f"  ({skip_count} already downloaded, {new_count} new)")

    return matches


# ---------------------------------------------------------------------------
# Match event download
# ---------------------------------------------------------------------------

async def _download_match(page: Page, match: dict, output_dir: Path) -> Path | None:
    """Navigate to match player-stats page, intercept the event data, save it."""

    # Build player-stats URL: strip any trailing sub-page, then append
    url = match["url"].rstrip("/")
    for suffix in ("/match-summary", "/player-stats", "/stats", "/lineups"):
        if url.endswith(suffix):
            url = url[: -len(suffix)]
            break
    player_stats_url = url + "/player-stats"
    print(f"  URL: {player_stats_url}")

    captured: dict = {}
    api_urls_seen: list[str] = []

    async def on_response(response: Response) -> None:
        url = response.url
        # Log any API-like requests for debugging
        if "api." in url or "performfeeds" in url or "opta" in url.lower():
            api_urls_seen.append(url[:120])
        if "performfeeds" not in url:
            return
        if "matchevent" not in url:
            return
        try:
            body = await response.body()
            file_id = _extract_match_id_from_response(body, url, match["match_id"])
            captured["body"] = body
            captured["file_id"] = file_id
        except Exception:
            pass

    page.on("response", on_response)

    try:
        await page.goto(player_stats_url, wait_until="domcontentloaded", timeout=30_000)
        for _ in range(15):
            if captured:
                break
            await page.wait_for_timeout(1000)
    finally:
        page.remove_listener("response", on_response)

    if not captured.get("body") and api_urls_seen:
        print(f"  DEBUG — API requests seen:")
        for u in api_urls_seen:
            print(f"    {u}")

    if not captured.get("body"):
        return None

    file_path = output_dir / captured["file_id"]
    file_path.write_bytes(captured["body"])
    return file_path


def _extract_match_id_from_response(body: bytes, url: str, fallback_id: str) -> str:
    """Extract the match ID to use as filename.

    Priority:
      1. matchInfo.id from the JSONP body
      2. match ID from the API URL path
      3. fallback to the scoresway match ID
    """
    try:
        text = body.decode("utf-8")
        json_start = text.index("{")
        json_text = text[json_start:].rstrip(")")
        data = json.loads(json_text)
        return data["matchInfo"]["id"]
    except Exception:
        pass

    id_from_url = re.search(r"matchevent/([^?/]+)", url)
    if id_from_url:
        return id_from_url.group(1)

    return fallback_id


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_selection(text: str, max_val: int) -> list[int]:
    """Parse '1-5', '3,7,12', 'all' into 0-based indices."""
    text = text.strip().lower()
    if text == "all":
        return list(range(max_val))

    indices = set()
    for part in text.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            bounds = part.split("-", 1)
            try:
                start, end = int(bounds[0].strip()), int(bounds[1].strip())
                for i in range(start, end + 1):
                    if 1 <= i <= max_val:
                        indices.add(i - 1)
            except ValueError:
                pass
        else:
            try:
                i = int(part)
                if 1 <= i <= max_val:
                    indices.add(i - 1)
            except ValueError:
                pass

    return sorted(indices)
