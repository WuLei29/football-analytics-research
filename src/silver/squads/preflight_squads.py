"""
preflight_squads.py
───────────────────
Pre-flight check before running the squad loader for 2024-2025.

Covers the three requirements:
  1. List squad snapshot files found for BOTH competition-season folders
  2. Confirm 2024-2025 snapshots are NOT yet in squad_snapshot_log → will be loaded
     Confirm 2025-2026 snapshots ARE     in squad_snapshot_log → will be skipped
  3. Verify competition_seasons rows exist for 2024-2025 so FK resolution won't fail

Run from the project root:
    python preflight_squads.py

Requirements:
    pip install psycopg2-binary python-dotenv
"""

import os
import sys
from collections import defaultdict
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

# ── Config ─────────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parents[3]
load_dotenv(PROJECT_ROOT / ".env")

DSN      = os.getenv("FOOTBALL_DB_DSN")
RAW_ROOT = PROJECT_ROOT / "data" / "raw"

# ── Helpers ────────────────────────────────────────────────────────────────────

def section(title: str):
    print(f"\n{'─' * 60}")
    print(f"  {title}")
    print(f"{'─' * 60}")


def ok(msg):   print(f"  ✓  {msg}")
def warn(msg): print(f"  ⚠  {msg}")
def err(msg):  print(f"  ✗  {msg}")


# ── Step 1: Scan filesystem ────────────────────────────────────────────────────

def scan_squad_files(raw_root: Path) -> dict[str, list[Path]]:
    """
    Returns {season_code: [list of .json paths]} for every season folder found.
    Mirrors the glob pattern used by scanner.py: */*/squads/*/*.json
    """
    all_files = sorted(raw_root.glob("*/*/squads/*/*.json"))
    by_season: dict[str, list[Path]] = defaultdict(list)

    for path in all_files:
        parts = path.parts
        # Expected: .../{competition_code}/{season}/squads/{snapshot_date}/{file}.json
        try:
            season = parts[-4]   # e.g. "2024-2025"
        except IndexError:
            warn(f"Unexpected path depth: {path}")
            continue
        by_season[season].append(path)

    return dict(by_season)


def report_filesystem(by_season: dict[str, list[Path]]):
    section("1. Squad files on disk")

    if not by_season:
        err(f"No squad files found under {RAW_ROOT}")
        return

    for season in sorted(by_season):
        files = by_season[season]
        # Group by snapshot date
        snapshots: dict[str, set[str]] = defaultdict(set)
        for path in files:
            snapshot_date = path.parts[-2]
            team_id       = path.stem
            snapshots[snapshot_date].add(team_id)

        print(f"\n  Season: {season}  ({len(files)} files, {len(snapshots)} snapshot(s))")
        for snap_date in sorted(snapshots):
            teams = sorted(snapshots[snap_date])
            print(f"    [{snap_date}]  {len(teams)} team(s)")
            for t in teams:
                print(f"      • {t}")


# ── Step 2: Check squad_snapshot_log ──────────────────────────────────────────

def report_snapshot_log(conn, by_season: dict[str, list[Path]]):
    section("2. squad_snapshot_log — idempotency state")

    with conn.cursor() as cur:
        cur.execute("""
            SELECT source_team_id, source_season_id, snapshot_date, processed_at
            FROM   silver.squad_snapshot_log
            ORDER  BY snapshot_date, source_team_id
        """)
        log_rows = cur.fetchall()

    if not log_rows:
        warn("squad_snapshot_log is EMPTY — no squads have been processed yet.")
        warn("If 2025-2026 squads were loaded, this is a problem (see note below).")
    else:
        print(f"\n  {len(log_rows)} row(s) in squad_snapshot_log:\n")
        print(f"  {'source_team_id':<30}  {'source_season_id':<20}  {'snapshot_date':<12}  processed_at")
        print(f"  {'─'*30}  {'─'*20}  {'─'*12}  {'─'*24}")
        for row in log_rows:
            print(f"  {row[0]:<30}  {row[1]:<20}  {str(row[2]):<12}  {row[3]}")

    # Cross-reference: for each file on disk, check if it's already logged.
    # We need the tournamentCalendarId from each file to match against log.
    # We use source_team_id + snapshot_date (which is enough to predict skip/load).
    logged_keys: set[tuple] = {
        (row[0], str(row[2]))   # (source_team_id, snapshot_date)
        for row in log_rows
    }

    print()
    for season in sorted(by_season):
        files = by_season[season]
        will_load = 0
        will_skip = 0
        for path in files:
            team_id       = path.stem
            snapshot_date = path.parts[-2]
            key = (team_id, snapshot_date)
            if key in logged_keys:
                will_skip += 1
            else:
                will_load += 1

        status = "already loaded" if will_load == 0 else "PENDING"
        symbol = "✓" if will_load == 0 else "→"
        print(f"  {symbol}  Season {season}: {will_skip} file(s) will be skipped, {will_load} file(s) will be LOADED  [{status}]")


# ── Step 3: Verify competition_seasons FK rows ─────────────────────────────────

def report_competition_seasons(conn, by_season: dict[str, list[Path]]):
    section("3. competition_seasons — FK pre-flight")

    with conn.cursor() as cur:
        cur.execute("""
            SELECT cs.competition_season_id,
                   cs.source_season_id,
                   cs.source_stage_id,
                   c.name           AS competition_name,
                   s.label          AS season_label,
                   cs.status
            FROM   silver.competition_seasons cs
            JOIN   silver.competitions        c  ON c.competition_id  = cs.competition_id
            JOIN   silver.seasons             s  ON s.season_id       = cs.season_id
            ORDER  BY s.label, c.name
        """)
        rows = cur.fetchall()

    if not rows:
        err("No rows found in silver.competition_seasons — Phase 0 is incomplete!")
        return

    print(f"\n  {'ID':<4}  {'source_season_id':<22}  {'competition':<25}  {'season':<12}  status")
    print(f"  {'─'*4}  {'─'*22}  {'─'*25}  {'─'*12}  {'─'*10}")
    for r in rows:
        print(f"  {r[0]:<4}  {r[1]:<22}  {r[2]:<25}  {r[3]:<12}  {r[5]}")

    # Now check: for each squad file, extract tournamentCalendarId and verify it resolves
    print()
    missing_ids: set[str] = set()
    found_source_season_ids = {r[1] for r in rows}

    import json, re

    # Sample one file per season to read the tournamentCalendarId
    season_to_source_id: dict[str, str] = {}
    for season, files in by_season.items():
        if not files:
            continue
        sample_file = files[0]
        try:
            raw = sample_file.read_text(encoding="utf-8")
            # Try plain JSON first (squad files should be plain JSON after bronze parsing)
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                # Some files may still have a JSONP wrapper
                m = re.search(r'\((\{.*\})\s*\)?\s*;?\s*$', raw, re.DOTALL)
                if m:
                    data = json.loads(m.group(1))
                else:
                    warn(f"Could not parse sample file for season {season}: {sample_file.name}")
                    continue

            squad_list = data.get("squad", [])
            if squad_list:
                tc_id = squad_list[0].get("tournamentCalendarId")
                if tc_id:
                    season_to_source_id[season] = tc_id
        except Exception as e:
            warn(f"Could not read sample file for season {season}: {e}")

    for season, source_id in season_to_source_id.items():
        if source_id in found_source_season_ids:
            ok(f"Season {season}: tournamentCalendarId '{source_id}' → resolves in competition_seasons ✓")
        else:
            err(f"Season {season}: tournamentCalendarId '{source_id}' NOT found in competition_seasons!")
            err(f"  → Insert this source_season_id into silver.competition_seasons before loading squads.")
            missing_ids.add(source_id)

    for season in by_season:
        if season not in season_to_source_id:
            warn(f"Season {season}: could not read tournamentCalendarId from sample file — check manually")

    return missing_ids


# ── Step 4: team_competition_seasons check ─────────────────────────────────────

def report_team_enrollment(conn, by_season: dict[str, list[Path]]):
    section("4. team_competition_seasons — team enrollment check")

    with conn.cursor() as cur:
        cur.execute("""
            SELECT t.source_team_id,
                   t.name,
                   s.label AS season_label,
                   c.name  AS competition_name
            FROM   silver.team_competition_seasons tcs
            JOIN   silver.teams             t  ON t.team_id             = tcs.team_id
            JOIN   silver.competition_seasons cs ON cs.competition_season_id = tcs.competition_season_id
            JOIN   silver.seasons            s  ON s.season_id           = cs.season_id
            JOIN   silver.competitions       c  ON c.competition_id      = cs.competition_id
            ORDER  BY s.label, t.name
        """)
        rows = cur.fetchall()

    by_season_db: dict[str, list] = defaultdict(list)
    for r in rows:
        by_season_db[r[2]].append(r)

    for season, season_rows in sorted(by_season_db.items()):
        print(f"\n  Season {season}:  {len(season_rows)} team(s) enrolled")
        for r in season_rows:
            print(f"    • {r[0]:<30}  {r[1]}")

    if not rows:
        warn("No rows in team_competition_seasons. Run load_teams.py for the new season first.")


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("  Football Analytics — Squad Loader Pre-Flight Check")
    print("=" * 60)

    # Filesystem scan (no DB needed)
    by_season = scan_squad_files(RAW_ROOT)
    report_filesystem(by_season)

    if not DSN:
        err("FOOTBALL_DB_DSN not set — skipping database checks.")
        err("Create a .env file at the project root with:")
        err("  FOOTBALL_DB_DSN=postgresql://user:password@host:5432/dbname")
        sys.exit(1)

    try:
        conn = psycopg2.connect(DSN)
    except Exception as e:
        err(f"Could not connect to database: {e}")
        sys.exit(1)

    try:
        report_snapshot_log(conn, by_season)
        missing = report_competition_seasons(conn, by_season)
        report_team_enrollment(conn, by_season)
    finally:
        conn.close()

    print(f"\n{'═' * 60}")
    print("  Summary")
    print(f"{'═' * 60}")

    if not by_season:
        err("No squad files found. Check RAW_ROOT path.")
        sys.exit(1)

    seasons_with_files = sorted(by_season)
    for s in seasons_with_files:
        print(f"  • {s}: {len(by_season[s])} file(s) on disk")

    print()
    if missing:
        err("Fix competition_seasons entries above before running the loader.")
        print(f"\n  Run command (after fixing):")
    else:
        ok("All pre-flight checks passed. Safe to run:")

    print()
    print("    python -m src.silver.squads --raw-root data/raw")
    print()


if __name__ == "__main__":
    main()