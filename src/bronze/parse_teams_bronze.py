"""
parse_teams_bronze.py
---------------------
Walks data/raw/{competition_code}/{season_code}/squads/{snapshot_date}/
for every competition/season combination found on disk, extracts team
(contestant) data from the earliest snapshot, and writes one bronze JSON
per competition/season to data/teams/.

Output filename: {competition_code}_{season_code}.json
e.g. PRD_2024-2025.json, PRD_2025-2026.json, SD_2024-2025.json

Run from project root:
    python src/bronze/parse_teams_bronze.py
"""

import json
import re
from pathlib import Path
from datetime import datetime

# ---------------------------------------------------------------------------
# Paths (relative to project root)
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_ROOT     = PROJECT_ROOT / "data" / "raw"
TEAMS_OUTPUT = PROJECT_ROOT / "data" / "teams"



# ---------------------------------------------------------------------------
# Helper: strip the JSONP wrapper
# e.g.  W30be1904ad...( { ... } )
# ---------------------------------------------------------------------------
def extract_json(raw: str) -> dict:
    match = re.search(r'\((\{.*\})\s*\)\s*;?\s*$', raw, re.DOTALL)
    if not match:
        raise ValueError("Could not find JSON payload inside JSONP wrapper")
    return json.loads(match.group(1))


# ---------------------------------------------------------------------------
# Core extractor: pull the fields we care about from one squad entry
# ---------------------------------------------------------------------------
def extract_team_record(squad_entry: dict, competition_code: str, season_code: str) -> dict:
    return {
        # --- teams table ---
        "source_team_id":   squad_entry.get("contestantId"),
        "name":             squad_entry.get("contestantName"),
        "short_name":       squad_entry.get("contestantShortName"),
        "club_name":        squad_entry.get("contestantClubName"),
        "abbreviation":     squad_entry.get("contestantCode"),
        "stadium_name":     squad_entry.get("venueName"),
        "source_venue_id":  squad_entry.get("venueId"),
        # not in provider feed — filled manually
        "country":          None,
        "city":             None,
        "stadium_capacity": None,
        "founded_year":     None,

        # --- team_competition_seasons table ---
        "source_competition_id": squad_entry.get("competitionId"),
        "competition_name":      squad_entry.get("competitionName"),
        "source_season_id":      squad_entry.get("tournamentCalendarId"),
        "season_start_date":     squad_entry.get("tournamentCalendarStartDate"),
        "season_end_date":       squad_entry.get("tournamentCalendarEndDate"),
        # manual enrichment columns
        "kit_home_color": None,
        "kit_away_color": None,
        "badge_url":      None,

        # --- path-derived context (not written to DB, useful for auditing) ---
        "competition_code": competition_code,
        "season_code":      season_code,
    }


# ---------------------------------------------------------------------------
# Snapshot discovery: return the earliest snapshot folder for a given
# competition/season, since team identity doesn't change across snapshots
# ---------------------------------------------------------------------------
def get_earliest_snapshot(squads_dir: Path) -> Path | None:
    snapshot_folders = sorted(
        [f for f in squads_dir.iterdir() if f.is_dir()]
    )
    if not snapshot_folders:
        print(f"  [WARN] No snapshot folders found in {squads_dir}")
        return None
    earliest = snapshot_folders[0]
    print(f"  Using snapshot: {earliest.name} ({len(snapshot_folders)} available)")
    return earliest


# ---------------------------------------------------------------------------
# Parse all squad files inside one snapshot folder
# ---------------------------------------------------------------------------
def parse_snapshot(
    snapshot_dir: Path,
    competition_code: str,
    season_code: str,
) -> list[dict]:
    teams: dict[str, dict] = {}  # keyed by source_team_id to deduplicate

    squad_files = [f for f in snapshot_dir.iterdir() if f.is_file()]
    if not squad_files:
        print(f"  [WARN] No files in snapshot {snapshot_dir}")
        return []

    for squad_file in squad_files:
        try:
            with open(squad_file, "r", encoding="utf-8") as fh:
                data = json.load(fh)
        except Exception as e:
            print(f"  [ERROR] Could not parse {squad_file.name}: {e}")
            continue

        squad_list = data.get("squad", [])
        if not squad_list:
            print(f"  [WARN] Empty squad list in {squad_file.name}")
            continue

        # All person entries in a file share the same team — use the first entry
        entry = squad_list[0]
        record = extract_team_record(entry, competition_code, season_code)

        source_team_id = record["source_team_id"]
        if source_team_id and source_team_id not in teams:
            teams[source_team_id] = record

    return sorted(teams.values(), key=lambda t: t["short_name"])


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    print(f"Scanning raw data root: {RAW_ROOT.resolve()}\n")

    if not RAW_ROOT.exists():
        print(f"[ERROR] RAW_ROOT not found: {RAW_ROOT.resolve()}")
        return

    competition_dirs = [d for d in sorted(RAW_ROOT.iterdir()) if d.is_dir()]
    if not competition_dirs:
        print("[ERROR] No competition folders found under data/raw/")
        return

    total_written = 0

    for comp_dir in competition_dirs:
        competition_code = comp_dir.name  # e.g. "PRD"

        season_dirs = [d for d in sorted(comp_dir.iterdir()) if d.is_dir()]
        if not season_dirs:
            print(f"[SKIP] No season folders found under {comp_dir.name}/")
            continue

        for season_dir in season_dirs:
            season_code = season_dir.name  # e.g. "2024-2025"
            label = f"{competition_code}/{season_code}"
            print(f"Processing: {label}")

            squads_dir = season_dir / "squads"
            if not squads_dir.exists():
                print(f"  [SKIP] No squads/ folder in {label}")
                continue

            snapshot_dir = get_earliest_snapshot(squads_dir)
            if snapshot_dir is None:
                continue

            team_records = parse_snapshot(snapshot_dir, competition_code, season_code)
            if not team_records:
                print(f"  [SKIP] No team records extracted from {label}\n")
                continue

            output = {
                "_meta": {
                    "competition_code": competition_code,
                    "season_code":      season_code,
                    "snapshot_used":    snapshot_dir.name,
                    "team_count":       len(team_records),
                    "parsed_at":        datetime.now().isoformat(),
                },
                "teams": team_records,
            }

            output_filename = f"{competition_code}_{season_code}.json"
            output_path = TEAMS_OUTPUT / output_filename

            if output_path.exists():
                print(f"  [SKIP] {output_filename} already exists — skipping\n")
                continue

            output_path.write_text(
                json.dumps(output, indent=2, ensure_ascii=False),
                encoding="utf-8",
            )
            print(f"  ✓ {len(team_records)} teams → {output_filename}\n")
            total_written += 1

    print(f"Done. {total_written} file(s) written to {TEAMS_OUTPUT.resolve()}")


if __name__ == "__main__":
    main()