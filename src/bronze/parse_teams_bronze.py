"""
parse_teams_bronze.py
---------------------
Reads all squad files from the squads folder, extracts team (contestant) data,
and writes one bronze JSON per competition to the teams output folder.

Run from: C:\\Users\\Usuario\\OneDrive\\football-analytics-research\\src\\bronze
"""

import json
import re
import os
from pathlib import Path
from datetime import datetime

# ---------------------------------------------------------------------------
# Paths  (relative to the notebooks folder)
# ---------------------------------------------------------------------------
SQUADS_ROOT  = Path("../../data/squads")
TEAMS_OUTPUT = Path("../../data/teams")

TEAMS_OUTPUT.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# Helper: strip the JSONP wrapper that leads every file
# e.g.  W30be1904ad...(  { ... }  )
# ---------------------------------------------------------------------------
def extract_json(raw: str) -> dict:
    match = re.search(r'\((\{.*\})\s*\)\s*;?\s*$', raw, re.DOTALL)
    if not match:
        raise ValueError("Could not find JSON payload inside JSONP wrapper")
    return json.loads(match.group(1))


# ---------------------------------------------------------------------------
# Core extractor: pull the fields we care about from one squad record
# ---------------------------------------------------------------------------
def extract_team_record(squad_entry: dict) -> dict:
    return {
        # --- teams table ---
        "source_team_id":   squad_entry.get("contestantId"),
        "name":             squad_entry.get("contestantName"),
        "short_name":       squad_entry.get("contestantShortName"),
        "club_name":        squad_entry.get("contestantClubName"),   # extra, may differ
        "abbreviation":     squad_entry.get("contestantCode"),
        "stadium_name":     squad_entry.get("venueName"),
        "source_venue_id":  squad_entry.get("venueId"),             # useful for future enrichment
        # country not present in squad files — will be derived or filled manually
        "country":          None,
        # manual enrichment columns — intentionally blank
        "city":             None,
        "stadium_capacity": None,
        "founded_year":     None,

        # --- team_competition_seasons table ---
        "source_competition_id":   squad_entry.get("competitionId"),
        "competition_name":        squad_entry.get("competitionName"),
        "source_season_id":        squad_entry.get("tournamentCalendarId"),
        "season_start_date":       squad_entry.get("tournamentCalendarStartDate"),
        "season_end_date":         squad_entry.get("tournamentCalendarEndDate"),
        # manual enrichment columns for team_competition_seasons
        "kit_home_color":  None,
        "kit_away_color":  None,
        "badge_url":       None,
    }


# ---------------------------------------------------------------------------
# Main parse loop
# ---------------------------------------------------------------------------
def parse_competition_folder(competition_folder: Path) -> tuple[str, str, list[dict]]:
    """
    Parse all squad files inside one competition sub-folder.
    Returns (competition_id, list_of_team_records).
    """
    teams: dict[str, dict] = {}   # keyed by source_team_id to deduplicate

    squad_files = list(competition_folder.glob("*"))
    if not squad_files:
        print(f"  [WARN] No files found in {competition_folder}")
        return None, []

    competition_id = None

    for squad_file in squad_files:
        if not squad_file.is_file():
            continue
        try:
            raw = squad_file.read_text(encoding="utf-8")
            data = extract_json(raw)
        except Exception as e:
            print(f"  [ERROR] Could not parse {squad_file.name}: {e}")
            continue

        squad_list = data.get("squad", [])
        if not squad_list:
            print(f"  [WARN] Empty squad list in {squad_file.name}")
            continue

        # All entries in the same file share the same team — use the first entry
        entry = squad_list[0]
        record = extract_team_record(entry)

        source_team_id = record["source_team_id"]
        if source_team_id and source_team_id not in teams:
            teams[source_team_id] = record

        # Capture competition_id from the first valid record
        if competition_id is None:
            competition_id = record.get("source_competition_id")
            competition_name = record.get("competition_name")

    return competition_id, competition_name, list(teams.values())


def main():
    print(f"Scanning squads root: {SQUADS_ROOT.resolve()}\n")

    competition_folders = [f for f in SQUADS_ROOT.iterdir() if f.is_dir()]
    if not competition_folders:
        print("[ERROR] No competition sub-folders found. Check SQUADS_ROOT path.")
        return

    for comp_folder in competition_folders:
        print(f"Processing: {comp_folder.name}")
        competition_id, competition_name, team_records = parse_competition_folder(comp_folder)

        if not team_records:
            print(f"  [SKIP] No team records extracted.\n")
            continue

        if not competition_name:
            competition_name = "unknown"
            print(f"  [WARN] Could not resolve competition_id — using 'unknown' as filename")

        output = {
            "_meta": {
                "source_folder":  comp_folder.name,
                "competition_name": competition_name,
                "team_count":     len(team_records),
                "parsed_at":     datetime.now().isoformat(),
            },
            "teams": team_records,
        }

        output_path = TEAMS_OUTPUT / f"{competition_name}.json"
        output_path.write_text(
            json.dumps(output, indent=2, ensure_ascii=False),
            encoding="utf-8"
        )
        print(f"  ✓ {len(team_records)} teams → {output_path.name}\n")

    print("Done.")


if __name__ == "__main__":
    main()