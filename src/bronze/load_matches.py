"""
load_matches.py
---------------
Scans data/raw/{competition_code}/{season_code}/matches/ for every known
competition/season combination and loads new match records into the
`matches` silver table.

Idempotent: matches already present (matched on source_match_id) are skipped.
Only the matches/ subdirectory is processed — squads/ and any future
subdirectories are ignored.

Usage:
    python scripts/load_matches.py

Requirements:
    pip install psycopg2-binary python-dotenv
"""

import json
import logging
import os
import re
from datetime import datetime
from pathlib import Path

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

load_dotenv()

DSN = os.getenv("FOOTBALL_DB_DSN")
if not DSN:
    raise EnvironmentError("FOOTBALL_DB_DSN is not set in your .env file.")


# ---------------------------------------------------------------------------
# Paths  (relative to src/bronze/)
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[2]

DATA_ROOT = PROJECT_ROOT / "data" / "raw"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# File parsing
# ---------------------------------------------------------------------------

def parse_raw_file(path: Path) -> dict:
    """
    Strip the provider prefix (everything up to and including the first '(')
    and the trailing ')' if present, then parse the JSON payload.
    """
    raw = path.read_text(encoding="utf-8").strip()

    # The prefix format is: W3c<hash>({json...})
    # Find the first '(' and slice from there
    paren_open = raw.index("(")
    payload = raw[paren_open + 1:]

    # Remove trailing ')' if present
    if payload.endswith(")"):
        payload = payload[:-1]

    return json.loads(payload)


# ---------------------------------------------------------------------------
# Field extractors
# ---------------------------------------------------------------------------

def extract_match_record(data: dict) -> dict:
    """
    Extract all fields needed for the matches table from the parsed JSON.
    Returns a flat dict with Python-native types. FK resolution (team IDs,
    competition_season_id) is done later inside PostgreSQL.
    """
    mi = data["matchInfo"]
    ld = data["liveData"]
    md = ld["matchDetails"]

    # ---- Contestants -------------------------------------------------------
    contestants = {c["position"]: c for c in mi["contestant"]}
    home_source_id = contestants["home"]["id"]
    away_source_id = contestants["away"]["id"]

    # ---- Scores ------------------------------------------------------------
    scores = md.get("scores", {})
    ft = scores.get("ft", {})
    ht = scores.get("ht", {})

    # ---- Injury time per half ----------------------------------------------
    periods = {p["id"]: p for p in md.get("period", [])}
    ht_injury = periods.get(1, {}).get("announcedInjuryTime")
    ft_injury = periods.get(2, {}).get("announcedInjuryTime")

    # ---- Match date (local Spain time) -------------------------------------
    local_dt_str = f"{mi['localDate']} {mi['localTime']}"
    match_date = datetime.strptime(local_dt_str, "%Y-%m-%d %H:%M:%S")

    # ---- Venue -------------------------------------------------------------
    venue = mi.get("venue", {})

    # ---- Last updated ------------------------------------------------------
    last_updated_raw = mi.get("lastUpdated")
    last_updated = (
        datetime.fromisoformat(last_updated_raw.replace("Z", "+00:00"))
        if last_updated_raw
        else None
    )

    return {
        "source_match_id":      mi["id"],
        "source_stage_id":      mi["stage"]["id"],       # used to resolve competition_season_id
        "home_source_team_id":  home_source_id,           # used to resolve home_team_id
        "away_source_team_id":  away_source_id,           # used to resolve away_team_id
        "description":          mi.get("description"),
        "matchday":             int(mi["week"]) if mi.get("week") else None,
        "match_date":           match_date,
        "winner":               md.get("winner"),         # 'home' / 'away' / 'draw'
        "home_score":           ft.get("home"),
        "away_score":           ft.get("away"),
        "home_score_ht":        ht.get("home"),
        "away_score_ht":        ht.get("away"),
        "match_length_min":     md.get("matchLengthMin"),
        "match_length_sec":     md.get("matchLengthSec"),
        "ht_injury_time_sec":   ht_injury,
        "ft_injury_time_sec":   ft_injury,
        "var_used":             mi.get("var") == "1",
        "venue":                venue.get("longName"),
        "source_venue_id":      venue.get("id"),
        "venue_latitude":       float(venue["latitude"]) if venue.get("latitude") else None,
        "venue_longitude":      float(venue["longitude"]) if venue.get("longitude") else None,
        "neutral_venue":        venue.get("neutral", "").lower() == "yes",
        "coverage_level":       mi.get("coverageLevel"),
        "last_updated_at":      last_updated,
    }


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

UPSERT_SQL = """
INSERT INTO silver.matches (
    source_match_id,
    competition_season_id,
    home_team_id,
    away_team_id,
    description,
    matchday,
    match_date,
    status,
    winner,
    home_score,
    away_score,
    home_score_ht,
    away_score_ht,
    match_length_min,
    match_length_sec,
    ht_injury_time_sec,
    ft_injury_time_sec,
    var_used,
    venue,
    source_venue_id,
    venue_latitude,
    venue_longitude,
    neutral_venue,
    coverage_level,
    last_updated_at
)
SELECT
    %(source_match_id)s,
    cs.competition_season_id,
    ht.team_id,
    at.team_id,
    %(description)s,
    %(matchday)s,
    %(match_date)s,
    'completed',
    %(winner)s,
    %(home_score)s,
    %(away_score)s,
    %(home_score_ht)s,
    %(away_score_ht)s,
    %(match_length_min)s,
    %(match_length_sec)s,
    %(ht_injury_time_sec)s,
    %(ft_injury_time_sec)s,
    %(var_used)s,
    %(venue)s,
    %(source_venue_id)s,
    %(venue_latitude)s,
    %(venue_longitude)s,
    %(neutral_venue)s,
    %(coverage_level)s,
    %(last_updated_at)s
FROM
    silver.competition_seasons  cs,
    silver.teams                ht,
    silver.teams                at
WHERE
    cs.source_stage_id      = %(source_stage_id)s
    AND ht.source_team_id   = %(home_source_team_id)s
    AND at.source_team_id   = %(away_source_team_id)s
ON CONFLICT (source_match_id) DO NOTHING;
"""


def load_matches(conn, records: list[dict]) -> tuple[int, int]:
    """
    Insert records that don't yet exist. Returns (inserted, skipped) counts.
    """
    inserted = 0
    skipped = 0

    with conn.cursor() as cur:
        for rec in records:
            cur.execute(UPSERT_SQL, rec)
            if cur.rowcount == 1:
                inserted += 1
            else:
                skipped += 1

    conn.commit()
    return inserted, skipped


# ---------------------------------------------------------------------------
# Directory scanner
# ---------------------------------------------------------------------------

def discover_match_files(data_root: Path) -> list[Path]:
    """
    Walk data/raw/{competition_code}/{season_code}/matches/
    and return all files found. Ignores every subdirectory that is
    not named 'matches' (e.g. squads/).
    """
    files = []

    if not data_root.exists():
        log.warning("DATA_ROOT does not exist: %s", data_root.resolve())
        return files

    for competition_dir in sorted(data_root.iterdir()):
        if not competition_dir.is_dir():
            continue
        for season_dir in sorted(competition_dir.iterdir()):
            if not season_dir.is_dir():
                continue
            matches_dir = season_dir / "matches"
            if not matches_dir.is_dir():
                log.debug("No matches/ dir found in %s — skipping", season_dir)
                continue
            batch = sorted(matches_dir.iterdir())
            log.info(
                "Found %d file(s) in %s/%s/matches/",
                len(batch),
                competition_dir.name,
                season_dir.name,
            )
            files.extend(batch)

    return files


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    log.info("Starting match ingestion")
    log.info("Scanning: %s", DATA_ROOT.resolve())

    match_files = discover_match_files(DATA_ROOT)
    if not match_files:
        log.warning("No match files found. Nothing to do.")
        return

    log.info("Total files to process: %d", len(match_files))

    records = []
    parse_errors = []

    for path in match_files:
        try:
            data = parse_raw_file(path)
            records.append(extract_match_record(data))
        except Exception as exc:
            log.error("Failed to parse %s — %s", path.name, exc)
            parse_errors.append(path)

    log.info("Parsed: %d OK, %d errors", len(records), len(parse_errors))

    if not records:
        log.warning("No valid records to insert.")
        return

    with psycopg2.connect(DSN) as conn:
        inserted, skipped = load_matches(conn, records)

    log.info("Done — inserted: %d, skipped (already loaded): %d", inserted, skipped)

    if parse_errors:
        log.warning("Files that failed to parse (%d):", len(parse_errors))
        for p in parse_errors:
            log.warning("  %s", p)


if __name__ == "__main__":
    main()