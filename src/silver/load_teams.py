"""
load_teams_silver.py
---------------------
1. Reads all bronze team JSONs from  ../../data/teams/
2. Generates silver seed CSVs in     ../../data/seeds/silver/
3. Loads both CSVs into PostgreSQL   (teams + team_competition_seasons)

Run from: src/bronze/
"""

import json
import csv
import os
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Paths  (relative to src/bronze/)
# ---------------------------------------------------------------------------
BASE_DIR        = Path(__file__).resolve().parent.parent.parent   # project root
TEAMS_JSON_DIR  = BASE_DIR / "data" / "teams"
SEEDS_DIR       = BASE_DIR / "data" / "seeds" / "silver"

TEAMS_CSV                    = SEEDS_DIR / "teams.csv"
TEAM_COMPETITION_SEASONS_CSV = SEEDS_DIR / "team_competition_seasons.csv"

SEEDS_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# PostgreSQL connection — reads FOOTBALL_DB_DSN from .env
# ---------------------------------------------------------------------------
load_dotenv(BASE_DIR / ".env")

DSN = os.getenv("FOOTBALL_DB_DSN")
if not DSN:
    raise EnvironmentError("FOOTBALL_DB_DSN is not set in your .env file.")

# ---------------------------------------------------------------------------
# Column definitions — must match your PostgreSQL schema exactly
# ---------------------------------------------------------------------------
TEAMS_COLUMNS = [
    "source_team_id",
    "name",
    "short_name",
    "abbreviation",
    "stadium_name",
    "source_venue_id",
    "country",
    "city",
    "stadium_capacity",
    "founded_year",
]

TEAM_CS_COLUMNS = [
    "source_team_id",
    "source_competition_id",
    "source_season_id",
    "kit_home_color",
    "kit_away_color",
    "badge_url",
]


# ---------------------------------------------------------------------------
# Step 1 — Read all bronze JSONs and collect records
# ---------------------------------------------------------------------------
def load_bronze_records() -> list[dict]:
    records = []
    json_files = list(TEAMS_JSON_DIR.glob("*.json"))

    if not json_files:
        raise FileNotFoundError(f"No JSON files found in {TEAMS_JSON_DIR}")

    for json_file in json_files:
        data = json.loads(json_file.read_text(encoding="utf-8"))
        records.extend(data.get("teams", []))
        print(f"  Loaded {len(data.get('teams', []))} teams from {json_file.name}")

    print(f"\n  Total team records loaded: {len(records)}")
    return records


# ---------------------------------------------------------------------------
# Step 2 — Write silver seed CSVs
# ---------------------------------------------------------------------------
def write_teams_csv(records: list[dict]) -> list[dict]:
    seen = set()
    rows = []

    for r in records:
        tid = r["source_team_id"]
        if tid in seen:
            continue
        seen.add(tid)
        rows.append({col: r.get(col) for col in TEAMS_COLUMNS})

    with TEAMS_CSV.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=TEAMS_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)

    print(f"  teams.csv → {len(rows)} rows written to {TEAMS_CSV}")
    return rows


def write_team_competition_seasons_csv(records: list[dict]) -> list[dict]:
    seen = set()
    rows = []

    for r in records:
        key = (r["source_team_id"], r["source_competition_id"], r["source_season_id"])
        if key in seen:
            continue
        seen.add(key)
        rows.append({col: r.get(col) for col in TEAM_CS_COLUMNS})

    with TEAM_COMPETITION_SEASONS_CSV.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=TEAM_CS_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)

    print(f"  team_competition_seasons.csv → {len(rows)} rows written to {TEAM_COMPETITION_SEASONS_CSV}")
    return rows


# ---------------------------------------------------------------------------
# Step 3 — Load into PostgreSQL
# ---------------------------------------------------------------------------
def load_teams(cur, rows: list[dict]) -> None:
    sql = """
        INSERT INTO silver.teams (
            source_team_id, name, short_name, abbreviation,
            stadium_name, source_venue_id, country, city,
            stadium_capacity, founded_year
        )
        VALUES %s
        ON CONFLICT (source_team_id) DO NOTHING
    """
    values = [
        (
            r["source_team_id"], r["name"], r["short_name"], r["abbreviation"],
            r["stadium_name"], r["source_venue_id"], r["country"], r["city"],
            r["stadium_capacity"], r["founded_year"],
        )
        for r in rows
    ]
    execute_values(cur, sql, values)
    print(f"  Inserted/skipped {len(values)} rows into teams")


def load_team_competition_seasons(cur, rows: list[dict]) -> None:
    sql = """
        INSERT INTO silver.team_competition_seasons (
            team_id, competition_season_id,
            kit_home_color, kit_away_color, badge_url
        )
        SELECT
            t.team_id, cs.competition_season_id,
            v.kit_home_color, v.kit_away_color, v.badge_url
        FROM (VALUES %s) AS v(
            source_team_id, source_competition_id, source_season_id,
            kit_home_color, kit_away_color, badge_url
        )
        JOIN silver.teams t ON t.source_team_id = v.source_team_id
        JOIN silver.competition_seasons cs ON cs.source_season_id = v.source_season_id
        ON CONFLICT (team_id, competition_season_id) DO NOTHING
    """
    values = [
        (
            r["source_team_id"], r["source_competition_id"], r["source_season_id"],
            r["kit_home_color"], r["kit_away_color"], r["badge_url"],
        )
        for r in rows
    ]
    execute_values(cur, sql, values)
    print(f"  Inserted/skipped {len(values)} rows into team_competition_seasons")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    print("=" * 60)
    print("Step 1 — Loading bronze JSON records")
    print("=" * 60)
    records = load_bronze_records()

    print("\n" + "=" * 60)
    print("Step 2 — Writing silver seed CSVs")
    print("=" * 60)
    teams_rows   = write_teams_csv(records)
    team_cs_rows = write_team_competition_seasons_csv(records)

    print("\n" + "=" * 60)
    print("Step 3 — Loading into PostgreSQL")
    print("=" * 60)
    conn = psycopg2.connect(DSN)
    try:
        with conn:
            with conn.cursor() as cur:
                load_teams(cur, teams_rows)
                load_team_competition_seasons(cur, team_cs_rows)
        print("\n  ✓ Transaction committed successfully.")
    except Exception as e:
        print(f"\n  [ERROR] Transaction rolled back: {e}")
        raise
    finally:
        conn.close()

    print("\nDone.")


if __name__ == "__main__":
    main()