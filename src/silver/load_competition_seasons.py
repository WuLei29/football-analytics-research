"""
load_competition_seasons.py
---------------------------
Scans data/raw/{competition_code}/{season_code}/matches/ and seeds the two
competition-framework tables for any season not yet present:

    silver.seasons              (label, e.g. '2026/2027')
    silver.competition_seasons  (the central FK anchor)

Every downstream loader resolves competition_season_id via source_stage_id,
so this MUST run before load_matches.py for a new season. Without the anchor
row, load_matches.py inserts zero rows and reports them as "skipped" — no error.

Only one match file per season directory is read; the tournamentCalendar and
stage blocks are identical across every match of a season.

Derived from the raw files:
    source_season_id, source_stage_id, competition_id, season label,
    stage_name, start_date, end_date, stage_start_date, stage_end_date, status

NOT derived (nullable, editorial — set them by hand if you need them):
    num_teams, total_matchdays, promo_spots, relegation_spots

Idempotent: seasons conflict on `label`, competition_seasons on `source_stage_id`.

Usage:
    python src/silver/load_competition_seasons.py
    python src/silver/load_competition_seasons.py --dry-run

Requirements:
    pip install psycopg2-binary python-dotenv
"""

import argparse
import json
import logging
import os
from datetime import date, datetime
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

load_dotenv()

DSN = os.getenv("FOOTBALL_DB_DSN")
if not DSN:
    raise EnvironmentError("FOOTBALL_DB_DSN is not set in your .env file.")

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
    paren_open = raw.index("(")
    payload = raw[paren_open + 1:]
    if payload.endswith(")"):
        payload = payload[:-1]
    return json.loads(payload)


def _opta_date(value):
    """Opta dates carry a trailing 'Z' on a plain date: '2026-08-15Z'."""
    if not value:
        return None
    return datetime.strptime(value.rstrip("Z"), "%Y-%m-%d").date()


# ---------------------------------------------------------------------------
# Field extraction
# ---------------------------------------------------------------------------

def derive_status(start, end, today: date) -> str:
    """Map the season's date window onto the chk_cs_status enum."""
    if start and today < start:
        return "upcoming"
    if end and today > end:
        return "completed"
    return "active"


def extract_season_record(data: dict, today: date) -> dict:
    """
    Pull the competition-framework fields out of a single match file.
    competition_id and season_id are resolved later, inside PostgreSQL.
    """
    mi = data["matchInfo"]
    tcal = mi["tournamentCalendar"]
    stage = mi["stage"]

    start_date = _opta_date(tcal.get("startDate"))
    end_date = _opta_date(tcal.get("endDate"))

    if start_date is None:
        raise ValueError("tournamentCalendar %s has no startDate" % tcal["id"])

    return {
        "source_competition_id": mi["competition"]["id"],   # resolves competition_id
        "season_label":          tcal["name"],              # resolves season_id
        "source_season_id":      tcal["id"],
        "source_stage_id":       stage["id"],
        "stage_name":            stage.get("name"),
        "start_date":            start_date,
        "end_date":              end_date,
        "stage_start_date":      _opta_date(stage.get("startDate")),
        "stage_end_date":        _opta_date(stage.get("endDate")),
        "status":                derive_status(start_date, end_date, today),
    }


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

INSERT_SEASON_SQL = """
INSERT INTO silver.seasons (label)
VALUES (%(season_label)s)
ON CONFLICT (label) DO NOTHING;
"""

INSERT_COMPETITION_SEASON_SQL = """
INSERT INTO silver.competition_seasons (
    source_season_id,
    source_stage_id,
    competition_id,
    season_id,
    stage_name,
    start_date,
    end_date,
    stage_start_date,
    stage_end_date,
    status
)
SELECT
    %(source_season_id)s,
    %(source_stage_id)s,
    c.competition_id,
    s.season_id,
    %(stage_name)s,
    %(start_date)s,
    %(end_date)s,
    %(stage_start_date)s,
    %(stage_end_date)s,
    %(status)s
FROM
    silver.competitions c,
    silver.seasons      s
WHERE
    c.source_competition_id = %(source_competition_id)s
    AND s.label             = %(season_label)s
ON CONFLICT (source_stage_id) DO NOTHING;
"""


def seed(conn, records: list, dry_run: bool):
    """Insert missing framework rows. Returns (inserted, skipped) for stages."""
    inserted = 0
    skipped = 0

    with conn.cursor() as cur:
        for rec in records:
            cur.execute(INSERT_SEASON_SQL, rec)
            if cur.rowcount == 1:
                log.info("  + seasons: %s", rec["season_label"])

            cur.execute(INSERT_COMPETITION_SEASON_SQL, rec)
            if cur.rowcount == 1:
                log.info(
                    "  + competition_seasons: %s (%s) — stage %s",
                    rec["season_label"],
                    rec["status"],
                    rec["source_stage_id"],
                )
                inserted += 1
            else:
                # Either already present, or the competitions row is missing.
                cur.execute(
                    "SELECT 1 FROM silver.competition_seasons WHERE source_stage_id = %s",
                    (rec["source_stage_id"],),
                )
                if cur.fetchone():
                    log.info("  = already present: %s", rec["season_label"])
                else:
                    log.error(
                        "  ! FAILED %s — no silver.competitions row for "
                        "source_competition_id '%s'. Insert the competition first.",
                        rec["season_label"],
                        rec["source_competition_id"],
                    )
                skipped += 1

    if dry_run:
        conn.rollback()
        log.info("DRY RUN — rolled back.")
    else:
        conn.commit()

    return inserted, skipped


# ---------------------------------------------------------------------------
# Directory scanner
# ---------------------------------------------------------------------------

def discover_season_files(data_root: Path) -> list:
    """
    Walk data/raw/{competition_code}/{season_code}/matches/ and return the
    first match file of each season directory — one representative per season.
    """
    representatives = []

    if not data_root.exists():
        log.warning("DATA_ROOT does not exist: %s", data_root.resolve())
        return representatives

    for competition_dir in sorted(data_root.iterdir()):
        if not competition_dir.is_dir():
            continue
        for season_dir in sorted(competition_dir.iterdir()):
            if not season_dir.is_dir():
                continue
            matches_dir = season_dir / "matches"
            if not matches_dir.is_dir():
                continue
            files = sorted(f for f in matches_dir.iterdir() if f.is_file())
            if not files:
                log.warning("No match files in %s — skipping", matches_dir)
                continue
            log.info(
                "%s/%s -> reading %s",
                competition_dir.name,
                season_dir.name,
                files[0].name,
            )
            representatives.append(files[0])

    return representatives


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Seed silver.seasons and silver.competition_seasons from raw match files"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Parse and attempt the inserts, then roll back",
    )
    args = parser.parse_args()

    log.info("Seeding competition framework")
    log.info("Scanning: %s", DATA_ROOT.resolve())

    files = discover_season_files(DATA_ROOT)
    if not files:
        log.warning("No season directories found. Nothing to do.")
        return

    today = date.today()
    records = []
    for path in files:
        try:
            records.append(extract_season_record(parse_raw_file(path), today))
        except Exception as exc:
            log.error("Failed to parse %s: %s", path.name, exc)

    if not records:
        log.error("No season records extracted. Aborting.")
        return

    conn = psycopg2.connect(DSN)
    try:
        inserted, skipped = seed(conn, records, args.dry_run)
    finally:
        conn.close()

    log.info(
        "Done — %d competition_season(s) inserted, %d already present or failed",
        inserted,
        skipped,
    )
    if inserted:
        log.info(
            "Reminder: num_teams / total_matchdays / promo_spots / relegation_spots "
            "are left NULL and must be set by hand if you need them."
        )


if __name__ == "__main__":
    main()
