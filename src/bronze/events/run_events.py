"""
run_events.py
─────────────
CLI runner for FootballEventsProcessor.

Reads FOOTBALL_DSN from .env and processes all match files
found under data/raw/{competition_code}/{season}/matches/.

Usage examples
──────────────
# Full run with all defaults
python run_events.py

# Dry run — no DB writes, just prints what would be processed
python run_events.py --dry-run

# Disable carries and xT (faster, for testing)
python run_events.py --no-carries --no-xt

# Force re-load matches that already exist in silver.events
python run_events.py --no-skip-existing

# Custom raw data path
python run_events.py --base-path /data/football/raw

# Custom mapping paths
python run_events.py --events-map mappings/OptaEvents.js --qualifiers-map mappings/OptaQualifiers.js
"""

import argparse
import sys
from pathlib import Path

from dotenv import load_dotenv
import os
import psycopg2

from load_events import FootballEventsProcessor


# ---------------------------------------------------------------------------
# Defaults — edit these to match your project layout
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[3]  # src/bronze/events → project root
DEFAULT_BASE_PATH = str(PROJECT_ROOT / "data" / "raw")
DEFAULT_EVENTS_MAP      = str(PROJECT_ROOT / "data" / "mapping" / "opta-events.js")
DEFAULT_QUALIFIERS_MAP  = str(PROJECT_ROOT / "data"/ "mapping" / "opta-qualifiers.js")
DEFAULT_ENV_FILE        = str(PROJECT_ROOT / ".env")
DEFAULT_DSN_VAR         = "FOOTBALL_DB_DSN"
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load football event files into silver.events",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    parser.add_argument(
        "--base-path",
        default=DEFAULT_BASE_PATH,
        help=f"Root of raw data directory tree (default: {DEFAULT_BASE_PATH})"
    )
    parser.add_argument(
        "--events-map",
        default=DEFAULT_EVENTS_MAP,
        help=f"Path to Opta events mapping JS file (default: {DEFAULT_EVENTS_MAP})"
    )
    parser.add_argument(
        "--qualifiers-map",
        default=DEFAULT_QUALIFIERS_MAP,
        help=f"Path to Opta qualifiers mapping JS file (default: {DEFAULT_QUALIFIERS_MAP})"
    )
    parser.add_argument(
        "--env-file",
        default=DEFAULT_ENV_FILE,
        help=f"Path to .env file (default: {DEFAULT_ENV_FILE})"
    )
    parser.add_argument(
        "--dsn-var",
        default=DEFAULT_DSN_VAR,
        help=f"Name of the DSN environment variable (default: {DEFAULT_DSN_VAR})"
    )
    parser.add_argument(
        "--no-carries",
        action="store_true",
        help="Skip carry calculation"
    )
    parser.add_argument(
        "--no-xt",
        action="store_true",
        help="Skip Expected Threat (xT) calculation"
    )
    parser.add_argument(
        "--no-skip-existing",
        action="store_true",
        help="Re-load matches that already have events in silver (default: skip them)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Scan files and resolve match IDs but do not write to the database"
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    # ── Load environment ──────────────────────────────────────────────────
    env_path = Path(args.env_file)
    if not env_path.exists():
        print(f".env file not found at '{env_path}'. "
              f"Set --env-file or create the file.")
        sys.exit(1)

    load_dotenv(env_path)
    dsn = os.getenv(args.dsn_var)

    if not dsn:
        print(f"Environment variable '{args.dsn_var}' not set in '{env_path}'.")
        sys.exit(1)

    # ── Validate paths ────────────────────────────────────────────────────
    for label, path in [
        ("base_path",       args.base_path),
        ("events_map",      args.events_map),
        ("qualifiers_map",  args.qualifiers_map),
    ]:
        if not Path(path).exists():
            print(f"Path not found for {label}: '{path}'")
            sys.exit(1)

    # ── Connect ───────────────────────────────────────────────────────────
    print("Connecting to database...")
    try:
        conn = psycopg2.connect(dsn)
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
        print("Connection OK")
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        sys.exit(1)

    # ── Initialise processor ──────────────────────────────────────────────
    processor = FootballEventsProcessor(
        events_mapping_path=args.events_map,
        qualifiers_mapping_path=args.qualifiers_map
    )

    # ── Run ───────────────────────────────────────────────────────────────
    if args.dry_run:
        print("\nDRY RUN — no data will be written to the database\n")

    try:
        summary = processor.load_all_matches(
            base_path=args.base_path,
            conn=conn,
        include_carries=not args.no_carries,
        include_xt=not args.no_xt,
        skip_existing=not args.no_skip_existing,
            dry_run=args.dry_run
        )
    finally:
        conn.close()

    # ── Exit code ─────────────────────────────────────────────────────────
    # Non-zero exit if any files failed — useful for cron alerting
    sys.exit(1 if summary['failed'] > 0 else 0)


if __name__ == "__main__":
    main()