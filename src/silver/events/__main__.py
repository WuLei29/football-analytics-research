"""
__main__.py
───────────
CLI entry point for events ingestion.

Usage:
    python -m src.silver.events \
        --raw-root "data/raw" \
        --events-map "data/mapping/opta-events.js" \
        --qualifiers-map "data/mapping/opta-qualifiers.js"

    # Dry run
    python -m src.silver.events --raw-root data/raw --dry-run

    # Skip carries and xT (faster for testing)
    python -m src.silver.events --raw-root data/raw --no-carries --no-xt

Environment:
    FOOTBALL_DB_DSN — PostgreSQL connection string (or use --dsn flag)
"""

import argparse
import logging
import os
import sys
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[3]
load_dotenv(PROJECT_ROOT / ".env")

from .processor import EventsProcessor

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Load football event files into silver.events")

    parser.add_argument("--raw-root",        default="data/raw")
    parser.add_argument("--events-map",      default=str(PROJECT_ROOT / "data/mapping/opta-events.js"))
    parser.add_argument("--qualifiers-map",  default=str(PROJECT_ROOT / "data/mapping/opta-qualifiers.js"))
    parser.add_argument("--dsn",             default=os.getenv("FOOTBALL_DB_DSN"))
    parser.add_argument("--no-carries",      action="store_true")
    parser.add_argument("--no-xt",           action="store_true")
    parser.add_argument("--no-skip-existing",action="store_true")
    parser.add_argument("--dry-run",         action="store_true")

    args = parser.parse_args()

    if not args.dsn:
        print("Error: FOOTBALL_DB_DSN not set. Use --dsn or set the env variable.", file=sys.stderr)
        sys.exit(1)

    conn = psycopg2.connect(args.dsn)
    try:
        processor = EventsProcessor(
            events_mapping_path=args.events_map,
            qualifiers_mapping_path=args.qualifiers_map,
        )
        summary = processor.run(
            base_path=args.raw_root,
            conn=conn,
            include_carries=not args.no_carries,
            include_xt=not args.no_xt,
            skip_existing=not args.no_skip_existing,
            dry_run=args.dry_run,
        )
    finally:
        conn.close()

    sys.exit(1 if summary["failed"] > 0 else 0)


if __name__ == "__main__":
    main()