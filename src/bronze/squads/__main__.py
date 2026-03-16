"""
Command-line entry point for squad ingestion.

Usage:
    python -m src.bronze.squads \
        --dsn "postgresql://user:password@localhost:5432/football" \
        --raw-root "data/raw"

Environment variable alternative (avoids credentials in shell history):
    export FOOTBALL_DB_DSN="postgresql://user:password@localhost:5432/football"
    python -m src.bronze.squads --raw-root "data/raw"
"""

import argparse
import os
import sys
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

# Load .env from the project root (three levels up from src/bronze/squads/)
load_dotenv(Path(__file__).resolve().parents[3] / ".env")

from .squad_processor import SquadProcessor
from . import db as db_ops


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Ingest squad JSON files into the players and player_squads tables."
    )
    parser.add_argument(
        "--dsn",
        default=os.getenv("FOOTBALL_DB_DSN"),
        help=(
            "PostgreSQL connection string. "
            "Defaults to the FOOTBALL_DB_DSN environment variable if not provided."
        ),
    )
    parser.add_argument(
        "--raw-root",
        default="data/raw",
        help="Root directory of the raw data tree (default: data/raw).",
    )

    args = parser.parse_args()

    if not args.dsn:
        print(
            "Error: no DSN provided. "
            "Use --dsn or set the FOOTBALL_DB_DSN environment variable.",
            file=sys.stderr,
        )
        sys.exit(1)

    conn = psycopg2.connect(args.dsn)
    try:
        processor = SquadProcessor(raw_root=args.raw_root, db_conn=conn)
        summary   = processor.run()
        sys.exit(0 if summary["failed"] == 0 else 1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()