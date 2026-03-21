"""
Squad ingestion processor — the main entry point.

Orchestrates the full pipeline for a single run:
    1. Scan data/raw/ for all squad JSON files
    2. Sort chronologically (ensures initial loads precede diffs)
    3. For each file:
        a. Extract path context
        b. Load and validate JSON
        c. Check idempotency log — skip if already processed
        d. Determine initial vs. diff load
        e. Parse into dataclasses
        f. Apply DB writes inside a single transaction per file
        g. Log result to squad_snapshot_log

Usage:
    from src.bronze.squads.processor import SquadProcessor

    processor = SquadProcessor(
        raw_root="data/raw",
        db_conn=psycopg2.connect(DSN),
    )
    processor.run()

    # Or process a single file:
    processor.process_file(Path("data/raw/PRD/2025-26/squads/2025-08-15/4dtd....json"))
"""

import json
import logging
from pathlib import Path

import psycopg2

from .scanner   import scan_squad_files, extract_path_context
from .validator import validate_squad_file
from .parser    import parse_squad_file
from . import db as db_ops

logger = logging.getLogger(__name__)


class SquadProcessor:

    def __init__(self, raw_root: str | Path, db_conn):
        """
        Args:
            raw_root: Path to the root of the raw data directory (e.g. "data/raw").
            db_conn:  Open psycopg2 connection. The processor does NOT open or
                      close it — the caller owns the lifecycle.
        """
        self.raw_root = Path(raw_root)
        self.conn     = db_conn

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #

    def run(self) -> dict:
        """
        Discover and process all squad files under raw_root.

        Returns a summary dict:
            {
                "total_files":    int,
                "processed":      int,
                "skipped":        int,  # already in log
                "failed":         int,
                "validation_errors": int,
            }
        """
        files = scan_squad_files(self.raw_root)
        summary = {"total_files": len(files), "processed": 0, "skipped": 0,
                   "failed": 0, "validation_errors": 0}

        for file_path in files:
            result = self.process_file(file_path)
            if result == "skipped":
                summary["skipped"] += 1
            elif result == "invalid":
                summary["validation_errors"] += 1
            elif result == "error":
                summary["failed"] += 1
            else:
                summary["processed"] += 1

        logger.info(
            "Run complete — %d processed, %d skipped, %d validation errors, %d failed",
            summary["processed"], summary["skipped"],
            summary["validation_errors"], summary["failed"],
        )
        return summary

    def process_file(self, file_path: Path) -> str:
        """
        Process a single squad JSON file end-to-end.

        Returns one of: "ok" | "skipped" | "invalid" | "error"
        """
        label = str(file_path)
        logger.info("Processing %s", label)

        # ---------------------------------------------------------------- #
        # 1. Extract path context
        # ---------------------------------------------------------------- #
        ctx = extract_path_context(file_path)
        if any(v is None for v in ctx.values()):
            logger.error("Could not extract path context from %s — skipping.", label)
            return "invalid"

        competition_code  = ctx["competition_code"]
        season            = ctx["season"]
        snapshot_date     = ctx["snapshot_date"]
        source_team_id    = ctx["source_team_id"]

        # ---------------------------------------------------------------- #
        # 2. Load JSON
        # ---------------------------------------------------------------- #
        try:
            with open(file_path, "r", encoding="utf-8") as fh:
                raw_json = json.load(fh)
        except (json.JSONDecodeError, OSError) as exc:
            logger.error("Failed to load JSON from %s: %s", label, exc)
            return "error"

        # ---------------------------------------------------------------- #
        # 3. Validate
        # ---------------------------------------------------------------- #
        validation = validate_squad_file(raw_json, source_team_id, file_path)
        if not validation.is_valid:
            return "invalid"

        squad        = raw_json["squad"][0]
        source_season_id = squad["tournamentCalendarId"]

        # ---------------------------------------------------------------- #
        # 4. Idempotency check
        # ---------------------------------------------------------------- #
        if db_ops.is_snapshot_already_processed(
            self.conn, source_team_id, source_season_id, snapshot_date
        ):
            logger.info("Already processed — skipping %s", label)
            return "skipped"

        # ---------------------------------------------------------------- #
        # 5. Determine load type
        # ---------------------------------------------------------------- #
        initial = db_ops.is_initial_load(self.conn, source_team_id, source_season_id)
        logger.info(
            "Load type: %s for team '%s' season '%s'",
            "INITIAL" if initial else "DIFF",
            source_team_id,
            source_season_id,
        )

        # ---------------------------------------------------------------- #
        # 6. Parse
        # ---------------------------------------------------------------- #
        snapshot = parse_squad_file(
            raw_json         = raw_json,
            competition_code = competition_code,
            season           = season,
            snapshot_date    = snapshot_date,
            source_team_id   = source_team_id,
            is_initial_load  = initial,
        )

        # ---------------------------------------------------------------- #
        # 7. Write to DB — single transaction per file
        # ---------------------------------------------------------------- #
        try:
            players_upserted, entries_inserted, entries_closed = self._write(
                snapshot, initial
            )
            db_ops.log_snapshot_processed(
                self.conn, snapshot,
                players_upserted, entries_inserted, entries_closed,
            )
            self.conn.commit()
            logger.info(
                "Committed %s — players upserted: %d, entries inserted: %d, entries closed: %d",
                label, players_upserted, entries_inserted, entries_closed,
            )
            return "ok"

        except Exception as exc:
            self.conn.rollback()
            logger.error("Transaction rolled back for %s: %s", label, exc, exc_info=True)
            return "error"

    # ------------------------------------------------------------------ #
    # Private — DB write logic
    # ------------------------------------------------------------------ #

    def _write(self, snapshot, is_initial: bool) -> tuple[int, int, int]:
        """
        Apply all DB writes for a snapshot. Called inside a transaction.

        Returns (players_upserted, entries_inserted, entries_closed).
        """
        # Always upsert players — safe on re-runs, updates provider-sourced fields
        players_upserted = db_ops.upsert_players(self.conn, snapshot.players)

        incoming_ids = {e.source_player_id for e in snapshot.squad_entries}

        if is_initial:
            # Insert all entries as-is using provider start/end dates
            entries_inserted = db_ops.insert_squad_entries(self.conn, snapshot.squad_entries)
            entries_closed   = 0

        else:
            # Diff load — compare incoming against currently active rows
            active_ids = db_ops.get_active_squad_player_ids(
                self.conn,
                snapshot.source_team_id,
                snapshot.source_season_id,
            )

            new_player_ids      = incoming_ids - active_ids
            departed_player_ids = active_ids   - incoming_ids
            continuing_ids      = incoming_ids & active_ids

            # Insert new arrivals
            new_entries = [
                e for e in snapshot.squad_entries
                if e.source_player_id in new_player_ids
            ]
            entries_inserted = db_ops.insert_squad_entries(self.conn, new_entries)

            # Close departed players
            entries_closed = db_ops.close_squad_entries(
                self.conn,
                departed_player_ids,
                snapshot.source_team_id,
                snapshot.source_season_id,
                snapshot.snapshot_date,
            )

            # Update shirt numbers for continuing players (in-place correction)
            continuing_entries = [
                e for e in snapshot.squad_entries
                if e.source_player_id in continuing_ids
            ]
            db_ops.update_shirt_numbers(
                self.conn,
                continuing_entries,
                snapshot.source_team_id,
                snapshot.source_season_id,
            )

            if departed_player_ids:
                logger.info("Closed %d departed player(s): %s", len(departed_player_ids), departed_player_ids)
            if new_player_ids:
                logger.info("Inserted %d new arrival(s): %s", len(new_player_ids), new_player_ids)

        return players_upserted, entries_inserted, entries_closed