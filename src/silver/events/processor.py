"""
processor.py
────────────
Orchestrates the full events pipeline for one run.

Pipeline per file:
  1. Extract source_match_id (cheap — no full parse)
  2. Resolve to internal match_id
  3. Skip if already loaded (idempotency guard)
  4. Parse events → list of row dicts
  5. Build DataFrame
  6. (Optional) Insert carries
  7. (Optional) Calculate xT
  8. Resolve FK columns (source → internal IDs)
  9. Insert to silver.events
  10. Commit

No SQL lives here — all DB calls go through db.py.
No transformation logic lives here — parser, carries, xt own that.
"""

import logging
from pathlib import Path
from typing import Any, Dict

import pandas as pd

from . import db as db_ops
from .carries import calculate_carries
from .parser import extract_source_match_id, load_mapping_file, parse_event_file
from .xt import calculate_xt

log = logging.getLogger(__name__)

CARRY_NOISE_EVENTS = frozenset({
    "Card",
    "Coach Setup",
    "Collection End",
    "Condition change",
    "Contentious referee decision",
    "Corner Awarded",
    "Deleted event",
    "End",
    "End delay",
    "Formation change",
    "Injury Time Announcement",
    "Official change",
    "Penalty faced",
    "Player off",
    "Player on",
    "Player retired",
    "Start",
    "Start delay",
    "Team set up",
})

class EventsProcessor:

    def __init__(
        self,
        events_mapping_path: str,
        qualifiers_mapping_path: str,
    ):
        self.event_codes     = load_mapping_file(events_mapping_path)
        self.qualifier_codes = load_mapping_file(qualifiers_mapping_path)

        # Populated once per run() call
        self._team_cache:   Dict[str, int] = {}
        self._player_cache: Dict[str, int] = {}

    # ── Public API ─────────────────────────────────────────────────────────────

    def run(
        self,
        base_path: str,
        conn,
        include_carries: bool = True,
        include_xt: bool = True,
        skip_existing: bool = True,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """
        Discover and process all match files under base_path.

        Returns:
            {"total_files", "loaded", "skipped", "failed", "failed_files"}
        """
        base = Path(base_path)
        match_files = sorted(
            f for f in base.rglob("matches/*") if f.is_file()
        )

        if not match_files:
            log.warning("No match files found under %s", base)
            return {"total_files": 0, "loaded": 0, "skipped": 0,
                    "failed": 0, "failed_files": []}

        log.info("%d match files found under %s", len(match_files), base)

        # Load FK caches once for the whole run
        self._team_cache, self._player_cache = db_ops.load_id_caches(conn)

        summary = {
            "total_files": len(match_files),
            "loaded": 0, "skipped": 0, "failed": 0, "failed_files": [],
        }

        for file_path in match_files:
            result = self.process_file(
                file_path, conn,
                include_carries=include_carries,
                include_xt=include_xt,
                skip_existing=skip_existing,
                dry_run=dry_run,
            )
            if result == "loaded":
                summary["loaded"] += 1
            elif result == "skipped":
                summary["skipped"] += 1
            else:
                summary["failed"] += 1
                summary["failed_files"].append(str(file_path))

        log.info(
            "Run complete — loaded: %d, skipped: %d, failed: %d",
            summary["loaded"], summary["skipped"], summary["failed"],
        )
        return summary

    def process_file(
        self,
        file_path: Path,
        conn,
        include_carries: bool = True,
        include_xt: bool = True,
        skip_existing: bool = True,
        dry_run: bool = False,
    ) -> str:
        """
        Full pipeline for a single match file.
        Returns: "loaded" | "skipped" | "error"
        """
        label = str(file_path)
        log.info("Processing %s", label)

        try:
            # Step 1 — cheap source ID extraction
            source_match_id = extract_source_match_id(label)
            if not source_match_id:
                log.error("Could not extract source_match_id from %s", label)
                return "error"

            # Step 2 — resolve internal ID
            match_id = db_ops.resolve_match_id(source_match_id, conn)
            if match_id is None:
                log.warning("source_match_id '%s' not in silver.matches — skipping", source_match_id)
                return "skipped"

            # Step 3 — idempotency guard
            if skip_existing and db_ops.is_match_already_loaded(match_id, conn):
                log.debug("match_id=%s already loaded — skipping", match_id)
                return "skipped"

            # Step 4 — parse
            rows = parse_event_file(
                label, match_id, self.event_codes, self.qualifier_codes
            )
            if not rows:
                log.warning("No events parsed from %s", label)
                return "error"

            df = pd.DataFrame(rows)

            # Step 5 — carries
            if include_carries:
                df_clean = (
                    df[~df["event_type"].isin(CARRY_NOISE_EVENTS)]
                    .copy()
                    .reset_index(drop=True)
                )

                df_clean_with_carries = calculate_carries(df_clean)

                new_carries = df_clean_with_carries[
                    df_clean_with_carries["event_type"] == "Carry"
                ].copy()

                if not new_carries.empty:
                    df = pd.concat([df, new_carries], ignore_index=True)
                    df = df.sort_values(
                        ["json_index"],
                        na_position="last",
                    ).reset_index(drop=True)

            # Step 6 — xT
            if include_xt:
                df = calculate_xt(df)

            # Step 7 — resolve FKs
            df = db_ops.resolve_fk_columns(df, self._team_cache, self._player_cache)

            # Step 8 — insert
            if dry_run:
                log.info("[dry-run] Would insert %d events for match_id=%s", len(df), match_id)
            else:
                inserted = db_ops.insert_events(df, conn)
                conn.commit()
                log.info("✓ match_id=%-6s  rows=%-4d  source=%s", match_id, inserted, source_match_id)

            return "loaded"

        except Exception as exc:
            conn.rollback()
            log.error("Error processing %s: %s", label, exc, exc_info=True)
            return "error"