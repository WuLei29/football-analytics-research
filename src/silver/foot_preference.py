"""
foot_preference.py
──────────────────
Standalone enrichment — derives preferred_foot for players and
is_weak_foot for events from Opta qualifier data.

Two-phase process (runs in a single transaction):
  Phase 1 — Count Q20 (Right foot) and Q72 (Left foot) per player
            across all events. UPDATE silver.players.preferred_foot.
  Phase 2 — For each event carrying a foot qualifier, compare against
            the player's preferred_foot. UPDATE silver.events.is_weak_foot.

Idempotent — safe to re-run after new matches are loaded.

Run:
    python src/silver/foot_preference.py
    python src/silver/foot_preference.py --dry-run
"""

import argparse
import logging
import os
import sys

import psycopg2
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Phase 1: preferred_foot ──────────────────────────────────────────────────

PHASE1_SQL = """
WITH foot_counts AS (
    SELECT
        e.player_id,
        SUM(CASE WHEN (q->>'qualifierId')::int = 20 THEN 1 ELSE 0 END) AS right_count,
        SUM(CASE WHEN (q->>'qualifierId')::int = 72 THEN 1 ELSE 0 END) AS left_count
    FROM silver.events e,
         jsonb_array_elements(e.raw_data->'qualifier') AS q
    WHERE (q->>'qualifierId')::int IN (20, 72)
      AND e.player_id IS NOT NULL
    GROUP BY e.player_id
)
UPDATE silver.players p
SET preferred_foot = CASE
    WHEN fc.right_count > fc.left_count THEN 'right'
    WHEN fc.left_count > fc.right_count THEN 'left'
    WHEN fc.right_count = fc.left_count AND fc.right_count > 0 THEN 'both'
END
FROM foot_counts fc
WHERE p.player_id = fc.player_id;
"""

PHASE1_SUMMARY_SQL = """
SELECT preferred_foot, COUNT(*)
FROM silver.players
WHERE preferred_foot IS NOT NULL
GROUP BY 1 ORDER BY 2 DESC;
"""

# ── Phase 2: is_weak_foot ────────────────────────────────────────────────────

ADD_COLUMN_SQL = """
ALTER TABLE silver.events ADD COLUMN IF NOT EXISTS is_weak_foot BOOLEAN;
"""

PHASE2_SQL = """
UPDATE silver.events e
SET is_weak_foot = CASE
    WHEN p.preferred_foot = 'both' THEN FALSE
    WHEN p.preferred_foot IS NULL  THEN NULL
    WHEN foot_q.foot != p.preferred_foot THEN TRUE
    ELSE FALSE
END
FROM (
    SELECT DISTINCT ON (e2.event_id)
        e2.event_id,
        e2.player_id,
        CASE (q->>'qualifierId')::int
            WHEN 20 THEN 'right'
            WHEN 72 THEN 'left'
        END AS foot
    FROM silver.events e2,
         jsonb_array_elements(e2.raw_data->'qualifier') AS q
    WHERE (q->>'qualifierId')::int IN (20, 72)
      AND e2.player_id IS NOT NULL
      AND e2.raw_data IS NOT NULL
    ORDER BY e2.event_id
) foot_q
JOIN silver.players p ON p.player_id = foot_q.player_id
WHERE e.event_id = foot_q.event_id;
"""

PHASE2_SUMMARY_SQL = """
SELECT is_weak_foot, COUNT(*)
FROM silver.events
WHERE is_weak_foot IS NOT NULL
GROUP BY 1 ORDER BY 2 DESC;
"""


def enrich_foot_preference(conn):
    """Run both enrichment phases and commit.

    Phase 1: derive preferred_foot on silver.players from event qualifiers.
    Phase 2: derive is_weak_foot on silver.events from preferred_foot.

    Idempotent — safe to re-run after new events are loaded.
    """
    with conn.cursor() as cur:
        cur.execute(ADD_COLUMN_SQL)

        log.info("Deriving preferred_foot from event qualifiers ...")
        cur.execute(PHASE1_SQL)
        log.info("  Updated %d player(s)", cur.rowcount)

        log.info("Deriving is_weak_foot for events with foot qualifiers ...")
        cur.execute(PHASE2_SQL)
        log.info("  Updated %d event(s)", cur.rowcount)

    conn.commit()


def main():
    parser = argparse.ArgumentParser(
        description="Derive preferred_foot (players) and is_weak_foot (events) from Opta qualifiers"
    )
    parser.add_argument("--dsn", default=os.getenv("FOOTBALL_DB_DSN"))
    parser.add_argument("--dry-run", action="store_true", help="Preview changes without writing")
    parser.add_argument("--phase2-only", action="store_true", help="Skip preferred_foot derivation, only update is_weak_foot")
    args = parser.parse_args()

    if not args.dsn:
        print("Error: FOOTBALL_DB_DSN not set. Use --dsn or set the env variable.", file=sys.stderr)
        sys.exit(1)

    conn = psycopg2.connect(args.dsn)
    try:
        with conn.cursor() as cur:
            # DDL — add is_weak_foot column (transactional in PostgreSQL)
            cur.execute(ADD_COLUMN_SQL)

            # Phase 1
            if args.phase2_only:
                log.info("Phase 1 — Skipped (--phase2-only)")
            else:
                log.info("Phase 1 — Deriving preferred_foot from event qualifiers ...")
                cur.execute(PHASE1_SQL)
                log.info("  Updated %d player(s)", cur.rowcount)

                cur.execute(PHASE1_SUMMARY_SQL)
                for foot, count in cur.fetchall():
                    log.info("    %s: %d", foot, count)

            # Phase 2
            log.info("Phase 2 — Deriving is_weak_foot for events with foot qualifiers ...")
            cur.execute(PHASE2_SQL)
            log.info("  Updated %d event(s)", cur.rowcount)

            cur.execute(PHASE2_SUMMARY_SQL)
            for weak, count in cur.fetchall():
                log.info("    is_weak_foot=%s: %d", weak, count)

        if args.dry_run:
            conn.rollback()
            log.info("Dry run — all changes rolled back")
        else:
            conn.commit()
            log.info("Done — all changes committed")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
