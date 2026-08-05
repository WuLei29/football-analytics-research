"""
spadl.py
────────
SPADL (Soccer Player Action Description Language) column calculation.

Maps Opta event data to SPADL action types, results, and body parts.
Adds three columns: spadl_type_id, spadl_result_id, spadl_bodypart_id.

Stateless: takes a DataFrame, returns a DataFrame.
No DB access in the transform path. No parsing. No carries.

Backfill CLI:
    python -m src.silver.events.spadl
    python -m src.silver.events.spadl --match-ids 1 2 3
"""

import json
import logging
from typing import List, Optional, Set, Tuple

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)


# ── SPADL constants ───────────────────────────────────────────────────────────

# Action types (0–22)
PASS             = 0
CROSS            = 1
THROW_IN         = 2
FREEKICK_CROSSED = 3
FREEKICK_SHORT   = 4
CORNER_CROSSED   = 5
CORNER_SHORT     = 6
TAKE_ON          = 7
FOUL             = 8
TACKLE           = 9
INTERCEPTION     = 10
SHOT             = 11
SHOT_PENALTY     = 12
SHOT_FREEKICK    = 13
KEEPER_SAVE      = 14
KEEPER_CLAIM     = 15
KEEPER_PUNCH     = 16
KEEPER_PICK_UP   = 17
CLEARANCE        = 18
BAD_TOUCH        = 19
# 20 = non_action (internal, mapped to NULL)
DRIBBLE          = 21
GOALKICK         = 22

# Results (0–5)
FAIL        = 0
SUCCESS     = 1
OFFSIDE     = 2
OWNGOAL     = 3
YELLOW_CARD = 4
RED_CARD    = 5

# Body parts (0–5)
FOOT       = 0
HEAD       = 1
OTHER      = 2
# 3 = head/other (Wyscout only, not used here)
FOOT_LEFT  = 4
FOOT_RIGHT = 5

# Types whose result is always forced
_ALWAYS_SUCCESS = frozenset({KEEPER_SAVE, KEEPER_PUNCH, KEEPER_PICK_UP, CLEARANCE, DRIBBLE})
_ALWAYS_FAIL    = frozenset({FOUL, BAD_TOUCH})

# Opta event_type strings that map to shots
_SHOT_EVENTS = frozenset({"Miss", "Post", "Attempt Saved", "Goal"})

# Shot events that did NOT result in a goal.  Opta sets outcome='success' on
# these too (the outcome flag means "attempt recorded", not "scored"), so they
# must be resolved before the generic outcome fallback or every shot maps to
# SUCCESS.  Matches socceraction's Opta converter:
#     elif e in ['attempt saved', 'miss', 'post']: r = 'fail'
_SHOT_NO_GOAL_EVENTS = frozenset({"Miss", "Post", "Attempt Saved"})


# ── Qualifier extraction ─────────────────────────────────────────────────────

def _get_qualifier_ids(raw_data) -> Set[int]:
    """Extract qualifier IDs from a raw_data payload.

    Handles: None, NaN, JSON string, dict.
    """
    if raw_data is None or (isinstance(raw_data, float) and np.isnan(raw_data)):
        return set()
    if isinstance(raw_data, str):
        try:
            raw_data = json.loads(raw_data)
        except (json.JSONDecodeError, TypeError):
            return set()
    if not isinstance(raw_data, dict):
        return set()
    qualifiers = raw_data.get("qualifier", [])
    ids: Set[int] = set()
    for q in qualifiers:
        if isinstance(q, dict):
            qid = q.get("qualifierId")
            if qid is None:
                qid = q.get("id")
            if qid is not None:
                ids.add(int(qid))
    return ids


# ── Row-level mapping ─────────────────────────────────────────────────────────

def _map_spadl_type(event_type: str, type_id, qids: Set[int]) -> Optional[int]:
    """Map an Opta event to a SPADL action type. Returns None for non-actions."""

    if event_type == "Pass":
        if 6 in qids and 2 in qids:
            return CORNER_CROSSED
        if 6 in qids:
            return CORNER_SHORT
        if 5 in qids and 2 in qids:
            return FREEKICK_CROSSED
        if 5 in qids:
            return FREEKICK_SHORT
        if 107 in qids:
            return THROW_IN
        # Q124 = Goal Kick (NOT Q195 which is Pull Back)
        if 124 in qids:
            return GOALKICK
        if 2 in qids:
            return CROSS
        return PASS

    if event_type == "Offside Pass":
        return PASS

    if event_type == "Take On":
        return TAKE_ON
    if event_type == "Foul":
        return FOUL
    if event_type == "Tackle":
        return TACKLE
    if event_type in ("Interception", "Blocked Pass"):
        return INTERCEPTION

    if event_type in _SHOT_EVENTS:
        if 9 in qids:
            return SHOT_PENALTY
        if 26 in qids:
            return SHOT_FREEKICK
        return SHOT

    if event_type == "Save":
        if 94 in qids:
            return None
        return KEEPER_SAVE

    if event_type == "Claim":
        return KEEPER_CLAIM
    if event_type == "Punch":
        return KEEPER_PUNCH
    if event_type == "Keeper pick-up":
        return KEEPER_PICK_UP
    if event_type == "Clearance":
        return CLEARANCE

    if event_type == "Ball touch":
        return BAD_TOUCH

    if event_type == "Carry" or type_id == -1:
        return DRIBBLE

    return None


def _map_spadl_result(
    event_type: str,
    outcome: Optional[str],
    spadl_type: int,
    qids: Set[int],
) -> int:
    """Map an Opta outcome to a SPADL result."""

    if spadl_type in _ALWAYS_SUCCESS:
        return SUCCESS
    if spadl_type in _ALWAYS_FAIL:
        return FAIL

    if event_type == "Offside Pass":
        return OFFSIDE

    if event_type == "Goal":
        if 28 in qids:
            return OWNGOAL
        return SUCCESS

    # Must precede the outcome fallback: Opta reports outcome='success' on
    # off-target, woodwork and saved attempts alike.
    if event_type in _SHOT_NO_GOAL_EVENTS:
        return FAIL

    if outcome == "success":
        return SUCCESS
    return FAIL


def _map_spadl_bodypart(spadl_type: int, qids: Set[int]) -> int:
    """Map Opta qualifiers to a SPADL body part."""
    if spadl_type == DRIBBLE:
        return FOOT
    if qids & {15, 3, 168}:
        return HEAD
    if 21 in qids:
        return OTHER
    if 20 in qids:
        return FOOT_RIGHT
    if 72 in qids:
        return FOOT_LEFT
    return FOOT


# ── Public transform ──────────────────────────────────────────────────────────

def calculate_spadl(df: pd.DataFrame) -> pd.DataFrame:
    """Add spadl_type_id, spadl_result_id, spadl_bodypart_id columns.

    Events that don't map to SPADL actions get NULL for all three.

    Args:
        df: Events DataFrame with columns: event_type, outcome, type_id, raw_data.

    Returns:
        DataFrame with the three SPADL columns added.
    """
    df = df.copy()

    types: List[Optional[int]] = []
    results: List[Optional[int]] = []
    bodyparts: List[Optional[int]] = []

    for event_type, outcome, type_id, raw_data in zip(
        df["event_type"], df["outcome"], df["type_id"], df["raw_data"],
    ):
        qids = _get_qualifier_ids(raw_data)
        spadl_type = _map_spadl_type(event_type, type_id, qids)

        if spadl_type is None:
            types.append(None)
            results.append(None)
            bodyparts.append(None)
            continue

        types.append(spadl_type)
        results.append(_map_spadl_result(event_type, outcome, spadl_type, qids))
        bodyparts.append(_map_spadl_bodypart(spadl_type, qids))

    df["spadl_type_id"]     = pd.array(types,     dtype=pd.Int16Dtype())
    df["spadl_result_id"]   = pd.array(results,   dtype=pd.Int16Dtype())
    df["spadl_bodypart_id"] = pd.array(bodyparts,  dtype=pd.Int16Dtype())

    mapped = df["spadl_type_id"].notna().sum()
    log.debug("SPADL mapped %d / %d events", mapped, len(df))
    return df


# ── Backfill ──────────────────────────────────────────────────────────────────

_BACKFILL_SELECT = """
    SELECT event_id, event_type, outcome, type_id, raw_data
    FROM silver.events
    WHERE match_id = ANY(%s)
"""

_BACKFILL_UPDATE = """
    UPDATE silver.events
    SET spadl_type_id     = data.st,
        spadl_result_id   = data.sr,
        spadl_bodypart_id = data.sb
    FROM (VALUES %s) AS data(st, sr, sb, eid)
    WHERE silver.events.event_id = data.eid
"""


def _get_unprocessed_match_ids(
    conn,
    limit: Optional[int] = None,
) -> List[int]:
    """Find match_ids that have no SPADL data yet."""
    sql = """
        SELECT DISTINCT e.match_id
        FROM silver.events e
        WHERE NOT EXISTS (
            SELECT 1 FROM silver.events e2
            WHERE e2.match_id = e.match_id
              AND e2.spadl_type_id IS NOT NULL
        )
        ORDER BY e.match_id
    """
    if limit:
        sql += f" LIMIT {int(limit)}"

    with conn.cursor() as cur:
        cur.execute(sql)
        return [row[0] for row in cur.fetchall()]


def backfill_spadl(
    conn,
    match_ids: Optional[List[int]] = None,
    limit: Optional[int] = None,
    batch_size: int = 50,
) -> dict:
    """Backfill SPADL columns for existing events.

    Args:
        conn: psycopg2 connection.
        match_ids: Specific match IDs, or None to auto-discover.
        limit: Max matches to process (auto-discover only).
        batch_size: Matches per DB batch.

    Returns:
        Stats dict with matches_processed, events_updated counts.
    """
    import psycopg2.extras

    if match_ids is None:
        match_ids = _get_unprocessed_match_ids(conn, limit=limit)
        if not match_ids:
            log.info("No unprocessed matches found — nothing to backfill.")
            return {"matches_processed": 0, "events_updated": 0}
        log.info("Auto-discovered %d unprocessed matches", len(match_ids))
    else:
        log.info("Processing %d specified matches", len(match_ids))

    stats = {"matches_processed": 0, "events_updated": 0}

    for batch_start in range(0, len(match_ids), batch_size):
        batch_ids = match_ids[batch_start : batch_start + batch_size]

        with conn.cursor() as cur:
            cur.execute(_BACKFILL_SELECT, (batch_ids,))
            cols = [desc[0] for desc in cur.description]
            rows = cur.fetchall()

        if not rows:
            log.warning("No events found for match_ids %s", batch_ids)
            continue

        df = pd.DataFrame(rows, columns=cols)

        updates: List[Tuple] = []
        for _, row in df.iterrows():
            qids = _get_qualifier_ids(row["raw_data"])
            spadl_type = _map_spadl_type(row["event_type"], row["type_id"], qids)

            if spadl_type is None:
                continue

            spadl_result = _map_spadl_result(
                row["event_type"], row["outcome"], spadl_type, qids,
            )
            spadl_bodypart = _map_spadl_bodypart(spadl_type, qids)
            updates.append((spadl_type, spadl_result, spadl_bodypart, row["event_id"]))

        if not updates:
            log.warning("No SPADL-mappable events in batch starting at %d", batch_start)
            continue

        with conn.cursor() as cur:
            for page_start in range(0, len(updates), 500):
                page = updates[page_start : page_start + 500]
                psycopg2.extras.execute_values(
                    cur, _BACKFILL_UPDATE, page,
                    template="(%s::smallint, %s::smallint, %s::smallint, %s::bigint)",
                    page_size=500,
                )

        conn.commit()

        n_matches = df["match_id"].nunique() if "match_id" in df.columns else len(batch_ids)
        stats["matches_processed"] += len(batch_ids)
        stats["events_updated"] += len(updates)
        log.info(
            "Batch done: %d matches, %d events updated",
            len(batch_ids), len(updates),
        )

    log.info(
        "Backfill complete: %(matches_processed)d matches, "
        "%(events_updated)d events updated",
        stats,
    )
    return stats


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    import os
    import sys

    import psycopg2
    from dotenv import load_dotenv

    load_dotenv()

    parser = argparse.ArgumentParser(
        description="Backfill SPADL columns in silver.events",
    )
    parser.add_argument(
        "--match-ids", nargs="+", type=int, default=None,
        help="Specific match IDs to process (default: auto-discover unprocessed)",
    )
    parser.add_argument(
        "--limit", type=int, default=None,
        help="Max number of matches to process (auto-discover only)",
    )
    parser.add_argument(
        "--batch-size", type=int, default=50,
        help="Matches per DB batch (default: 50)",
    )
    parser.add_argument(
        "--log-level", default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity (default: INFO)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%H:%M:%S",
    )

    dsn = os.getenv("FOOTBALL_DB_DSN")
    if not dsn:
        log.error("FOOTBALL_DB_DSN is not set in your .env file.")
        sys.exit(1)

    conn = psycopg2.connect(dsn)
    try:
        stats = backfill_spadl(
            conn,
            match_ids=args.match_ids,
            limit=args.limit,
            batch_size=args.batch_size,
        )
        print(f"\nDone: {stats}")
    except Exception:
        log.exception("Backfill failed")
        conn.rollback()
        sys.exit(1)
    finally:
        conn.close()
