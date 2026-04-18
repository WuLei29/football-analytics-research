"""
db.py
─────
All database operations for the events pipeline.

No parsing. No carries. No xT. Only SQL.

Connection management: this module receives a psycopg2 connection and does
not open or close it — the caller (processor.py) owns the lifecycle.
"""

import logging
from typing import Dict, Optional, Tuple

import pandas as pd
import psycopg2.extras

log = logging.getLogger(__name__)

# Columns written to silver.events — must match DDL order exactly.
SILVER_COLUMNS = [
    "source_event_id", "provider_event_id", "json_index", "match_id",
    "period", "minute", "second", "timestamp",
    "team_id", "source_team_id",
    "player_id", "source_player_id",
    "player_name", "jersey_number",
    "team_name", "opposition_team_name", "h_a",
    "type_id", "event_type", "outcome",
    "x", "y", "end_x", "end_y",
    "blocked_x", "blocked_y",
    "goal_mouth_z", "goal_mouth_y",
    "start_zone_value_xt", "end_zone_value_xt", "xt",
    "sequence_id", "sequence_start", "sequence_end", "sequence_event_number",
    "value_assist",      # ← new
    "raw_data"
]


# ── Cache loaders ──────────────────────────────────────────────────────────────

def load_id_caches(conn) -> Tuple[Dict[str, int], Dict[str, int]]:
    """
    Load source_id → internal_id mappings from silver into memory.
    Called once per processor run — not per file.

    Returns:
        (team_cache, player_cache)
    """
    with conn.cursor() as cur:
        cur.execute("SELECT source_team_id, team_id FROM silver.teams")
        team_cache = {row[0]: row[1] for row in cur.fetchall()}

        cur.execute("SELECT source_player_id, player_id FROM silver.players")
        player_cache = {row[0]: row[1] for row in cur.fetchall()}

    log.info("FK caches loaded: %d teams, %d players", len(team_cache), len(player_cache))
    return team_cache, player_cache


def resolve_fk_columns(
    df: pd.DataFrame,
    team_cache: Dict[str, int],
    player_cache: Dict[str, int],
) -> pd.DataFrame:
    """
    Resolve source_team_id → team_id and source_player_id → player_id
    using the in-memory caches.
    """
    df = df.copy()
    df["team_id"]   = df["source_team_id"].map(team_cache)
    df["player_id"] = df["source_player_id"].map(player_cache)

    unresolved_teams = df.loc[df["team_id"].isna(), "source_team_id"].dropna().unique()
    if len(unresolved_teams):
        log.warning("Unresolved source_team_ids: %s", list(unresolved_teams))

    unresolved_players = df.loc[
        df["player_id"].isna() & df["source_player_id"].notna(),
        "source_player_id",
    ].unique()
    if len(unresolved_players):
        log.warning(
            "%d unresolved source_player_ids (first 5): %s",
            len(unresolved_players),
            list(unresolved_players[:5]),
        )

    return df


# ── Match resolution + idempotency ────────────────────────────────────────────

def resolve_match_id(source_match_id: str, conn) -> Optional[int]:
    """Resolve provider source_match_id to internal match_id."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT match_id FROM silver.matches WHERE source_match_id = %s",
            (source_match_id,),
        )
        row = cur.fetchone()
    return row[0] if row else None


def is_match_already_loaded(match_id: int, conn) -> bool:
    """Return True if silver.events already has rows for this match_id."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM silver.events WHERE match_id = %s LIMIT 1",
            (match_id,),
        )
        return cur.fetchone() is not None


# ── Insert ─────────────────────────────────────────────────────────────────────

def insert_events(df: pd.DataFrame, conn) -> int:
    """
    Insert all event rows for one match into silver.events.
    Commit is handled by the caller.

    Returns the number of rows inserted.
    """
    # Ensure all silver columns exist (some may be absent for edge cases)
    for col in SILVER_COLUMNS:
        if col not in df.columns:
            df[col] = None

    silver_df = df[SILVER_COLUMNS].copy()

    # Coerce timestamps; replace pandas NA with None for psycopg2
    silver_df["timestamp"] = pd.to_datetime(
        silver_df["timestamp"], errors="coerce", utc=True
    )
    silver_df = silver_df.astype(object).where(pd.notnull(silver_df), None)

    rows = [tuple(r) for r in silver_df.itertuples(index=False, name=None)]
    cols = ", ".join(SILVER_COLUMNS)

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            f"INSERT INTO silver.events ({cols}) VALUES %s",
            rows,
            page_size=500,
        )
        return len(rows)