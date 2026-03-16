"""
Database operations for squad ingestion.

All SQL is here. No SQL lives in parser.py, processor.py, or anywhere else.

Connection management: this module receives a psycopg2 connection and does
not open or close it — the caller (processor.py) owns the connection lifecycle.
All operations within a single snapshot file are wrapped in a single transaction
so a failure rolls back the entire file cleanly.

Table dependencies (must exist before this module runs):
    Silver layer: players, player_squads, teams, competition_seasons
    Audit:        squad_snapshot_log  (DDL provided at bottom of this file)
"""

import logging
from datetime import date
from typing import Optional

import psycopg2
import psycopg2.extras

from .models import PlayerRecord, SquadEntry, SquadSnapshot

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------ #
# Snapshot log — idempotency guard
# ------------------------------------------------------------------ #

def is_snapshot_already_processed(
    conn,
    source_team_id: str,
    source_season_id: str,
    snapshot_date: date,
) -> bool:
    """Return True if this exact snapshot has been successfully processed before."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1 FROM silver.squad_snapshot_log
            WHERE source_team_id   = %s
              AND source_season_id = %s
              AND snapshot_date    = %s
            LIMIT 1
            """,
            (source_team_id, source_season_id, snapshot_date),
        )
        return cur.fetchone() is not None


def log_snapshot_processed(
    conn,
    snapshot: SquadSnapshot,
    players_upserted: int,
    entries_inserted: int,
    entries_closed: int,
) -> None:
    """Insert a record into squad_snapshot_log after a successful load."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO silver.squad_snapshot_log (
                source_team_id, source_season_id, competition_code,
                season, snapshot_date, team_name,
                players_upserted, entries_inserted, entries_closed
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                snapshot.source_team_id,
                snapshot.source_season_id,
                snapshot.competition_code,
                snapshot.season,
                snapshot.snapshot_date,
                snapshot.team_name,
                players_upserted,
                entries_inserted,
                entries_closed,
            ),
        )


# ------------------------------------------------------------------ #
# Initial load detection
# ------------------------------------------------------------------ #

def is_initial_load(
    conn,
    source_team_id: str,
    source_season_id: str,
) -> bool:
    """
    Return True if no player_squads rows exist yet for this team-season.

    Resolves source IDs to internal IDs via JOIN — no application-side
    ID lookup required.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM   silver.player_squads ps
            JOIN   silver.teams t
                   ON t.team_id = ps.team_id
            JOIN   silver.competition_seasons cs
                   ON cs.competition_season_id = ps.competition_season_id
            WHERE  t.source_team_id    = %s
              AND  cs.source_season_id = %s
            LIMIT  1
            """,
            (source_team_id, source_season_id),
        )
        return cur.fetchone() is None


# ------------------------------------------------------------------ #
# players table
# ------------------------------------------------------------------ #

def upsert_players(conn, players: list[PlayerRecord]) -> int:
    """
    Insert new player rows. On conflict (source_player_id already exists)
    update mutable biographical fields that may change between snapshots
    (e.g. a player's matchName or nationality may be corrected by the provider).

    Fields that are manually enriched (preferred_position, preferred_foot,
    height_cm, date_of_birth) are never overwritten by this upsert — they
    use DO UPDATE SET only on provider-sourced columns.

    Returns the number of rows inserted or updated.
    """
    if not players:
        return 0

    rows = [
        (
            p.source_player_id,
            p.first_name,
            p.last_name,
            p.short_first_name,
            p.short_last_name,
            p.known_name,
            p.match_name,
            p.gender,
            p.place_of_birth,
            p.nationality,
            p.nationality_source_id,
            p.second_nationality,
            p.second_nationality_source_id,
            p.position_raw,
        )
        for p in players
    ]

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO silver.players (
                source_player_id,
                first_name, last_name,
                short_first_name, short_last_name,
                known_name, match_name,
                gender, place_of_birth,
                nationality, nationality_source_id,
                second_nationality, second_nationality_source_id,
                position_raw
            )
            VALUES %s
            ON CONFLICT (source_player_id) DO UPDATE SET
                first_name                   = EXCLUDED.first_name,
                last_name                    = EXCLUDED.last_name,
                short_first_name             = EXCLUDED.short_first_name,
                short_last_name              = EXCLUDED.short_last_name,
                known_name                   = EXCLUDED.known_name,
                match_name                   = EXCLUDED.match_name,
                gender                       = EXCLUDED.gender,
                place_of_birth               = EXCLUDED.place_of_birth,
                nationality                  = EXCLUDED.nationality,
                nationality_source_id        = EXCLUDED.nationality_source_id,
                second_nationality           = EXCLUDED.second_nationality,
                second_nationality_source_id = EXCLUDED.second_nationality_source_id,
                position_raw                 = EXCLUDED.position_raw
            """,
            rows,
        )
        return cur.rowcount


# ------------------------------------------------------------------ #
# player_squads table — active spell lookup
# ------------------------------------------------------------------ #

def get_active_squad_player_ids(
    conn,
    source_team_id: str,
    source_season_id: str,
) -> set[str]:
    """
    Return the set of source_player_ids currently active (end_date IS NULL)
    for this team-season. Used by the diff logic.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT p.source_player_id
            FROM   silver.player_squads ps
            JOIN   silver.players p
                   ON p.player_id = ps.player_id
            JOIN   silver.teams t
                   ON t.team_id = ps.team_id
            JOIN   silver.competition_seasons cs
                   ON cs.competition_season_id = ps.competition_season_id
            WHERE  t.source_team_id    = %s
              AND  cs.source_season_id = %s
              AND  ps.end_date IS NULL
            """,
            (source_team_id, source_season_id),
        )
        return {row[0] for row in cur.fetchall()}


# ------------------------------------------------------------------ #
# player_squads table — insert / close
# ------------------------------------------------------------------ #

def insert_squad_entries(conn, entries: list[SquadEntry]) -> int:
    """
    Insert new player_squads rows, resolving source IDs to internal IDs
    via subquery — no application-side ID resolution needed.

    Skips any entry where team_id or competition_season_id cannot be resolved
    (e.g. team not yet in the teams table) with a warning.

    Returns the number of rows inserted.
    """
    if not entries:
        return 0

    inserted = 0
    with conn.cursor() as cur:
        for e in entries:
            cur.execute(
                """
                INSERT INTO silver.player_squads (
                    player_id,
                    team_id,
                    competition_season_id,
                    start_date,
                    end_date,
                    shirt_number,
                    squad_role,
                    transfer_type
                )
                SELECT
                    p.player_id,
                    t.team_id,
                    cs.competition_season_id,
                    %s, %s, %s, %s, %s
                FROM  silver.players p
                JOIN  silver.teams t
                      ON t.source_team_id = %s
                JOIN  silver.competition_seasons cs
                      ON cs.source_season_id = %s
                WHERE p.source_player_id = %s
                """,
                (
                    e.start_date,
                    e.end_date,
                    e.shirt_number,
                    e.squad_role,
                    e.transfer_type,
                    e.source_team_id,
                    e.source_season_id,
                    e.source_player_id,
                ),
            )
            if cur.rowcount == 0:
                logger.warning(
                    "Could not insert squad entry for player '%s' — "
                    "team '%s' or season '%s' not found in DB.",
                    e.source_player_id,
                    e.source_team_id,
                    e.source_season_id,
                )
            else:
                inserted += cur.rowcount

    return inserted


def close_squad_entries(
    conn,
    source_player_ids: set[str],
    source_team_id: str,
    source_season_id: str,
    end_date: date,
) -> int:
    """
    Set end_date on active squad rows for players no longer in the snapshot.
    Only rows with end_date IS NULL are closed — already-closed rows are untouched.

    Returns the number of rows updated.
    """
    if not source_player_ids:
        return 0

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            UPDATE silver.player_squads ps
            SET    end_date = %s
            FROM   silver.players p,
                   silver.teams t,
                   silver.competition_seasons cs
            WHERE  ps.player_id             = p.player_id
              AND  ps.team_id               = t.team_id
              AND  ps.competition_season_id = cs.competition_season_id
              AND  p.source_player_id       IN %s
              AND  t.source_team_id         = %s
              AND  cs.source_season_id      = %s
              AND  ps.end_date IS NULL
            """,
            [(end_date, tuple(source_player_ids), source_team_id, source_season_id)],
            template="(%s, %s, %s, %s)",
        )
        return cur.rowcount


def update_shirt_numbers(
    conn,
    entries: list[SquadEntry],
    source_team_id: str,
    source_season_id: str,
) -> int:
    """
    Update shirt_number on active rows where it has changed.
    A shirt number change does NOT open a new spell — it's a correction in place.

    Returns the number of rows updated.
    """
    if not entries:
        return 0

    updated = 0
    with conn.cursor() as cur:
        for e in entries:
            if e.shirt_number is None:
                continue
            cur.execute(
                """
                UPDATE silver.player_squads ps
                SET    shirt_number = %s
                FROM   silver.players p,
                       silver.teams t,
                       silver.competition_seasons cs
                WHERE  ps.player_id             = p.player_id
                  AND  ps.team_id               = t.team_id
                  AND  ps.competition_season_id = cs.competition_season_id
                  AND  p.source_player_id       = %s
                  AND  t.source_team_id         = %s
                  AND  cs.source_season_id      = %s
                  AND  ps.end_date IS NULL
                  AND  ps.shirt_number         IS DISTINCT FROM %s
                """,
                (e.shirt_number, e.source_player_id, source_team_id, source_season_id, e.shirt_number),
            )
            updated += cur.rowcount

    return updated


# ------------------------------------------------------------------ #
# DDL — squad_snapshot_log
# ------------------------------------------------------------------ #

CREATE_SNAPSHOT_LOG_TABLE = """
CREATE TABLE IF NOT EXISTS squad_snapshot_log (
    log_id             SERIAL      PRIMARY KEY,
    source_team_id     VARCHAR(50) NOT NULL,
    source_season_id   VARCHAR(50) NOT NULL,
    competition_code   VARCHAR(10) NOT NULL,
    season             VARCHAR(10) NOT NULL,
    snapshot_date      DATE        NOT NULL,
    team_name          VARCHAR(100),
    players_upserted   INT         NOT NULL DEFAULT 0,
    entries_inserted   INT         NOT NULL DEFAULT 0,
    entries_closed     INT         NOT NULL DEFAULT 0,
    processed_at       TIMESTAMP   NOT NULL DEFAULT NOW(),

    UNIQUE (source_team_id, source_season_id, snapshot_date)
);
"""