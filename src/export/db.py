"""
db.py
-----
Every SQL statement the export runs. Nothing else in src/export/ touches
psycopg2, the same separation the events pipeline uses (parser/carries/xt are
pure, db.py is the only SQL).

Spec: md/WEB_DATA.md §4, §5, §6.

The connection is opened READ ONLY: the export is a consumer of gold and
silver and must never be able to write to either (WEB_PLAN.md §7).
"""

from __future__ import annotations

import os
from typing import Any

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

from .config import PROFILE_METRICS


def connect():
    """Open a read-only connection using FOOTBALL_DB_DSN from .env."""
    load_dotenv()
    dsn = os.getenv("FOOTBALL_DB_DSN")
    if not dsn:
        raise EnvironmentError("FOOTBALL_DB_DSN is not set in your .env file.")
    conn = psycopg2.connect(dsn)
    conn.set_session(readonly=True, autocommit=True)
    return conn


def _rows(conn, sql: str, params: dict | tuple | None = None) -> list[dict[str, Any]]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, params)
        return [dict(row) for row in cur.fetchall()]


def _one(conn, sql: str, params: dict | tuple | None = None) -> dict[str, Any] | None:
    found = _rows(conn, sql, params)
    return found[0] if found else None


# ---------------------------------------------------------------------------
# Column validation
# ---------------------------------------------------------------------------

def assert_columns_exist(conn, table: str, columns: list[str]) -> None:
    """Fail loudly on a metric key that is not a real column (§11).

    The registries in config.py are interpolated into SELECT lists, so a typo
    would otherwise surface as a Postgres error deep inside a build or -- worse,
    if the name happened to exist elsewhere -- as a plausible wrong number.
    This is the export's equivalent of gold gate G12.
    """
    schema, name = table.split(".")
    actual = {
        row["column_name"]
        for row in _rows(
            conn,
            """SELECT column_name FROM information_schema.columns
               WHERE table_schema = %s AND table_name = %s""",
            (schema, name),
        )
    }
    missing = [c for c in columns if c not in actual]
    if missing:
        raise KeyError(f"{table} has no column(s): {', '.join(sorted(missing))}")


# ---------------------------------------------------------------------------
# Seasons and teams (manifest, §4)
# ---------------------------------------------------------------------------

SEASON_SQL = """
SELECT cs.competition_season_id,
       s.label                AS season_label,
       c.competition_code,
       c.name                 AS competition_name,
       c.tier_level,
       cs.num_teams,
       cs.total_matchdays
FROM silver.competition_seasons cs
JOIN silver.competitions c USING (competition_id)
JOIN silver.seasons      s USING (season_id)
WHERE cs.competition_season_id = %(cs_id)s
"""


def fetch_season(conn, cs_id: int) -> dict[str, Any]:
    row = _one(conn, SEASON_SQL, {"cs_id": cs_id})
    if row is None:
        raise LookupError(f"competition_season_id {cs_id} does not exist in silver")
    return row


SEASON_AGG_SQL = """
SELECT count(*)                        AS clubs,
       min(matches_played)             AS min_played,
       max(matches_played)             AS max_played,
       max(matchdays_scheduled)        AS matchdays_scheduled,
       max(last_matchday_played)       AS last_matchday_played,
       min(first_match_date)           AS first_match_date,
       max(through_match_date)         AS through_match_date
FROM gold.team_season_stats
WHERE competition_season_id = %(cs_id)s
"""


def fetch_season_aggregate(conn, cs_id: int) -> dict[str, Any]:
    """League-wide coverage of a season, from the 20 gold rows."""
    row = _one(conn, SEASON_AGG_SQL, {"cs_id": cs_id})
    if not row or not row["clubs"]:
        raise LookupError(
            f"gold.team_season_stats is empty for competition_season_id {cs_id}; "
            "run `python -m src.gold.build_gold` before exporting"
        )
    return row


MATCHDAY_COUNTS_SQL = """
SELECT matchday, count(*) AS matches
FROM silver.matches
WHERE competition_season_id = %(cs_id)s AND matchday IS NOT NULL
GROUP BY matchday
"""


def fetch_matchday_counts(conn, cs_id: int) -> dict[int, int]:
    """matchday -> matches loaded. Feeds the §3.5 coverage derivation."""
    return {
        int(row["matchday"]): int(row["matches"])
        for row in _rows(conn, MATCHDAY_COUNTS_SQL, {"cs_id": cs_id})
    }


TEAM_SQL = """
SELECT team_id, name, short_name, abbreviation, city
FROM silver.teams
WHERE team_id = %(team_id)s
"""


def fetch_team(conn, team_id: int) -> dict[str, Any]:
    row = _one(conn, TEAM_SQL, {"team_id": team_id})
    if row is None:
        raise LookupError(f"team_id {team_id} does not exist in silver.teams")
    return row


# ---------------------------------------------------------------------------
# League table + team profile (§5)
# ---------------------------------------------------------------------------

TABLE_COLUMNS = [
    "team_id", "team_name", "team_short_name", "team_abbreviation",
    "league_position", "matches_played", "wins", "draws", "losses",
    "goals_for", "goals_against", "goal_difference",
    "xg_for", "xg_against", "xg_difference", "points",
]


def fetch_league_table(conn, cs_id: int) -> list[dict[str, Any]]:
    """One row per club: standings columns plus the 24 profile metrics (§5.1)."""
    metric_cols = [m.key for m in PROFILE_METRICS]
    assert_columns_exist(conn, "gold.team_season_stats", TABLE_COLUMNS + metric_cols)
    cols = ", ".join(TABLE_COLUMNS + metric_cols)
    return _rows(
        conn,
        f"""SELECT {cols}
            FROM gold.team_season_stats
            WHERE competition_season_id = %(cs_id)s
            ORDER BY league_position""",
        {"cs_id": cs_id},
    )


# ---------------------------------------------------------------------------
# Season overview (§6)
# ---------------------------------------------------------------------------

TEAM_SEASON_COLUMNS = [
    "matches_played", "matchdays_scheduled", "last_matchday_played",
    "wins", "draws", "losses", "points", "points_per_match", "league_position",
    "goals_for", "goals_against", "goal_difference", "form_last_5",
    "xg_for", "xg_against", "xg_difference", "possession_pct", "ppda",
    "set_piece_goals_for",
]


def fetch_team_season(conn, cs_id: int, team_id: int) -> dict[str, Any]:
    assert_columns_exist(conn, "gold.team_season_stats", TEAM_SEASON_COLUMNS)
    cols = ", ".join(TEAM_SEASON_COLUMNS)
    row = _one(
        conn,
        f"""SELECT {cols}
            FROM gold.team_season_stats
            WHERE competition_season_id = %(cs_id)s AND team_id = %(team_id)s""",
        {"cs_id": cs_id, "team_id": team_id},
    )
    if row is None:
        raise LookupError(
            f"gold.team_season_stats has no row for team {team_id} "
            f"in competition_season {cs_id}"
        )
    return row


def fetch_kpi_column(conn, cs_id: int, column: str) -> list[dict[str, Any]]:
    """Every club's value of one KPI, for the rank chips (§6)."""
    assert_columns_exist(conn, "gold.team_season_stats", [column])
    return _rows(
        conn,
        f"""SELECT team_id, {column} AS value
            FROM gold.team_season_stats
            WHERE competition_season_id = %(cs_id)s""",
        {"cs_id": cs_id},
    )


TEAM_MATCHES_SQL = """
SELECT tms.match_id,
       tms.matchday,
       tms.match_date,
       tms.is_home,
       tms.result,
       tms.goals_for,
       tms.goals_against,
       tms.xg_for,
       tms.xg_against,
       tms.xt_for,
       tms.xt_against,
       opp.team_id      AS opponent_team_id,
       opp.name         AS opponent_name,
       opp.short_name   AS opponent_short_name,
       opp.abbreviation AS opponent_abbr
FROM gold.team_match_stats tms
JOIN silver.teams opp ON opp.team_id = tms.opponent_team_id
WHERE tms.competition_season_id = %(cs_id)s AND tms.team_id = %(team_id)s
ORDER BY tms.matchday, tms.match_date
"""


def fetch_team_matches(conn, cs_id: int, team_id: int) -> list[dict[str, Any]]:
    return _rows(conn, TEAM_MATCHES_SQL, {"cs_id": cs_id, "team_id": team_id})


def fetch_leaders(conn, cs_id: int, team_id: int, metric: str, limit: int
                  ) -> list[dict[str, Any]]:
    """Top `limit` players of one club by `metric` (§6 leader boxes).

    Ties break on minutes played then name, so a refresh that changes nothing
    does not reshuffle the box.
    """
    assert_columns_exist(conn, "gold.player_season_stats", [metric])
    return _rows(
        conn,
        f"""SELECT player_id, player_name, shirt_number,
                   primary_position_group, minutes_played,
                   {metric} AS value
            FROM gold.player_season_stats
            WHERE competition_season_id = %(cs_id)s
              AND team_id = %(team_id)s
              AND {metric} IS NOT NULL
            ORDER BY {metric} DESC, minutes_played DESC, player_name
            LIMIT %(limit)s""",
        {"cs_id": cs_id, "team_id": team_id, "limit": limit},
    )
