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

    The name is `silver.players.match_name` -- the provider's short form,
    "M. Dmitrović" -- not the full name gold carries: a leader box is 88 px
    wide and "Luca Warrick Daeovie Koleosho" does not fit it.
    """
    assert_columns_exist(conn, "gold.player_season_stats", [metric])
    return _rows(
        conn,
        f"""SELECT pss.player_id,
                   COALESCE(p.match_name, pss.player_name) AS player_name,
                   pss.shirt_number,
                   pss.primary_position_group, pss.minutes_played,
                   pss.{metric} AS value
            FROM gold.player_season_stats pss
            LEFT JOIN silver.players p ON p.player_id = pss.player_id
            WHERE pss.competition_season_id = %(cs_id)s
              AND pss.team_id = %(team_id)s
              AND pss.{metric} IS NOT NULL
            ORDER BY pss.{metric} DESC, pss.minutes_played DESC, pss.player_name
            LIMIT %(limit)s""",
        {"cs_id": cs_id, "team_id": team_id, "limit": limit},
    )


# ---------------------------------------------------------------------------
# Match file (§7)
#
# This block is where the export stops reading gold aggregates and starts
# reading silver.events. Every query below is scoped to ONE match, and every
# one of them ships a derived structure -- a shot list, an aggregated network,
# a binned surface, a simplified polyline -- never an event table
# (WEB_PLAN.md §6, level B).
#
# The project rule for anything sequential is ORDER BY (json_index, event_id):
# json_index alone is not unique (GOLD_SEQUENCES.md §6, Fix 0), and a tie there
# silently reorders a possession chain.
# ---------------------------------------------------------------------------

MATCH_SQL = """
SELECT m.match_id,
       m.matchday,
       m.match_date::date AS match_date,
       m.venue,
       m.match_length_min,
       m.home_team_id,
       m.away_team_id,
       m.home_score,
       m.away_score,
       m.home_score_ht,
       m.away_score_ht,
       h.short_name   AS home_short_name,
       h.abbreviation AS home_abbr,
       a.short_name   AS away_short_name,
       a.abbreviation AS away_abbr
FROM silver.matches m
JOIN silver.teams h ON h.team_id = m.home_team_id
JOIN silver.teams a ON a.team_id = m.away_team_id
WHERE m.match_id = %(match_id)s
"""


def fetch_match(conn, match_id: int) -> dict[str, Any]:
    row = _one(conn, MATCH_SQL, {"match_id": match_id})
    if row is None:
        raise LookupError(f"match_id {match_id} does not exist in silver.matches")
    return row


def fetch_team_match_ids(conn, cs_id: int, team_id: int) -> list[int]:
    """Every match of one club in one season, oldest first."""
    return [
        int(row["match_id"])
        for row in _rows(
            conn,
            """SELECT match_id
               FROM gold.team_match_stats
               WHERE competition_season_id = %(cs_id)s AND team_id = %(team_id)s
               ORDER BY matchday, match_date""",
            {"cs_id": cs_id, "team_id": team_id},
        )
    ]


# The 14 totals of §7.2 plus the columns other blocks read from the same row:
# the defensive line the pitch draws and the goals the header prints.
MATCH_TOTAL_COLUMNS = [
    "possession_pct", "xg_for", "shots", "shots_on_target", "big_chances",
    "passes_completed", "final_third_entries", "xt_for", "yellow_cards",
    "red_cards", "fouls_committed", "corners_for", "xg_open_play",
    "xg_set_piece",
]
MATCH_EXTRA_COLUMNS = ["defensive_line_height", "goals_for", "goals_against"]


def fetch_match_team_stats(conn, match_id: int) -> list[dict[str, Any]]:
    """Both rows of gold.team_match_stats for one match."""
    assert_columns_exist(conn, "gold.team_match_stats",
                         MATCH_TOTAL_COLUMNS + MATCH_EXTRA_COLUMNS)
    cols = ", ".join(MATCH_TOTAL_COLUMNS + MATCH_EXTRA_COLUMNS)
    rows = _rows(
        conn,
        f"""SELECT team_id, is_home, {cols}
            FROM gold.team_match_stats
            WHERE match_id = %(match_id)s""",
        {"match_id": match_id},
    )
    if len(rows) != 2:
        raise LookupError(
            f"gold.team_match_stats has {len(rows)} rows for match {match_id}, "
            "expected 2; run `python -m src.gold.build_gold`"
        )
    return rows


MOMENTUM_SQL = """
SELECT team_id,
       greatest(minute, 1) AS minute,
       sum(xt)             AS xt
FROM silver.events
WHERE match_id = %(match_id)s AND xt IS NOT NULL
GROUP BY team_id, greatest(minute, 1)
"""


def fetch_momentum(conn, match_id: int) -> list[dict[str, Any]]:
    """xT per team per minute. Minute 0 is folded into minute 1.

    The stream's clock starts at 0 (kick-off is second 0 of minute 0) and the
    chart's first bin is minute 1, so those few events belong to the opening
    bin rather than to a bin the axis never draws.
    """
    return _rows(conn, MOMENTUM_SQL, {"match_id": match_id})


MARKERS_SQL = """
SELECT event_id, minute, period, type_id, event_type, team_id,
       player_id, player_name,
       raw_data -> 'qualifier' AS qualifiers
FROM silver.events
WHERE match_id = %(match_id)s
  AND (event_type = 'Goal' OR type_id IN (17, 19, 30))
ORDER BY minute, json_index, event_id
"""


def fetch_match_markers(conn, match_id: int) -> list[dict[str, Any]]:
    """Goals, cards, players coming on, and period ends.

    Only typeId 19 (player on) is read for substitutions: 18 and 19 come in
    pairs and drawing both would put two lines on the same minute.
    """
    return _rows(conn, MARKERS_SQL, {"match_id": match_id})


# On-ball events, for a player's mean position. The excluded types are the
# administrative ones that carry a coordinate but describe no touch.
NETWORK_NODES_SQL = """
SELECT ml.player_id,
       COALESCE(p.short_last_name, p.last_name) AS surname,
       ml.shirt_number,
       avg(e.x)          AS x,
       avg(e.y)          AS y,
       max(pms.touches)  AS touches,
       count(e.event_id) AS on_ball_events
FROM silver.match_lineups ml
JOIN silver.players p ON p.player_id = ml.player_id
LEFT JOIN gold.player_match_stats pms
       ON pms.match_id = ml.match_id AND pms.player_id = ml.player_id
LEFT JOIN silver.events e
       ON e.match_id = ml.match_id
      AND e.player_id = ml.player_id
      AND e.x IS NOT NULL
      AND e.event_type NOT IN ('Card', 'Player off', 'Player on',
                               'Team set up', 'Formation change')
WHERE ml.match_id = %(match_id)s
  AND ml.team_id = %(team_id)s
  AND ml.starting_xi
GROUP BY ml.player_id, surname, ml.shirt_number, ml.formation_position
HAVING count(e.event_id) > 0
ORDER BY ml.formation_position
"""


def fetch_network_nodes(conn, match_id: int, team_id: int) -> list[dict[str, Any]]:
    return _rows(conn, NETWORK_NODES_SQL, {"match_id": match_id, "team_id": team_id})


# The receiver of a pass is not a column: it is the next event of the same team
# in (json_index, event_id) order, which after a completed pass is the
# receiving player's own touch or carry.
NETWORK_EDGES_SQL = """
WITH ev AS (
    SELECT team_id, player_id, event_type, outcome,
           lead(player_id) OVER w AS next_player,
           lead(team_id)   OVER w AS next_team
    FROM silver.events
    WHERE match_id = %(match_id)s
    WINDOW w AS (ORDER BY json_index, event_id)
)
SELECT least(player_id, next_player)    AS player_a,
       greatest(player_id, next_player) AS player_b,
       count(*)                         AS passes
FROM ev
WHERE team_id = %(team_id)s
  AND event_type = 'Pass'
  AND outcome = 'success'
  AND next_team = %(team_id)s
  AND player_id IS NOT NULL
  AND next_player IS NOT NULL
  AND next_player <> player_id
GROUP BY 1, 2
HAVING count(*) >= %(min_combinations)s
ORDER BY passes DESC
"""


def fetch_network_edges(conn, match_id: int, team_id: int,
                        min_combinations: int) -> list[dict[str, Any]]:
    return _rows(conn, NETWORK_EDGES_SQL, {
        "match_id": match_id,
        "team_id": team_id,
        "min_combinations": min_combinations,
    })


# Body part, blocked and big chance are qualifier reads (§14 item 3 keeps the
# option of moving to spadl_bodypart_id open). Q82 is what separates a save
# from a block: both arrive as 'Attempt Saved'.
SHOTS_SQL = """
SELECT event_id,
       team_id,
       player_id,
       player_name,
       minute,
       event_type,
       x, y, xg,
       shot_play_pattern,
       first_time,
       goal_mouth_y,
       goal_mouth_z,
       raw_data -> 'qualifier' @> '[{"qualifierId": 82}]'  AS blocked,
       raw_data -> 'qualifier' @> '[{"qualifierId": 214}]' AS big_chance,
       raw_data -> 'qualifier' @> '[{"qualifierId": 15}]'  AS headed,
       raw_data -> 'qualifier' @> '[{"qualifierId": 20}]'  AS right_foot,
       raw_data -> 'qualifier' @> '[{"qualifierId": 72}]'  AS left_foot
FROM silver.events
WHERE match_id = %(match_id)s
  AND event_type IN ('Goal', 'Attempt Saved', 'Miss', 'Post')
ORDER BY minute, json_index, event_id
"""


def fetch_shots(conn, match_id: int) -> list[dict[str, Any]]:
    """Both teams' shots. Mirroring the opponent happens in match.py."""
    return _rows(conn, SHOTS_SQL, {"match_id": match_id})


# The 12 x 8 grid of xt.py (§3.3). Binned with those exact edges, so the
# surface is a straight sum over the cells the model itself scored on.
XT_GRID_SQL = """
SELECT least(floor(x / 105 * 12)::int, 11) AS cx,
       least(floor(y / 68  *  8)::int,  7) AS cy,
       sum(xt)                             AS xt
FROM silver.events
WHERE match_id = %(match_id)s
  AND team_id = %(team_id)s
  AND xt IS NOT NULL
  AND x BETWEEN 0 AND 105
  AND y BETWEEN 0 AND 68
GROUP BY 1, 2
HAVING sum(xt) <> 0
ORDER BY 1, 2
"""


def fetch_xt_grid(conn, match_id: int, team_id: int) -> list[dict[str, Any]]:
    return _rows(conn, XT_GRID_SQL, {"match_id": match_id, "team_id": team_id})


# The same sum on the 30 zones of gold.pitch_zones, which is the grid the rest
# of the site draws on. Bucketed in SQL against the zone edges rather than
# re-binned from the 12 x 8 cells on the site: the two grids do not nest, and
# an area-weighted split of a cell is an estimate where this is exact.
# `x_max`/`y_max` are inclusive on the last strip/channel only, so a shot from
# the goal line (x = 105) still lands in strip 6.
XT_ZONES_SQL = """
SELECT z.zone_id,
       sum(e.xt) AS xt
FROM silver.events e
JOIN gold.pitch_zones z
  ON e.x >= z.x_min AND (e.x < z.x_max OR (z.x_strip = 6 AND e.x <= z.x_max))
 AND e.y >= z.y_min AND (e.y < z.y_max OR (z.y_channel = 5 AND e.y <= z.y_max))
WHERE e.match_id = %(match_id)s
  AND e.team_id = %(team_id)s
  AND e.xt IS NOT NULL
GROUP BY z.zone_id
HAVING sum(e.xt) <> 0
ORDER BY z.zone_id
"""


def fetch_xt_zones(conn, match_id: int, team_id: int) -> list[dict[str, Any]]:
    return _rows(conn, XT_ZONES_SQL, {"match_id": match_id, "team_id": team_id})


PROGRESSION_SQL = """
SELECT event_type, x, y, end_x, end_y, xt, outcome
FROM silver.events
WHERE match_id = %(match_id)s
  AND team_id = %(team_id)s
  AND event_type IN ('Pass', 'Carry')
  AND x IS NOT NULL AND end_x IS NOT NULL
  AND gold.is_progressive(x, y, end_x, end_y)
ORDER BY xt DESC NULLS LAST
LIMIT %(limit)s
"""


def fetch_progression(conn, match_id: int, team_id: int, limit: int
                      ) -> list[dict[str, Any]]:
    return _rows(conn, PROGRESSION_SQL,
                 {"match_id": match_id, "team_id": team_id, "limit": limit})


# The highest defensive actions, which is what the block is about: the shape of
# the press, read against the average line.
DEFENCE_SQL = """
SELECT event_type, x, y, outcome, minute, player_name
FROM silver.events
WHERE match_id = %(match_id)s
  AND team_id = %(team_id)s
  AND event_type IN ('Tackle', 'Interception', 'Ball recovery',
                     'Challenge', 'Blocked Pass', 'Clearance')
  AND x IS NOT NULL
ORDER BY x DESC
LIMIT %(limit)s
"""


def fetch_defensive_actions(conn, match_id: int, team_id: int, limit: int
                            ) -> list[dict[str, Any]]:
    return _rows(conn, DEFENCE_SQL,
                 {"match_id": match_id, "team_id": team_id, "limit": limit})


SEQUENCES_SQL = """
SELECT sequence_id, period, start_minute, start_second, duration_seconds,
       event_count, pass_count, shot_count, xt, xg, vaep,
       start_zone, start_trigger, outcome, primary_phase,
       final_third_entry, penalty_box_entry
FROM gold.sequences
WHERE match_id = %(match_id)s
  AND team_id = %(team_id)s
  AND event_count >= %(min_events)s
ORDER BY xt DESC NULLS LAST, start_minute, start_second
"""


def fetch_sequences(conn, match_id: int, team_id: int, min_events: int
                    ) -> list[dict[str, Any]]:
    """All of the match's sequences above the event floor, best xT first."""
    return _rows(conn, SEQUENCES_SQL, {
        "match_id": match_id, "team_id": team_id, "min_events": min_events,
    })


SEQUENCE_EVENTS_SQL = """
SELECT e.sequence_id,
       e.event_id,
       e.json_index,
       e.event_type,
       e.x, e.y, e.end_x, e.end_y,
       e.outcome,
       e.spadl_result_id,
       e.player_id,
       COALESCE(p.short_last_name, p.last_name)   AS surname,
       COALESCE(e.jersey_number, ml.shirt_number) AS shirt_number,
       e.raw_data -> 'qualifier' @> '[{"qualifierId": 2}]' AS is_cross
FROM silver.events e
LEFT JOIN silver.players p ON p.player_id = e.player_id
LEFT JOIN silver.match_lineups ml
       ON ml.match_id = e.match_id AND ml.player_id = e.player_id
WHERE e.match_id = %(match_id)s
  AND e.team_id = %(team_id)s
  AND e.sequence_id = ANY(%(sequence_ids)s)
  AND e.x IS NOT NULL
ORDER BY e.sequence_id, e.json_index, e.event_id
"""


def fetch_sequence_events(conn, match_id: int, team_id: int,
                          sequence_ids: list[str]) -> list[dict[str, Any]]:
    """Every event of the given sequences, in the project's canonical order.

    Filtered to the sequence owner's events. A sequence carries the OPPONENT's
    failed contests too -- an attempted tackle, a lost aerial, a challenge --
    under the same `sequence_id` (GOLD_SEQUENCES.md §6); they are part of the
    possession but not of the chain being drawn, and without this filter each
    one put an opponent's shirt number on the published team's pitch.
    """
    if not sequence_ids:
        return []
    return _rows(conn, SEQUENCE_EVENTS_SQL, {
        "match_id": match_id, "team_id": team_id, "sequence_ids": sequence_ids,
    })


MATCH_PLAYER_COLUMNS = [
    "ball_recoveries", "passes_completed", "passes_into_final_third", "xg", "xa",
]

MATCH_PLAYERS_SQL = """
SELECT pms.player_id,
       pms.team_id,
       t.abbreviation AS abbr,
       COALESCE(p.match_name, p.known_name, p.full_name) AS name,
       pms.minutes_played,
       {cols}
FROM gold.player_match_stats pms
JOIN silver.players p ON p.player_id = pms.player_id
JOIN silver.teams   t ON t.team_id  = pms.team_id
WHERE pms.match_id = %(match_id)s
"""


def fetch_match_players(conn, match_id: int) -> list[dict[str, Any]]:
    """Both teams' player rows; the four boxes are ranked in match.py."""
    assert_columns_exist(conn, "gold.player_match_stats", MATCH_PLAYER_COLUMNS)
    cols = ", ".join(f"pms.{c}" for c in MATCH_PLAYER_COLUMNS)
    return _rows(conn, MATCH_PLAYERS_SQL.format(cols=cols), {"match_id": match_id})
