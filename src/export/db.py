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
    "xg_for", "xg_against", "xg_difference", "possession_pct",
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


def fetch_leaders(conn, cs_id: int, team_id: int, metric: str, limit: int,
                  min_minutes: float = 0, exclude_gk: bool = False,
                  ) -> list[dict[str, Any]]:
    """Top `limit` players of one club by `metric` (§6 leader boxes).

    `min_minutes` is the §3.6 floor, applied to per-90 boxes only, and
    `exclude_gk` drops keepers from a box where their number is not
    comparable (VAEP). Both default to "everyone", which is what the count
    boxes want.

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
              AND pss.minutes_played >= %(min_minutes)s
              {"AND pss.primary_position_group <> 'GK'" if exclude_gk else ""}
            ORDER BY pss.{metric} DESC, pss.minutes_played DESC, pss.player_name
            LIMIT %(limit)s""",
        {"cs_id": cs_id, "team_id": team_id, "limit": limit,
         "min_minutes": min_minutes},
    )


# ---------------------------------------------------------------------------
# Player percentiles (§3.6, §9)
# ---------------------------------------------------------------------------

def fetch_percentile_metrics(conn) -> list[dict[str, Any]]:
    """gold.percentile_metrics: which metric is ranked for which role, which
    way is good, and the attempt floor. The site reads the same seed gold
    ranks with, so the two cannot disagree on WHAT is ranked."""
    return _rows(
        conn,
        """SELECT metric, position_group, higher_is_better,
                  attempts_column, min_attempts
           FROM gold.percentile_metrics
           ORDER BY metric, position_group""",
    )


def fetch_player_season_rows(conn, cs_id: int, columns: list[str]) -> list[dict[str, Any]]:
    """Every player-club row of one season, with the given metric columns.

    The whole league, not one club: a percentile is a rank among every La
    Liga player of the same role. Ordered on the primary key so the output
    is stable between runs.
    """
    assert_columns_exist(conn, "gold.player_season_stats", columns)
    cols = ", ".join(f"pss.{c}" for c in sorted(set(columns)))
    return _rows(
        conn,
        f"""SELECT pss.player_id, pss.team_id, pss.role_group,
                   pss.minutes_played, pss.team_matches, {cols}
            FROM gold.player_season_stats pss
            WHERE pss.competition_season_id = %(cs_id)s
            ORDER BY pss.player_id, pss.team_id""",
        {"cs_id": cs_id},
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
    "passes_completed", "pass_completion_rate", "final_third_entries",
    "crosses_completed", "xt_for", "yellow_cards", "red_cards",
    "fouls_committed", "duels_won", "ball_recoveries", "corners_for",
    "xg_open_play", "xg_set_piece",
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
       period,
       greatest(minute, 1) AS minute,
       sum(xt)             AS xt
FROM silver.events
WHERE match_id = %(match_id)s AND xt IS NOT NULL AND period IN (1, 2)
GROUP BY team_id, period, greatest(minute, 1)
"""


def fetch_momentum(conn, match_id: int) -> list[dict[str, Any]]:
    """xT per team per (period, minute). Minute 0 is folded into minute 1.

    The stream's clock starts at 0 (kick-off is second 0 of minute 0) and the
    chart's first bin is minute 1, so those few events belong to the opening
    bin rather than to a bin the axis never draws.

    The period is part of the key because the clock does NOT run through:
    the second half starts again at minute 45, so a first half with three
    minutes of stoppage has minutes 45-47 twice in the same match. Grouping
    by minute alone stacked the end of one half onto the start of the other.
    """
    return _rows(conn, MOMENTUM_SQL, {"match_id": match_id})


# Every name the site prints comes from `silver.players.match_name` — the
# provider's short display form ("Pere Milla"), the one column that is filled
# for every player and never drifts. `silver.events.player_name` is a copy
# taken at load time and disagrees with it on ~17k rows (accents, mostly), so
# the event queries join `players` rather than read the copy.
MARKERS_SQL = """
SELECT e.event_id, e.minute, e.period, e.type_id, e.event_type, e.team_id,
       e.player_id,
       COALESCE(p.match_name, e.player_name) AS player_name,
       e.raw_data -> 'qualifier' AS qualifiers
FROM silver.events e
LEFT JOIN silver.players p ON p.player_id = e.player_id
WHERE e.match_id = %(match_id)s
  AND (e.event_type = 'Goal' OR e.type_id IN (17, 19, 30))
ORDER BY e.period, e.minute, e.json_index, e.event_id
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
ORDER BY passes DESC, 1, 2
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
SELECT e.event_id,
       e.team_id,
       e.player_id,
       COALESCE(p.match_name, e.player_name) AS player_name,
       e.minute,
       e.event_type,
       e.x, e.y, e.xg,
       e.shot_play_pattern,
       e.first_time,
       e.goal_mouth_y,
       e.goal_mouth_z,
       e.raw_data -> 'qualifier' @> '[{"qualifierId": 82}]'  AS blocked,
       e.raw_data -> 'qualifier' @> '[{"qualifierId": 214}]' AS big_chance,
       e.raw_data -> 'qualifier' @> '[{"qualifierId": 15}]'  AS headed,
       e.raw_data -> 'qualifier' @> '[{"qualifierId": 20}]'  AS right_foot,
       e.raw_data -> 'qualifier' @> '[{"qualifierId": 72}]'  AS left_foot
FROM silver.events e
LEFT JOIN silver.players p ON p.player_id = e.player_id
WHERE e.match_id = %(match_id)s
  AND e.event_type IN ('Goal', 'Attempt Saved', 'Miss', 'Post')
ORDER BY e.period, e.minute, e.json_index, e.event_id
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
-- xT comes from a zone grid, so ties are common: without the tiebreaker the
-- order, and which tied action makes the LIMIT, changed between runs.
ORDER BY xt DESC NULLS LAST, json_index, event_id
LIMIT %(limit)s
"""


def fetch_progression(conn, match_id: int, team_id: int, limit: int
                      ) -> list[dict[str, Any]]:
    return _rows(conn, PROGRESSION_SQL,
                 {"match_id": match_id, "team_id": team_id, "limit": limit})


# Every defensive action of the team, which is what the block is about: the
# shape of the whole block, read against the average line.
#
# This used to be `ORDER BY e.x DESC LIMIT 34`. That drew a press, not a block:
# a team makes ~105 of these a match (median over the 458 loaded), so a third
# of them reached the page -- and not a random third, the third furthest up the
# pitch. `defensive_line_height` is averaged over all of them in
# `gold.team_match_stats`, so the dashed marker sat below the visible cloud and
# read as a bug. Chronological order now, so overlapping marks stack by time
# rather than by depth, with `json_index` as the tiebreaker the rest of the
# pipeline uses.
#
# `Aerial` joined the set on 22 Sep 2026. Opta writes a mirror record for
# both contesting players, so a team's own rows are its aerials won AND lost
# and the 50/50 split is real, not a sampling artefact -- it is the one type
# here that is a genuine two-sided duel. Median per team-match 102 -> 128.
DEFENCE_SQL = """
SELECT e.event_type, e.x, e.y, e.outcome, e.minute,
       COALESCE(p.match_name, e.player_name) AS player_name
FROM silver.events e
LEFT JOIN silver.players p ON p.player_id = e.player_id
WHERE e.match_id = %(match_id)s
  AND e.team_id = %(team_id)s
  AND e.event_type IN ('Tackle', 'Interception', 'Ball recovery',
                       'Challenge', 'Blocked Pass', 'Clearance', 'Aerial')
  AND e.x IS NOT NULL
ORDER BY e.minute, e.second, e.json_index, e.event_id
"""


def fetch_defensive_actions(conn, match_id: int, team_id: int
                            ) -> list[dict[str, Any]]:
    return _rows(conn, DEFENCE_SQL, {"match_id": match_id, "team_id": team_id})


SEQUENCES_SQL = """
SELECT sequence_id, period, start_minute, start_second, duration_seconds,
       event_count, pass_count, shot_count, xt, xg, vaep,
       start_zone, start_trigger, outcome, primary_phase,
       final_third_entry, penalty_box_entry,
       sequence_type,
       has_counter_attack, has_high_transition, has_set_piece, has_direct_long,
       has_buildup, has_midblock, has_attacking
FROM gold.sequences
WHERE match_id = %(match_id)s
  AND team_id = %(team_id)s
  AND event_count >= %(min_events)s
ORDER BY xt DESC NULLS LAST, start_minute, start_second, start_event_id
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
       e.minute, e.second,
       e.x, e.y, e.end_x, e.end_y,
       e.outcome,
       e.spadl_result_id,
       e.player_id,
       e.xg,
       e.goal_mouth_y,
       COALESCE(p.short_last_name, p.last_name)   AS surname,
       COALESCE(e.jersey_number, ml.shirt_number) AS shirt_number,
       -- Every qualifier id on the event. match.py reads the cross (Q2), the
       -- pass sub-types (Q1/Q3/Q4) and the restarts (Q5/Q6/Q107/Q124/Q279).
       COALESCE(ARRAY(SELECT (q ->> 'qualifierId')::int
                      FROM jsonb_array_elements(e.raw_data -> 'qualifier') q),
                ARRAY[]::int[])                    AS qualifier_ids
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


# ---------------------------------------------------------------------------
# Squad and player files (§8, §9, README-PLAYER "Data contract")
#
# Everything below is scoped to ONE club's season. The event-level queries
# read the player's own rows of silver.events, which are already in his
# team's attacking frame (§3.1), so nothing here mirrors a coordinate. Every
# ORDER BY ends on event_id, the unique key, so a refresh that changes nothing
# writes the same file.
# ---------------------------------------------------------------------------

# The bio fields of silver.players plus the season row of gold. The names are
# silver's: gold's `player_name` is the full name, too long for a 52px H1.
SQUAD_SQL = """
SELECT pss.player_id,
       p.first_name, p.last_name, p.short_first_name, p.short_last_name,
       p.known_name, p.match_name,
       p.nationality, p.preferred_foot, p.height_cm, p.date_of_birth,
       pss.shirt_number,
       pss.primary_position, pss.primary_position_group, pss.role_group,
       pss.primary_position_pct, pss.is_utility,
       pss.matches_played, pss.matches_started, pss.matches_subbed_off,
       pss.minutes_played, pss.team_matches,
       pss.yellow_cards, pss.red_cards,
       pss.penalties_taken, pss.penalties_scored
FROM gold.player_season_stats pss
JOIN silver.players p ON p.player_id = pss.player_id
WHERE pss.competition_season_id = %(cs_id)s
  AND pss.team_id = %(team_id)s
  AND pss.minutes_played > 0
ORDER BY pss.minutes_played DESC, pss.player_id
"""


def fetch_squad(conn, cs_id: int, team_id: int) -> list[dict[str, Any]]:
    """Every player who has played for the club this season."""
    return _rows(conn, SQUAD_SQL, {"cs_id": cs_id, "team_id": team_id})


def fetch_player_kpi_rows(conn, cs_id: int, team_id: int, columns: list[str]
                          ) -> list[dict[str, Any]]:
    """The KPI columns for every player of the club, for the rank-in-squad
    sub line. Ordered on the key so ties rank the same way every run."""
    assert_columns_exist(conn, "gold.player_season_stats", columns)
    cols = ", ".join(f"pss.{c}" for c in sorted(set(columns)))
    return _rows(
        conn,
        f"""SELECT pss.player_id, pss.minutes_played, pss.team_matches, {cols}
            FROM gold.player_season_stats pss
            WHERE pss.competition_season_id = %(cs_id)s
              AND pss.team_id = %(team_id)s
              AND pss.minutes_played > 0
            ORDER BY pss.player_id""",
        {"cs_id": cs_id, "team_id": team_id},
    )


# The player's events of the season, for this club only: a player who
# changed clubs mid-season gets one page per club, each with its own events.
_PLAYER_EVENTS_FROM = """
FROM silver.events e
JOIN silver.matches m ON m.match_id = e.match_id
WHERE m.competition_season_id = %(cs_id)s
  AND e.team_id = %(team_id)s
  AND e.player_id = %(player_id)s
"""

# One row per match the player played for the club this season, in date
# order: the match context for the page's match filter, the bio counts, and
# the gold columns the headline KPIs are rebuilt from (KPI_FORMULAS). `{cols}`
# is that column list, checked against the table before it is formatted in.
PLAYER_MATCHES_SQL = """
SELECT pms.match_id, pms.matchday, pms.match_date, pms.is_home,
       tms.goals_for, tms.goals_against,
       opp.abbreviation AS opponent_abbr, opp.short_name AS opponent_short_name,
       pms.started, pms.subbed_off, pms.minutes_played,
       pms.yellow_cards, pms.red_cards,
       COALESCE(pms.penalties_taken, 0)  AS penalties_taken,
       COALESCE(pms.penalties_scored, 0) AS penalties_scored
       {cols}
FROM gold.player_match_stats pms
JOIN gold.team_match_stats tms
  ON tms.match_id = pms.match_id AND tms.team_id = pms.team_id
JOIN silver.teams opp ON opp.team_id = pms.opponent_team_id
WHERE pms.competition_season_id = %(cs_id)s
  AND pms.team_id = %(team_id)s
  AND pms.player_id = %(player_id)s
  AND pms.minutes_played > 0
ORDER BY pms.match_date, pms.match_id
"""


def fetch_player_matches(conn, cs_id: int, team_id: int, player_id: int,
                         columns: list[str]) -> list[dict[str, Any]]:
    assert_columns_exist(conn, "gold.player_match_stats", columns)
    # clean_sheet is a boolean; the KPI sums it. Everything else is numeric
    # and may be NULL for a role it does not apply to (saves on a striker).
    cols = "".join(
        f",\n       (pms.{c})::int AS {c}" if c == "clean_sheet"
        else f",\n       COALESCE(pms.{c}, 0) AS {c}"
        for c in sorted(set(columns))
    )
    return _rows(conn, PLAYER_MATCHES_SQL.format(cols=cols),
                 _player_params(cs_id, team_id, player_id))


# `value_assist` is the typeId of the shot the pass created: 16 is a goal
# (assist), 13/14/15 a miss, post or save (key pass). Key passes EXCLUDE
# assists (football-metrics §3).
#
# Every pass of the season ships (16 Oct 2026): the page filters by match, so
# the caps -- the 150 most recent, the 60 best progressive by xT -- are applied
# on the site AFTER the filter, not here. Chronological, so "most recent" is
# the tail of the list.
PLAYER_PASSES_SQL = f"""
SELECT e.match_id, e.x, e.y, e.end_x, e.end_y, e.xt,
       e.outcome = 'success'                       AS completed,
       gold.is_progressive(e.x, e.y, e.end_x, e.end_y) AS progressive,
       e.value_assist IN ('13', '14', '15')        AS key_pass,
       e.value_assist = '16'                       AS assist
{_PLAYER_EVENTS_FROM}
  AND e.event_type = 'Pass'
  AND e.x IS NOT NULL AND e.end_x IS NOT NULL
ORDER BY m.match_date, e.match_id, e.json_index, e.event_id
"""


def fetch_player_passes(conn, cs_id: int, team_id: int, player_id: int
                        ) -> list[dict[str, Any]]:
    return _rows(conn, PLAYER_PASSES_SQL, _player_params(cs_id, team_id, player_id))


# Progressive carries only: all carries are hundreds of short arrows
# (README-PLAYER §3b). 'Carry' is the synthesised type_id = -1. Uncapped and
# chronological, like the passes; the site keeps the best 60 by xT.
PLAYER_CARRIES_SQL = f"""
SELECT e.match_id, e.x, e.y, e.end_x, e.end_y, e.xt, e.outcome = 'success' AS completed
{_PLAYER_EVENTS_FROM}
  AND e.event_type = 'Carry'
  AND e.x IS NOT NULL AND e.end_x IS NOT NULL
  AND gold.is_progressive(e.x, e.y, e.end_x, e.end_y)
ORDER BY m.match_date, e.match_id, e.json_index, e.event_id
"""


def fetch_player_carries(conn, cs_id: int, team_id: int, player_id: int
                         ) -> list[dict[str, Any]]:
    return _rows(conn, PLAYER_CARRIES_SQL, _player_params(cs_id, team_id, player_id))


# The on-ball touch set of gold.player_match_stats.touches, word for word, so
# the heat map and the touch counts on the page describe the same events.
TOUCH_TYPES = ("Pass", "Carry", "Take On", "Ball touch", "Clearance",
               "Ball recovery", "Keeper pick-up", "Claim", "Punch", "Smother",
               "Goal", "Attempt Saved", "Miss", "Post")

PLAYER_TOUCH_ZONES_SQL = f"""
SELECT e.match_id, gold.zone_id(e.x, e.y) AS zone_id, count(*) AS touches
{_PLAYER_EVENTS_FROM}
  AND e.event_type = ANY(%(touch_types)s)
  AND e.x BETWEEN 0 AND 105 AND e.y BETWEEN 0 AND 68
GROUP BY 1, 2
ORDER BY 1, 2
"""


def fetch_player_touch_zones(conn, cs_id: int, team_id: int, player_id: int
                             ) -> list[dict[str, Any]]:
    params = _player_params(cs_id, team_id, player_id)
    params["touch_types"] = list(TOUCH_TYPES)
    return _rows(conn, PLAYER_TOUCH_ZONES_SQL, params)


# Receptions: gold.pass_receptions by receiver. Opta records no receiver; the
# table resolves it as the next same-team action after a completed pass, and
# the reception point is the pass end (GOLD_LAYER §4.7).
PLAYER_RECEPTION_ZONES_SQL = """
SELECT match_id, rx_zone_id AS zone_id, count(*) AS receptions
FROM gold.pass_receptions
WHERE competition_season_id = %(cs_id)s
  AND team_id = %(team_id)s
  AND receiver_id = %(player_id)s
GROUP BY match_id, rx_zone_id
ORDER BY match_id, rx_zone_id
"""

PLAYER_PROGRESSIVE_RECEPTIONS_SQL = """
SELECT match_id, rx_x, rx_y, rx_in_box
FROM gold.pass_receptions
WHERE competition_season_id = %(cs_id)s
  AND team_id = %(team_id)s
  AND receiver_id = %(player_id)s
  AND is_progressive
  AND rx_gap_m <= %(max_gap)s
ORDER BY match_date, match_id, pass_event_id
"""


def fetch_player_reception_zones(conn, cs_id: int, team_id: int, player_id: int
                                 ) -> list[dict[str, Any]]:
    return _rows(conn, PLAYER_RECEPTION_ZONES_SQL,
                 _player_params(cs_id, team_id, player_id))


def fetch_player_progressive_receptions(conn, cs_id: int, team_id: int,
                                        player_id: int, max_gap: float
                                        ) -> list[dict[str, Any]]:
    params = _player_params(cs_id, team_id, player_id)
    params["max_gap"] = max_gap
    return _rows(conn, PLAYER_PROGRESSIVE_RECEPTIONS_SQL, params)


# Same columns and qualifier reads as SHOTS_SQL, so a shot here and the same
# shot on its match page are drawn identically.
PLAYER_SHOTS_SQL = f"""
SELECT e.event_id, e.match_id, m.matchday,
       e.minute, e.event_type, e.x, e.y, e.xg,
       e.shot_play_pattern, e.first_time, e.goal_mouth_y, e.goal_mouth_z,
       e.raw_data -> 'qualifier' @> '[{{"qualifierId": 82}}]'  AS blocked,
       e.raw_data -> 'qualifier' @> '[{{"qualifierId": 214}}]' AS big_chance,
       e.raw_data -> 'qualifier' @> '[{{"qualifierId": 15}}]'  AS headed,
       e.raw_data -> 'qualifier' @> '[{{"qualifierId": 20}}]'  AS right_foot,
       e.raw_data -> 'qualifier' @> '[{{"qualifierId": 72}}]'  AS left_foot
{_PLAYER_EVENTS_FROM}
  AND e.event_type IN ('Goal', 'Attempt Saved', 'Miss', 'Post')
  AND e.x IS NOT NULL
ORDER BY m.match_date, e.match_id, e.period, e.minute, e.json_index, e.event_id
"""


def fetch_player_shots(conn, cs_id: int, team_id: int, player_id: int
                       ) -> list[dict[str, Any]]:
    return _rows(conn, PLAYER_SHOTS_SQL, _player_params(cs_id, team_id, player_id))


# README-PLAYER §3b: won = Tackle success, Interception, Ball recovery,
# Blocked Pass, Clearance; lost = Tackle failure, Attempted Tackle, Challenge.
# Aerials are left out on purpose: they are duels on the percentile cards,
# and on a centre back's map ~150 of them bury every other mark.
PLAYER_DEFENCE_SQL = f"""
SELECT e.match_id, e.event_type, e.x, e.y, e.outcome, e.minute, m.matchday
{_PLAYER_EVENTS_FROM}
  AND e.event_type IN ('Tackle', 'Interception', 'Ball recovery', 'Blocked Pass',
                       'Clearance', 'Attempted Tackle', 'Challenge')
  AND e.x IS NOT NULL
ORDER BY m.match_date, e.match_id, e.json_index, e.event_id
"""


def fetch_player_defence(conn, cs_id: int, team_id: int, player_id: int
                         ) -> list[dict[str, Any]]:
    return _rows(conn, PLAYER_DEFENCE_SQL, _player_params(cs_id, team_id, player_id))


# Every chain the player touched while his team had the ball, with 3+ events
# (GOLD_LAYER §4.1.9). `is_possessing_team` drops the sandwich and deflection
# rows (§4.1.0 #2). The page ranks this index for whatever matches are
# selected, and draws a chain from its match file (WEB_DATA §7.3) -- the same
# sequence_id, because both files come out of the same export run.
PLAYER_SEQUENCE_INDEX_SQL = """
SELECT s.sequence_id, s.match_id, s.start_event_id,
       s.xt, s.vaep, s.shot_count, s.pass_count
FROM gold.sequence_players sp
JOIN gold.sequences s ON s.sequence_id = sp.sequence_id
JOIN silver.matches m ON m.match_id = s.match_id
WHERE sp.competition_season_id = %(cs_id)s
  AND sp.team_id = %(team_id)s
  AND sp.player_id = %(player_id)s
  AND sp.is_possessing_team
  AND s.event_count >= %(min_events)s
ORDER BY m.match_date, s.match_id, s.start_event_id
"""


def fetch_player_sequence_index(conn, cs_id: int, team_id: int, player_id: int,
                                min_events: int) -> list[dict[str, Any]]:
    params = _player_params(cs_id, team_id, player_id)
    params["min_events"] = min_events
    return _rows(conn, PLAYER_SEQUENCE_INDEX_SQL, params)


# The match file's sequence header (SEQUENCES_SQL), for chosen chains across
# matches: the season top lists ship in full so the default view needs no
# fetch.
PLAYER_SEQUENCE_ROWS_SQL = """
SELECT sequence_id, match_id, period, start_minute, start_second, duration_seconds,
       event_count, pass_count, shot_count, xt, xg, vaep,
       start_zone, start_trigger, outcome, primary_phase,
       final_third_entry, penalty_box_entry,
       sequence_type,
       has_counter_attack, has_high_transition, has_set_piece, has_direct_long,
       has_buildup, has_midblock, has_attacking
FROM gold.sequences
WHERE team_id = %(team_id)s
  AND sequence_id = ANY(%(sequence_ids)s)
ORDER BY sequence_id
"""

# SEQUENCE_EVENTS_SQL without the match scope.
PLAYER_SEQUENCE_EVENTS_SQL = """
SELECT e.sequence_id,
       e.event_id,
       e.json_index,
       e.event_type,
       e.minute, e.second,
       e.x, e.y, e.end_x, e.end_y,
       e.outcome,
       e.spadl_result_id,
       e.player_id,
       e.xg,
       e.goal_mouth_y,
       COALESCE(p.short_last_name, p.last_name)   AS surname,
       COALESCE(e.jersey_number, ml.shirt_number) AS shirt_number,
       COALESCE(ARRAY(SELECT (q ->> 'qualifierId')::int
                      FROM jsonb_array_elements(e.raw_data -> 'qualifier') q),
                ARRAY[]::int[])                    AS qualifier_ids
FROM silver.events e
LEFT JOIN silver.players p ON p.player_id = e.player_id
LEFT JOIN silver.match_lineups ml
       ON ml.match_id = e.match_id AND ml.player_id = e.player_id
WHERE e.team_id = %(team_id)s
  AND e.sequence_id = ANY(%(sequence_ids)s)
  AND e.x IS NOT NULL
ORDER BY e.sequence_id, e.json_index, e.event_id
"""


def fetch_player_sequence_rows(conn, team_id: int, sequence_ids: list[str]
                               ) -> list[dict[str, Any]]:
    if not sequence_ids:
        return []
    return _rows(conn, PLAYER_SEQUENCE_ROWS_SQL,
                 {"team_id": team_id, "sequence_ids": sequence_ids})


def fetch_player_sequence_events(conn, team_id: int, sequence_ids: list[str]
                                 ) -> list[dict[str, Any]]:
    if not sequence_ids:
        return []
    return _rows(conn, PLAYER_SEQUENCE_EVENTS_SQL,
                 {"team_id": team_id, "sequence_ids": sequence_ids})


def _player_params(cs_id: int, team_id: int, player_id: int,
                   limit: int | None = None) -> dict[str, Any]:
    params: dict[str, Any] = {"cs_id": cs_id, "team_id": team_id, "player_id": player_id}
    if limit is not None:
        params["limit"] = limit
    return params
