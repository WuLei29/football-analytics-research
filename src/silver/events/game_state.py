"""
game_state.py
─────────────
Running match state on every silver.events row: the scoreline and the number
of players on the pitch for each side, BEFORE the event.

    home_goals_before, away_goals_before       SMALLINT
    home_players_before, away_players_before   SMALLINT

"Before" means the event that changes the state still carries the old one: a
Goal row holds the score it was scored at, a red card row the eleven the player
was sent off from. The next event (by json_index, event_id) carries the new
state. Labels (winning / drawing / losing) are deliberately NOT stored — they
are one CASE on team_id = home_team_id, and the bucketing is the analyst's.

Rules (validated 27 Sep 2026 over 496 matches — md/GOLD_LAYER.md §4.8):
  * Goal events count for the scorer's team, except own goals (Q28), which
    count for the opponent. The final state reproduces silver.matches
    home_score / away_score on 496/496 matches, half time too.
  * A side loses a player on a Card with Q32 (second yellow) or Q33 (red),
    or a Player retired (injured, no substitutes left), ONLY if that player was
    on the pitch at that moment: in the starting XI or brought on by an earlier
    Player on, and not taken off by an earlier Player off. 17 of the 136 red
    cards are to bench players (10) or staff with no player_id (7) and change
    nothing.

Ordering is (json_index, event_id), per the project convention.

Idempotent. By default only matches with a NULL state column are processed, so
the events pipeline pays for new matches only; --all recomputes everything and
writes only the rows whose value changed.

Run:
    python -m src.silver.events.game_state
    python -m src.silver.events.game_state --all
    python -m src.silver.events.game_state --match-ids 1 2 3
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[3]

log = logging.getLogger(__name__)

ADD_COLUMNS_SQL = """
ALTER TABLE silver.events
    ADD COLUMN IF NOT EXISTS home_goals_before   SMALLINT,
    ADD COLUMN IF NOT EXISTS away_goals_before   SMALLINT,
    ADD COLUMN IF NOT EXISTS home_players_before SMALLINT,
    ADD COLUMN IF NOT EXISTS away_players_before SMALLINT;
"""

NULL_STATE_MATCHES_SQL = """
SELECT DISTINCT match_id FROM silver.events
WHERE home_goals_before IS NULL OR home_players_before IS NULL
ORDER BY 1;
"""

# %(match_ids)s :: int[] — NULL means every match.
UPDATE_SQL = """
WITH ev AS (
    SELECT e.event_id, e.match_id, e.json_index, e.team_id, e.player_id,
           e.event_type,
           m.home_team_id,
           e.raw_data->'qualifier' @> '[{"qualifierId": 28}]' AS q_own_goal,
           (e.raw_data->'qualifier' @> '[{"qualifierId": 32}]'
            OR e.raw_data->'qualifier' @> '[{"qualifierId": 33}]') AS q_sent_off
    FROM silver.events e
    JOIN silver.matches m USING (match_id)
    WHERE %(match_ids)s::int[] IS NULL OR e.match_id = ANY (%(match_ids)s::int[])
),

subs AS (
    SELECT match_id, player_id, json_index, event_id, event_type
    FROM ev
    WHERE event_type IN ('Player on', 'Player off', 'Player retired')
),

-- A dismissal only changes the numbers if the player was on the pitch.
removals AS (
    SELECT ev.event_id
    FROM ev
    JOIN silver.match_lineups ml
      ON ml.match_id = ev.match_id AND ml.player_id = ev.player_id
    WHERE ((ev.event_type = 'Card' AND ev.q_sent_off)
           OR ev.event_type = 'Player retired')
      AND (ml.starting_xi
           OR EXISTS (SELECT 1 FROM subs s
                      WHERE s.match_id = ev.match_id AND s.player_id = ev.player_id
                        AND s.event_type = 'Player on'
                        AND (s.json_index, s.event_id) < (ev.json_index, ev.event_id)))
      AND NOT EXISTS (SELECT 1 FROM subs s
                      WHERE s.match_id = ev.match_id AND s.player_id = ev.player_id
                        AND s.event_type IN ('Player off', 'Player retired')
                        AND (s.json_index, s.event_id) < (ev.json_index, ev.event_id))
),

deltas AS (
    SELECT ev.event_id, ev.match_id, ev.json_index,
           CASE WHEN ev.event_type = 'Goal'
                 AND (ev.team_id = ev.home_team_id) <> ev.q_own_goal
                THEN 1 ELSE 0 END AS home_goal,
           CASE WHEN ev.event_type = 'Goal'
                 AND (ev.team_id = ev.home_team_id) = ev.q_own_goal
                THEN 1 ELSE 0 END AS away_goal,
           CASE WHEN r.event_id IS NOT NULL AND ev.team_id =  ev.home_team_id
                THEN 1 ELSE 0 END AS home_off,
           CASE WHEN r.event_id IS NOT NULL AND ev.team_id <> ev.home_team_id
                THEN 1 ELSE 0 END AS away_off
    FROM ev
    LEFT JOIN removals r USING (event_id)
),

state AS (
    -- Running totals EXCLUDING the current row: the state before the event.
    SELECT event_id,
           (COALESCE(sum(home_goal) OVER w, 0))::smallint      AS home_goals_before,
           (COALESCE(sum(away_goal) OVER w, 0))::smallint      AS away_goals_before,
           (11 - COALESCE(sum(home_off) OVER w, 0))::smallint  AS home_players_before,
           (11 - COALESCE(sum(away_off) OVER w, 0))::smallint  AS away_players_before
    FROM deltas
    WINDOW w AS (PARTITION BY match_id ORDER BY json_index, event_id
                 ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
)
UPDATE silver.events e
SET home_goals_before   = s.home_goals_before,
    away_goals_before   = s.away_goals_before,
    home_players_before = s.home_players_before,
    away_players_before = s.away_players_before
FROM state s
WHERE e.event_id = s.event_id
  -- Write only what changed: a full recompute over loaded rows is then a
  -- no-op instead of a rewrite of the whole heap (CLAUDE.md, VACUUM note).
  AND (e.home_goals_before,   e.away_goals_before,
       e.home_players_before, e.away_players_before)
      IS DISTINCT FROM
      (s.home_goals_before,   s.away_goals_before,
       s.home_players_before, s.away_players_before);
"""

# The state after a match's last event must be its final score. The last
# event's "before" state plus its own delta; a match cannot end on a goal
# event, so the last row's before-state is the final state.
VALIDATE_SQL = """
WITH last_ev AS (
    SELECT DISTINCT ON (match_id)
           match_id, home_goals_before, away_goals_before
    FROM silver.events
    WHERE %(match_ids)s::int[] IS NULL OR match_id = ANY (%(match_ids)s::int[])
    ORDER BY match_id, json_index DESC, event_id DESC
)
SELECT l.match_id, l.home_goals_before, l.away_goals_before,
       m.home_score, m.away_score
FROM last_ev l JOIN silver.matches m USING (match_id)
WHERE (l.home_goals_before, l.away_goals_before)
      IS DISTINCT FROM (m.home_score, m.away_score);
"""


def enrich_game_state(conn, match_ids: list[int] | None = None,
                      recompute_all: bool = False) -> dict:
    """Populate the four state columns and validate against the final score.

    match_ids: restrict to these matches. Otherwise every match with a NULL
    state column, or every match when recompute_all is True.

    Raises RuntimeError (after rolling back) if any processed match does not
    end on its silver.matches score.
    """
    with conn.cursor() as cur:
        cur.execute(ADD_COLUMNS_SQL)

        if not match_ids and not recompute_all:
            cur.execute(NULL_STATE_MATCHES_SQL)
            match_ids = [r[0] for r in cur.fetchall()]
            if not match_ids:
                conn.commit()
                return {"matches": 0, "events_updated": 0}

        params = {"match_ids": match_ids or None}
        cur.execute(UPDATE_SQL, params)
        updated = cur.rowcount

        cur.execute(VALIDATE_SQL, params)
        bad = cur.fetchall()
        if bad:
            conn.rollback()
            sample = ", ".join(
                f"match {m}: {h}-{a} vs {hs}-{as_}" for m, h, a, hs, as_ in bad[:5]
            )
            raise RuntimeError(
                f"game state does not reproduce the final score on {len(bad)} "
                f"match(es) ({sample}). Nothing was written."
            )

    conn.commit()
    return {"matches": len(match_ids) if match_ids else "all",
            "events_updated": updated}


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Populate the running scoreline / players-on-pitch columns on silver.events"
    )
    parser.add_argument("--dsn", default=None)
    parser.add_argument("--match-ids", nargs="+", type=int)
    parser.add_argument("--all", action="store_true",
                        help="recompute every match (writes only changed rows)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s  %(levelname)-7s  %(message)s",
                        datefmt="%H:%M:%S")
    load_dotenv(PROJECT_ROOT / ".env")
    dsn = args.dsn or os.getenv("FOOTBALL_DB_DSN")
    if not dsn:
        print("Error: FOOTBALL_DB_DSN not set. Use --dsn or set the env variable.",
              file=sys.stderr)
        sys.exit(1)

    conn = psycopg2.connect(dsn)
    try:
        result = enrich_game_state(conn, args.match_ids, recompute_all=args.all)
        log.info("game state: %s matches, %d events updated",
                 result["matches"], result["events_updated"])
    finally:
        conn.close()


if __name__ == "__main__":
    main()
