-- ═══════════════════════════════════════════════════════════════════════════
-- gold_game_states.sql — DDL for the game-state layer
-- ═══════════════════════════════════════════════════════════════════════════
--
-- Spec: md/GOLD_LAYER.md §4.8 (definition, measured numbers), §9.5 gate G17.
--
-- Creates:
--   gold.match_game_states   one row per match segment of constant state
--   gold.team_game_states    VIEW: the same segments from each team's side,
--                            with winning / drawing / losing labels
--
-- Upstream contract: silver.events.{home,away}_{goals,players}_before, written
-- by src/silver/events/game_state.py. A segment is a maximal run of events in
-- one period (1 or 2) with the same (score, players on the pitch). Goals only
-- rise and players only fall, so each state occurs in exactly one run per
-- period and GROUP BY is enough — no gaps-and-islands needed.
--
-- Clock: minute*60 + second, the same clock as possession_pct (§4.5.0). A
-- state change takes effect at the clock of the event that caused it (the
-- goal, the red card); the first segment of a period starts at the period's
-- first event and the last ends at its last event. Segment durations in a
-- period therefore sum to exactly that period's clock span (gate G17).
--
-- Idempotent: safe to run repeatedly. Drops only the view, which it recreates.
-- ═══════════════════════════════════════════════════════════════════════════

CREATE SCHEMA IF NOT EXISTS gold;


CREATE TABLE IF NOT EXISTS gold.match_game_states (
    match_id          INT      NOT NULL REFERENCES silver.matches(match_id),
    segment_no        SMALLINT NOT NULL,          -- 1.. in match order
    period            SMALLINT NOT NULL CHECK (period IN (1, 2)),

    -- ── the state (before-event values of every event in the segment) ─────
    home_goals        SMALLINT NOT NULL,
    away_goals        SMALLINT NOT NULL,
    home_players      SMALLINT NOT NULL,
    away_players      SMALLINT NOT NULL,

    -- ── extent ─────────────────────────────────────────────────────────────
    start_event_id    BIGINT   NOT NULL REFERENCES silver.events(event_id),
    end_event_id      BIGINT   NOT NULL REFERENCES silver.events(event_id),
    start_json_index  DOUBLE PRECISION NOT NULL,
    end_json_index    DOUBLE PRECISION NOT NULL,
    event_count       INT      NOT NULL,
    start_clock_sec   INT      NOT NULL,
    end_clock_sec     INT      NOT NULL,
    duration_sec      INT      NOT NULL CHECK (duration_sec >= 0),

    PRIMARY KEY (match_id, segment_no)
);


-- Team-relative reading. One row per segment per team. Labels live here, not
-- in storage, so a different bucketing is a new view, not a rebuild.
DROP VIEW IF EXISTS gold.team_game_states;
CREATE VIEW gold.team_game_states AS
SELECT
    s.match_id,
    s.segment_no,
    s.period,
    side.team_id,
    side.opponent_team_id,
    side.is_home,
    side.goals_for,
    side.goals_against,
    (side.goals_for - side.goals_against)::smallint           AS goal_diff,
    CASE WHEN side.goals_for > side.goals_against THEN 'winning'
         WHEN side.goals_for < side.goals_against THEN 'losing'
         ELSE 'drawing' END                                    AS score_state,
    side.players_for,
    side.players_against,
    CASE WHEN side.players_for > side.players_against THEN 'advantage'
         WHEN side.players_for < side.players_against THEN 'disadvantage'
         ELSE 'even' END                                       AS numbers_state,
    s.start_event_id, s.end_event_id,
    s.start_json_index, s.end_json_index,
    s.start_clock_sec, s.end_clock_sec, s.duration_sec
FROM gold.match_game_states s
JOIN silver.matches m USING (match_id)
CROSS JOIN LATERAL (VALUES
    (m.home_team_id, m.away_team_id, TRUE,
     s.home_goals, s.away_goals, s.home_players, s.away_players),
    (m.away_team_id, m.home_team_id, FALSE,
     s.away_goals, s.home_goals, s.away_players, s.home_players)
) AS side(team_id, opponent_team_id, is_home,
          goals_for, goals_against, players_for, players_against);
