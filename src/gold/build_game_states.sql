-- ═══════════════════════════════════════════════════════════════════════════
-- build_game_states.sql — gold.match_game_states
-- ═══════════════════════════════════════════════════════════════════════════
--
-- Spec: md/GOLD_LAYER.md §4.8. DDL and clock rule: sql/ddl/gold_game_states.sql.
--
-- Runs as:  python -m src.gold.build_gold --step game_states
--
-- Parameter
--   %(match_ids)s :: int[]   NULL = every match (full rebuild, the default).
--
-- Reads only silver (the four *_before columns), so it has no gold
-- dependency. Ordering is (json_index, event_id) throughout.
-- ═══════════════════════════════════════════════════════════════════════════

DELETE FROM gold.match_game_states
WHERE %(match_ids)s::int[] IS NULL
   OR match_id = ANY (%(match_ids)s::int[]);


WITH ev AS (
    SELECT e.match_id, e.period, e.event_id, e.json_index,
           e.minute * 60 + e.second AS clock,
           e.home_goals_before   AS home_goals,
           e.away_goals_before   AS away_goals,
           e.home_players_before AS home_players,
           e.away_players_before AS away_players
    FROM silver.events e
    WHERE e.period IN (1, 2)
      AND (%(match_ids)s::int[] IS NULL OR e.match_id = ANY (%(match_ids)s::int[]))
),

periods AS (
    SELECT match_id, period,
           min(clock) AS period_start_clock,
           max(clock) AS period_end_clock
    FROM ev
    GROUP BY match_id, period
),

-- Goals only rise and players only fall, so a state is one contiguous run
-- within a period: grouping by it is the segmentation.
seg AS (
    SELECT match_id, period, home_goals, away_goals, home_players, away_players,
           (array_agg(event_id ORDER BY json_index,      event_id))[1]      AS start_event_id,
           (array_agg(event_id ORDER BY json_index DESC, event_id DESC))[1] AS end_event_id,
           min(json_index)                                                  AS start_json_index,
           max(json_index)                                                  AS end_json_index,
           count(*)                                                         AS event_count,
           -- clock of the event that ends the state (the goal, the red card)
           (array_agg(clock ORDER BY json_index DESC, event_id DESC))[1]    AS last_clock
    FROM ev
    GROUP BY match_id, period, home_goals, away_goals, home_players, away_players
),

bounded AS (
    SELECT s.*, p.period_start_clock,
           CASE WHEN lead(s.start_json_index) OVER wp IS NULL
                THEN p.period_end_clock
                ELSE s.last_clock END AS end_clock
    FROM seg s
    JOIN periods p USING (match_id, period)
    WINDOW wp AS (PARTITION BY s.match_id, s.period ORDER BY s.start_json_index)
),

clocked AS (
    SELECT b.*,
           COALESCE(lag(b.end_clock) OVER wp, b.period_start_clock) AS start_clock,
           row_number() OVER (PARTITION BY b.match_id
                              ORDER BY b.period, b.start_json_index) AS segment_no
    FROM bounded b
    WINDOW wp AS (PARTITION BY b.match_id, b.period ORDER BY b.start_json_index)
)

INSERT INTO gold.match_game_states (
    match_id, segment_no, period,
    home_goals, away_goals, home_players, away_players,
    start_event_id, end_event_id, start_json_index, end_json_index, event_count,
    start_clock_sec, end_clock_sec, duration_sec
)
SELECT match_id, segment_no, period,
       home_goals, away_goals, home_players, away_players,
       start_event_id, end_event_id, start_json_index, end_json_index, event_count,
       start_clock, end_clock,
       -- GREATEST guards a state-changing event logged with an earlier clock
       -- than the previous boundary; gate G17 checks the sums still close.
       GREATEST(end_clock - start_clock, 0)
FROM clocked;
