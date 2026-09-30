-- ═══════════════════════════════════════════════════════════════════════════
-- build_pass_receptions.sql — gold.pass_receptions
-- ═══════════════════════════════════════════════════════════════════════════
--
-- Spec: md/GOLD_LAYER.md §4.7, §9.5 gate G16.
--
-- Runs as:  python -m src.gold.build_gold --step pass_receptions
--
-- Parameter
--   %(match_ids)s :: int[]   NULL = every completed match.
--
-- Sources
--   silver.events            passes, the receiver's actions, opponent duels
--   gold.team_match_stats    match context (season, opponent, date)
--
-- THE RECEIVER IS RESOLVED, NOT RECORDED (§4.7.1). Opta has no reception event.
-- After a completed pass the receiver is the next event of the SAME team in
-- (json_index, event_id) order -- the ordering every sequential query in this
-- project uses, with event_id as the tiebreaker (GOLD_LAYER §4.1.0). Opponent
-- rows are skipped by partitioning on team: 8,152 Attempted Tackles and 1,543
-- Challenges sit between a completed pass and its receiver, and a match-wide
-- lead() would lose exactly those contested receptions. Administrative rows
-- (cards, subs, delays, annotations) are removed before the window.
--
-- THE SPELL (§4.7.2) is a gaps-and-islands run over the same team stream: it
-- breaks when the player changes, when sequence_id changes (the classifier has
-- already decided that possession moved on) or when the period changes. The
-- receiver's first action always opens a new run, because the player changed.
--
-- Strategy: scoped DELETE + INSERT, as for every match-grain gold table.
-- ═══════════════════════════════════════════════════════════════════════════

DELETE FROM gold.pass_receptions
WHERE %(match_ids)s::int[] IS NULL
   OR match_id = ANY (%(match_ids)s::int[]);


WITH
-- ── the team's own stream of player actions ───────────────────────────────
ev AS (
    SELECT e.event_id, e.match_id, e.team_id, e.player_id, e.period,
           e.minute, e.second, e.json_index, e.event_type, e.outcome,
           e.x, e.y, e.end_x, e.end_y, e.sequence_id, e.xt, e.vaep_value,
           -- Qualifier tests materialised here so raw_data is not carried
           -- through the window sorts (same reason as build_sequences.sql).
           (e.event_type = 'Pass' AND e.raw_data->'qualifier' @> '[{"qualifierId": 2}]')   AS q_cross,
           (e.event_type = 'Pass' AND e.raw_data->'qualifier' @> '[{"qualifierId": 4}]')   AS q_through_ball,
           (e.event_type = 'Pass' AND e.raw_data->'qualifier' @> '[{"qualifierId": 157}]') AS q_long_ball,
           CASE WHEN e.event_type <> 'Pass' THEN NULL
                WHEN e.raw_data->'qualifier' @> '[{"qualifierId": 279}]' THEN 'kick_off'
                WHEN e.raw_data->'qualifier' @> '[{"qualifierId": 6}]'   THEN 'corner'
                WHEN e.raw_data->'qualifier' @> '[{"qualifierId": 107}]' THEN 'throw_in'
                WHEN e.raw_data->'qualifier' @> '[{"qualifierId": 124}]' THEN 'goal_kick'
                WHEN e.raw_data->'qualifier' @> '[{"qualifierId": 5}]'   THEN 'free_kick'
           END AS restart
    FROM silver.events e
    WHERE e.team_id IS NOT NULL
      AND e.player_id IS NOT NULL
      -- Rows that carry a player but describe no touch of the ball. Error and
      -- Good Skill are annotations on an action recorded separately.
      AND e.event_type NOT IN ('Card', 'Player off', 'Player on', 'Team set up',
                               'Formation change', 'Start', 'End', 'Start delay',
                               'End delay', 'Collection End',
                               'Injury Time Announcement', 'Referee Drop Ball',
                               'Contentious referee decision', 'Coach Setup',
                               'Official change', 'Condition change',
                               'Player retired', 'Player becomes goalkeeper',
                               'Corner Awarded', 'Error', 'Good Skill',
                               'Deleted event')
      AND (%(match_ids)s::int[] IS NULL OR e.match_id = ANY (%(match_ids)s::int[]))
),

-- ── neighbours and run breaks ──────────────────────────────────────────────
marked AS (
    SELECT ev.*,
           lead(event_id) OVER w AS next_event_id,
           CASE WHEN lag(player_id)   OVER w IS DISTINCT FROM player_id
                  OR lag(sequence_id) OVER w IS DISTINCT FROM sequence_id
                  OR lag(period)      OVER w IS DISTINCT FROM period
                THEN 1 ELSE 0 END AS run_break
    FROM ev
    WINDOW w AS (PARTITION BY match_id, team_id ORDER BY json_index, event_id)
),

runs AS (
    SELECT marked.*,
           sum(run_break) OVER (PARTITION BY match_id, team_id
                                ORDER BY json_index, event_id) AS run_no
    FROM marked
),

-- ── one row per run: the spell, whoever it belongs to ─────────────────────
-- Built for every run (not only receptions) because the aggregate is cheaper
-- than a lateral lookup per pass. The array_agg(...)[1] picks the first or
-- last action. A spell is keyed by its FIRST event_id, which is the receiver's
-- first action: joining on (match_id, team_id, run_no) instead let the planner
-- merge on the first two keys alone and filter 272M intermediate rows (8 min).
spells AS (
    SELECT (array_agg(event_id ORDER BY json_index, event_id))[1] AS start_event_id,
           count(*)::smallint AS spell_events,
           (array_agg(event_id   ORDER BY json_index DESC, event_id DESC))[1] AS end_event_id,
           (array_agg(event_type ORDER BY json_index DESC, event_id DESC))[1] AS end_type,
           (array_agg(outcome    ORDER BY json_index DESC, event_id DESC))[1] AS end_outcome,
           (array_agg(x          ORDER BY json_index DESC, event_id DESC))[1] AS end_x,
           (array_agg(y          ORDER BY json_index DESC, event_id DESC))[1] AS end_y,
           max(json_index) AS end_json_index,
           COALESCE(sum(gold.action_length(x, y, end_x, end_y))
                        FILTER (WHERE event_type = 'Carry'), 0)::real AS carry_m,
           COALESCE(bool_or(gold.is_progressive(x, y, end_x, end_y))
                        FILTER (WHERE event_type = 'Carry'), false)   AS progressive_carry,
           bool_or(event_type = 'Take On')                            AS take_on,
           COALESCE(sum(xt), 0)::real                                 AS xt,
           COALESCE(sum(vaep_value), 0)::real                         AS vaep
    FROM runs
    GROUP BY match_id, team_id, run_no
),

-- ── completed passes joined to the receiver's first action ────────────────
receptions AS (
    SELECT p.event_id AS pass_event_id, p.match_id, p.team_id, p.period,
           p.minute, p.second, p.sequence_id, p.json_index AS pass_json_index,
           p.player_id AS passer_id, r.player_id AS receiver_id,
           p.x, p.y, p.end_x, p.end_y, p.xt AS pass_xt,
           p.q_cross, p.q_through_ball, p.q_long_ball, p.restart,
           r.event_id AS rx_event_id, r.event_type AS rx_event_type,
           r.x AS rx_first_x, r.y AS rx_first_y
    FROM runs p
    JOIN runs r ON r.event_id = p.next_event_id
    WHERE p.event_type = 'Pass'
      AND p.outcome = 'success'
      AND p.end_x IS NOT NULL
      AND r.player_id <> p.player_id   -- ~60 passes to oneself: no receiver
      AND r.period = p.period          -- ~100 passes end a half: no receiver
)

INSERT INTO gold.pass_receptions (
    pass_event_id, match_id, competition_season_id, team_id, opponent_team_id,
    match_date, period, minute, second, sequence_id,
    passer_id, receiver_id,
    pass_x, pass_y, pass_length_m, is_progressive, is_cross, is_through_ball,
    is_long_ball, restart, pass_xt,
    rx_x, rx_y, rx_zone_id, rx_in_box, rx_event_id, rx_event_type, rx_gap_m,
    spell_events, spell_end_event_id, spell_end, spell_end_x, spell_end_y,
    spell_carry_m, spell_progressive_carry, spell_take_on, spell_contested,
    spell_xt, spell_vaep
)
SELECT
    rc.pass_event_id, rc.match_id, tms.competition_season_id, rc.team_id,
    tms.opponent_team_id, tms.match_date, rc.period, rc.minute, rc.second,
    rc.sequence_id,
    rc.passer_id, rc.receiver_id,
    rc.x, rc.y, gold.action_length(rc.x, rc.y, rc.end_x, rc.end_y),
    COALESCE(gold.is_progressive(rc.x, rc.y, rc.end_x, rc.end_y), false),
    rc.q_cross, rc.q_through_ball, rc.q_long_ball, rc.restart, rc.pass_xt,
    rc.end_x, rc.end_y, gold.zone_id(rc.end_x, rc.end_y),
    COALESCE(gold.in_box(rc.end_x, rc.end_y), false),
    rc.rx_event_id, rc.rx_event_type,
    COALESCE(gold.action_length(rc.end_x, rc.end_y, rc.rx_first_x, rc.rx_first_y), 0),
    s.spell_events, s.end_event_id,
    CASE
        WHEN s.end_type IN ('Goal', 'Miss', 'Post', 'Attempt Saved')      THEN 'shot'
        WHEN s.end_type = 'Pass' AND s.end_outcome = 'success'           THEN 'pass_completed'
        WHEN s.end_type IN ('Pass', 'Offside Pass')                      THEN 'pass_failed'
        WHEN s.end_type = 'Take On' AND s.end_outcome = 'failure'        THEN 'take_on_failed'
        WHEN s.end_type = 'Dispossessed'                                 THEN 'dispossessed'
        -- Ball touch 'failure' is a miscontrol; 'success' is a deflection
        -- (CLAUDE.md), which falls through to 'other'.
        WHEN s.end_type = 'Ball touch' AND s.end_outcome = 'failure'     THEN 'miscontrol'
        WHEN s.end_type = 'Foul' AND s.end_outcome = 'success'           THEN 'foul_won'
        ELSE 'other'
    END,
    s.end_x, s.end_y,
    s.carry_m, s.progressive_carry, s.take_on,
    EXISTS (
        SELECT 1
        FROM silver.events o
        WHERE o.match_id = rc.match_id
          AND o.team_id = tms.opponent_team_id
          AND o.json_index BETWEEN rc.pass_json_index AND s.end_json_index
          AND (o.event_type IN ('Tackle', 'Attempted Tackle', 'Challenge', 'Aerial')
               OR (o.event_type = 'Foul' AND o.outcome = 'failure'))
    ),
    s.xt, s.vaep
FROM receptions rc
JOIN spells s ON s.start_event_id = rc.rx_event_id
-- Inner join: only completed matches have a team row, and the context comes
-- from there so that season, opponent and date cannot disagree with it.
JOIN gold.team_match_stats tms
  ON tms.match_id = rc.match_id AND tms.team_id = rc.team_id;
