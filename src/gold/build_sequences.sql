-- ═══════════════════════════════════════════════════════════════════════════
-- build_sequences.sql — gold.sequences + gold.sequence_players
-- ═══════════════════════════════════════════════════════════════════════════
--
-- Spec: md/GOLD_LAYER.md §4.1 (tables), §4.1.0 (the five classifier
--       properties this build must respect), §4.1.2a (decisive actions),
--       §4.1.3 (start / end / outcome), §4.1.4 (space), §4.1.5 (model values),
--       §6 Step 3.
--
-- Runs as:  python -m src.gold.build_gold --step sequences
--
-- Parameter
--   %(match_ids)s :: int[]   NULL = every match (full rebuild, the default);
--                            otherwise only those matches are rebuilt.
--   The DELETE is scoped the same way and cascades to sequence_players and
--   sequence_phase_segments, so a scoped run cannot leave orphans behind.
--
-- Four things this query has to get right, all from §4.1.0:
--
--   1. duration comes from minute*60 + second, NEVER from timestamp, which is
--      NULL on all 229,662 synthesised carries.
--   2. a sequence is not guaranteed to be one team's possession — 17.6 per cent carry
--      opponent events. Every possession metric is filtered to the possessing
--      team (the team of the FIRST event); the three counts are all stored so
--      they can never be confused.
--   3. the last event IN the sequence is often not the action that ended it,
--      so `outcome` also reads the first event AFTER it (the `nxt` LATERAL).
--   4. ordering is (json_index, event_id) everywhere. 11,766 events share a
--      json_index with another event in the same match; without the event_id
--      tiebreaker this build is non-deterministic (Fix 0, 16 Aug 2026).
-- ═══════════════════════════════════════════════════════════════════════════

DELETE FROM gold.sequences
WHERE %(match_ids)s::int[] IS NULL
   OR match_id = ANY (%(match_ids)s::int[]);


-- ───────────────────────────────────────────────────────────────────────────
-- gold.sequences
-- ───────────────────────────────────────────────────────────────────────────
WITH ev AS (
    SELECT e.event_id, e.match_id, e.sequence_id, e.team_id, e.player_id,
           e.event_type, e.outcome, e.period, e.minute, e.second,
           e.x, e.y, e.end_x, e.end_y, e.json_index,
           e.spadl_type_id, e.xg, e.xt,
           e.vaep_value, e.vaep_offensive, e.vaep_defensive,
           e.shot_play_pattern, e.raw_data,
           m.competition_season_id,
           m.match_date::date AS match_date,
           m.home_team_id, m.away_team_id
    FROM silver.events e
    JOIN silver.matches m USING (match_id)
    WHERE e.sequence_id IS NOT NULL
      AND (%(match_ids)s::int[] IS NULL OR e.match_id = ANY (%(match_ids)s::int[]))
),

ordered AS (
    SELECT ev.*,
           row_number() OVER w_asc                       AS rn,
           row_number() OVER w_desc                      AS rn_desc,
           count(*)     OVER (PARTITION BY sequence_id)  AS total_event_count
    FROM ev
    WINDOW w_asc  AS (PARTITION BY sequence_id ORDER BY json_index,      event_id),
           w_desc AS (PARTITION BY sequence_id ORDER BY json_index DESC, event_id DESC)
),

-- Per-sequence header: everything that comes from the first or the last event.
-- The possessing team is the team of the FIRST event — it agrees with the h_a
-- token in 144,763/144,763 sequences (§4.1.0 #2).
hdr AS (
    SELECT
        sequence_id,
        min(match_id)                    AS match_id,
        min(competition_season_id)       AS competition_season_id,
        min(match_date)                  AS match_date,
        min(home_team_id)                AS home_team_id,
        min(away_team_id)                AS away_team_id,
        max(total_event_count)::smallint AS total_event_count,

        -- first event
        max(team_id)       FILTER (WHERE rn = 1) AS possessing_team_id,
        max(event_id)      FILTER (WHERE rn = 1) AS start_event_id,
        max(event_type)    FILTER (WHERE rn = 1) AS start_event_type,
        max(spadl_type_id) FILTER (WHERE rn = 1) AS start_spadl_type_id,
        max(period)        FILTER (WHERE rn = 1) AS period,
        max(minute)        FILTER (WHERE rn = 1) AS start_minute,
        max(second)        FILTER (WHERE rn = 1) AS start_second,
        max(x)             FILTER (WHERE rn = 1) AS start_x,
        max(y)             FILTER (WHERE rn = 1) AS start_y,
        -- Q279 kick-off must be tested first or a kick-off taken as a free
        -- kick is mis-typed (§4.1.3 rule 1).
        bool_or(raw_data->'qualifier' @> '[{"qualifierId": 279}]')
            FILTER (WHERE rn = 1)                AS start_is_kick_off,

        -- last event
        max(event_id)      FILTER (WHERE rn_desc = 1) AS end_event_id,
        max(team_id)       FILTER (WHERE rn_desc = 1) AS end_team_id,
        max(event_type)    FILTER (WHERE rn_desc = 1) AS end_event_type,
        max(outcome)       FILTER (WHERE rn_desc = 1) AS end_outcome,
        max(spadl_type_id) FILTER (WHERE rn_desc = 1) AS end_spadl_type_id,
        max(minute)        FILTER (WHERE rn_desc = 1) AS end_minute,
        max(second)        FILTER (WHERE rn_desc = 1) AS end_second,
        max(x)             FILTER (WHERE rn_desc = 1) AS end_x,
        max(y)             FILTER (WHERE rn_desc = 1) AS end_y,
        max(json_index)    FILTER (WHERE rn_desc = 1) AS end_json_index,
        -- Q82 is the ONLY way to split Attempt Saved into saved vs blocked.
        bool_or(raw_data->'qualifier' @> '[{"qualifierId": 82}]')
            FILTER (WHERE rn_desc = 1)                AS end_is_blocked,

        -- Outcome rule 1 checks ANY event, not the last: a goal is frequently
        -- followed by a Corner Awarded or absorbed mid-sequence (§4.1.3).
        bool_or(event_type = 'Goal')                  AS has_goal_any
    FROM ordered
    GROUP BY sequence_id
),

-- The possessing team's events only. Every possession metric below is
-- computed from this CTE, per the resolution of GOLD_SEQUENCES §12.7.
poss AS (
    SELECT o.*,
           gold.zone_id(o.x, o.y)      AS zone,
           gold.x_strip(o.x)           AS x_strip,
           gold.y_channel(o.y)         AS y_channel,
           gold.third(o.x)             AS third_start,
           gold.third(o.end_x)         AS third_end,
           gold.y_channel(o.end_y)     AS y_channel_end
    FROM ordered o
    JOIN hdr h USING (sequence_id)
    WHERE o.team_id = h.possessing_team_id
),

-- zone_path: the ordered 30-zone path with consecutive duplicates collapsed.
-- Computed in its own CTE so the lag() window skips rows with no coordinates
-- rather than emitting a spurious repeat around them.
zoned AS (
    SELECT sequence_id, zone, json_index, event_id,
           lag(zone) OVER (PARTITION BY sequence_id
                           ORDER BY json_index, event_id) AS prev_zone
    FROM poss
    WHERE zone IS NOT NULL
),
zone_paths AS (
    SELECT sequence_id,
           array_agg(zone ORDER BY json_index, event_id)
               FILTER (WHERE prev_zone IS NULL OR zone <> prev_zone) AS zone_path
    FROM zoned
    GROUP BY sequence_id
),

-- Channel-to-channel flow as sparse JSONB: a typical sequence has 3-6 distinct
-- entries rather than 25 mostly-zero columns, and the schema survives a future
-- change to the channel count (§4.1.4).
pass_flow AS (
    SELECT sequence_id, jsonb_object_agg(k, n) AS flow
    FROM (
        SELECT sequence_id,
               y_channel || '_' || y_channel_end AS k,
               count(*)                          AS n
        FROM poss
        WHERE event_type = 'Pass'
          AND y_channel IS NOT NULL AND y_channel_end IS NOT NULL
        GROUP BY 1, 2
    ) t
    GROUP BY sequence_id
),
carry_flow AS (
    SELECT sequence_id, jsonb_object_agg(k, n) AS flow
    FROM (
        SELECT sequence_id,
               y_channel || '_' || y_channel_end AS k,
               count(*)                          AS n
        FROM poss
        WHERE event_type = 'Carry'
          AND y_channel IS NOT NULL AND y_channel_end IS NOT NULL
        GROUP BY 1, 2
    ) t
    GROUP BY sequence_id
),

agg AS (
    SELECT
        sequence_id,
        count(*)::smallint                        AS event_count,
        count(DISTINCT player_id)::smallint       AS unique_players,

        -- ── space (§4.1.4) ─────────────────────────────────────────────────
        max(x)                                    AS max_x,
        avg(x)::real                              AS avg_x,
        avg(y)::real                              AS avg_y,
        (max(y) - min(y))::real                   AS width_used_m,
        count(DISTINCT y_channel)::smallint       AS channels_used,
        sum(gold.action_length(x, y, end_x, end_y))
            FILTER (WHERE event_type IN ('Pass','Carry'))::real
                                                  AS distance_covered_m,
        bool_or(x >= 70 OR end_x >= 70)           AS final_third_entry,
        bool_or(gold.in_box(x, y) OR gold.in_box(end_x, end_y))
                                                  AS penalty_box_entry,
        count(*) FILTER (WHERE gold.in_box(x, y))::smallint
                                                  AS box_touches,

        -- ── passing ────────────────────────────────────────────────────────
        count(*) FILTER (WHERE event_type = 'Pass')::smallint AS pass_count,
        count(*) FILTER (WHERE event_type = 'Pass' AND outcome = 'success')::smallint
                                                              AS pass_completed,
        count(*) FILTER (WHERE event_type = 'Pass'
                           AND gold.action_length(x, y, end_x, end_y) < 15)::smallint
                                                              AS pass_short,
        count(*) FILTER (WHERE event_type = 'Pass'
                           AND gold.action_length(x, y, end_x, end_y) >= 15
                           AND gold.action_length(x, y, end_x, end_y) <  30)::smallint
                                                              AS pass_medium,
        count(*) FILTER (WHERE event_type = 'Pass'
                           AND gold.action_length(x, y, end_x, end_y) >= 30)::smallint
                                                              AS pass_long,
        count(*) FILTER (WHERE event_type = 'Pass'
                           AND gold.is_progressive(x, y, end_x, end_y))::smallint
                                                              AS progressive_passes,
        count(*) FILTER (WHERE event_type = 'Pass' AND outcome = 'success'
                           AND gold.is_progressive(x, y, end_x, end_y))::smallint
                                                              AS progressive_passes_completed,
        count(*) FILTER (WHERE event_type = 'Pass' AND x < 70 AND end_x >= 70)::smallint
                                                              AS passes_into_final_third,
        count(*) FILTER (WHERE event_type = 'Pass'
                           AND gold.in_box(end_x, end_y)
                           AND NOT gold.in_box(x, y))::smallint
                                                              AS passes_into_box,
        count(*) FILTER (WHERE event_type = 'Pass'
                           AND raw_data->'qualifier' @> '[{"qualifierId": 2}]')::smallint
                                                              AS cross_count,
        count(*) FILTER (WHERE event_type = 'Pass' AND outcome = 'success'
                           AND raw_data->'qualifier' @> '[{"qualifierId": 2}]')::smallint
                                                              AS cross_completed,
        count(*) FILTER (WHERE event_type = 'Pass'
                           AND raw_data->'qualifier' @> '[{"qualifierId": 4}]')::smallint
                                                              AS through_ball_count,
        count(*) FILTER (WHERE event_type = 'Pass'
                           AND raw_data->'qualifier' @> '[{"qualifierId": 157}]')::smallint
                                                              AS long_ball_count,
        count(*) FILTER (WHERE event_type = 'Pass'
                           AND abs(end_y - y) >= 30)::smallint AS switch_count,

        -- ── carrying & dribbling ───────────────────────────────────────────
        count(*) FILTER (WHERE event_type = 'Carry')::smallint AS carry_count,
        COALESCE(sum(gold.action_length(x, y, end_x, end_y))
                     FILTER (WHERE event_type = 'Carry'), 0)::real
                                                               AS carry_distance_m,
        count(*) FILTER (WHERE event_type = 'Carry'
                           AND gold.is_progressive(x, y, end_x, end_y))::smallint
                                                               AS progressive_carries,
        count(*) FILTER (WHERE event_type = 'Carry' AND x < 70 AND end_x >= 70)::smallint
                                                               AS carries_into_final_third,
        count(*) FILTER (WHERE event_type = 'Carry'
                           AND gold.in_box(end_x, end_y)
                           AND NOT gold.in_box(x, y))::smallint AS carries_into_box,
        count(*) FILTER (WHERE event_type = 'Take On')::smallint AS take_ons,
        count(*) FILTER (WHERE event_type = 'Take On' AND outcome = 'success')::smallint
                                                                 AS take_ons_won,

        -- ── shooting ───────────────────────────────────────────────────────
        count(*) FILTER (WHERE event_type IN ('Goal','Attempt Saved','Miss','Post'))::smallint
                                                                 AS shot_count,
        count(*) FILTER (WHERE event_type = 'Goal'
                            OR (event_type = 'Attempt Saved'
                                AND NOT raw_data->'qualifier' @> '[{"qualifierId": 82}]'))::smallint
                                                                 AS shots_on_target,
        count(*) FILTER (WHERE event_type IN ('Goal','Attempt Saved','Miss','Post')
                           AND gold.in_box(x, y))::smallint      AS shots_in_box,
        count(*) FILTER (WHERE event_type = 'Goal')::smallint    AS goal_count,
        count(*) FILTER (WHERE event_type = 'Post')::smallint    AS woodwork_count,
        count(*) FILTER (WHERE raw_data->'qualifier' @> '[{"qualifierId": 214}]')::smallint
                                                                 AS big_chance_count,

        -- ── set-piece restarts and regains inside the sequence ─────────────
        count(*) FILTER (WHERE spadl_type_id IN (5, 6))::smallint     AS corner_count,
        count(*) FILTER (WHERE spadl_type_id = 2)::smallint           AS throw_in_count,
        count(*) FILTER (WHERE spadl_type_id IN (3, 4, 13))::smallint AS free_kick_count,
        count(*) FILTER (WHERE spadl_type_id = 22)::smallint          AS goal_kick_count,
        count(*) FILTER (WHERE event_type = 'Ball recovery')::smallint AS ball_recovery_count,
        count(*) FILTER (WHERE event_type = 'Interception')::smallint  AS interception_count,

        -- ── model values (§4.1.5) ──────────────────────────────────────────
        -- SUM skips NULLs, so these are safe; xt_per_event below divides by
        -- event_count, which is what the spec asks for.
        sum(xg)::real                                             AS xg,
        sum(xg) FILTER (WHERE shot_play_pattern IS DISTINCT FROM 'penalty')::real
                                                                  AS npxg,
        max(xg)::real                                             AS xg_max,
        sum(xt) FILTER (WHERE event_type IN ('Pass','Carry'))::real AS xt,
        max(xt) FILTER (WHERE event_type IN ('Pass','Carry'))::real AS xt_max,
        sum(vaep_value)::real                                     AS vaep,
        sum(vaep_offensive)::real                                 AS vaep_offensive,
        sum(vaep_defensive)::real                                 AS vaep_defensive,
        max(vaep_value)::real                                     AS vaep_max,

        -- ── third-to-third flow, the complete 3x3 for both action types ────
        -- All nine directions are stored so regressive play (3->2, 2->1,
        -- 3->1) is measurable — the explicit request in GOLD_SEQUENCES §5.3.
        count(*) FILTER (WHERE event_type='Pass'  AND third_start=1 AND third_end=1)::smallint AS pass_1_to_1,
        count(*) FILTER (WHERE event_type='Pass'  AND third_start=1 AND third_end=2)::smallint AS pass_1_to_2,
        count(*) FILTER (WHERE event_type='Pass'  AND third_start=1 AND third_end=3)::smallint AS pass_1_to_3,
        count(*) FILTER (WHERE event_type='Pass'  AND third_start=2 AND third_end=1)::smallint AS pass_2_to_1,
        count(*) FILTER (WHERE event_type='Pass'  AND third_start=2 AND third_end=2)::smallint AS pass_2_to_2,
        count(*) FILTER (WHERE event_type='Pass'  AND third_start=2 AND third_end=3)::smallint AS pass_2_to_3,
        count(*) FILTER (WHERE event_type='Pass'  AND third_start=3 AND third_end=1)::smallint AS pass_3_to_1,
        count(*) FILTER (WHERE event_type='Pass'  AND third_start=3 AND third_end=2)::smallint AS pass_3_to_2,
        count(*) FILTER (WHERE event_type='Pass'  AND third_start=3 AND third_end=3)::smallint AS pass_3_to_3,
        count(*) FILTER (WHERE event_type='Carry' AND third_start=1 AND third_end=1)::smallint AS carry_1_to_1,
        count(*) FILTER (WHERE event_type='Carry' AND third_start=1 AND third_end=2)::smallint AS carry_1_to_2,
        count(*) FILTER (WHERE event_type='Carry' AND third_start=1 AND third_end=3)::smallint AS carry_1_to_3,
        count(*) FILTER (WHERE event_type='Carry' AND third_start=2 AND third_end=1)::smallint AS carry_2_to_1,
        count(*) FILTER (WHERE event_type='Carry' AND third_start=2 AND third_end=2)::smallint AS carry_2_to_2,
        count(*) FILTER (WHERE event_type='Carry' AND third_start=2 AND third_end=3)::smallint AS carry_2_to_3,
        count(*) FILTER (WHERE event_type='Carry' AND third_start=3 AND third_end=1)::smallint AS carry_3_to_1,
        count(*) FILTER (WHERE event_type='Carry' AND third_start=3 AND third_end=2)::smallint AS carry_3_to_2,
        count(*) FILTER (WHERE event_type='Carry' AND third_start=3 AND third_end=3)::smallint AS carry_3_to_3
    FROM poss
    GROUP BY sequence_id
),

-- The first event AFTER the sequence, in any team's possession and regardless
-- of sequence membership. This is what recovers the orphaned fouls and corner
-- awards that actually terminate sequences (§4.1.0 #3).
-- MEASURED, and it is why the probe reads three events rather than one.
-- Fouls and corner awards arrive as MIRROR PAIRS, and neither the order nor
-- the side is reliable from a single look:
--
--   * Foul: the foul-WON row (outcome 'success') is very often the last event
--     INSIDE the sequence, with the opponent's foul-committed row next. Only
--     650 of 11,535 fouls won sit where a strict "next event" test can see
--     them.
--   * Corner Awarded: the conceding row comes first in 2,570 of 4,473 pairs,
--     so the immediately-next event is the wrong side more than half the time.
--
-- Both semantics were verified against the restart taker rather than assumed:
-- the team on the outcome='success' row takes the resulting free kick in
-- 11,217/11,482 cases and the resulting corner in 4,457/4,461.
nxt AS (
    SELECT h.sequence_id,
           n.event_type AS next_event_type,
           n.team_id    AS next_event_team_id,
           n.outcome    AS next_outcome,
           n.raw_data   AS next_raw_data,
           p.corner_won_after,
           p.foul_won_after
    FROM hdr h
    LEFT JOIN LATERAL (
        SELECT e2.event_type, e2.team_id, e2.outcome, e2.raw_data
        FROM silver.events e2
        WHERE e2.match_id = h.match_id
          AND (e2.json_index, e2.event_id) > (h.end_json_index, h.end_event_id)
        ORDER BY e2.json_index, e2.event_id
        LIMIT 1
    ) n ON TRUE
    LEFT JOIN LATERAL (
        SELECT bool_or(w.event_type = 'Corner Awarded'
                       AND w.outcome = 'success'
                       AND w.team_id = h.possessing_team_id) AS corner_won_after,
               bool_or(w.event_type = 'Foul'
                       AND w.outcome = 'success'
                       AND w.team_id = h.possessing_team_id) AS foul_won_after
        FROM (
            SELECT e3.event_type, e3.outcome, e3.team_id
            FROM silver.events e3
            WHERE e3.match_id = h.match_id
              AND (e3.json_index, e3.event_id) > (h.end_json_index, h.end_event_id)
            ORDER BY e3.json_index, e3.event_id
            LIMIT 3
        ) w
    ) p ON TRUE
)

INSERT INTO gold.sequences (
    sequence_id, match_id, team_id, opponent_team_id, competition_season_id,
    match_date, is_home, sequence_number, period,
    start_minute, start_second, end_minute, end_second, duration_seconds,
    start_event_id, end_event_id,
    event_count, total_event_count, opponent_event_count,
    is_suspect_segmentation, unique_players, touches_per_player,
    sequence_type, start_trigger, start_event_type, start_spadl_type_id,
    end_event_type, end_spadl_type_id, next_event_type, next_event_team_id, outcome,
    start_x, start_y, end_x, end_y, max_x, avg_x, avg_y,
    start_zone, end_zone, start_x_strip, start_y_channel, end_x_strip, end_y_channel,
    zone_path, field_progression, distance_covered_m, directness, direct_speed,
    width_used_m, channels_used, final_third_entry, penalty_box_entry, box_touches,
    pass_count, pass_completed, pass_completion_rate,
    pass_short, pass_medium, pass_long,
    progressive_passes, progressive_passes_completed,
    passes_into_final_third, passes_into_box,
    cross_count, cross_completed, through_ball_count, long_ball_count, switch_count,
    carry_count, carry_distance_m, progressive_carries,
    carries_into_final_third, carries_into_box, take_ons, take_ons_won,
    shot_count, shots_on_target, shots_in_box, goal_count, woodwork_count,
    big_chance_count,
    corner_count, throw_in_count, free_kick_count, goal_kick_count,
    ball_recovery_count, interception_count,
    xg, npxg, xg_max, xt, xt_max, xt_per_event,
    vaep, vaep_offensive, vaep_defensive, vaep_max,
    pass_1_to_1, pass_1_to_2, pass_1_to_3,
    pass_2_to_1, pass_2_to_2, pass_2_to_3,
    pass_3_to_1, pass_3_to_2, pass_3_to_3,
    carry_1_to_1, carry_1_to_2, carry_1_to_3,
    carry_2_to_1, carry_2_to_2, carry_2_to_3,
    carry_3_to_1, carry_3_to_2, carry_3_to_3,
    pass_channel_flow, carry_channel_flow
)
SELECT
    h.sequence_id,
    h.match_id,
    h.possessing_team_id,
    CASE WHEN h.possessing_team_id = h.home_team_id
         THEN h.away_team_id ELSE h.home_team_id END,
    h.competition_season_id,
    h.match_date,
    h.possessing_team_id = h.home_team_id,
    -- sequence_id is '{match_id}_{h_a}_{NNN}' (not a UUID) — the ordinal is
    -- the third token.
    split_part(h.sequence_id, '_', 3)::smallint,
    h.period,
    h.start_minute, h.start_second, h.end_minute, h.end_second,
    -- §4.1.0 #1: minute*60 + second, never timestamp.
    GREATEST((h.end_minute * 60 + h.end_second)
             - (h.start_minute * 60 + h.start_second), 0)::real,
    h.start_event_id, h.end_event_id,

    a.event_count,
    h.total_event_count,
    (h.total_event_count - a.event_count)::smallint,
    -- Flags the ~1.9 per cent to exclude from clustering sets and leaderboards.
    (h.total_event_count - a.event_count) >= 3
        OR a.event_count * 2 < h.total_event_count,
    a.unique_players,
    a.event_count::real / NULLIF(a.unique_players, 0),

    -- ── sequence_type (§4.1.3) — rules are order-dependent ─────────────────
    CASE
        WHEN h.start_is_kick_off                    THEN 'kick_off'
        WHEN h.start_spadl_type_id = 2              THEN 'throw_in'
        WHEN h.start_spadl_type_id IN (5, 6)        THEN 'corner'
        WHEN h.start_spadl_type_id IN (3, 4, 13)    THEN 'free_kick'
        WHEN h.start_spadl_type_id = 22             THEN 'goal_kick'
        WHEN h.start_spadl_type_id = 12             THEN 'penalty'
        WHEN h.start_event_type = 'Referee Drop Ball' THEN 'drop_ball'
        ELSE 'open_play'
    END,

    -- ── start_trigger (§4.1.3) — how possession was won ────────────────────
    -- Ball recovery has no SPADL type, which is why this column exists
    -- alongside start_spadl_type_id rather than being derivable from it.
    CASE h.start_event_type
        WHEN 'Pass'              THEN 'pass'
        WHEN 'Carry'             THEN 'pass'
        WHEN 'Clearance'         THEN 'clearance'
        WHEN 'Ball recovery'     THEN 'recovery'
        WHEN 'Tackle'            THEN 'tackle'
        WHEN 'Interception'      THEN 'interception'
        WHEN 'Blocked Pass'      THEN 'block'
        WHEN 'Keeper pick-up'    THEN 'keeper'
        WHEN 'Claim'             THEN 'keeper'
        WHEN 'Save'              THEN 'keeper'
        WHEN 'Attempt Saved'     THEN 'rebound'
        WHEN 'Miss'              THEN 'rebound'
        WHEN 'Post'              THEN 'rebound'
        WHEN 'Goal'              THEN 'rebound'
        WHEN 'Take On'           THEN 'take_on'
        WHEN 'Referee Drop Ball' THEN 'drop_ball'
        ELSE 'other'
    END,
    h.start_event_type, h.start_spadl_type_id,
    h.end_event_type,   h.end_spadl_type_id,
    n.next_event_type,  n.next_event_team_id,

    -- ── outcome (§4.1.3) — a closed enum, resolved in priority order ───────
    CASE
        WHEN h.has_goal_any                                    THEN 'goal'
        WHEN h.end_event_type = 'Attempt Saved' AND h.end_is_blocked
                                                               THEN 'shot_blocked'
        WHEN h.end_event_type = 'Attempt Saved'                THEN 'shot_saved'
        WHEN h.end_event_type = 'Miss'                         THEN 'shot_off_target'
        WHEN h.end_event_type = 'Post'                         THEN 'shot_woodwork'
        -- Corner Awarded and Foul: outcome 'success' is the side that WON it.
        -- Both are checked on the sequence's own last event AND across the
        -- next three events, because the mirror pair straddles the boundary in
        -- either order (see the `nxt` CTE).
        WHEN (h.end_event_type = 'Corner Awarded'
              AND h.end_team_id = h.possessing_team_id
              AND h.end_outcome = 'success')
          OR COALESCE(n.corner_won_after, FALSE)               THEN 'corner_won'
        WHEN (h.end_event_type = 'Foul'
              AND h.end_team_id = h.possessing_team_id
              AND h.end_outcome = 'success')
          OR COALESCE(n.foul_won_after, FALSE)                 THEN 'foul_won'
        WHEN h.end_event_type = 'Offside Pass'
          OR n.next_event_type = 'Offside Pass'                THEN 'offside'
        WHEN n.next_event_team_id IS DISTINCT FROM h.possessing_team_id
             AND (n.next_raw_data->'qualifier' @> '[{"qualifierId": 107}]'
               OR n.next_raw_data->'qualifier' @> '[{"qualifierId": 124}]')
                                                               THEN 'ball_out'
        WHEN h.end_event_type IN ('Claim','Keeper pick-up')
             AND h.end_team_id IS DISTINCT FROM h.possessing_team_id
                                                               THEN 'keeper_collected'
        WHEN n.next_event_type IS NULL
          OR n.next_event_type IN ('End','Player on','Player off')
                                                               THEN 'period_end'
        ELSE 'turnover'
    END,

    -- ── space (§4.1.4) ─────────────────────────────────────────────────────
    h.start_x, h.start_y, h.end_x, h.end_y,
    a.max_x, a.avg_x, a.avg_y,
    gold.zone_id(h.start_x, h.start_y),
    gold.zone_id(h.end_x,   h.end_y),
    gold.x_strip(h.start_x), gold.y_channel(h.start_y),
    gold.x_strip(h.end_x),   gold.y_channel(h.end_y),
    zp.zone_path,
    (h.end_x - h.start_x)::real,
    a.distance_covered_m,
    ((h.end_x - h.start_x) / NULLIF(a.distance_covered_m, 0))::real,
    -- 31 per cent of sequences have duration 0 (one-second resolution), so this is
    -- NULL there — not zero, and not infinity (§4.1.4).
    ((h.end_x - h.start_x)
        / NULLIF(GREATEST((h.end_minute * 60 + h.end_second)
                          - (h.start_minute * 60 + h.start_second), 0), 0))::real,
    a.width_used_m, a.channels_used,
    COALESCE(a.final_third_entry, FALSE),
    COALESCE(a.penalty_box_entry, FALSE),
    a.box_touches,

    -- ── passing ────────────────────────────────────────────────────────────
    a.pass_count, a.pass_completed,
    a.pass_completed::real / NULLIF(a.pass_count, 0),
    a.pass_short, a.pass_medium, a.pass_long,
    a.progressive_passes, a.progressive_passes_completed,
    a.passes_into_final_third, a.passes_into_box,
    a.cross_count, a.cross_completed, a.through_ball_count, a.long_ball_count,
    a.switch_count,

    -- ── carrying & dribbling ───────────────────────────────────────────────
    a.carry_count, a.carry_distance_m, a.progressive_carries,
    a.carries_into_final_third, a.carries_into_box, a.take_ons, a.take_ons_won,

    -- ── shooting ───────────────────────────────────────────────────────────
    a.shot_count, a.shots_on_target, a.shots_in_box, a.goal_count,
    a.woodwork_count, a.big_chance_count,

    -- ── set pieces & regains ───────────────────────────────────────────────
    a.corner_count, a.throw_in_count, a.free_kick_count, a.goal_kick_count,
    a.ball_recovery_count, a.interception_count,

    -- ── model values (§4.1.5) ──────────────────────────────────────────────
    a.xg, a.npxg, a.xg_max, a.xt, a.xt_max,
    a.xt / NULLIF(a.event_count, 0),
    a.vaep, a.vaep_offensive, a.vaep_defensive, a.vaep_max,

    -- ── third-to-third flow ────────────────────────────────────────────────
    a.pass_1_to_1,  a.pass_1_to_2,  a.pass_1_to_3,
    a.pass_2_to_1,  a.pass_2_to_2,  a.pass_2_to_3,
    a.pass_3_to_1,  a.pass_3_to_2,  a.pass_3_to_3,
    a.carry_1_to_1, a.carry_1_to_2, a.carry_1_to_3,
    a.carry_2_to_1, a.carry_2_to_2, a.carry_2_to_3,
    a.carry_3_to_1, a.carry_3_to_2, a.carry_3_to_3,

    pf.flow, cf.flow
FROM hdr h
JOIN agg a          USING (sequence_id)
LEFT JOIN nxt n     USING (sequence_id)
LEFT JOIN zone_paths zp USING (sequence_id)
LEFT JOIN pass_flow  pf USING (sequence_id)
LEFT JOIN carry_flow cf USING (sequence_id);


-- ───────────────────────────────────────────────────────────────────────────
-- gold.sequence_players — §4.1.2 / §4.1.2a
--
-- Involvement, NOT totals. 8.9 per cent of events carry no sequence_id, so SUM(passes)
-- here sits below a player's true match total; gold.player_match_stats is the
-- authority for totals.
--
-- Rows for BOTH teams are written (sandwich / deflection events included) and
-- separated by is_possessing_team, so a player-facing query defaults to
-- WHERE is_possessing_team (§4.1.0 #2).
--
-- The parent DELETE above already removed these rows by cascade.
-- ───────────────────────────────────────────────────────────────────────────
WITH assist_xg AS (
    -- The shot a value_assist pass created is, by construction, in the SAME
    -- sequence and is the next shot by json_index (§4.1.2a) — no Q55 join
    -- needed. Resolved here rather than as a LATERAL on the main query
    -- because only ~8,000 passes carry a value_assist: this runs the lookup
    -- 8,000 times instead of once per sequenced event, which is the
    -- difference between seconds and many minutes.
    SELECT a.event_id,
           (SELECT sh.xg
            FROM silver.events sh
            WHERE sh.sequence_id = a.sequence_id
              AND (sh.json_index, sh.event_id) > (a.json_index, a.event_id)
              AND sh.xg IS NOT NULL
            ORDER BY sh.json_index, sh.event_id
            LIMIT 1) AS xg
    FROM silver.events a
    WHERE a.value_assist IS NOT NULL
      AND a.sequence_id IS NOT NULL
      AND (%(match_ids)s::int[] IS NULL OR a.match_id = ANY (%(match_ids)s::int[]))
)
INSERT INTO gold.sequence_players (
    sequence_id, player_id, match_id, team_id, competition_season_id,
    is_possessing_team, first_event_number, last_event_number,
    is_sequence_starter, is_sequence_finisher, is_assister,
    touches, passes, passes_completed, carries, take_ons, take_ons_won,
    shots, goals, progression_m,
    assists, key_passes, shots_on_target, big_chances,
    xg, xa, xt, vaep, vaep_offensive, vaep_defensive
)
SELECT
    e.sequence_id,
    e.player_id,
    s.match_id,
    e.team_id,
    s.competition_season_id,
    e.team_id = s.team_id,
    min(e.sequence_event_number)::smallint,
    max(e.sequence_event_number)::smallint,
    COALESCE(bool_or(e.event_id = s.start_event_id), FALSE),
    COALESCE(bool_or(e.event_id = s.end_event_id), FALSE),
    -- STRICTLY value_assist = '16' (the pass created a Goal). "Any
    -- value_assist" would silently fold key passes into a goal-assist column.
    COALESCE(bool_or(e.value_assist = '16'), FALSE),

    count(*)::smallint,
    count(*) FILTER (WHERE e.event_type = 'Pass')::smallint,
    count(*) FILTER (WHERE e.event_type = 'Pass' AND e.outcome = 'success')::smallint,
    count(*) FILTER (WHERE e.event_type = 'Carry')::smallint,
    count(*) FILTER (WHERE e.event_type = 'Take On')::smallint,
    count(*) FILTER (WHERE e.event_type = 'Take On' AND e.outcome = 'success')::smallint,
    count(*) FILTER (WHERE e.event_type IN ('Goal','Attempt Saved','Miss','Post'))::smallint,
    count(*) FILTER (WHERE e.event_type = 'Goal')::smallint,
    sum(e.end_x - e.x) FILTER (WHERE e.event_type IN ('Pass','Carry'))::real,

    count(*) FILTER (WHERE e.value_assist = '16')::smallint,
    count(*) FILTER (WHERE e.value_assist IN ('13','14','15'))::smallint,
    count(*) FILTER (WHERE e.event_type = 'Goal'
                       OR (e.event_type = 'Attempt Saved'
                           AND NOT e.raw_data->'qualifier' @> '[{"qualifierId": 82}]'))::smallint,
    count(*) FILTER (WHERE e.raw_data->'qualifier' @> '[{"qualifierId": 214}]')::smallint,

    sum(e.xg)::real,
    -- value_assist = '60' (Chance missed, 24 rows league-wide) has no shot, so
    -- its lookup is NULL — correct, and why assists + key_passes is not the
    -- same as the value_assist count.
    sum(ax.xg)::real,
    sum(e.xt) FILTER (WHERE e.event_type IN ('Pass','Carry'))::real,
    sum(e.vaep_value)::real,
    sum(e.vaep_offensive)::real,
    sum(e.vaep_defensive)::real
FROM silver.events e
JOIN gold.sequences s USING (sequence_id)
LEFT JOIN assist_xg ax ON ax.event_id = e.event_id
WHERE e.player_id IS NOT NULL
  AND (%(match_ids)s::int[] IS NULL OR e.match_id = ANY (%(match_ids)s::int[]))
GROUP BY e.sequence_id, e.player_id, s.match_id, e.team_id,
         s.competition_season_id, s.team_id;
