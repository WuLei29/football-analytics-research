-- ═══════════════════════════════════════════════════════════════════════════
-- build_player_match_stats.sql — gold.player_match_stats
-- ═══════════════════════════════════════════════════════════════════════════
--
-- Spec: md/GOLD_LAYER.md §4.2.2, §4.6.0-§4.6.14, §6 Step 7.
--
-- Runs as:  python -m src.gold.build_gold --step player_match_stats
--
-- Parameter
--   %(match_ids)s :: int[]   NULL = every completed match.
--
-- Sources
--   silver.match_lineups            appearance, minutes, formation slot, captaincy
--   silver.matches                  match_length_min for the minutes formula
--   silver.events                   every action count, filtered by player_id
--   gold.formation_slot_positions   the granular position (§4.6.0)
--   gold.team_match_stats           context AND the opponent's possession share
--
-- WHY CONTEXT COMES FROM gold.team_match_stats AND NOT silver.matches:
-- matchday, date, venue and result would otherwise be re-derived here, and a
-- player's `result` could then disagree with their team's row for the same
-- match. Reading the team row makes that impossible, and the same join already
-- has to happen for the possession adjustment.
--
-- THREE SEMANTICS CONFIRMED BY COUNTING, NOT ASSUMED (§4.6.8):
--   * Challenge (6,998) = THIS PLAYER WAS DRIBBLED PAST. It pairs 1:1 with the
--     6,998 successful Take Ons. Reading it as a won duel inverts the column.
--   * Foul: outcome 'failure' committed, 'success' won. Both sides recorded.
--   * Attempted Tackle (typeId 83, 12,403 rows, all failures) is a SEPARATE
--     event from Tackle and belongs in the tackle_success_rate denominator.
--
-- Strategy: scoped DELETE + INSERT, as for team_match_stats and for the same
-- reason -- a ~125-column ON CONFLICT SET list has to be kept in step with the
-- INSERT list by hand.
-- ═══════════════════════════════════════════════════════════════════════════

DELETE FROM gold.player_match_stats
WHERE %(match_ids)s::int[] IS NULL
   OR match_id = ANY (%(match_ids)s::int[]);


WITH
-- ── who played, in what role, for how long ─────────────────────────────────
appearances AS (
    SELECT
        ml.match_id, ml.player_id, ml.team_id,
        tms.opponent_team_id, tms.competition_season_id,
        tms.matchday, tms.match_date, tms.is_home, tms.result,
        f.position, f.position_group,
        ml.formation_position, ml.team_formation,
        ml.starting_xi AS started,
        (NOT ml.starting_xi AND ml.minute_in IS NOT NULL)        AS subbed_on,
        (ml.exit_reason = 'substitution')                        AS subbed_off,
        (ml.starting_xi AND ml.minute_out IS NULL)               AS played_full_match,
        (COALESCE(ml.minute_out, m.match_length_min)
            - COALESCE(ml.minute_in, 0))::smallint               AS minutes_played,
        ml.is_captain,
        COALESCE(ml.minute_in, 0)                                AS window_from,
        COALESCE(ml.minute_out, m.match_length_min)              AS window_to,
        opp.possession_pct                                       AS opponent_possession_pct
    FROM silver.match_lineups ml
    JOIN silver.matches m USING (match_id)
    JOIN gold.team_match_stats tms
      ON tms.match_id = ml.match_id AND tms.team_id = ml.team_id
    LEFT JOIN gold.team_match_stats opp
      ON opp.match_id = ml.match_id AND opp.team_id = tms.opponent_team_id
    -- The slot map. A missing row here violates position NOT NULL and the
    -- build fails loudly, which is the intended behaviour (§5): a silent NULL
    -- position would quietly drop the player out of every percentile peer set.
    LEFT JOIN gold.formation_slot_positions f
      ON f.team_formation     = ml.team_formation
     AND f.formation_position = ml.formation_position
    -- Unused substitutes get no row (§4.2.2): no minutes, no slot, no actions,
    -- and including them makes every COUNT(*) wrong.
    WHERE ml.minute_in IS NOT NULL
      AND m.status = 'completed'
      AND (%(match_ids)s::int[] IS NULL OR ml.match_id = ANY (%(match_ids)s::int[]))
),

-- ── everything countable off the player's own events ───────────────────────
event_stats AS (
    SELECT
        e.match_id, e.player_id,

        -- ═══ §4.6.4 Scoring & finishing ════════════════════════════════════
        count(*) FILTER (WHERE e.event_type = 'Goal')::smallint AS goals,
        count(*) FILTER (WHERE e.event_type = 'Goal'
                           AND e.shot_play_pattern IS DISTINCT FROM 'penalty'
                        )::smallint AS goals_non_penalty,
        count(*) FILTER (WHERE e.event_type IN ('Goal','Attempt Saved','Miss','Post')
                        )::smallint AS shots,
        count(*) FILTER (WHERE e.event_type = 'Goal'
                            OR (e.event_type = 'Attempt Saved'
                                AND NOT e.raw_data->'qualifier' @> '[{"qualifierId": 82}]')
                        )::smallint AS shots_on_target,
        count(*) FILTER (WHERE e.event_type IN ('Goal','Attempt Saved','Miss','Post')
                           AND gold.in_box(e.x, e.y))::smallint AS shots_in_box,
        count(*) FILTER (WHERE e.event_type IN ('Goal','Attempt Saved','Miss','Post')
                           AND NOT gold.in_box(e.x, e.y))::smallint AS shots_outside_box,
        count(*) FILTER (WHERE e.event_type IN ('Goal','Attempt Saved','Miss','Post')
                           AND e.first_time)::smallint AS shots_first_time,
        -- spadl_bodypart_id 1 = HEAD (0 foot, 2 other, 4/5 left/right foot).
        count(*) FILTER (WHERE e.event_type = 'Goal'
                           AND e.spadl_bodypart_id = 1)::smallint AS headed_goals,
        COALESCE(sum(e.xg), 0)::real AS xg,
        COALESCE(sum(e.xg) FILTER (WHERE e.shot_play_pattern IS DISTINCT FROM 'penalty'),
                 0)::real AS npxg,
        count(*) FILTER (WHERE e.raw_data->'qualifier' @> '[{"qualifierId": 214}]'
                        )::smallint AS big_chances,
        count(*) FILTER (WHERE e.event_type = 'Goal'
                           AND e.raw_data->'qualifier' @> '[{"qualifierId": 214}]'
                        )::smallint AS big_chances_scored,
        count(*) FILTER (WHERE e.event_type = 'Chance missed')::smallint AS big_chances_missed,
        count(*) FILTER (WHERE e.shot_play_pattern = 'penalty')::smallint AS penalties_taken,
        count(*) FILTER (WHERE e.shot_play_pattern = 'penalty'
                           AND e.event_type = 'Goal')::smallint AS penalties_scored,

        -- ═══ §4.6.5 Chance creation ════════════════════════════════════════
        -- value_assist holds the typeId of the shot the pass created and
        -- co-occurs with Q210 exactly, so no self-join is needed. '16' is a
        -- Goal (an assist); 13/14/15 are Miss/Post/Attempt Saved (key passes).
        count(*) FILTER (WHERE e.value_assist = '16')::smallint AS assists,
        count(*) FILTER (WHERE e.value_assist IN ('13','14','15'))::smallint AS key_passes,
        count(*) FILTER (WHERE e.raw_data->'qualifier' @> '[{"qualifierId": 218}]'
                        )::smallint AS second_assists,
        count(*) FILTER (WHERE e.event_type = 'Pass'
                           AND e.raw_data->'qualifier' @> '[{"qualifierId": 2}]'
                        )::smallint AS crosses,
        count(*) FILTER (WHERE e.event_type = 'Pass' AND e.outcome = 'success'
                           AND e.raw_data->'qualifier' @> '[{"qualifierId": 2}]'
                        )::smallint AS crosses_completed,
        count(*) FILTER (WHERE e.event_type = 'Pass'
                           AND e.raw_data->'qualifier' @> '[{"qualifierId": 2}]'
                           AND NOT e.raw_data->'qualifier' @> '[{"qualifierId": 5}]'
                           AND NOT e.raw_data->'qualifier' @> '[{"qualifierId": 6}]'
                        )::smallint AS crosses_from_open_play,
        count(*) FILTER (WHERE e.event_type = 'Pass'
                           AND e.raw_data->'qualifier' @> '[{"qualifierId": 4}]'
                        )::smallint AS through_balls,
        count(*) FILTER (WHERE e.event_type = 'Pass'
                           AND gold.in_box(e.end_x, e.end_y)
                           AND NOT gold.in_box(e.x, e.y)
                        )::smallint AS passes_into_penalty_area,

        -- ═══ §4.6.6 Passing & progression ══════════════════════════════════
        count(*) FILTER (WHERE e.event_type = 'Pass')::smallint AS passes,
        count(*) FILTER (WHERE e.event_type = 'Pass' AND e.outcome = 'success'
                        )::smallint AS passes_completed,
        count(*) FILTER (WHERE e.event_type = 'Pass'
                           AND gold.action_length(e.x, e.y, e.end_x, e.end_y) < 15
                        )::smallint AS passes_short,
        count(*) FILTER (WHERE e.event_type = 'Pass'
                           AND gold.action_length(e.x, e.y, e.end_x, e.end_y) >= 15
                           AND gold.action_length(e.x, e.y, e.end_x, e.end_y) <  30
                        )::smallint AS passes_medium,
        count(*) FILTER (WHERE e.event_type = 'Pass'
                           AND gold.action_length(e.x, e.y, e.end_x, e.end_y) >= 30
                        )::smallint AS passes_long,
        count(*) FILTER (WHERE e.event_type = 'Pass' AND e.end_x - e.x >  2
                        )::smallint AS passes_forward,
        count(*) FILTER (WHERE e.event_type = 'Pass' AND e.end_x - e.x < -2
                        )::smallint AS passes_backward,
        count(*) FILTER (WHERE e.event_type = 'Pass'
                           AND gold.is_progressive(e.x, e.y, e.end_x, e.end_y)
                        )::smallint AS progressive_passes,
        count(*) FILTER (WHERE e.event_type = 'Pass' AND e.outcome = 'success'
                           AND gold.is_progressive(e.x, e.y, e.end_x, e.end_y)
                        )::smallint AS progressive_passes_completed,
        count(*) FILTER (WHERE e.event_type = 'Pass' AND e.x < 70 AND e.end_x >= 70
                        )::smallint AS passes_into_final_third,
        count(*) FILTER (WHERE e.raw_data->'qualifier' @> '[{"qualifierId": 157}]'
                        )::smallint AS long_balls,
        count(*) FILTER (WHERE e.outcome = 'success'
                           AND e.raw_data->'qualifier' @> '[{"qualifierId": 157}]'
                        )::smallint AS long_balls_completed,
        count(*) FILTER (WHERE e.event_type = 'Pass'
                           AND abs(e.end_y - e.y) >= 30)::smallint AS switches,
        count(*) FILTER (WHERE e.event_type = 'Offside Pass')::smallint AS offside_passes,
        COALESCE(sum(e.xt) FILTER (WHERE e.event_type = 'Pass'), 0)::real AS xt_pass,

        -- ═══ §4.6.7 Carrying, dribbling & retention ════════════════════════
        count(*) FILTER (WHERE e.event_type = 'Carry')::smallint AS carries,
        COALESCE(sum(gold.action_length(e.x, e.y, e.end_x, e.end_y))
                     FILTER (WHERE e.event_type = 'Carry'), 0)::real AS carry_distance_total,
        count(*) FILTER (WHERE e.event_type = 'Carry'
                           AND gold.is_progressive(e.x, e.y, e.end_x, e.end_y)
                        )::smallint AS progressive_carries,
        count(*) FILTER (WHERE e.event_type = 'Carry' AND e.x < 70 AND e.end_x >= 70
                        )::smallint AS carries_into_final_third,
        count(*) FILTER (WHERE e.event_type = 'Carry'
                           AND gold.in_box(e.end_x, e.end_y)
                           AND NOT gold.in_box(e.x, e.y)
                        )::smallint AS carries_into_penalty_area,
        count(*) FILTER (WHERE e.event_type = 'Take On')::smallint AS take_ons,
        count(*) FILTER (WHERE e.event_type = 'Take On' AND e.outcome = 'success'
                        )::smallint AS take_ons_won,
        count(*) FILTER (WHERE e.event_type = 'Dispossessed')::smallint AS dispossessed,
        -- Same on-ball set as team touches (§9.6 deviation 5): actions taken
        -- while the team has the ball, excluding defensive interventions.
        count(*) FILTER (WHERE e.event_type IN ('Pass','Carry','Take On','Ball touch',
                                                'Clearance','Ball recovery',
                                                'Keeper pick-up','Claim','Punch','Smother',
                                                'Goal','Attempt Saved','Miss','Post')
                        )::smallint AS touches,
        count(*) FILTER (WHERE e.event_type IN ('Pass','Carry','Take On','Ball touch',
                                                'Clearance','Ball recovery',
                                                'Keeper pick-up','Claim','Punch','Smother',
                                                'Goal','Attempt Saved','Miss','Post')
                           AND gold.in_box(e.x, e.y))::smallint AS touches_in_box,
        count(*) FILTER (WHERE e.event_type IN ('Pass','Carry','Take On','Ball touch',
                                                'Clearance','Ball recovery',
                                                'Keeper pick-up','Claim','Punch','Smother',
                                                'Goal','Attempt Saved','Miss','Post')
                           AND e.x >= 70)::smallint AS touches_final_third,
        COALESCE(sum(e.xt) FILTER (WHERE e.event_type = 'Carry'), 0)::real AS xt_carry,

        -- ═══ §4.6.8 Defending ══════════════════════════════════════════════
        count(*) FILTER (WHERE e.event_type = 'Tackle')::smallint AS tackles,
        count(*) FILTER (WHERE e.event_type = 'Tackle' AND e.outcome = 'success'
                        )::smallint AS tackles_won,
        count(*) FILTER (WHERE e.event_type = 'Attempted Tackle')::smallint AS tackles_missed,
        count(*) FILTER (WHERE e.event_type = 'Challenge')::smallint AS dribbled_past,
        count(*) FILTER (WHERE e.event_type = 'Interception')::smallint AS interceptions,
        count(*) FILTER (WHERE e.event_type = 'Clearance')::smallint AS clearances,
        count(*) FILTER (WHERE e.event_type = 'Blocked Pass')::smallint AS blocked_passes,
        count(*) FILTER (WHERE e.event_type = 'Ball recovery')::smallint AS ball_recoveries,
        count(*) FILTER (WHERE e.event_type IN ('Tackle','Interception','Clearance',
                                                'Blocked Pass','Ball recovery')
                        )::smallint AS defensive_actions,
        count(*) FILTER (WHERE e.event_type IN ('Tackle','Interception','Clearance',
                                                'Blocked Pass','Ball recovery')
                           AND e.x >= 70)::smallint AS def_actions_final_third,
        avg(e.x) FILTER (WHERE e.event_type IN ('Tackle','Interception','Clearance',
                                                'Blocked Pass','Ball recovery')
                        )::real AS def_action_avg_x,
        count(*) FILTER (WHERE e.event_type = 'Foul' AND e.outcome = 'failure'
                        )::smallint AS fouls_committed,
        count(*) FILTER (WHERE e.event_type = 'Foul' AND e.outcome = 'success'
                        )::smallint AS fouls_won,
        count(*) FILTER (WHERE e.event_type = 'Error')::smallint AS errors,
        count(*) FILTER (WHERE e.event_type = 'Offside provoked')::smallint AS offsides_provoked,
        -- Opta emits no 'Caught Offside'; the attacking record of being caught
        -- is the Offside Pass itself (§9.6 deviation 4).
        count(*) FILTER (WHERE e.event_type = 'Offside Pass')::smallint AS caught_offside,
        count(*) FILTER (WHERE e.event_type = 'Shield ball opp')::smallint AS shield_ball_opp,

        -- ═══ §4.6.9 Aerial duels ═══════════════════════════════════════════
        count(*) FILTER (WHERE e.event_type = 'Aerial' AND e.outcome = 'success'
                        )::smallint AS aerials_won,
        count(*) FILTER (WHERE e.event_type = 'Aerial' AND e.outcome = 'failure'
                        )::smallint AS aerials_lost,
        count(*) FILTER (WHERE e.event_type = 'Aerial' AND e.outcome = 'success'
                           AND e.x >= 70)::smallint AS aerials_won_att_third,
        count(*) FILTER (WHERE e.event_type = 'Aerial' AND e.outcome = 'success'
                           AND e.x < 35)::smallint AS aerials_won_def_third,

        -- ═══ §4.6.10 Goalkeeping — own events ══════════════════════════════
        count(*) FILTER (WHERE e.event_type = 'Save')::smallint AS saves,
        count(*) FILTER (WHERE e.event_type = 'Penalty faced')::smallint AS penalties_faced,
        count(*) FILTER (WHERE e.event_type = 'Claim')::smallint AS claims,
        count(*) FILTER (WHERE e.event_type = 'Claim' AND e.outcome = 'success'
                        )::smallint AS claims_successful,
        count(*) FILTER (WHERE e.event_type = 'Punch')::smallint AS punches,
        count(*) FILTER (WHERE e.event_type = 'Cross not claimed')::smallint AS crosses_not_claimed,
        count(*) FILTER (WHERE e.event_type = 'Smother')::smallint AS smothers,
        count(*) FILTER (WHERE e.event_type = 'Keeper Sweeper')::smallint AS keeper_sweeper_actions,
        avg(e.x) FILTER (WHERE e.event_type = 'Keeper Sweeper')::real AS sweeper_avg_x,
        count(*) FILTER (WHERE e.event_type = 'Keeper pick-up')::smallint AS keeper_pickups,
        count(*) FILTER (WHERE e.event_type = 'Pass')::smallint AS gk_passes,
        count(*) FILTER (WHERE e.event_type = 'Pass' AND e.outcome = 'success'
                        )::smallint AS gk_passes_completed,
        count(*) FILTER (WHERE e.event_type = 'Pass'
                           AND e.raw_data->'qualifier' @> '[{"qualifierId": 157}]'
                        )::smallint AS gk_long_balls,

        -- ═══ §4.6.11 Model values ══════════════════════════════════════════
        COALESCE(sum(e.xt) FILTER (WHERE e.event_type IN ('Pass','Carry')), 0)::real AS xt,
        COALESCE(sum(e.vaep_value), 0)::real     AS vaep,
        COALESCE(sum(e.vaep_offensive), 0)::real AS vaep_offensive,
        COALESCE(sum(e.vaep_defensive), 0)::real AS vaep_defensive,
        -- COUNT(vaep_value), NOT COUNT(*): only SPADL-valid actions carry a
        -- value and the first action of each match is NULL (§4.6.11).
        count(e.vaep_value)::smallint AS vaep_actions,

        -- ═══ §4.6.12 Discipline ════════════════════════════════════════════
        count(*) FILTER (WHERE e.event_type = 'Card'
                           AND e.raw_data->'qualifier' @> '[{"qualifierId": 31}]'
                        )::smallint AS yellow_cards,
        count(*) FILTER (WHERE e.event_type = 'Card'
                           AND e.raw_data->'qualifier' @> '[{"qualifierId": 32}]'
                        )::smallint AS second_yellows,
        count(*) FILTER (WHERE e.event_type = 'Card'
                           AND e.raw_data->'qualifier' @> '[{"qualifierId": 33}]'
                        )::smallint AS red_cards,

        -- ═══ §4.6.13 Set pieces ════════════════════════════════════════════
        count(*) FILTER (WHERE e.raw_data->'qualifier' @> '[{"qualifierId": 6}]'
                        )::smallint AS corners_taken,
        count(*) FILTER (WHERE e.raw_data->'qualifier' @> '[{"qualifierId": 5}]'
                        )::smallint AS free_kicks_taken,
        count(*) FILTER (WHERE e.shot_play_pattern = 'free_kick')::smallint
                 AS direct_free_kick_shots,
        count(*) FILTER (WHERE e.raw_data->'qualifier' @> '[{"qualifierId": 107}]'
                        )::smallint AS throw_ins_taken,
        count(*) FILTER (WHERE e.event_type = 'Goal'
                           AND e.shot_play_pattern
                               IN ('set_piece','free_kick','from_corner'))::smallint
                 AS set_piece_goals,
        COALESCE(sum(e.xg) FILTER (WHERE e.shot_play_pattern
                                        IN ('set_piece','free_kick','from_corner')),
                 0)::real AS set_piece_xg
    FROM silver.events e
    WHERE e.player_id IS NOT NULL
      AND (%(match_ids)s::int[] IS NULL OR e.match_id = ANY (%(match_ids)s::int[]))
    GROUP BY e.match_id, e.player_id
),

-- ── xa: the xg of the shot each assist pass created (§4.1.2a) ──────────────
-- Same lookup as gold.sequence_players.xa, so the two cannot disagree. Only
-- ~8,000 passes carry a value_assist, so it runs 8,000 times rather than once
-- per event.
assist_xa AS (
    SELECT a.match_id, a.player_id, COALESCE(sum(created.xg), 0)::real AS xa
    FROM silver.events a
    LEFT JOIN LATERAL (
        SELECT sh.xg
        FROM silver.events sh
        WHERE sh.sequence_id = a.sequence_id
          AND (sh.json_index, sh.event_id) > (a.json_index, a.event_id)
          AND sh.xg IS NOT NULL
        ORDER BY sh.json_index, sh.event_id
        LIMIT 1
    ) created ON TRUE
    WHERE a.value_assist IS NOT NULL
      AND a.sequence_id IS NOT NULL
      AND a.player_id IS NOT NULL
      AND (%(match_ids)s::int[] IS NULL OR a.match_id = ANY (%(match_ids)s::int[]))
    GROUP BY a.match_id, a.player_id
),

-- ── passes_received: the next same-team event after a completed pass ───────
-- Synthesised carries make this reliable: a receiver's first action is almost
-- always their own Carry, so the "next same-team event" is the receiver.
passes_received AS (
    SELECT match_id, next_player_id AS player_id, count(*)::smallint AS passes_received
    FROM (
        SELECT e.match_id, e.event_type, e.outcome,
               lead(e.player_id) OVER (PARTITION BY e.match_id, e.team_id
                                       ORDER BY e.json_index, e.event_id) AS next_player_id
        FROM silver.events e
        WHERE (%(match_ids)s::int[] IS NULL OR e.match_id = ANY (%(match_ids)s::int[]))
    ) t
    WHERE event_type = 'Pass' AND outcome = 'success' AND next_player_id IS NOT NULL
    GROUP BY match_id, next_player_id
),

-- ── shot-creating actions: the two TEAMMATE actions before a shot ──────────
-- Scoped to the same sequence_id, so a "build-up" that is actually the
-- opponent's previous possession cannot leak in.
shot_creating AS (
    SELECT match_id, player_id, count(*)::smallint AS shot_creating_actions
    FROM (
        SELECT s.match_id, prev.player_id
        FROM silver.events s
        CROSS JOIN LATERAL (
            SELECT e2.player_id
            FROM silver.events e2
            WHERE e2.sequence_id = s.sequence_id
              AND e2.team_id     = s.team_id
              AND (e2.json_index, e2.event_id) < (s.json_index, s.event_id)
              AND e2.player_id IS NOT NULL
              AND e2.player_id <> s.player_id
            ORDER BY e2.json_index DESC, e2.event_id DESC
            LIMIT 2
        ) prev
        WHERE s.event_type IN ('Goal','Attempt Saved','Miss','Post')
          AND s.sequence_id IS NOT NULL
          AND s.player_id IS NOT NULL
          AND (%(match_ids)s::int[] IS NULL OR s.match_id = ANY (%(match_ids)s::int[]))
    ) t
    GROUP BY match_id, player_id
),

-- ── what the keeper faced while ON THE PITCH ───────────────────────────────
-- Derived from the minute window, NOT from the match score: a keeper subbed at
-- 60' owns only the goals conceded before then (§4.6.10).
--
-- A goal counts as conceded when it increases the opponent's score, which is
-- an opponent goal that is NOT an own goal, or an own goal (Q28) by this
-- keeper's own team.
keeper_faced AS (
    SELECT a.match_id, a.player_id,
           count(*) FILTER (
               WHERE (e.team_id <> a.team_id
                      AND e.event_type = 'Goal'
                      AND NOT e.raw_data->'qualifier' @> '[{"qualifierId": 28}]')
                  OR (e.team_id = a.team_id
                      AND e.event_type = 'Goal'
                      AND e.raw_data->'qualifier' @> '[{"qualifierId": 28}]')
           )::smallint AS goals_conceded,
           COALESCE(sum(e.xg) FILTER (WHERE e.team_id <> a.team_id), 0)::real AS xg_faced,
           count(*) FILTER (
               WHERE e.team_id <> a.team_id
                 AND e.shot_play_pattern = 'penalty'
                 AND e.event_type = 'Attempt Saved'
                 AND NOT e.raw_data->'qualifier' @> '[{"qualifierId": 82}]'
           )::smallint AS penalties_saved
    FROM appearances a
    JOIN silver.events e
      ON e.match_id = a.match_id
     AND e.minute >= a.window_from
     AND e.minute <= a.window_to
     AND e.event_type IN ('Goal','Attempt Saved','Miss','Post')
    WHERE a.position = 'GK'
    GROUP BY a.match_id, a.player_id
)

INSERT INTO gold.player_match_stats (
    match_id, player_id, team_id, opponent_team_id, competition_season_id,
    matchday, match_date, is_home, result,
    position, position_group, formation_position, team_formation,
    started, subbed_on, subbed_off, played_full_match, minutes_played, is_captain,
    goals, goals_non_penalty, shots, shots_on_target, shots_in_box,
    shots_outside_box, shots_first_time, headed_goals, xg, npxg,
    big_chances, big_chances_scored, big_chances_missed,
    penalties_taken, penalties_scored,
    assists, key_passes, xa, second_assists,
    crosses, crosses_completed, crosses_from_open_play, through_balls,
    passes_into_penalty_area, shot_creating_actions,
    passes, passes_completed, passes_received, passes_short, passes_medium,
    passes_long, passes_forward, passes_backward,
    progressive_passes, progressive_passes_completed, passes_into_final_third,
    long_balls, long_balls_completed, switches, offside_passes, xt_pass,
    carries, carry_distance_total, progressive_carries,
    carries_into_final_third, carries_into_penalty_area,
    take_ons, take_ons_won, dispossessed,
    touches, touches_in_box, touches_final_third, xt_carry,
    tackles, tackles_won, tackles_missed, dribbled_past, interceptions,
    clearances, blocked_passes, ball_recoveries, defensive_actions,
    def_actions_final_third, def_action_avg_x, fouls_committed, fouls_won,
    errors, offsides_provoked, caught_offside, shield_ball_opp,
    aerials_won, aerials_lost, aerials_won_att_third, aerials_won_def_third,
    saves, goals_conceded, xg_faced, penalties_faced, penalties_saved,
    claims, claims_successful, punches, crosses_not_claimed, smothers,
    keeper_sweeper_actions, sweeper_avg_x, keeper_pickups,
    gk_passes, gk_passes_completed, gk_long_balls,
    xt, vaep, vaep_offensive, vaep_defensive, vaep_actions,
    yellow_cards, second_yellows, red_cards,
    corners_taken, free_kicks_taken, direct_free_kick_shots, throw_ins_taken,
    set_piece_goals, set_piece_xg,
    opponent_possession_pct
)
SELECT
    a.match_id, a.player_id, a.team_id, a.opponent_team_id, a.competition_season_id,
    a.matchday, a.match_date, a.is_home, a.result,
    a.position, a.position_group, a.formation_position, a.team_formation,
    a.started, a.subbed_on, COALESCE(a.subbed_off, FALSE), a.played_full_match,
    GREATEST(a.minutes_played, 0), a.is_captain,

    -- ── §4.6.4 ─────────────────────────────────────────────────────────────
    COALESCE(es.goals, 0), COALESCE(es.goals_non_penalty, 0),
    COALESCE(es.shots, 0), COALESCE(es.shots_on_target, 0),
    COALESCE(es.shots_in_box, 0), COALESCE(es.shots_outside_box, 0),
    COALESCE(es.shots_first_time, 0), COALESCE(es.headed_goals, 0),
    COALESCE(es.xg, 0), COALESCE(es.npxg, 0),
    COALESCE(es.big_chances, 0), COALESCE(es.big_chances_scored, 0),
    COALESCE(es.big_chances_missed, 0),
    COALESCE(es.penalties_taken, 0), COALESCE(es.penalties_scored, 0),

    -- ── §4.6.5 ─────────────────────────────────────────────────────────────
    COALESCE(es.assists, 0), COALESCE(es.key_passes, 0), COALESCE(ax.xa, 0),
    COALESCE(es.second_assists, 0),
    COALESCE(es.crosses, 0), COALESCE(es.crosses_completed, 0),
    COALESCE(es.crosses_from_open_play, 0), COALESCE(es.through_balls, 0),
    COALESCE(es.passes_into_penalty_area, 0), COALESCE(sc.shot_creating_actions, 0),

    -- ── §4.6.6 ─────────────────────────────────────────────────────────────
    COALESCE(es.passes, 0), COALESCE(es.passes_completed, 0),
    COALESCE(pr.passes_received, 0),
    COALESCE(es.passes_short, 0), COALESCE(es.passes_medium, 0),
    COALESCE(es.passes_long, 0), COALESCE(es.passes_forward, 0),
    COALESCE(es.passes_backward, 0),
    COALESCE(es.progressive_passes, 0), COALESCE(es.progressive_passes_completed, 0),
    COALESCE(es.passes_into_final_third, 0),
    COALESCE(es.long_balls, 0), COALESCE(es.long_balls_completed, 0),
    COALESCE(es.switches, 0), COALESCE(es.offside_passes, 0),
    COALESCE(es.xt_pass, 0),

    -- ── §4.6.7 ─────────────────────────────────────────────────────────────
    COALESCE(es.carries, 0), COALESCE(es.carry_distance_total, 0),
    COALESCE(es.progressive_carries, 0), COALESCE(es.carries_into_final_third, 0),
    COALESCE(es.carries_into_penalty_area, 0),
    COALESCE(es.take_ons, 0), COALESCE(es.take_ons_won, 0),
    COALESCE(es.dispossessed, 0),
    COALESCE(es.touches, 0), COALESCE(es.touches_in_box, 0),
    COALESCE(es.touches_final_third, 0), COALESCE(es.xt_carry, 0),

    -- ── §4.6.8 ─────────────────────────────────────────────────────────────
    COALESCE(es.tackles, 0), COALESCE(es.tackles_won, 0),
    COALESCE(es.tackles_missed, 0), COALESCE(es.dribbled_past, 0),
    COALESCE(es.interceptions, 0), COALESCE(es.clearances, 0),
    COALESCE(es.blocked_passes, 0), COALESCE(es.ball_recoveries, 0),
    COALESCE(es.defensive_actions, 0), COALESCE(es.def_actions_final_third, 0),
    es.def_action_avg_x,
    COALESCE(es.fouls_committed, 0), COALESCE(es.fouls_won, 0),
    COALESCE(es.errors, 0), COALESCE(es.offsides_provoked, 0),
    COALESCE(es.caught_offside, 0), COALESCE(es.shield_ball_opp, 0),

    -- ── §4.6.9 ─────────────────────────────────────────────────────────────
    COALESCE(es.aerials_won, 0), COALESCE(es.aerials_lost, 0),
    COALESCE(es.aerials_won_att_third, 0), COALESCE(es.aerials_won_def_third, 0),

    -- ── §4.6.10 — NULL for every outfield player ───────────────────────────
    CASE WHEN a.position = 'GK' THEN COALESCE(es.saves, 0) END,
    CASE WHEN a.position = 'GK' THEN COALESCE(kf.goals_conceded, 0) END,
    CASE WHEN a.position = 'GK' THEN COALESCE(kf.xg_faced, 0) END,
    CASE WHEN a.position = 'GK' THEN COALESCE(es.penalties_faced, 0) END,
    CASE WHEN a.position = 'GK' THEN COALESCE(kf.penalties_saved, 0) END,
    CASE WHEN a.position = 'GK' THEN COALESCE(es.claims, 0) END,
    CASE WHEN a.position = 'GK' THEN COALESCE(es.claims_successful, 0) END,
    CASE WHEN a.position = 'GK' THEN COALESCE(es.punches, 0) END,
    CASE WHEN a.position = 'GK' THEN COALESCE(es.crosses_not_claimed, 0) END,
    CASE WHEN a.position = 'GK' THEN COALESCE(es.smothers, 0) END,
    CASE WHEN a.position = 'GK' THEN COALESCE(es.keeper_sweeper_actions, 0) END,
    CASE WHEN a.position = 'GK' THEN es.sweeper_avg_x END,
    CASE WHEN a.position = 'GK' THEN COALESCE(es.keeper_pickups, 0) END,
    CASE WHEN a.position = 'GK' THEN COALESCE(es.gk_passes, 0) END,
    CASE WHEN a.position = 'GK' THEN COALESCE(es.gk_passes_completed, 0) END,
    CASE WHEN a.position = 'GK' THEN COALESCE(es.gk_long_balls, 0) END,

    -- ── §4.6.11 ────────────────────────────────────────────────────────────
    COALESCE(es.xt, 0), COALESCE(es.vaep, 0),
    COALESCE(es.vaep_offensive, 0), COALESCE(es.vaep_defensive, 0),
    COALESCE(es.vaep_actions, 0),

    -- ── §4.6.12 ────────────────────────────────────────────────────────────
    COALESCE(es.yellow_cards, 0), COALESCE(es.second_yellows, 0),
    COALESCE(es.red_cards, 0),

    -- ── §4.6.13 ────────────────────────────────────────────────────────────
    COALESCE(es.corners_taken, 0), COALESCE(es.free_kicks_taken, 0),
    COALESCE(es.direct_free_kick_shots, 0), COALESCE(es.throw_ins_taken, 0),
    COALESCE(es.set_piece_goals, 0), COALESCE(es.set_piece_xg, 0),

    -- ── §4.6.14 input ──────────────────────────────────────────────────────
    a.opponent_possession_pct
FROM appearances a
LEFT JOIN event_stats     es ON es.match_id = a.match_id AND es.player_id = a.player_id
LEFT JOIN assist_xa       ax ON ax.match_id = a.match_id AND ax.player_id = a.player_id
LEFT JOIN passes_received pr ON pr.match_id = a.match_id AND pr.player_id = a.player_id
LEFT JOIN shot_creating   sc ON sc.match_id = a.match_id AND sc.player_id = a.player_id
LEFT JOIN keeper_faced    kf ON kf.match_id = a.match_id AND kf.player_id = a.player_id;
