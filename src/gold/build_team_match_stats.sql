-- ═══════════════════════════════════════════════════════════════════════════
-- build_team_match_stats.sql — gold.team_match_stats
-- ═══════════════════════════════════════════════════════════════════════════
--
-- Spec: md/GOLD_LAYER.md §4.3, §4.5.0-§4.5.12, §6 Step 4.
--
-- Runs as:  python -m src.gold.build_gold --step team_match_stats
--
-- Parameter
--   %(match_ids)s :: int[]   NULL = every completed match.
--
-- Sources
--   silver.matches  -> the two team perspectives, result, points, venue
--   silver.events   -> shots, passes, xG, xT, VAEP, defensive actions, cards
--   gold.sequences  -> possession duration, sequence shape, phase counts
--
-- THE _against COLUMNS ARE A SELF-JOIN, NOT A SECOND AGGREGATION. Every
-- mirror column reads the opponent's own row of event_agg (alias `oa`), which
-- guarantees symmetry: xg_for summed over both rows of a match is exactly the
-- match total. Re-aggregating opponent events would not.
--
-- FRAME CONVENTION, and it matters for PPDA. Opta records every event in the
-- acting team's own attacking frame (confirmed on 10,855 aerial-duel pairs:
-- x_a + x_b = 105.00). So an opponent pass at their x = 50 sits at 55 in our
-- frame. The PPDA zone -- the 60 pct of the pitch furthest from the pressing
-- team's goal -- is therefore `x >= 42` for our own defensive actions and
-- `x <= 63` for the opponent's passes. Using x >= 42 for both would measure
-- the opponent's build-up in their own third and invert the metric.
--
-- Strategy: scoped DELETE + INSERT.
--   §4.4.2 specifies ON CONFLICT (match_id, team_id) DO UPDATE. That needs a
--   ~150-column SET list which has to be kept in step with the INSERT list by
--   hand -- a standing invitation to update one and not the other. DELETE +
--   INSERT inside the one transaction gives the same idempotency with one
--   column list. Deliberate deviation, recorded in §9.
-- ═══════════════════════════════════════════════════════════════════════════

DELETE FROM gold.team_match_stats
WHERE %(match_ids)s::int[] IS NULL
   OR match_id = ANY (%(match_ids)s::int[]);


WITH
-- ── one row per team per match, both perspectives ──────────────────────────
-- NOT scoped: team_match_number is a running count over the team's whole
-- season, so a scoped rebuild must still see every earlier match or the
-- numbering silently restarts.
team_matches AS (
    SELECT match_id, competition_season_id,
           home_team_id AS team_id,
           away_team_id AS opponent_team_id,
           matchday, match_date::date AS match_date,
           TRUE AS is_home,
           home_score AS goals_for, away_score AS goals_against,
           home_score_ht AS goals_for_ht, away_score_ht AS goals_against_ht,
           CASE winner WHEN 'home' THEN 'W' WHEN 'away' THEN 'L' ELSE 'D' END AS result,
           CASE winner WHEN 'home' THEN 3   WHEN 'away' THEN 0   ELSE 1   END AS points
    FROM silver.matches
    WHERE status = 'completed'
    UNION ALL
    SELECT match_id, competition_season_id,
           away_team_id, home_team_id,
           matchday, match_date::date,
           FALSE,
           away_score, home_score,
           away_score_ht, home_score_ht,
           CASE winner WHEN 'away' THEN 'W' WHEN 'home' THEN 'L' ELSE 'D' END,
           CASE winner WHEN 'away' THEN 3   WHEN 'home' THEN 0   ELSE 1   END
    FROM silver.matches
    WHERE status = 'completed'
),

numbered AS (
    SELECT tm.*,
           ROW_NUMBER() OVER (PARTITION BY competition_season_id, team_id
                              ORDER BY match_date, match_id)::smallint
               AS team_match_number
    FROM team_matches tm
),

-- ── event-level aggregation, one row per (match, team) ─────────────────────
-- The LEFT JOIN to gold.sequences is only for xt_open_play: an event with no
-- sequence_id is open play by default, which is why it is a LEFT JOIN and not
-- an inner one.
event_agg AS (
    SELECT
        e.match_id, e.team_id,

        -- ═══ §4.5.1 goals by period, and own goals ═════════════════════════
        -- goals_for / goals_against come from silver.matches, which already
        -- attributes own goals to the right side. These per-period counts have
        -- to do it by hand: Q28 marks a Goal event scored into the scorer's
        -- OWN net, so it counts for the opponent.
        count(*) FILTER (WHERE e.event_type = 'Goal' AND e.period = 1
                           AND NOT e.raw_data->'qualifier' @> '[{"qualifierId": 28}]'
                        )::smallint AS goals_scored_p1,
        count(*) FILTER (WHERE e.event_type = 'Goal' AND e.period = 2
                           AND NOT e.raw_data->'qualifier' @> '[{"qualifierId": 28}]'
                        )::smallint AS goals_scored_p2,
        count(*) FILTER (WHERE e.event_type = 'Goal' AND e.period = 1
                           AND e.raw_data->'qualifier' @> '[{"qualifierId": 28}]'
                        )::smallint AS own_goals_p1,
        count(*) FILTER (WHERE e.event_type = 'Goal' AND e.period = 2
                           AND e.raw_data->'qualifier' @> '[{"qualifierId": 28}]'
                        )::smallint AS own_goals_p2,
        count(*) FILTER (WHERE e.event_type = 'Goal'
                           AND e.raw_data->'qualifier' @> '[{"qualifierId": 28}]'
                        )::smallint AS own_goals_total,

        -- ═══ §4.5.2 Shots ═════════════════════════════════════════════════
        count(*) FILTER (WHERE e.event_type IN ('Goal','Attempt Saved','Miss','Post')
                        )::smallint AS shots,
        count(*) FILTER (WHERE e.event_type = 'Goal'
                            OR (e.event_type = 'Attempt Saved'
                                AND NOT e.raw_data->'qualifier' @> '[{"qualifierId": 82}]')
                        )::smallint AS shots_on_target,
        count(*) FILTER (WHERE e.event_type = 'Attempt Saved'
                           AND e.raw_data->'qualifier' @> '[{"qualifierId": 82}]'
                        )::smallint AS shots_blocked,
        count(*) FILTER (WHERE e.event_type = 'Miss')::smallint AS shots_off_target,
        count(*) FILTER (WHERE e.event_type = 'Post')::smallint AS shots_woodwork,
        count(*) FILTER (WHERE e.event_type IN ('Goal','Attempt Saved','Miss','Post')
                           AND gold.in_box(e.x, e.y))::smallint AS shots_inside_box,
        count(*) FILTER (WHERE e.event_type IN ('Goal','Attempt Saved','Miss','Post')
                           AND NOT gold.in_box(e.x, e.y))::smallint AS shots_outside_box,
        count(*) FILTER (WHERE e.event_type IN ('Goal','Attempt Saved','Miss','Post')
                           AND e.first_time)::smallint AS shots_first_time,
        count(*) FILTER (WHERE e.event_type IN ('Goal','Attempt Saved','Miss','Post')
                           AND e.raw_data->'qualifier' @> '[{"qualifierId": 215}]'
                        )::smallint AS shots_individual_play,
        count(*) FILTER (WHERE e.raw_data->'qualifier' @> '[{"qualifierId": 214}]'
                        )::smallint AS big_chances,
        count(*) FILTER (WHERE e.event_type = 'Goal'
                           AND e.raw_data->'qualifier' @> '[{"qualifierId": 214}]'
                        )::smallint AS big_chances_scored,
        count(*) FILTER (WHERE e.shot_play_pattern = 'penalty')::smallint AS penalties_taken,
        count(*) FILTER (WHERE e.shot_play_pattern = 'penalty'
                           AND e.event_type = 'Goal')::smallint AS penalties_scored,

        -- ═══ §4.5.3 Expected goals ════════════════════════════════════════
        COALESCE(sum(e.xg), 0)::real AS xg_for,
        COALESCE(sum(e.xg) FILTER (WHERE e.shot_play_pattern IS DISTINCT FROM 'penalty'),
                 0)::real AS npxg_for,
        COALESCE(sum(e.xg) FILTER (WHERE e.shot_play_pattern = 'regular_play'),
                 0)::real AS xg_open_play,
        COALESCE(sum(e.xg) FILTER (WHERE e.shot_play_pattern
                                        IN ('set_piece','free_kick','from_corner')),
                 0)::real AS xg_set_piece,
        COALESCE(sum(e.xg) FILTER (WHERE e.shot_play_pattern = 'from_corner'),
                 0)::real AS xg_from_corner,
        COALESCE(sum(e.xg) FILTER (WHERE e.shot_play_pattern = 'free_kick'),
                 0)::real AS xg_free_kick,
        COALESCE(sum(e.xg) FILTER (WHERE e.shot_play_pattern = 'fast_break'),
                 0)::real AS xg_fast_break,
        COALESCE(sum(e.xg) FILTER (WHERE e.shot_play_pattern = 'penalty'),
                 0)::real AS xg_penalty,
        count(*) FILTER (WHERE e.event_type = 'Goal'
                           AND e.shot_play_pattern = 'regular_play')::smallint
                 AS goals_open_play,
        count(*) FILTER (WHERE e.event_type = 'Goal'
                           AND e.shot_play_pattern
                               IN ('set_piece','free_kick','from_corner'))::smallint
                 AS goals_set_piece,
        count(*) FILTER (WHERE e.event_type = 'Goal'
                           AND e.shot_play_pattern = 'fast_break')::smallint
                 AS goals_fast_break,

        -- ═══ §4.5.4 Passing ═══════════════════════════════════════════════
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
        count(*) FILTER (WHERE e.event_type = 'Pass' AND abs(e.end_x - e.x) <= 2
                        )::smallint AS passes_sideways,
        count(*) FILTER (WHERE e.event_type = 'Pass'
                           AND gold.is_progressive(e.x, e.y, e.end_x, e.end_y)
                        )::smallint AS progressive_passes,
        count(*) FILTER (WHERE e.event_type = 'Pass' AND e.outcome = 'success'
                           AND gold.is_progressive(e.x, e.y, e.end_x, e.end_y)
                        )::smallint AS progressive_passes_completed,
        count(*) FILTER (WHERE e.event_type = 'Pass' AND e.x < 70 AND e.end_x >= 70
                        )::smallint AS passes_into_final_third,
        count(*) FILTER (WHERE e.event_type = 'Pass'
                           AND gold.in_box(e.end_x, e.end_y)
                           AND NOT gold.in_box(e.x, e.y)
                        )::smallint AS passes_into_penalty_area,
        count(*) FILTER (WHERE e.event_type = 'Pass'
                           AND e.raw_data->'qualifier' @> '[{"qualifierId": 2}]'
                        )::smallint AS crosses,
        count(*) FILTER (WHERE e.event_type = 'Pass' AND e.outcome = 'success'
                           AND e.raw_data->'qualifier' @> '[{"qualifierId": 2}]'
                        )::smallint AS crosses_completed,
        count(*) FILTER (WHERE e.event_type = 'Pass'
                           AND e.raw_data->'qualifier' @> '[{"qualifierId": 4}]'
                        )::smallint AS through_balls,
        count(*) FILTER (WHERE e.event_type = 'Pass'
                           AND e.raw_data->'qualifier' @> '[{"qualifierId": 157}]'
                        )::smallint AS long_balls,
        -- value_assist carries the typeId of the shot the pass created and
        -- co-occurs with Q210 exactly (7,439/7,439), so no join is needed.
        -- '16' is a Goal, so it is an assist; 13/14/15 are Miss/Post/Attempt
        -- Saved, so they are key passes.
        count(*) FILTER (WHERE e.value_assist IN ('13','14','15'))::smallint AS key_passes,
        count(*) FILTER (WHERE e.value_assist = '16')::smallint AS assists,
        count(*) FILTER (WHERE e.raw_data->'qualifier' @> '[{"qualifierId": 218}]'
                        )::smallint AS second_assists,
        count(*) FILTER (WHERE e.event_type = 'Offside Pass')::smallint AS offside_passes,
        count(*) FILTER (WHERE e.event_type = 'Pass' AND e.x <  35)::smallint AS passes_def_third,
        count(*) FILTER (WHERE e.event_type = 'Pass' AND e.x >= 35 AND e.x < 70
                        )::smallint AS passes_mid_third,
        count(*) FILTER (WHERE e.event_type = 'Pass' AND e.x >= 70)::smallint AS passes_att_third,
        -- PPDA numerator, seen from the OTHER side: these are the passes this
        -- team made inside the zone its opponent is pressing (x <= 63 in our
        -- own frame == x >= 42 in theirs). The final SELECT reads it from the
        -- opponent's row.
        count(*) FILTER (WHERE e.event_type = 'Pass' AND e.x <= 63
                        )::smallint AS passes_in_opp_press_zone,

        -- ═══ §4.5.5 Carrying & dribbling ══════════════════════════════════
        count(*) FILTER (WHERE e.event_type = 'Carry')::smallint AS carries,
        COALESCE(sum(gold.action_length(e.x, e.y, e.end_x, e.end_y))
                     FILTER (WHERE e.event_type = 'Carry'), 0)::real
                 AS carry_distance_total,
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
        count(*) FILTER (WHERE e.event_type = 'Error')::smallint AS errors,

        -- ═══ §4.5.6 Model values ══════════════════════════════════════════
        COALESCE(sum(e.xt) FILTER (WHERE e.event_type IN ('Pass','Carry')), 0)::real
                 AS xt_for,
        COALESCE(sum(e.xt) FILTER (WHERE e.event_type IN ('Pass','Carry')
                                     AND COALESCE(sq.sequence_type, 'open_play')
                                         NOT IN ('corner','free_kick','throw_in')), 0)::real
                 AS xt_open_play,
        COALESCE(sum(e.vaep_value), 0)::real      AS vaep_for,
        COALESCE(sum(e.vaep_offensive), 0)::real  AS vaep_offensive_for,
        COALESCE(sum(e.vaep_defensive), 0)::real  AS vaep_defensive_for,

        -- ═══ §4.5.7 Defensive actions ═════════════════════════════════════
        count(*) FILTER (WHERE e.event_type = 'Tackle')::smallint AS tackles,
        count(*) FILTER (WHERE e.event_type = 'Tackle' AND e.outcome = 'success'
                        )::smallint AS tackles_won,
        count(*) FILTER (WHERE e.event_type = 'Interception')::smallint AS interceptions,
        count(*) FILTER (WHERE e.event_type = 'Clearance')::smallint AS clearances,
        count(*) FILTER (WHERE e.event_type = 'Blocked Pass')::smallint AS blocked_passes,
        count(*) FILTER (WHERE e.event_type = 'Ball recovery')::smallint AS ball_recoveries,
        count(*) FILTER (WHERE e.event_type = 'Aerial' AND e.outcome = 'success'
                        )::smallint AS aerials_won,
        count(*) FILTER (WHERE e.event_type = 'Aerial' AND e.outcome = 'failure'
                        )::smallint AS aerials_lost,
        -- Ground + aerial duels. The original definition was `Challenge` +
        -- `50/50`, which can only ever count zero wins: Opta emits no `50/50`
        -- on this feed, and a `Challenge` IS the record of being dribbled past,
        -- so all 6,998 of them carry outcome = 'failure'. Tackle and Aerial
        -- are the two contested events that carry both outcomes, so the pair
        -- is symmetric and duels_won + duels_lost is duels contested.
        -- Note this makes duels_won == tackles_won + aerials_won by
        -- construction; the columns are kept separate because the season
        -- profile reads them as different rows.
        count(*) FILTER (WHERE e.event_type IN ('Tackle','Aerial')
                           AND e.outcome = 'success')::smallint AS duels_won,
        count(*) FILTER (WHERE e.event_type IN ('Tackle','Aerial')
                           AND e.outcome = 'failure')::smallint AS duels_lost,
        -- Foul is a mirror pair: 'failure' is the foul COMMITTED, 'success'
        -- the foul WON. Reversing these inverts both columns.
        count(*) FILTER (WHERE e.event_type = 'Foul' AND e.outcome = 'failure'
                        )::smallint AS fouls_committed,
        count(*) FILTER (WHERE e.event_type = 'Foul' AND e.outcome = 'success'
                        )::smallint AS fouls_won,
        count(*) FILTER (WHERE e.event_type = 'Offside provoked')::smallint
                 AS offsides_provoked,
        -- Opta emits no 'Caught Offside' event; the attacking side's record of
        -- being caught is the Offside Pass itself (§9 deviation 4).
        count(*) FILTER (WHERE e.event_type = 'Offside Pass')::smallint AS caught_offside,
        count(*) FILTER (WHERE e.event_type IN ('Tackle','Interception','Clearance',
                                                'Blocked Pass','Ball recovery')
                        )::smallint AS defensive_actions,

        -- ═══ §4.5.8 Goalkeeping ═══════════════════════════════════════════
        -- Q94 on a Save event marks an OUTFIELD BLOCK, not a goalkeeper save:
        -- 2,941 of 5,680 Save events league-wide carry it, and zero of them
        -- belong to a goalkeeper. Counting them would push save_pct above 1.
        count(*) FILTER (WHERE e.event_type = 'Save'
                           AND NOT e.raw_data->'qualifier' @> '[{"qualifierId": 94}]'
                        )::smallint AS saves,
        count(*) FILTER (WHERE e.event_type = 'Claim')::smallint AS claims,
        count(*) FILTER (WHERE e.event_type = 'Punch')::smallint AS punches,
        count(*) FILTER (WHERE e.event_type = 'Smother')::smallint AS smothers,
        count(*) FILTER (WHERE e.event_type = 'Cross not claimed')::smallint
                 AS crosses_not_claimed,
        count(*) FILTER (WHERE e.event_type = 'Keeper Sweeper')::smallint
                 AS keeper_sweeper_actions,
        count(*) FILTER (WHERE e.event_type = 'Keeper pick-up')::smallint
                 AS keeper_pickups,

        -- ═══ §4.5.9 Possession & territory ════════════════════════════════
        -- "Touches" is the on-ball set: actions taken while the team has the
        -- ball. Defensive interventions (Tackle, Interception, Blocked Pass,
        -- Aerial, Save) are deliberately excluded -- they happen while the
        -- OPPONENT has the ball, and counting them would drag field_tilt down
        -- for exactly the teams that defend most actively (§9 deviation 5).
        count(*) FILTER (WHERE e.event_type IN ('Pass','Carry','Take On','Ball touch',
                                                'Clearance','Ball recovery',
                                                'Keeper pick-up','Claim','Punch','Smother',
                                                'Goal','Attempt Saved','Miss','Post')
                        )::smallint AS touches,
        count(*) FILTER (WHERE e.event_type IN ('Pass','Carry','Take On','Ball touch',
                                                'Clearance','Ball recovery',
                                                'Keeper pick-up','Claim','Punch','Smother',
                                                'Goal','Attempt Saved','Miss','Post')
                           AND e.x < 35)::smallint AS touches_def_third,
        count(*) FILTER (WHERE e.event_type IN ('Pass','Carry','Take On','Ball touch',
                                                'Clearance','Ball recovery',
                                                'Keeper pick-up','Claim','Punch','Smother',
                                                'Goal','Attempt Saved','Miss','Post')
                           AND e.x >= 35 AND e.x < 70)::smallint AS touches_mid_third,
        count(*) FILTER (WHERE e.event_type IN ('Pass','Carry','Take On','Ball touch',
                                                'Clearance','Ball recovery',
                                                'Keeper pick-up','Claim','Punch','Smother',
                                                'Goal','Attempt Saved','Miss','Post')
                           AND e.x >= 70)::smallint AS touches_final_third,
        count(*) FILTER (WHERE e.event_type IN ('Pass','Carry','Take On','Ball touch',
                                                'Clearance','Ball recovery',
                                                'Keeper pick-up','Claim','Punch','Smother',
                                                'Goal','Attempt Saved','Miss','Post')
                           AND gold.in_box(e.x, e.y))::smallint AS touches_in_box,
        avg(e.x) FILTER (WHERE e.event_type IN ('Pass','Carry','Take On','Ball touch',
                                                'Clearance','Ball recovery',
                                                'Keeper pick-up','Claim','Punch','Smother',
                                                'Goal','Attempt Saved','Miss','Post')
                        )::real AS avg_action_x,

        -- ═══ §4.5.11 Pressing ═════════════════════════════════════════════
        -- The NARROW set: Tackle + Interception + Challenge + Foul committed.
        -- Clearances and ball recoveries are excluded on purpose -- including
        -- them roughly doubles the denominator and the extra volume lands on
        -- deep-blocking sides, which would report them as pressing hardest.
        count(*) FILTER (WHERE e.x >= 42
                           AND (e.event_type IN ('Tackle','Interception','Challenge')
                                OR (e.event_type = 'Foul' AND e.outcome = 'failure'))
                        )::smallint AS ppda_def_actions,
        count(*) FILTER (WHERE e.event_type = 'Ball recovery' AND e.x >= 70
                        )::smallint AS high_turnovers,
        avg(e.x) FILTER (WHERE e.event_type IN ('Tackle','Interception','Clearance',
                                                'Blocked Pass','Ball recovery')
                        )::real AS defensive_line_height,

        -- ═══ §4.5.12 Set pieces & discipline ══════════════════════════════
        -- Corner Awarded is a perfect mirror pair (4,473 / 4,473); 'success'
        -- is the side that won it.
        count(*) FILTER (WHERE e.event_type = 'Corner Awarded' AND e.outcome = 'success'
                        )::smallint AS corners_for,
        count(*) FILTER (WHERE e.raw_data->'qualifier' @> '[{"qualifierId": 5}]'
                        )::smallint AS free_kicks_taken,
        count(*) FILTER (WHERE e.raw_data->'qualifier' @> '[{"qualifierId": 107}]'
                        )::smallint AS throw_ins_taken,
        count(*) FILTER (WHERE e.event_type = 'Goal'
                           AND e.shot_play_pattern
                               IN ('set_piece','free_kick','from_corner')
                        )::smallint AS set_piece_goals_for,
        -- Cards come from events (Q31/32/33), not from match_lineups: a card
        -- shown to an already-substituted player produces no exit.
        count(*) FILTER (WHERE e.event_type = 'Card'
                           AND e.raw_data->'qualifier' @> '[{"qualifierId": 31}]'
                        )::smallint AS yellow_cards,
        count(*) FILTER (WHERE e.event_type = 'Card'
                           AND e.raw_data->'qualifier' @> '[{"qualifierId": 32}]'
                        )::smallint AS second_yellows,
        count(*) FILTER (WHERE e.event_type = 'Card'
                           AND e.raw_data->'qualifier' @> '[{"qualifierId": 33}]'
                        )::smallint AS red_cards
    FROM silver.events e
    LEFT JOIN gold.sequences sq ON sq.sequence_id = e.sequence_id
    WHERE (%(match_ids)s::int[] IS NULL OR e.match_id = ANY (%(match_ids)s::int[]))
    GROUP BY e.match_id, e.team_id
),

-- ── xa: the xg of the shot each assist pass created ────────────────────────
-- Resolved within the sequence (§4.1.2a) rather than through the Q55 join, so
-- team xa and gold.sequence_players.xa cannot disagree. The ~301 orphaned
-- passes with no sequence_id are the known shortfall.
assist_xa AS (
    SELECT a.match_id, a.team_id, COALESCE(sum(created.xg), 0)::real AS xa
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
      AND (%(match_ids)s::int[] IS NULL OR a.match_id = ANY (%(match_ids)s::int[]))
    GROUP BY a.match_id, a.team_id
),

-- ── counterpress: won back within 5 s of losing it ─────────────────────────
-- Operational definition: our sequence opens with a regain, the sequence
-- immediately before it was the opponent's, the one before that was ours, and
-- the opponent held the ball for no more than 5 seconds.
--
-- THE ORDERING KEY IS CHRONOLOGY, NOT sequence_number. sequence_number is the
-- ordinal within ONE TEAM's series -- '933_home_001' and '933_away_001' both
-- carry 1 -- so ordering a match by it ties every value and interleaves the
-- two teams arbitrarily. lag() then reads a "previous sequence" that is not
-- the previous sequence, and because the tie is broken by whatever order the
-- scan returns, the answer changes between a scoped and a full rebuild.
-- Caught by the scoped-vs-full determinism check in §9.5 G10.
counterpress AS (
    SELECT match_id, team_id,
           count(*)::smallint AS counterpress_recoveries
    FROM (
        SELECT s.match_id, s.team_id, s.start_trigger,
               s.start_minute * 60 + s.start_second AS start_clock,
               lag(s.team_id, 1) OVER w AS prev_team,
               lag(s.team_id, 2) OVER w AS prev2_team,
               lag(s.start_minute * 60 + s.start_second, 1) OVER w AS prev_clock
        FROM gold.sequences s
        WHERE (%(match_ids)s::int[] IS NULL OR s.match_id = ANY (%(match_ids)s::int[]))
        WINDOW w AS (PARTITION BY s.match_id
                     ORDER BY s.period, s.start_minute, s.start_second,
                              s.start_event_id)
    ) t
    WHERE start_trigger IN ('tackle','interception','recovery','block')
      AND prev_team  IS DISTINCT FROM team_id
      AND prev2_team = team_id
      AND start_clock - prev_clock <= 5
    GROUP BY match_id, team_id
),

-- ── sequence-level aggregation, one row per (match, team) ──────────────────
sequence_agg AS (
    SELECT
        s.match_id, s.team_id,
        count(*)::smallint                              AS sequences,
        COALESCE(sum(s.pass_count), 0)::smallint        AS sequence_passes_total,
        COALESCE(sum(s.duration_seconds), 0)::real      AS sequence_duration_total,
        -- THE ONE-SECOND FLOOR (§4.5.0). duration_seconds is 0 on 31 pct of
        -- sequences because the clock has one-second resolution, and the loss
        -- is not symmetric: it falls on clearances and recoveries, which
        -- flatters the pressing side. Without the floor, possession_pct is
        -- measurably wrong.
        COALESCE(sum(GREATEST(s.duration_seconds, 1.0)), 0)::real
                                                        AS possession_duration_sec,
        count(*) FILTER (WHERE s.pass_count >= 10)::smallint AS long_sequences,
        count(*) FILTER (WHERE s.outcome IN ('goal','shot_saved','shot_blocked',
                                             'shot_off_target','shot_woodwork')
                        )::smallint AS sequences_ending_in_shot,
        count(*) FILTER (WHERE s.outcome = 'goal')::smallint AS sequences_ending_in_goal,
        count(*) FILTER (WHERE s.final_third_entry)::smallint AS final_third_entries,
        count(*) FILTER (WHERE s.penalty_box_entry)::smallint AS penalty_box_entries,
        count(*) FILTER (WHERE s.start_third = 1)::smallint AS sequences_started_def_third,
        count(*) FILTER (WHERE s.start_third = 2)::smallint AS sequences_started_mid_third,
        count(*) FILTER (WHERE s.start_third = 3)::smallint AS sequences_started_att_third,
        count(*) FILTER (WHERE s.sequence_type = 'goal_kick'
                            OR s.start_trigger = 'keeper')::smallint
                 AS buildup_sequences_from_gk,
        count(*) FILTER (WHERE s.has_counter_attack)::smallint  AS counter_attack_sequences,
        count(*) FILTER (WHERE s.has_high_transition)::smallint AS high_transition_sequences,
        count(*) FILTER (WHERE s.sequence_type IN ('corner','free_kick','throw_in')
                        )::smallint AS set_piece_sequences,
        COALESCE(sum(s.xt), 0)::real                     AS sequence_xt_total,
        COALESCE(sum(s.distance_covered_m), 0)::real     AS sequence_distance_total,
        COALESCE(sum(s.field_progression), 0)::real      AS field_progression_total,
        -- A turnover regain that reaches the final third (§4.5.11).
        count(*) FILTER (WHERE s.start_trigger IN ('tackle','interception','recovery','block')
                           AND s.final_third_entry)::smallint AS counterattack_sequences,
        -- A high regain that produced a shot in the same possession.
        count(*) FILTER (WHERE s.start_trigger = 'recovery'
                           AND s.start_x >= 70
                           AND s.shot_count > 0)::smallint AS high_turnover_shots,
        -- Read from the OPPONENT's row in the final SELECT: their sequences
        -- that died in their own third are this team's build-up disruption.
        count(*) FILTER (WHERE s.end_third = 1)::smallint AS sequences_ended_own_third
    FROM gold.sequences s
    WHERE (%(match_ids)s::int[] IS NULL OR s.match_id = ANY (%(match_ids)s::int[]))
    GROUP BY s.match_id, s.team_id
)

INSERT INTO gold.team_match_stats (
    match_id, team_id, competition_season_id, opponent_team_id,
    matchday, match_date, team_match_number, is_home, result, points,
    goals_for, goals_against, goals_for_ht, goals_against_ht,
    goals_for_p1, goals_for_p2, goals_against_p1, goals_against_p2,
    own_goals_for, own_goals_against,
    shots, shots_on_target, shots_blocked, shots_off_target, shots_woodwork,
    shots_inside_box, shots_outside_box, shots_first_time, shots_individual_play,
    big_chances, big_chances_scored, penalties_taken, penalties_scored,
    shots_against, shots_on_target_against, shots_inside_box_against,
    big_chances_against,
    xg_for, xg_against, npxg_for, npxg_against,
    xg_open_play, xg_set_piece, xg_from_corner, xg_free_kick, xg_fast_break,
    xg_penalty, goals_open_play, goals_set_piece, goals_fast_break,
    passes, passes_completed, passes_short, passes_medium, passes_long,
    passes_forward, passes_backward, passes_sideways,
    progressive_passes, progressive_passes_completed,
    passes_into_final_third, passes_into_penalty_area,
    crosses, crosses_completed, through_balls, long_balls,
    key_passes, assists, second_assists, xa, offside_passes, passes_against,
    passes_def_third, passes_mid_third, passes_att_third,
    carries, carry_distance_total, progressive_carries,
    carries_into_final_third, carries_into_penalty_area,
    take_ons, take_ons_won, dispossessed, errors,
    xt_for, xt_against, xt_open_play,
    vaep_for, vaep_against, vaep_offensive_for, vaep_defensive_for,
    tackles, tackles_won, interceptions, clearances, blocked_passes,
    ball_recoveries, aerials_won, aerials_lost, duels_won, duels_lost,
    fouls_committed, fouls_won, offsides_provoked, caught_offside,
    defensive_actions,
    saves, claims, punches, smothers, crosses_not_claimed,
    keeper_sweeper_actions, keeper_pickups,
    possession_duration_sec, possession_duration_opp_sec,
    touches, touches_def_third, touches_mid_third, touches_final_third,
    touches_in_box, touches_final_third_opp, avg_action_x,
    sequences, sequence_passes_total, sequence_duration_total, long_sequences,
    sequences_ending_in_shot, sequences_ending_in_goal,
    final_third_entries, penalty_box_entries,
    sequences_started_def_third, sequences_started_mid_third,
    sequences_started_att_third, buildup_sequences_from_gk,
    counter_attack_sequences, high_transition_sequences, set_piece_sequences,
    sequence_xt_total, sequence_distance_total, field_progression_total,
    ppda_opp_passes, ppda_def_actions, high_turnovers, high_turnover_shots,
    counterpress_recoveries, counterattack_sequences,
    opp_sequences_ended_own_third, defensive_line_height, ppda_against,
    corners_for, corners_against, free_kicks_taken, throw_ins_taken,
    set_piece_goals_for, set_piece_goals_against,
    yellow_cards, second_yellows, red_cards
)
SELECT
    n.match_id, n.team_id, n.competition_season_id, n.opponent_team_id,
    n.matchday, n.match_date, n.team_match_number, n.is_home, n.result, n.points,

    -- ── §4.5.1 ─────────────────────────────────────────────────────────────
    n.goals_for, n.goals_against, n.goals_for_ht, n.goals_against_ht,
    COALESCE(ea.goals_scored_p1, 0) + COALESCE(oa.own_goals_p1, 0),
    COALESCE(ea.goals_scored_p2, 0) + COALESCE(oa.own_goals_p2, 0),
    COALESCE(oa.goals_scored_p1, 0) + COALESCE(ea.own_goals_p1, 0),
    COALESCE(oa.goals_scored_p2, 0) + COALESCE(ea.own_goals_p2, 0),
    COALESCE(oa.own_goals_total, 0),
    COALESCE(ea.own_goals_total, 0),

    -- ── §4.5.2 ─────────────────────────────────────────────────────────────
    COALESCE(ea.shots, 0), COALESCE(ea.shots_on_target, 0),
    COALESCE(ea.shots_blocked, 0), COALESCE(ea.shots_off_target, 0),
    COALESCE(ea.shots_woodwork, 0), COALESCE(ea.shots_inside_box, 0),
    COALESCE(ea.shots_outside_box, 0), COALESCE(ea.shots_first_time, 0),
    COALESCE(ea.shots_individual_play, 0), COALESCE(ea.big_chances, 0),
    COALESCE(ea.big_chances_scored, 0), COALESCE(ea.penalties_taken, 0),
    COALESCE(ea.penalties_scored, 0),
    COALESCE(oa.shots, 0), COALESCE(oa.shots_on_target, 0),
    COALESCE(oa.shots_inside_box, 0), COALESCE(oa.big_chances, 0),

    -- ── §4.5.3 ─────────────────────────────────────────────────────────────
    COALESCE(ea.xg_for, 0), COALESCE(oa.xg_for, 0),
    COALESCE(ea.npxg_for, 0), COALESCE(oa.npxg_for, 0),
    COALESCE(ea.xg_open_play, 0), COALESCE(ea.xg_set_piece, 0),
    COALESCE(ea.xg_from_corner, 0), COALESCE(ea.xg_free_kick, 0),
    COALESCE(ea.xg_fast_break, 0), COALESCE(ea.xg_penalty, 0),
    COALESCE(ea.goals_open_play, 0), COALESCE(ea.goals_set_piece, 0),
    COALESCE(ea.goals_fast_break, 0),

    -- ── §4.5.4 ─────────────────────────────────────────────────────────────
    COALESCE(ea.passes, 0), COALESCE(ea.passes_completed, 0),
    COALESCE(ea.passes_short, 0), COALESCE(ea.passes_medium, 0),
    COALESCE(ea.passes_long, 0), COALESCE(ea.passes_forward, 0),
    COALESCE(ea.passes_backward, 0), COALESCE(ea.passes_sideways, 0),
    COALESCE(ea.progressive_passes, 0), COALESCE(ea.progressive_passes_completed, 0),
    COALESCE(ea.passes_into_final_third, 0), COALESCE(ea.passes_into_penalty_area, 0),
    COALESCE(ea.crosses, 0), COALESCE(ea.crosses_completed, 0),
    COALESCE(ea.through_balls, 0), COALESCE(ea.long_balls, 0),
    COALESCE(ea.key_passes, 0), COALESCE(ea.assists, 0),
    COALESCE(ea.second_assists, 0), COALESCE(ax.xa, 0),
    COALESCE(ea.offside_passes, 0), COALESCE(oa.passes, 0),
    COALESCE(ea.passes_def_third, 0), COALESCE(ea.passes_mid_third, 0),
    COALESCE(ea.passes_att_third, 0),

    -- ── §4.5.5 ─────────────────────────────────────────────────────────────
    COALESCE(ea.carries, 0), COALESCE(ea.carry_distance_total, 0),
    COALESCE(ea.progressive_carries, 0), COALESCE(ea.carries_into_final_third, 0),
    COALESCE(ea.carries_into_penalty_area, 0),
    COALESCE(ea.take_ons, 0), COALESCE(ea.take_ons_won, 0),
    COALESCE(ea.dispossessed, 0), COALESCE(ea.errors, 0),

    -- ── §4.5.6 ─────────────────────────────────────────────────────────────
    COALESCE(ea.xt_for, 0), COALESCE(oa.xt_for, 0), COALESCE(ea.xt_open_play, 0),
    COALESCE(ea.vaep_for, 0), COALESCE(oa.vaep_for, 0),
    COALESCE(ea.vaep_offensive_for, 0), COALESCE(ea.vaep_defensive_for, 0),

    -- ── §4.5.7 ─────────────────────────────────────────────────────────────
    COALESCE(ea.tackles, 0), COALESCE(ea.tackles_won, 0),
    COALESCE(ea.interceptions, 0), COALESCE(ea.clearances, 0),
    COALESCE(ea.blocked_passes, 0), COALESCE(ea.ball_recoveries, 0),
    COALESCE(ea.aerials_won, 0), COALESCE(ea.aerials_lost, 0),
    COALESCE(ea.duels_won, 0), COALESCE(ea.duels_lost, 0),
    COALESCE(ea.fouls_committed, 0), COALESCE(ea.fouls_won, 0),
    COALESCE(ea.offsides_provoked, 0), COALESCE(ea.caught_offside, 0),
    COALESCE(ea.defensive_actions, 0),

    -- ── §4.5.8 ─────────────────────────────────────────────────────────────
    COALESCE(ea.saves, 0), COALESCE(ea.claims, 0), COALESCE(ea.punches, 0),
    COALESCE(ea.smothers, 0), COALESCE(ea.crosses_not_claimed, 0),
    COALESCE(ea.keeper_sweeper_actions, 0), COALESCE(ea.keeper_pickups, 0),

    -- ── §4.5.9 ─────────────────────────────────────────────────────────────
    COALESCE(sa.possession_duration_sec, 0),
    COALESCE(soa.possession_duration_sec, 0),
    COALESCE(ea.touches, 0), COALESCE(ea.touches_def_third, 0),
    COALESCE(ea.touches_mid_third, 0), COALESCE(ea.touches_final_third, 0),
    COALESCE(ea.touches_in_box, 0), COALESCE(oa.touches_final_third, 0),
    ea.avg_action_x,

    -- ── §4.5.10 ────────────────────────────────────────────────────────────
    COALESCE(sa.sequences, 0), COALESCE(sa.sequence_passes_total, 0),
    COALESCE(sa.sequence_duration_total, 0), COALESCE(sa.long_sequences, 0),
    COALESCE(sa.sequences_ending_in_shot, 0), COALESCE(sa.sequences_ending_in_goal, 0),
    COALESCE(sa.final_third_entries, 0), COALESCE(sa.penalty_box_entries, 0),
    COALESCE(sa.sequences_started_def_third, 0),
    COALESCE(sa.sequences_started_mid_third, 0),
    COALESCE(sa.sequences_started_att_third, 0),
    COALESCE(sa.buildup_sequences_from_gk, 0),
    COALESCE(sa.counter_attack_sequences, 0),
    COALESCE(sa.high_transition_sequences, 0),
    COALESCE(sa.set_piece_sequences, 0),
    COALESCE(sa.sequence_xt_total, 0), COALESCE(sa.sequence_distance_total, 0),
    COALESCE(sa.field_progression_total, 0),

    -- ── §4.5.11 ────────────────────────────────────────────────────────────
    COALESCE(oa.passes_in_opp_press_zone, 0),
    COALESCE(ea.ppda_def_actions, 0),
    COALESCE(ea.high_turnovers, 0), COALESCE(sa.high_turnover_shots, 0),
    COALESCE(cp.counterpress_recoveries, 0),
    COALESCE(sa.counterattack_sequences, 0),
    COALESCE(soa.sequences_ended_own_third, 0),
    ea.defensive_line_height,
    -- The opponent's ratio, from the opponent's own stored inputs.
    COALESCE(ea.passes_in_opp_press_zone, 0)::real
        / NULLIF(COALESCE(oa.ppda_def_actions, 0), 0),

    -- ── §4.5.12 ────────────────────────────────────────────────────────────
    COALESCE(ea.corners_for, 0), COALESCE(oa.corners_for, 0),
    COALESCE(ea.free_kicks_taken, 0), COALESCE(ea.throw_ins_taken, 0),
    COALESCE(ea.set_piece_goals_for, 0), COALESCE(oa.set_piece_goals_for, 0),
    COALESCE(ea.yellow_cards, 0), COALESCE(ea.second_yellows, 0),
    COALESCE(ea.red_cards, 0)
FROM numbered n
LEFT JOIN event_agg    ea  ON ea.match_id  = n.match_id AND ea.team_id  = n.team_id
LEFT JOIN event_agg    oa  ON oa.match_id  = n.match_id AND oa.team_id  = n.opponent_team_id
LEFT JOIN assist_xa    ax  ON ax.match_id  = n.match_id AND ax.team_id  = n.team_id
LEFT JOIN sequence_agg sa  ON sa.match_id  = n.match_id AND sa.team_id  = n.team_id
LEFT JOIN sequence_agg soa ON soa.match_id = n.match_id AND soa.team_id = n.opponent_team_id
LEFT JOIN counterpress cp  ON cp.match_id  = n.match_id AND cp.team_id  = n.team_id
WHERE %(match_ids)s::int[] IS NULL
   OR n.match_id = ANY (%(match_ids)s::int[]);
