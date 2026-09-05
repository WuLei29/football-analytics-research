-- ═══════════════════════════════════════════════════════════════════════════
-- build_team_season_stats.sql — gold.team_season_stats
-- ═══════════════════════════════════════════════════════════════════════════
--
-- Spec: md/GOLD_LAYER.md §4.4, §4.4.2 (build rules), §4.5.13 (rollup rules),
--       §4.5.14 (season-only columns), §6 Step 5.
--
-- Runs as:  python -m src.gold.build_gold --step team_season_stats
--           (the runner loops over every competition_season_id, or one when
--            --competition-season-id is given)
--
-- Parameter
--   %(cs_id)s :: int   the competition_season to rebuild. Scoped rather than
--                      TRUNCATEd so loading a Segunda matchday never touches
--                      La Liga rows, and vice versa (§4.4.2).
--
-- Reads ONLY gold.team_match_stats plus the silver context tables needed for
-- the denormalised labels. It touches no event data, which is what makes it a
-- one-second step.
--
-- TWO RULES THIS FILE EXISTS TO ENFORCE
--
-- 1. Ratios are recomputed from summed inputs, never averaged. Every ‡ column
--    on gold.team_season_stats is a GENERATED column over the SUMmed
--    numerator and denominator, so the wrong form is not expressible here.
--    §4.5.13's worked example: a team with PPDA 6.0 in a 400-pass match and
--    14.0 in a 200-pass match has a season PPDA of 7.4, not 10.0.
--
-- 2. The row set is driven from ENROLMENT, not from matches. All 20 teams get
--    a row from the day the season is created. Driving from matches grows the
--    table from 0 to 20 rows across matchday 1 and silently drops a team whose
--    early fixtures were all postponed.
-- ═══════════════════════════════════════════════════════════════════════════

DELETE FROM gold.team_season_stats WHERE competition_season_id = %(cs_id)s;


WITH agg AS (
    SELECT
        competition_season_id,
        team_id,

        -- ── coverage ───────────────────────────────────────────────────────
        -- Sourced here, not from silver.matches, so the two tables cannot
        -- disagree about how many matches a team has played (§4.4.1).
        count(*)::smallint        AS matches_played,
        max(matchday)::smallint   AS last_matchday_played,
        min(match_date)           AS first_match_date,
        max(match_date)           AS through_match_date,

        -- ── §4.5.14 season-only ────────────────────────────────────────────
        count(*) FILTER (WHERE result = 'W')::smallint AS wins,
        count(*) FILTER (WHERE result = 'D')::smallint AS draws,
        count(*) FILTER (WHERE result = 'L')::smallint AS losses,
        sum(points)::smallint                          AS points,
        count(*) FILTER (WHERE is_home AND result = 'W')::smallint AS home_wins,
        count(*) FILTER (WHERE is_home AND result = 'D')::smallint AS home_draws,
        count(*) FILTER (WHERE is_home AND result = 'L')::smallint AS home_losses,
        count(*) FILTER (WHERE NOT is_home AND result = 'W')::smallint AS away_wins,
        count(*) FILTER (WHERE NOT is_home AND result = 'D')::smallint AS away_draws,
        count(*) FILTER (WHERE NOT is_home AND result = 'L')::smallint AS away_losses,
        sum(goals_for)     FILTER (WHERE is_home)::smallint     AS goals_for_home,
        sum(goals_for)     FILTER (WHERE NOT is_home)::smallint AS goals_for_away,
        sum(goals_against) FILTER (WHERE is_home)::smallint     AS goals_against_home,
        sum(goals_against) FILTER (WHERE NOT is_home)::smallint AS goals_against_away,
        -- Booleans roll up as a filtered count, never as a sum (§4.5.13).
        count(*) FILTER (WHERE clean_sheet)::smallint     AS clean_sheets,
        count(*) FILTER (WHERE failed_to_score)::smallint AS failed_to_score_count,

        -- ── §4.5.1 ─────────────────────────────────────────────────────────
        sum(goals_for)::smallint         AS goals_for,
        sum(goals_against)::smallint     AS goals_against,
        sum(goals_for_ht)::smallint      AS goals_for_ht,
        sum(goals_against_ht)::smallint  AS goals_against_ht,
        sum(goals_for_p1)::smallint      AS goals_for_p1,
        sum(goals_for_p2)::smallint      AS goals_for_p2,
        sum(goals_against_p1)::smallint  AS goals_against_p1,
        sum(goals_against_p2)::smallint  AS goals_against_p2,
        sum(own_goals_for)::smallint     AS own_goals_for,
        sum(own_goals_against)::smallint AS own_goals_against,

        -- ── §4.5.2 ─────────────────────────────────────────────────────────
        sum(shots)::smallint                    AS shots,
        sum(shots_on_target)::smallint          AS shots_on_target,
        sum(shots_blocked)::smallint            AS shots_blocked,
        sum(shots_off_target)::smallint         AS shots_off_target,
        sum(shots_woodwork)::smallint           AS shots_woodwork,
        sum(shots_inside_box)::smallint         AS shots_inside_box,
        sum(shots_outside_box)::smallint        AS shots_outside_box,
        sum(shots_first_time)::smallint         AS shots_first_time,
        sum(shots_individual_play)::smallint    AS shots_individual_play,
        sum(big_chances)::smallint              AS big_chances,
        sum(big_chances_scored)::smallint       AS big_chances_scored,
        sum(penalties_taken)::smallint          AS penalties_taken,
        sum(penalties_scored)::smallint         AS penalties_scored,
        sum(shots_against)::smallint            AS shots_against,
        sum(shots_on_target_against)::smallint  AS shots_on_target_against,
        sum(shots_inside_box_against)::smallint AS shots_inside_box_against,
        sum(big_chances_against)::smallint      AS big_chances_against,

        -- ── §4.5.3 ─────────────────────────────────────────────────────────
        sum(xg_for)::real           AS xg_for,
        sum(xg_against)::real       AS xg_against,
        sum(npxg_for)::real         AS npxg_for,
        sum(npxg_against)::real     AS npxg_against,
        sum(xg_open_play)::real     AS xg_open_play,
        sum(xg_set_piece)::real     AS xg_set_piece,
        sum(xg_from_corner)::real   AS xg_from_corner,
        sum(xg_free_kick)::real     AS xg_free_kick,
        sum(xg_fast_break)::real    AS xg_fast_break,
        sum(xg_penalty)::real       AS xg_penalty,
        sum(goals_open_play)::smallint  AS goals_open_play,
        sum(goals_set_piece)::smallint  AS goals_set_piece,
        sum(goals_fast_break)::smallint AS goals_fast_break,

        -- ── §4.5.4 ─────────────────────────────────────────────────────────
        sum(passes)::int                       AS passes,
        sum(passes_completed)::int             AS passes_completed,
        sum(passes_short)::int                 AS passes_short,
        sum(passes_medium)::int                AS passes_medium,
        sum(passes_long)::int                  AS passes_long,
        sum(passes_forward)::int               AS passes_forward,
        sum(passes_backward)::int              AS passes_backward,
        sum(passes_sideways)::int              AS passes_sideways,
        sum(progressive_passes)::int           AS progressive_passes,
        sum(progressive_passes_completed)::int AS progressive_passes_completed,
        sum(passes_into_final_third)::int      AS passes_into_final_third,
        sum(passes_into_penalty_area)::int     AS passes_into_penalty_area,
        sum(crosses)::smallint                 AS crosses,
        sum(crosses_completed)::smallint       AS crosses_completed,
        sum(through_balls)::smallint           AS through_balls,
        sum(long_balls)::smallint              AS long_balls,
        sum(key_passes)::smallint              AS key_passes,
        sum(assists)::smallint                 AS assists,
        sum(second_assists)::smallint          AS second_assists,
        sum(xa)::real                          AS xa,
        sum(offside_passes)::smallint          AS offside_passes,
        sum(passes_against)::int               AS passes_against,
        sum(passes_def_third)::int             AS passes_def_third,
        sum(passes_mid_third)::int             AS passes_mid_third,
        sum(passes_att_third)::int             AS passes_att_third,

        -- ── §4.5.5 ─────────────────────────────────────────────────────────
        sum(carries)::int                     AS carries,
        sum(carry_distance_total)::real       AS carry_distance_total,
        sum(progressive_carries)::int         AS progressive_carries,
        sum(carries_into_final_third)::int    AS carries_into_final_third,
        sum(carries_into_penalty_area)::int   AS carries_into_penalty_area,
        sum(take_ons)::smallint               AS take_ons,
        sum(take_ons_won)::smallint           AS take_ons_won,
        sum(dispossessed)::smallint           AS dispossessed,
        sum(errors)::smallint                 AS errors,

        -- ── §4.5.6 ─────────────────────────────────────────────────────────
        sum(xt_for)::real             AS xt_for,
        sum(xt_against)::real         AS xt_against,
        sum(xt_open_play)::real       AS xt_open_play,
        sum(vaep_for)::real           AS vaep_for,
        sum(vaep_against)::real       AS vaep_against,
        sum(vaep_offensive_for)::real AS vaep_offensive_for,
        sum(vaep_defensive_for)::real AS vaep_defensive_for,

        -- ── §4.5.7 ─────────────────────────────────────────────────────────
        sum(tackles)::smallint           AS tackles,
        sum(tackles_won)::smallint       AS tackles_won,
        sum(interceptions)::smallint     AS interceptions,
        sum(clearances)::smallint        AS clearances,
        sum(blocked_passes)::smallint    AS blocked_passes,
        sum(ball_recoveries)::int        AS ball_recoveries,
        sum(aerials_won)::smallint       AS aerials_won,
        sum(aerials_lost)::smallint      AS aerials_lost,
        sum(duels_won)::smallint         AS duels_won,
        sum(duels_lost)::smallint        AS duels_lost,
        sum(fouls_committed)::smallint   AS fouls_committed,
        sum(fouls_won)::smallint         AS fouls_won,
        sum(offsides_provoked)::smallint AS offsides_provoked,
        sum(caught_offside)::smallint    AS caught_offside,
        sum(defensive_actions)::int      AS defensive_actions,

        -- ── §4.5.8 ─────────────────────────────────────────────────────────
        sum(saves)::smallint                  AS saves,
        sum(claims)::smallint                 AS claims,
        sum(punches)::smallint                AS punches,
        sum(smothers)::smallint               AS smothers,
        sum(crosses_not_claimed)::smallint    AS crosses_not_claimed,
        sum(keeper_sweeper_actions)::smallint AS keeper_sweeper_actions,
        sum(keeper_pickups)::smallint         AS keeper_pickups,

        -- ── §4.5.9 ─────────────────────────────────────────────────────────
        sum(possession_duration_sec)::real     AS possession_duration_sec,
        sum(possession_duration_opp_sec)::real AS possession_duration_opp_sec,
        sum(touches)::int                      AS touches,
        sum(touches_def_third)::int            AS touches_def_third,
        sum(touches_mid_third)::int            AS touches_mid_third,
        sum(touches_final_third)::int          AS touches_final_third,
        sum(touches_in_box)::int               AS touches_in_box,
        sum(touches_final_third_opp)::int      AS touches_final_third_opp,
        -- A mean over sub-entities: weight by touches, not by match count
        -- (§4.5.13). A 30-touch red-card match must not pull the season mean
        -- as hard as a 700-touch one.
        -- The denominator is filtered to the matches that actually contributed
        -- a value, so a match with no touches recorded cannot dilute the mean.
        (sum(avg_action_x * touches)
            / NULLIF(sum(touches) FILTER (WHERE avg_action_x IS NOT NULL), 0)
        )::real AS avg_action_x,

        -- ── §4.5.10 ────────────────────────────────────────────────────────
        sum(sequences)::int                   AS sequences,
        sum(sequence_passes_total)::int       AS sequence_passes_total,
        sum(sequence_duration_total)::real    AS sequence_duration_total,
        sum(long_sequences)::int              AS long_sequences,
        sum(sequences_ending_in_shot)::int    AS sequences_ending_in_shot,
        sum(sequences_ending_in_goal)::smallint AS sequences_ending_in_goal,
        sum(final_third_entries)::int         AS final_third_entries,
        sum(penalty_box_entries)::int         AS penalty_box_entries,
        sum(sequences_started_def_third)::int AS sequences_started_def_third,
        sum(sequences_started_mid_third)::int AS sequences_started_mid_third,
        sum(sequences_started_att_third)::int AS sequences_started_att_third,
        sum(buildup_sequences_from_gk)::int   AS buildup_sequences_from_gk,
        sum(counter_attack_sequences)::int    AS counter_attack_sequences,
        sum(high_transition_sequences)::int   AS high_transition_sequences,
        sum(set_piece_sequences)::int         AS set_piece_sequences,
        sum(sequence_xt_total)::real          AS sequence_xt_total,
        sum(sequence_distance_total)::real    AS sequence_distance_total,
        sum(field_progression_total)::real    AS field_progression_total,

        -- ── §4.5.11 ────────────────────────────────────────────────────────
        -- Both PPDA numerators and denominators are stored so ppda and
        -- ppda_against are ratios of sums. ppda_against needs the opponent's
        -- inputs, which at match grain are this row's mirror: the opponent's
        -- press denominator is ppda_opp_passes / ppda_against.
        sum(ppda_opp_passes)::int            AS ppda_opp_passes,
        sum(ppda_def_actions)::int           AS ppda_def_actions,
        sum(high_turnovers)::int             AS high_turnovers,
        sum(high_turnover_shots)::smallint   AS high_turnover_shots,
        sum(counterpress_recoveries)::int    AS counterpress_recoveries,
        sum(counterattack_sequences)::int    AS counterattack_sequences,
        sum(opp_sequences_ended_own_third)::int AS opp_sequences_ended_own_third,
        (sum(defensive_line_height * defensive_actions)
            / NULLIF(sum(defensive_actions) FILTER (WHERE defensive_line_height IS NOT NULL), 0)
        )::real AS defensive_line_height,

        -- ── §4.5.12 ────────────────────────────────────────────────────────
        sum(corners_for)::smallint             AS corners_for,
        sum(corners_against)::smallint         AS corners_against,
        sum(free_kicks_taken)::smallint        AS free_kicks_taken,
        sum(throw_ins_taken)::smallint         AS throw_ins_taken,
        sum(set_piece_goals_for)::smallint     AS set_piece_goals_for,
        sum(set_piece_goals_against)::smallint AS set_piece_goals_against,
        sum(yellow_cards)::smallint            AS yellow_cards,
        sum(second_yellows)::smallint          AS second_yellows,
        sum(red_cards)::smallint               AS red_cards
    FROM gold.team_match_stats
    WHERE competition_season_id = %(cs_id)s
    GROUP BY competition_season_id, team_id
),

-- The opponent-side PPDA inputs, summed independently: how many passes this
-- team played inside the zone its opponents pressed, and how many narrow
-- defensive actions those opponents made there.
ppda_mirror AS (
    SELECT t.competition_season_id, t.team_id,
           sum(o.ppda_opp_passes)::int  AS ppda_opp_passes_against,
           sum(o.ppda_def_actions)::int AS ppda_def_actions_against
    FROM gold.team_match_stats t
    JOIN gold.team_match_stats o
      ON o.match_id = t.match_id AND o.team_id = t.opponent_team_id
    WHERE t.competition_season_id = %(cs_id)s
    GROUP BY t.competition_season_id, t.team_id
),

-- form_last_5 is ordered by match_date DESC, NOT by matchday: a postponed
-- fixture played last week is part of current form regardless of the number
-- printed next to it (§4.5.14).
form AS (
    SELECT competition_season_id, team_id,
           string_agg(result, '' ORDER BY match_date DESC, match_id DESC) AS form_last_5
    FROM (
        SELECT competition_season_id, team_id, result, match_date, match_id,
               ROW_NUMBER() OVER (PARTITION BY competition_season_id, team_id
                                  ORDER BY match_date DESC, match_id DESC) AS rn
        FROM gold.team_match_stats
        WHERE competition_season_id = %(cs_id)s
    ) t
    WHERE rn <= 5
    GROUP BY competition_season_id, team_id
)

INSERT INTO gold.team_season_stats (
    competition_season_id, team_id,
    competition_code, competition_name, tier_level, season_label,
    team_name, team_short_name, team_abbreviation,
    matches_played, last_matchday_played, matchdays_scheduled, matches_remaining,
    first_match_date, through_match_date, season_status,
    wins, draws, losses, points,
    home_wins, home_draws, home_losses, away_wins, away_draws, away_losses,
    goals_for_home, goals_for_away, goals_against_home, goals_against_away,
    clean_sheets, failed_to_score_count, form_last_5,
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
    ppda_opp_passes, ppda_def_actions,
    ppda_opp_passes_against, ppda_def_actions_against,
    high_turnovers, high_turnover_shots, counterpress_recoveries,
    counterattack_sequences, opp_sequences_ended_own_third,
    defensive_line_height,
    corners_for, corners_against, free_kicks_taken, throw_ins_taken,
    set_piece_goals_for, set_piece_goals_against,
    yellow_cards, second_yellows, red_cards
)
SELECT
    tcs.competition_season_id, tcs.team_id,
    c.competition_code, c.known_name, c.tier_level, se.label,
    t.name, t.short_name, t.abbreviation,

    COALESCE(a.matches_played, 0), a.last_matchday_played,
    cs.total_matchdays,
    (cs.total_matchdays - COALESCE(a.matches_played, 0))::smallint,
    a.first_match_date, a.through_match_date, cs.status,

    COALESCE(a.wins, 0), COALESCE(a.draws, 0), COALESCE(a.losses, 0),
    COALESCE(a.points, 0),
    COALESCE(a.home_wins, 0), COALESCE(a.home_draws, 0), COALESCE(a.home_losses, 0),
    COALESCE(a.away_wins, 0), COALESCE(a.away_draws, 0), COALESCE(a.away_losses, 0),
    COALESCE(a.goals_for_home, 0), COALESCE(a.goals_for_away, 0),
    COALESCE(a.goals_against_home, 0), COALESCE(a.goals_against_away, 0),
    COALESCE(a.clean_sheets, 0), COALESCE(a.failed_to_score_count, 0),
    f.form_last_5,

    COALESCE(a.goals_for, 0), COALESCE(a.goals_against, 0),
    COALESCE(a.goals_for_ht, 0), COALESCE(a.goals_against_ht, 0),
    COALESCE(a.goals_for_p1, 0), COALESCE(a.goals_for_p2, 0),
    COALESCE(a.goals_against_p1, 0), COALESCE(a.goals_against_p2, 0),
    COALESCE(a.own_goals_for, 0), COALESCE(a.own_goals_against, 0),

    COALESCE(a.shots, 0), COALESCE(a.shots_on_target, 0),
    COALESCE(a.shots_blocked, 0), COALESCE(a.shots_off_target, 0),
    COALESCE(a.shots_woodwork, 0), COALESCE(a.shots_inside_box, 0),
    COALESCE(a.shots_outside_box, 0), COALESCE(a.shots_first_time, 0),
    COALESCE(a.shots_individual_play, 0), COALESCE(a.big_chances, 0),
    COALESCE(a.big_chances_scored, 0), COALESCE(a.penalties_taken, 0),
    COALESCE(a.penalties_scored, 0), COALESCE(a.shots_against, 0),
    COALESCE(a.shots_on_target_against, 0), COALESCE(a.shots_inside_box_against, 0),
    COALESCE(a.big_chances_against, 0),

    COALESCE(a.xg_for, 0), COALESCE(a.xg_against, 0),
    COALESCE(a.npxg_for, 0), COALESCE(a.npxg_against, 0),
    COALESCE(a.xg_open_play, 0), COALESCE(a.xg_set_piece, 0),
    COALESCE(a.xg_from_corner, 0), COALESCE(a.xg_free_kick, 0),
    COALESCE(a.xg_fast_break, 0), COALESCE(a.xg_penalty, 0),
    COALESCE(a.goals_open_play, 0), COALESCE(a.goals_set_piece, 0),
    COALESCE(a.goals_fast_break, 0),

    COALESCE(a.passes, 0), COALESCE(a.passes_completed, 0),
    COALESCE(a.passes_short, 0), COALESCE(a.passes_medium, 0),
    COALESCE(a.passes_long, 0), COALESCE(a.passes_forward, 0),
    COALESCE(a.passes_backward, 0), COALESCE(a.passes_sideways, 0),
    COALESCE(a.progressive_passes, 0), COALESCE(a.progressive_passes_completed, 0),
    COALESCE(a.passes_into_final_third, 0), COALESCE(a.passes_into_penalty_area, 0),
    COALESCE(a.crosses, 0), COALESCE(a.crosses_completed, 0),
    COALESCE(a.through_balls, 0), COALESCE(a.long_balls, 0),
    COALESCE(a.key_passes, 0), COALESCE(a.assists, 0),
    COALESCE(a.second_assists, 0), COALESCE(a.xa, 0),
    COALESCE(a.offside_passes, 0), COALESCE(a.passes_against, 0),
    COALESCE(a.passes_def_third, 0), COALESCE(a.passes_mid_third, 0),
    COALESCE(a.passes_att_third, 0),

    COALESCE(a.carries, 0), COALESCE(a.carry_distance_total, 0),
    COALESCE(a.progressive_carries, 0), COALESCE(a.carries_into_final_third, 0),
    COALESCE(a.carries_into_penalty_area, 0),
    COALESCE(a.take_ons, 0), COALESCE(a.take_ons_won, 0),
    COALESCE(a.dispossessed, 0), COALESCE(a.errors, 0),

    COALESCE(a.xt_for, 0), COALESCE(a.xt_against, 0), COALESCE(a.xt_open_play, 0),
    COALESCE(a.vaep_for, 0), COALESCE(a.vaep_against, 0),
    COALESCE(a.vaep_offensive_for, 0), COALESCE(a.vaep_defensive_for, 0),

    COALESCE(a.tackles, 0), COALESCE(a.tackles_won, 0),
    COALESCE(a.interceptions, 0), COALESCE(a.clearances, 0),
    COALESCE(a.blocked_passes, 0), COALESCE(a.ball_recoveries, 0),
    COALESCE(a.aerials_won, 0), COALESCE(a.aerials_lost, 0),
    COALESCE(a.duels_won, 0), COALESCE(a.duels_lost, 0),
    COALESCE(a.fouls_committed, 0), COALESCE(a.fouls_won, 0),
    COALESCE(a.offsides_provoked, 0), COALESCE(a.caught_offside, 0),
    COALESCE(a.defensive_actions, 0),

    COALESCE(a.saves, 0), COALESCE(a.claims, 0), COALESCE(a.punches, 0),
    COALESCE(a.smothers, 0), COALESCE(a.crosses_not_claimed, 0),
    COALESCE(a.keeper_sweeper_actions, 0), COALESCE(a.keeper_pickups, 0),

    COALESCE(a.possession_duration_sec, 0),
    COALESCE(a.possession_duration_opp_sec, 0),
    COALESCE(a.touches, 0), COALESCE(a.touches_def_third, 0),
    COALESCE(a.touches_mid_third, 0), COALESCE(a.touches_final_third, 0),
    COALESCE(a.touches_in_box, 0), COALESCE(a.touches_final_third_opp, 0),
    a.avg_action_x,

    COALESCE(a.sequences, 0), COALESCE(a.sequence_passes_total, 0),
    COALESCE(a.sequence_duration_total, 0), COALESCE(a.long_sequences, 0),
    COALESCE(a.sequences_ending_in_shot, 0), COALESCE(a.sequences_ending_in_goal, 0),
    COALESCE(a.final_third_entries, 0), COALESCE(a.penalty_box_entries, 0),
    COALESCE(a.sequences_started_def_third, 0),
    COALESCE(a.sequences_started_mid_third, 0),
    COALESCE(a.sequences_started_att_third, 0),
    COALESCE(a.buildup_sequences_from_gk, 0),
    COALESCE(a.counter_attack_sequences, 0),
    COALESCE(a.high_transition_sequences, 0),
    COALESCE(a.set_piece_sequences, 0),
    COALESCE(a.sequence_xt_total, 0), COALESCE(a.sequence_distance_total, 0),
    COALESCE(a.field_progression_total, 0),

    COALESCE(a.ppda_opp_passes, 0), COALESCE(a.ppda_def_actions, 0),
    COALESCE(pm.ppda_opp_passes_against, 0), COALESCE(pm.ppda_def_actions_against, 0),
    COALESCE(a.high_turnovers, 0), COALESCE(a.high_turnover_shots, 0),
    COALESCE(a.counterpress_recoveries, 0), COALESCE(a.counterattack_sequences, 0),
    COALESCE(a.opp_sequences_ended_own_third, 0), a.defensive_line_height,

    COALESCE(a.corners_for, 0), COALESCE(a.corners_against, 0),
    COALESCE(a.free_kicks_taken, 0), COALESCE(a.throw_ins_taken, 0),
    COALESCE(a.set_piece_goals_for, 0), COALESCE(a.set_piece_goals_against, 0),
    COALESCE(a.yellow_cards, 0), COALESCE(a.second_yellows, 0),
    COALESCE(a.red_cards, 0)
FROM silver.team_competition_seasons tcs
JOIN silver.competition_seasons cs USING (competition_season_id)
JOIN silver.competitions c         USING (competition_id)
JOIN silver.seasons se             USING (season_id)
JOIN silver.teams t                USING (team_id)
LEFT JOIN agg a         ON a.competition_season_id  = tcs.competition_season_id
                       AND a.team_id                = tcs.team_id
LEFT JOIN ppda_mirror pm ON pm.competition_season_id = tcs.competition_season_id
                       AND pm.team_id               = tcs.team_id
LEFT JOIN form f        ON f.competition_season_id  = tcs.competition_season_id
                       AND f.team_id                = tcs.team_id
WHERE tcs.competition_season_id = %(cs_id)s;


-- ───────────────────────────────────────────────────────────────────────────
-- league_position — a ranking, not an aggregate, so it needs a second pass
-- once every row for the season exists (§4.5.14). Left NULL for anything that
-- is not a league format, where a table position means nothing.
-- ───────────────────────────────────────────────────────────────────────────
UPDATE gold.team_season_stats s
SET league_position = r.pos
FROM (
    SELECT ts.team_id,
           RANK() OVER (ORDER BY ts.points DESC,
                                 ts.goal_difference DESC,
                                 ts.goals_for DESC)::smallint AS pos
    FROM gold.team_season_stats ts
    JOIN silver.competition_seasons cs USING (competition_season_id)
    JOIN silver.competitions c         USING (competition_id)
    WHERE ts.competition_season_id = %(cs_id)s
      AND position('league' in lower(c.competition_format)) > 0
) r
WHERE s.competition_season_id = %(cs_id)s
  AND s.team_id = r.team_id;
