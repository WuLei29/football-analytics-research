-- ═══════════════════════════════════════════════════════════════════════════
-- build_player_season_stats.sql — gold.player_season_stats
-- ═══════════════════════════════════════════════════════════════════════════
--
-- Spec: md/GOLD_LAYER.md §4.2.1, §4.2.3, §4.6.2, §4.6.15, §4.6.16, §6 Step 8.
--
-- Runs as:  python -m src.gold.build_gold --step player_season_stats
--           (the runner loops over every competition_season_id)
--
-- Parameter
--   %(cs_id)s :: int   the competition_season to rebuild.
--
-- Reads gold.player_match_stats and gold.team_match_stats plus the silver
-- context tables for the denormalised labels. Touches no event data.
--
-- THE GRAIN IS THREE COLUMNS: (competition_season_id, player_id, team_id).
-- Grouping by team_id is what keeps a mid-season transfer as two rows. The
-- two-column grain would attribute a player's whole season to whichever club
-- they finished it at -- destroying exactly the case an analyst most wants to
-- look at. gold.player_season_totals is the view that rolls the clubs up.
--
-- THREE ROLLUP RULES, all enforced by the schema rather than by this file:
--   * ratios     recomputed from summed numerator and denominator
--   * per-90     SUM(metric) / (SUM(minutes) / 90), never the mean of
--                per-match rates -- a 5-minute cameo would otherwise weigh as
--                much as a full 90, and cameo rates are wildly volatile
--   * padj_*     summed from the match-grain adjustment, NEVER re-adjusted
--                here using a season possession figure (§4.6.14)
-- ═══════════════════════════════════════════════════════════════════════════

DELETE FROM gold.player_season_stats WHERE competition_season_id = %(cs_id)s;


WITH
-- ── position profile, weighted by MINUTES not match count (§4.6.2) ─────────
pos AS (
    SELECT competition_season_id, player_id, team_id, position,
           sum(minutes_played)::int AS pos_minutes,
           sum(sum(minutes_played)) OVER (
               PARTITION BY competition_season_id, player_id, team_id
           )::int AS total_minutes
    FROM gold.player_match_stats
    WHERE competition_season_id = %(cs_id)s
    GROUP BY 1, 2, 3, 4
),
pos_profile AS (
    SELECT competition_season_id, player_id, team_id,
           jsonb_object_agg(position, pos_minutes) AS minutes_by_position,
           -- argmax: cleanest as an ordered array_agg rather than JSONB
           -- gymnastics. Ties broken alphabetically so it is deterministic.
           (array_agg(position ORDER BY pos_minutes DESC, position))[1] AS primary_position,
           (max(pos_minutes)::real / NULLIF(max(total_minutes), 0))     AS primary_position_pct,
           count(*) FILTER (WHERE pos_minutes >= 0.10 * total_minutes)::smallint
                                                                       AS positions_played
    FROM pos
    GROUP BY 1, 2, 3
),

-- ── the club's own coverage, so player and team can never disagree ─────────
team_cov AS (
    SELECT competition_season_id, team_id,
           count(*)::smallint       AS team_matches,
           sum(goals_for)::smallint AS team_season_goals
    FROM gold.team_match_stats
    WHERE competition_season_id = %(cs_id)s
    GROUP BY 1, 2
),

-- ── the shirt the player wore most often for this club ─────────────────────
shirt AS (
    SELECT DISTINCT ON (pms.competition_season_id, pms.player_id, pms.team_id)
           pms.competition_season_id, pms.player_id, pms.team_id, ml.shirt_number
    FROM gold.player_match_stats pms
    JOIN silver.match_lineups ml
      ON ml.match_id = pms.match_id AND ml.player_id = pms.player_id
    WHERE pms.competition_season_id = %(cs_id)s
      AND ml.shirt_number IS NOT NULL
    ORDER BY pms.competition_season_id, pms.player_id, pms.team_id,
             ml.shirt_number, ml.match_id
),

agg AS (
    SELECT
        competition_season_id, player_id, team_id,

        -- ── coverage ───────────────────────────────────────────────────────
        count(*)::smallint                                    AS matches_played,
        count(*) FILTER (WHERE started)::smallint             AS matches_started,
        count(*) FILTER (WHERE subbed_on)::smallint           AS matches_sub_on,
        count(*) FILTER (WHERE played_full_match)::smallint   AS matches_full_90,
        count(*) FILTER (WHERE is_captain)::smallint          AS matches_as_captain,
        sum(minutes_played)::smallint                         AS minutes_played,
        min(match_date)                                       AS first_match_date,
        max(match_date)                                       AS through_match_date,

        -- ── §4.6.4 ─────────────────────────────────────────────────────────
        sum(goals)::smallint              AS goals,
        sum(goals_non_penalty)::smallint  AS goals_non_penalty,
        sum(shots)::smallint              AS shots,
        sum(shots_on_target)::smallint    AS shots_on_target,
        sum(shots_in_box)::smallint       AS shots_in_box,
        sum(shots_outside_box)::smallint  AS shots_outside_box,
        sum(shots_first_time)::smallint   AS shots_first_time,
        sum(headed_goals)::smallint       AS headed_goals,
        sum(xg)::real                     AS xg,
        sum(npxg)::real                   AS npxg,
        sum(big_chances)::smallint        AS big_chances,
        sum(big_chances_scored)::smallint AS big_chances_scored,
        sum(big_chances_missed)::smallint AS big_chances_missed,
        sum(penalties_taken)::smallint    AS penalties_taken,
        sum(penalties_scored)::smallint   AS penalties_scored,

        -- ── §4.6.5 ─────────────────────────────────────────────────────────
        sum(assists)::smallint                  AS assists,
        sum(key_passes)::smallint               AS key_passes,
        sum(xa)::real                           AS xa,
        sum(second_assists)::smallint           AS second_assists,
        sum(crosses)::smallint                  AS crosses,
        sum(crosses_completed)::smallint        AS crosses_completed,
        sum(crosses_from_open_play)::smallint   AS crosses_from_open_play,
        sum(through_balls)::smallint            AS through_balls,
        sum(passes_into_penalty_area)::smallint AS passes_into_penalty_area,
        sum(shot_creating_actions)::smallint    AS shot_creating_actions,

        -- ── §4.6.6 ─────────────────────────────────────────────────────────
        sum(passes)::int                       AS passes,
        sum(passes_completed)::int             AS passes_completed,
        sum(passes_received)::int              AS passes_received,
        sum(passes_short)::int                 AS passes_short,
        sum(passes_medium)::int                AS passes_medium,
        sum(passes_long)::int                  AS passes_long,
        sum(passes_forward)::int               AS passes_forward,
        sum(passes_backward)::int              AS passes_backward,
        sum(progressive_passes)::int           AS progressive_passes,
        sum(progressive_passes_completed)::int AS progressive_passes_completed,
        sum(passes_into_final_third)::int      AS passes_into_final_third,
        sum(long_balls)::smallint              AS long_balls,
        sum(long_balls_completed)::smallint    AS long_balls_completed,
        sum(switches)::smallint                AS switches,
        sum(offside_passes)::smallint          AS offside_passes,
        sum(xt_pass)::real                     AS xt_pass,

        -- ── §4.6.7 ─────────────────────────────────────────────────────────
        sum(carries)::int                        AS carries,
        sum(carry_distance_total)::real          AS carry_distance_total,
        sum(progressive_carries)::int            AS progressive_carries,
        sum(carries_into_final_third)::int       AS carries_into_final_third,
        sum(carries_into_penalty_area)::smallint AS carries_into_penalty_area,
        sum(take_ons)::smallint                  AS take_ons,
        sum(take_ons_won)::smallint              AS take_ons_won,
        sum(dispossessed)::smallint              AS dispossessed,
        sum(touches)::int                        AS touches,
        sum(touches_in_box)::smallint            AS touches_in_box,
        sum(touches_final_third)::int            AS touches_final_third,
        sum(xt_carry)::real                      AS xt_carry,

        -- ── §4.6.8 ─────────────────────────────────────────────────────────
        sum(tackles)::smallint                 AS tackles,
        sum(tackles_won)::smallint             AS tackles_won,
        sum(tackles_missed)::smallint          AS tackles_missed,
        sum(dribbled_past)::smallint           AS dribbled_past,
        sum(interceptions)::smallint           AS interceptions,
        sum(clearances)::smallint              AS clearances,
        sum(blocked_passes)::smallint          AS blocked_passes,
        sum(ball_recoveries)::smallint         AS ball_recoveries,
        sum(defensive_actions)::int            AS defensive_actions,
        sum(def_actions_final_third)::smallint AS def_actions_final_third,
        -- A mean over sub-entities: weight by the action count, not by match
        -- count, and filter the denominator to matches that contributed one.
        (sum(def_action_avg_x * defensive_actions)
            / NULLIF(sum(defensive_actions) FILTER (WHERE def_action_avg_x IS NOT NULL), 0)
        )::real                                AS def_action_avg_x,
        sum(fouls_committed)::smallint         AS fouls_committed,
        sum(fouls_won)::smallint               AS fouls_won,
        sum(errors)::smallint                  AS errors,
        sum(offsides_provoked)::smallint       AS offsides_provoked,
        sum(caught_offside)::smallint          AS caught_offside,
        sum(shield_ball_opp)::smallint         AS shield_ball_opp,

        -- ── §4.6.9 ─────────────────────────────────────────────────────────
        sum(aerials_won)::smallint           AS aerials_won,
        sum(aerials_lost)::smallint          AS aerials_lost,
        sum(aerials_won_att_third)::smallint AS aerials_won_att_third,
        sum(aerials_won_def_third)::smallint AS aerials_won_def_third,

        -- ── §4.6.10 — SUM over all-NULL returns NULL, which keeps every ────
        --    outfield player's goalkeeping block empty rather than zeroed.
        sum(saves)::smallint               AS saves,
        sum(goals_conceded)::smallint      AS goals_conceded,
        CASE WHEN count(*) FILTER (WHERE position_group = 'GK') > 0
             THEN count(*) FILTER (WHERE clean_sheet)::smallint END AS clean_sheets,
        sum(xg_faced)::real                AS xg_faced,
        sum(penalties_faced)::smallint     AS penalties_faced,
        sum(penalties_saved)::smallint     AS penalties_saved,
        sum(claims)::smallint              AS claims,
        sum(claims_successful)::smallint   AS claims_successful,
        sum(punches)::smallint             AS punches,
        sum(crosses_not_claimed)::smallint AS crosses_not_claimed,
        sum(smothers)::smallint            AS smothers,
        sum(keeper_sweeper_actions)::smallint AS keeper_sweeper_actions,
        (sum(sweeper_avg_x * keeper_sweeper_actions)
            / NULLIF(sum(keeper_sweeper_actions) FILTER (WHERE sweeper_avg_x IS NOT NULL), 0)
        )::real                            AS sweeper_avg_x,
        sum(keeper_pickups)::smallint      AS keeper_pickups,
        sum(gk_passes)::smallint           AS gk_passes,
        sum(gk_passes_completed)::smallint AS gk_passes_completed,
        sum(gk_long_balls)::smallint       AS gk_long_balls,

        -- ── §4.6.11 ────────────────────────────────────────────────────────
        sum(xt)::real             AS xt,
        sum(vaep)::real           AS vaep,
        sum(vaep_offensive)::real AS vaep_offensive,
        sum(vaep_defensive)::real AS vaep_defensive,
        sum(vaep_actions)::int    AS vaep_actions,

        -- ── §4.6.12 ────────────────────────────────────────────────────────
        sum(yellow_cards)::smallint   AS yellow_cards,
        sum(second_yellows)::smallint AS second_yellows,
        sum(red_cards)::smallint      AS red_cards,

        -- ── §4.6.13 ────────────────────────────────────────────────────────
        sum(corners_taken)::smallint          AS corners_taken,
        sum(free_kicks_taken)::smallint       AS free_kicks_taken,
        sum(direct_free_kick_shots)::smallint AS direct_free_kick_shots,
        sum(throw_ins_taken)::smallint        AS throw_ins_taken,
        sum(set_piece_goals)::smallint        AS set_piece_goals,
        sum(set_piece_xg)::real               AS set_piece_xg,

        -- ── §4.6.14 padj: summed, never re-adjusted ────────────────────────
        COALESCE(sum(padj_tackles), 0)::real           AS padj_tackles,
        COALESCE(sum(padj_interceptions), 0)::real     AS padj_interceptions,
        COALESCE(sum(padj_clearances), 0)::real        AS padj_clearances,
        COALESCE(sum(padj_ball_recoveries), 0)::real   AS padj_ball_recoveries,
        COALESCE(sum(padj_blocked_passes), 0)::real    AS padj_blocked_passes,
        COALESCE(sum(padj_defensive_actions), 0)::real AS padj_defensive_actions
    FROM gold.player_match_stats
    WHERE competition_season_id = %(cs_id)s
    GROUP BY competition_season_id, player_id, team_id
)

INSERT INTO gold.player_season_stats (
    competition_season_id, player_id, team_id,
    player_name, team_name, team_abbreviation, competition_code, tier_level,
    season_label, nationality, date_of_birth, age_at_season_start,
    preferred_foot, shirt_number,
    primary_position, primary_position_group, primary_position_pct,
    minutes_by_position, positions_played,
    matches_played, matches_started, matches_sub_on, matches_full_90,
    matches_as_captain, minutes_played, team_matches,
    first_match_date, through_match_date,
    goals, goals_non_penalty, shots, shots_on_target, shots_in_box,
    shots_outside_box, shots_first_time, headed_goals, xg, npxg,
    big_chances, big_chances_scored, big_chances_missed,
    penalties_taken, penalties_scored,
    assists, key_passes, xa, second_assists, crosses, crosses_completed,
    crosses_from_open_play, through_balls, passes_into_penalty_area,
    shot_creating_actions,
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
    saves, goals_conceded, clean_sheets, xg_faced, penalties_faced,
    penalties_saved, claims, claims_successful, punches, crosses_not_claimed,
    smothers, keeper_sweeper_actions, sweeper_avg_x, keeper_pickups,
    gk_passes, gk_passes_completed, gk_long_balls,
    xt, vaep, vaep_offensive, vaep_defensive, vaep_actions,
    yellow_cards, second_yellows, red_cards,
    corners_taken, free_kicks_taken, direct_free_kick_shots, throw_ins_taken,
    set_piece_goals, set_piece_xg,
    padj_tackles, padj_interceptions, padj_clearances, padj_ball_recoveries,
    padj_blocked_passes, padj_defensive_actions,
    team_season_goals
)
SELECT
    a.competition_season_id, a.player_id, a.team_id,

    COALESCE(p.known_name, p.full_name), t.name, t.abbreviation,
    c.competition_code, c.tier_level, se.label,
    p.nationality, p.date_of_birth,
    CASE WHEN p.date_of_birth IS NOT NULL AND cs.start_date IS NOT NULL
         THEN EXTRACT(YEAR FROM age(cs.start_date, p.date_of_birth))::smallint END,
    p.preferred_foot, sh.shirt_number,

    pp.primary_position, gmap.position_group, pp.primary_position_pct,
    pp.minutes_by_position, pp.positions_played,

    a.matches_played, a.matches_started, a.matches_sub_on, a.matches_full_90,
    a.matches_as_captain, a.minutes_played,
    COALESCE(tc.team_matches, 0),
    a.first_match_date, a.through_match_date,

    a.goals, a.goals_non_penalty, a.shots, a.shots_on_target, a.shots_in_box,
    a.shots_outside_box, a.shots_first_time, a.headed_goals, a.xg, a.npxg,
    a.big_chances, a.big_chances_scored, a.big_chances_missed,
    a.penalties_taken, a.penalties_scored,

    a.assists, a.key_passes, a.xa, a.second_assists, a.crosses,
    a.crosses_completed, a.crosses_from_open_play, a.through_balls,
    a.passes_into_penalty_area, a.shot_creating_actions,

    a.passes, a.passes_completed, a.passes_received, a.passes_short,
    a.passes_medium, a.passes_long, a.passes_forward, a.passes_backward,
    a.progressive_passes, a.progressive_passes_completed,
    a.passes_into_final_third, a.long_balls, a.long_balls_completed,
    a.switches, a.offside_passes, a.xt_pass,

    a.carries, a.carry_distance_total, a.progressive_carries,
    a.carries_into_final_third, a.carries_into_penalty_area,
    a.take_ons, a.take_ons_won, a.dispossessed,
    a.touches, a.touches_in_box, a.touches_final_third, a.xt_carry,

    a.tackles, a.tackles_won, a.tackles_missed, a.dribbled_past,
    a.interceptions, a.clearances, a.blocked_passes, a.ball_recoveries,
    a.defensive_actions, a.def_actions_final_third, a.def_action_avg_x,
    a.fouls_committed, a.fouls_won, a.errors, a.offsides_provoked,
    a.caught_offside, a.shield_ball_opp,

    a.aerials_won, a.aerials_lost, a.aerials_won_att_third,
    a.aerials_won_def_third,

    a.saves, a.goals_conceded, a.clean_sheets, a.xg_faced, a.penalties_faced,
    a.penalties_saved, a.claims, a.claims_successful, a.punches,
    a.crosses_not_claimed, a.smothers, a.keeper_sweeper_actions,
    a.sweeper_avg_x, a.keeper_pickups,
    a.gk_passes, a.gk_passes_completed, a.gk_long_balls,

    a.xt, a.vaep, a.vaep_offensive, a.vaep_defensive, a.vaep_actions,

    a.yellow_cards, a.second_yellows, a.red_cards,

    a.corners_taken, a.free_kicks_taken, a.direct_free_kick_shots,
    a.throw_ins_taken, a.set_piece_goals, a.set_piece_xg,

    a.padj_tackles, a.padj_interceptions, a.padj_clearances,
    a.padj_ball_recoveries, a.padj_blocked_passes, a.padj_defensive_actions,

    tc.team_season_goals
FROM agg a
JOIN silver.players p               ON p.player_id = a.player_id
JOIN silver.teams   t               ON t.team_id   = a.team_id
JOIN silver.competition_seasons cs  ON cs.competition_season_id = a.competition_season_id
JOIN silver.competitions c          ON c.competition_id = cs.competition_id
JOIN silver.seasons se              ON se.season_id     = cs.season_id
LEFT JOIN pos_profile pp ON pp.competition_season_id = a.competition_season_id
                        AND pp.player_id = a.player_id AND pp.team_id = a.team_id
-- position -> position_group from the seeded map, so the two cannot drift.
LEFT JOIN (SELECT DISTINCT position, position_group FROM gold.formation_slot_positions)
       AS gmap ON gmap.position = pp.primary_position
LEFT JOIN team_cov tc ON tc.competition_season_id = a.competition_season_id
                     AND tc.team_id = a.team_id
LEFT JOIN shirt sh ON sh.competition_season_id = a.competition_season_id
                  AND sh.player_id = a.player_id AND sh.team_id = a.team_id;


-- ───────────────────────────────────────────────────────────────────────────
-- vaep_rank_in_team — a ranking, so it needs a second pass once every row for
-- the season exists. Ranked on vaep_per_90 (a generated column) and gated on
-- meets_min_minutes, or the top of every squad is a substitute with 30 minutes.
-- ───────────────────────────────────────────────────────────────────────────
UPDATE gold.player_season_stats s
SET vaep_rank_in_team = r.pos
FROM (
    SELECT player_id, team_id,
           RANK() OVER (PARTITION BY team_id
                        ORDER BY vaep_per_90 DESC NULLS LAST)::smallint AS pos
    FROM gold.player_season_stats
    WHERE competition_season_id = %(cs_id)s
      AND meets_min_minutes
) r
WHERE s.competition_season_id = %(cs_id)s
  AND s.player_id = r.player_id
  AND s.team_id   = r.team_id;
