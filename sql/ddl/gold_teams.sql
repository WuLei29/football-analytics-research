-- ═══════════════════════════════════════════════════════════════════════════
-- gold_teams.sql — DDL for the team layer
-- ═══════════════════════════════════════════════════════════════════════════
--
-- Spec: md/GOLD_LAYER.md §4.3 (team_match_stats), §4.4 (team_season_stats),
--       §4.5 (the metric columns, defined once at match grain).
--
-- Creates:
--   gold.team_match_stats    one row per (match, team) — the atomic team fact
--   gold.team_season_stats   one row per (competition_season, team) — rollup
--
-- DEVIATION FROM §6 STEP 1, deliberate: the spec puts every remaining gold
-- table in one sql/ddl/gold_schema.sql. The team tables ship in their own file
-- so the player tables can be written when they are actually built, rather
-- than months ahead of anything that populates them. Same tables, same order.
--
-- Every ratio marked ‡ in §4.5 is a STORED GENERATED column rather than a
-- value the build writes. Two reasons: the numerator and denominator are
-- already columns on the same row, so a generated column cannot drift from
-- them; and it keeps ~25 expressions out of the INSERT list. The rule in
-- §4.5.13 — "recompute ratios from summed inputs, never AVG the per-match
-- value" — is therefore enforced by the schema on the season table too.
--
-- Idempotent: safe to run repeatedly. Drops nothing.
-- ═══════════════════════════════════════════════════════════════════════════

CREATE SCHEMA IF NOT EXISTS gold;


-- ───────────────────────────────────────────────────────────────────────────
-- gold.team_match_stats — §4.3
--
-- Lean on context by design: FKs plus only what is needed to order, split and
-- roll up. No denormalised labels — this is an intermediate table, not a
-- serving table (§4.3.1).
-- ───────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS gold.team_match_stats (
    -- ── grain ──────────────────────────────────────────────────────────────
    match_id              INT NOT NULL REFERENCES silver.matches(match_id),
    team_id               INT NOT NULL REFERENCES silver.teams(team_id),

    -- ── context ────────────────────────────────────────────────────────────
    competition_season_id INT      NOT NULL
                              REFERENCES silver.competition_seasons(competition_season_id),
    opponent_team_id      INT      NOT NULL REFERENCES silver.teams(team_id),
    matchday              SMALLINT NOT NULL,
    -- The ordering key, NOT matchday: a postponed matchday-14 fixture played in
    -- February is the team's most recent match in February (§4.3.1).
    match_date            DATE     NOT NULL,
    team_match_number     SMALLINT NOT NULL,
    is_home               BOOLEAN  NOT NULL,
    result                CHAR(1)  NOT NULL CHECK (result IN ('W','D','L')),
    points                SMALLINT NOT NULL CHECK (points IN (0,1,3)),

    -- ═══ §4.5.1 Result & goals ═════════════════════════════════════════════
    goals_for             SMALLINT NOT NULL DEFAULT 0,
    goals_against         SMALLINT NOT NULL DEFAULT 0,
    goal_difference       SMALLINT GENERATED ALWAYS AS (goals_for - goals_against) STORED,
    clean_sheet           BOOLEAN  GENERATED ALWAYS AS (goals_against = 0) STORED,
    failed_to_score       BOOLEAN  GENERATED ALWAYS AS (goals_for = 0) STORED,
    goals_for_ht          SMALLINT NOT NULL DEFAULT 0,
    goals_against_ht      SMALLINT NOT NULL DEFAULT 0,
    goals_for_p1          SMALLINT NOT NULL DEFAULT 0,
    goals_for_p2          SMALLINT NOT NULL DEFAULT 0,
    goals_against_p1      SMALLINT NOT NULL DEFAULT 0,
    goals_against_p2      SMALLINT NOT NULL DEFAULT 0,
    own_goals_for         SMALLINT NOT NULL DEFAULT 0,
    own_goals_against     SMALLINT NOT NULL DEFAULT 0,

    -- ═══ §4.5.2 Shots ══════════════════════════════════════════════════════
    -- shots_on_target is Goal + Attempt Saved WITHOUT Q82. Counting all
    -- Attempt Saved as on target is the single most common SoT error (§4.5.0).
    shots                    SMALLINT NOT NULL DEFAULT 0,
    shots_on_target          SMALLINT NOT NULL DEFAULT 0,
    shots_blocked            SMALLINT NOT NULL DEFAULT 0,
    shots_off_target         SMALLINT NOT NULL DEFAULT 0,
    shots_woodwork           SMALLINT NOT NULL DEFAULT 0,
    shots_inside_box         SMALLINT NOT NULL DEFAULT 0,
    shots_outside_box        SMALLINT NOT NULL DEFAULT 0,
    shots_first_time         SMALLINT NOT NULL DEFAULT 0,
    shots_individual_play    SMALLINT NOT NULL DEFAULT 0,
    big_chances              SMALLINT NOT NULL DEFAULT 0,
    big_chances_scored       SMALLINT NOT NULL DEFAULT 0,
    penalties_taken          SMALLINT NOT NULL DEFAULT 0,
    penalties_scored         SMALLINT NOT NULL DEFAULT 0,
    shots_against            SMALLINT NOT NULL DEFAULT 0,
    shots_on_target_against  SMALLINT NOT NULL DEFAULT 0,
    shots_inside_box_against SMALLINT NOT NULL DEFAULT 0,
    big_chances_against      SMALLINT NOT NULL DEFAULT 0,
    shot_accuracy            REAL GENERATED ALWAYS AS
                                 (shots_on_target::real / NULLIF(shots, 0)) STORED,
    shot_conversion_rate     REAL GENERATED ALWAYS AS
                                 (goals_for::real / NULLIF(shots, 0)) STORED,

    -- ═══ §4.5.3 Expected goals ═════════════════════════════════════════════
    xg_for                REAL NOT NULL DEFAULT 0,
    xg_against            REAL NOT NULL DEFAULT 0,
    xg_difference         REAL GENERATED ALWAYS AS (xg_for - xg_against) STORED,
    npxg_for              REAL NOT NULL DEFAULT 0,
    npxg_against          REAL NOT NULL DEFAULT 0,
    xg_overperformance    REAL GENERATED ALWAYS AS (goals_for::real - xg_for) STORED,
    xg_open_play          REAL NOT NULL DEFAULT 0,
    xg_set_piece          REAL NOT NULL DEFAULT 0,
    xg_from_corner        REAL NOT NULL DEFAULT 0,
    xg_free_kick          REAL NOT NULL DEFAULT 0,
    xg_fast_break         REAL NOT NULL DEFAULT 0,
    xg_penalty            REAL NOT NULL DEFAULT 0,
    goals_open_play       SMALLINT NOT NULL DEFAULT 0,
    goals_set_piece       SMALLINT NOT NULL DEFAULT 0,
    goals_fast_break      SMALLINT NOT NULL DEFAULT 0,
    xg_per_shot           REAL GENERATED ALWAYS AS
                              (xg_for / NULLIF(shots, 0)) STORED,

    -- ═══ §4.5.4 Passing ════════════════════════════════════════════════════
    passes                        SMALLINT NOT NULL DEFAULT 0,
    passes_completed              SMALLINT NOT NULL DEFAULT 0,
    passes_short                  SMALLINT NOT NULL DEFAULT 0,
    passes_medium                 SMALLINT NOT NULL DEFAULT 0,
    passes_long                   SMALLINT NOT NULL DEFAULT 0,
    passes_forward                SMALLINT NOT NULL DEFAULT 0,
    passes_backward               SMALLINT NOT NULL DEFAULT 0,
    passes_sideways               SMALLINT NOT NULL DEFAULT 0,
    progressive_passes            SMALLINT NOT NULL DEFAULT 0,
    progressive_passes_completed  SMALLINT NOT NULL DEFAULT 0,
    passes_into_final_third       SMALLINT NOT NULL DEFAULT 0,
    passes_into_penalty_area      SMALLINT NOT NULL DEFAULT 0,
    crosses                       SMALLINT NOT NULL DEFAULT 0,
    crosses_completed             SMALLINT NOT NULL DEFAULT 0,
    through_balls                 SMALLINT NOT NULL DEFAULT 0,
    long_balls                    SMALLINT NOT NULL DEFAULT 0,
    key_passes                    SMALLINT NOT NULL DEFAULT 0,
    assists                       SMALLINT NOT NULL DEFAULT 0,
    second_assists                SMALLINT NOT NULL DEFAULT 0,
    xa                            REAL     NOT NULL DEFAULT 0,
    offside_passes                SMALLINT NOT NULL DEFAULT 0,
    passes_against                SMALLINT NOT NULL DEFAULT 0,
    -- Numerators for the pass_share_* ratios. §4.5.9 lists the three shares
    -- but not their inputs; §4.5's own notation rule says a ratio whose inputs
    -- are not otherwise stored must store them, so they are stored here.
    passes_def_third              SMALLINT NOT NULL DEFAULT 0,
    passes_mid_third              SMALLINT NOT NULL DEFAULT 0,
    passes_att_third              SMALLINT NOT NULL DEFAULT 0,
    pass_completion_rate          REAL GENERATED ALWAYS AS
                                      (passes_completed::real / NULLIF(passes, 0)) STORED,
    cross_completion_rate         REAL GENERATED ALWAYS AS
                                      (crosses_completed::real / NULLIF(crosses, 0)) STORED,

    -- ═══ §4.5.5 Carrying & dribbling ═══════════════════════════════════════
    carries                     SMALLINT NOT NULL DEFAULT 0,
    carry_distance_total        REAL     NOT NULL DEFAULT 0,
    progressive_carries         SMALLINT NOT NULL DEFAULT 0,
    carries_into_final_third    SMALLINT NOT NULL DEFAULT 0,
    carries_into_penalty_area   SMALLINT NOT NULL DEFAULT 0,
    take_ons                    SMALLINT NOT NULL DEFAULT 0,
    take_ons_won                SMALLINT NOT NULL DEFAULT 0,
    dispossessed                SMALLINT NOT NULL DEFAULT 0,
    errors                      SMALLINT NOT NULL DEFAULT 0,
    take_on_success_rate        REAL GENERATED ALWAYS AS
                                    (take_ons_won::real / NULLIF(take_ons, 0)) STORED,

    -- ═══ §4.5.6 Model values — xT and VAEP ═════════════════════════════════
    -- SUM ignores NULLs, so no filter is needed; but the first action of each
    -- match has NULL VAEP and only SPADL-valid actions carry values, so any
    -- count-based denominator must COUNT(vaep_value), never COUNT(*) (§4.5.6).
    xt_for              REAL NOT NULL DEFAULT 0,
    xt_against          REAL NOT NULL DEFAULT 0,
    xt_net              REAL GENERATED ALWAYS AS (xt_for - xt_against) STORED,
    xt_open_play        REAL NOT NULL DEFAULT 0,
    vaep_for            REAL NOT NULL DEFAULT 0,
    vaep_against        REAL NOT NULL DEFAULT 0,
    vaep_offensive_for  REAL NOT NULL DEFAULT 0,
    vaep_defensive_for  REAL NOT NULL DEFAULT 0,

    -- ═══ §4.5.7 Defensive actions ══════════════════════════════════════════
    tackles              SMALLINT NOT NULL DEFAULT 0,
    tackles_won          SMALLINT NOT NULL DEFAULT 0,
    interceptions        SMALLINT NOT NULL DEFAULT 0,
    clearances           SMALLINT NOT NULL DEFAULT 0,
    blocked_passes       SMALLINT NOT NULL DEFAULT 0,
    ball_recoveries      SMALLINT NOT NULL DEFAULT 0,
    aerials_won          SMALLINT NOT NULL DEFAULT 0,
    aerials_lost         SMALLINT NOT NULL DEFAULT 0,
    duels_won            SMALLINT NOT NULL DEFAULT 0,
    duels_lost           SMALLINT NOT NULL DEFAULT 0,
    fouls_committed      SMALLINT NOT NULL DEFAULT 0,
    fouls_won            SMALLINT NOT NULL DEFAULT 0,
    offsides_provoked    SMALLINT NOT NULL DEFAULT 0,
    caught_offside       SMALLINT NOT NULL DEFAULT 0,
    defensive_actions    SMALLINT NOT NULL DEFAULT 0,
    aerial_win_rate      REAL GENERATED ALWAYS AS
                             (aerials_won::real / NULLIF(aerials_won + aerials_lost, 0)) STORED,
    tackle_success_rate  REAL GENERATED ALWAYS AS
                             (tackles_won::real / NULLIF(tackles, 0)) STORED,

    -- ═══ §4.5.8 Goalkeeping ════════════════════════════════════════════════
    -- Not a mirror of anything: derived from own events.
    saves                   SMALLINT NOT NULL DEFAULT 0,
    claims                  SMALLINT NOT NULL DEFAULT 0,
    punches                 SMALLINT NOT NULL DEFAULT 0,
    smothers                SMALLINT NOT NULL DEFAULT 0,
    crosses_not_claimed     SMALLINT NOT NULL DEFAULT 0,
    keeper_sweeper_actions  SMALLINT NOT NULL DEFAULT 0,
    keeper_pickups          SMALLINT NOT NULL DEFAULT 0,
    -- Pre-shot xG proxy, NOT a shot-stopping rating: the correct formulation
    -- needs post-shot xG (xGOT), which the current model does not produce.
    goals_prevented         REAL GENERATED ALWAYS AS
                                (xg_against - goals_against::real) STORED,
    save_pct                REAL GENERATED ALWAYS AS
                                (saves::real / NULLIF(shots_on_target_against, 0)) STORED,

    -- ═══ §4.5.9 Possession & territory ═════════════════════════════════════
    -- Duration is derived from minute*60+second and has one-second resolution,
    -- so a one-second floor is applied per sequence in the build (§4.5.0).
    possession_duration_sec      REAL     NOT NULL DEFAULT 0,
    possession_duration_opp_sec  REAL     NOT NULL DEFAULT 0,
    touches                      SMALLINT NOT NULL DEFAULT 0,
    touches_def_third            SMALLINT NOT NULL DEFAULT 0,
    touches_mid_third            SMALLINT NOT NULL DEFAULT 0,
    touches_final_third          SMALLINT NOT NULL DEFAULT 0,
    touches_in_box               SMALLINT NOT NULL DEFAULT 0,
    touches_final_third_opp      SMALLINT NOT NULL DEFAULT 0,
    avg_action_x                 REAL,
    possession_pct               REAL GENERATED ALWAYS AS
        (possession_duration_sec
           / NULLIF(possession_duration_sec + possession_duration_opp_sec, 0)) STORED,
    field_tilt                   REAL GENERATED ALWAYS AS
        (touches_final_third::real
           / NULLIF(touches_final_third + touches_final_third_opp, 0)) STORED,
    pass_share_def_third         REAL GENERATED ALWAYS AS
                                     (passes_def_third::real / NULLIF(passes, 0)) STORED,
    pass_share_mid_third         REAL GENERATED ALWAYS AS
                                     (passes_mid_third::real / NULLIF(passes, 0)) STORED,
    pass_share_att_third         REAL GENERATED ALWAYS AS
                                     (passes_att_third::real / NULLIF(passes, 0)) STORED,

    -- ═══ §4.5.10 Sequences ═════════════════════════════════════════════════
    -- Source is gold.sequences. NEVER derive a team total from it — 8.9% of
    -- events carry no sequence_id, so the team totals above read silver.events
    -- (§4.1.0 #3).
    sequences                    SMALLINT NOT NULL DEFAULT 0,
    sequence_passes_total        SMALLINT NOT NULL DEFAULT 0,
    sequence_duration_total      REAL     NOT NULL DEFAULT 0,
    long_sequences               SMALLINT NOT NULL DEFAULT 0,
    sequences_ending_in_shot     SMALLINT NOT NULL DEFAULT 0,
    sequences_ending_in_goal     SMALLINT NOT NULL DEFAULT 0,
    final_third_entries          SMALLINT NOT NULL DEFAULT 0,
    penalty_box_entries          SMALLINT NOT NULL DEFAULT 0,
    sequences_started_def_third  SMALLINT NOT NULL DEFAULT 0,
    sequences_started_mid_third  SMALLINT NOT NULL DEFAULT 0,
    sequences_started_att_third  SMALLINT NOT NULL DEFAULT 0,
    buildup_sequences_from_gk    SMALLINT NOT NULL DEFAULT 0,
    counter_attack_sequences     SMALLINT NOT NULL DEFAULT 0,
    high_transition_sequences    SMALLINT NOT NULL DEFAULT 0,
    set_piece_sequences          SMALLINT NOT NULL DEFAULT 0,
    sequence_xt_total            REAL     NOT NULL DEFAULT 0,
    sequence_distance_total      REAL     NOT NULL DEFAULT 0,
    field_progression_total      REAL     NOT NULL DEFAULT 0,
    directness                   REAL GENERATED ALWAYS AS
        (field_progression_total / NULLIF(sequence_distance_total, 0)) STORED,
    avg_sequence_passes          REAL GENERATED ALWAYS AS
        (sequence_passes_total::real / NULLIF(sequences, 0)) STORED,
    avg_sequence_duration_sec    REAL GENERATED ALWAYS AS
        (sequence_duration_total / NULLIF(sequences, 0)) STORED,
    direct_speed                 REAL GENERATED ALWAYS AS
        (field_progression_total / NULLIF(sequence_duration_total, 0)) STORED,
    shot_ending_sequence_rate    REAL GENERATED ALWAYS AS
        (sequences_ending_in_shot::real / NULLIF(sequences, 0)) STORED,

    -- ═══ §4.5.11 Pressing & transition ═════════════════════════════════════
    -- PPDA zone is x >= 42 in the PRESSING team's frame — the 60% of the pitch
    -- furthest from its own goal. The narrow denominator is Tackle +
    -- Interception + Challenge + Foul committed; clearances and recoveries are
    -- excluded deliberately (§4.5.0).
    ppda_opp_passes                SMALLINT NOT NULL DEFAULT 0,
    ppda_def_actions               SMALLINT NOT NULL DEFAULT 0,
    high_turnovers                 SMALLINT NOT NULL DEFAULT 0,
    high_turnover_shots            SMALLINT NOT NULL DEFAULT 0,
    counterpress_recoveries        SMALLINT NOT NULL DEFAULT 0,
    counterattack_sequences        SMALLINT NOT NULL DEFAULT 0,
    opp_sequences_ended_own_third  SMALLINT NOT NULL DEFAULT 0,
    defensive_line_height          REAL,
    ppda                           REAL GENERATED ALWAYS AS
        (ppda_opp_passes::real / NULLIF(ppda_def_actions, 0)) STORED,
    -- Not derivable from this row: it is the opponent's ratio, written from
    -- the opponent's stored numerator and denominator by the build.
    ppda_against                   REAL,

    -- ═══ §4.5.12 Set pieces & discipline ═══════════════════════════════════
    corners_for              SMALLINT NOT NULL DEFAULT 0,
    corners_against          SMALLINT NOT NULL DEFAULT 0,
    free_kicks_taken         SMALLINT NOT NULL DEFAULT 0,
    throw_ins_taken          SMALLINT NOT NULL DEFAULT 0,
    set_piece_goals_for      SMALLINT NOT NULL DEFAULT 0,
    set_piece_goals_against  SMALLINT NOT NULL DEFAULT 0,
    yellow_cards             SMALLINT NOT NULL DEFAULT 0,
    second_yellows           SMALLINT NOT NULL DEFAULT 0,
    red_cards                SMALLINT NOT NULL DEFAULT 0,

    computed_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (match_id, team_id)
);

CREATE INDEX IF NOT EXISTS team_match_stats_season_team_date_idx
    ON gold.team_match_stats (competition_season_id, team_id, match_date);
CREATE INDEX IF NOT EXISTS team_match_stats_season_date_idx
    ON gold.team_match_stats (competition_season_id, match_date);


-- ───────────────────────────────────────────────────────────────────────────
-- gold.team_season_stats — §4.4
--
-- A pure rollup of gold.team_match_stats plus denormalised display labels.
-- Touches no event data.
--
-- Rollup rules (§4.5.13), enforced by construction:
--   counts and sums          SUM
--   booleans                 COUNT(*) FILTER (WHERE …)
--   ratios (‡)               generated from the SUMmed inputs — never AVG'd
--   means over sub-entities  weight by sequence count, not match count
-- ───────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS gold.team_season_stats (
    -- ── grain ──────────────────────────────────────────────────────────────
    competition_season_id INT NOT NULL
                              REFERENCES silver.competition_seasons(competition_season_id),
    team_id               INT NOT NULL REFERENCES silver.teams(team_id),

    -- ── denormalised context — DISPLAY ONLY, never join or filter on these ─
    -- WHERE team_name = 'Barcelona' is a bug; filter on team_id (§4.4.1).
    competition_code      VARCHAR(10)  NOT NULL,
    competition_name      VARCHAR(100) NOT NULL,
    tier_level            SMALLINT     NOT NULL,
    season_label          VARCHAR(10)  NOT NULL,
    team_name             VARCHAR(100) NOT NULL,
    team_short_name       VARCHAR(30),
    team_abbreviation     VARCHAR(5),

    -- ── coverage / "as of" ─────────────────────────────────────────────────
    -- All of these derive from gold.team_match_stats, never from
    -- silver.matches, so the two tables cannot disagree (§4.4.1).
    matches_played        SMALLINT NOT NULL DEFAULT 0,
    last_matchday_played  SMALLINT,
    matchdays_scheduled   SMALLINT,
    matches_remaining     SMALLINT,
    first_match_date      DATE,
    through_match_date    DATE,
    season_status         VARCHAR(20) NOT NULL,

    -- ═══ §4.5.14 Season-only ═══════════════════════════════════════════════
    wins                  SMALLINT NOT NULL DEFAULT 0,
    draws                 SMALLINT NOT NULL DEFAULT 0,
    losses                SMALLINT NOT NULL DEFAULT 0,
    points                SMALLINT NOT NULL DEFAULT 0,
    home_wins             SMALLINT NOT NULL DEFAULT 0,
    home_draws            SMALLINT NOT NULL DEFAULT 0,
    home_losses           SMALLINT NOT NULL DEFAULT 0,
    away_wins             SMALLINT NOT NULL DEFAULT 0,
    away_draws            SMALLINT NOT NULL DEFAULT 0,
    away_losses           SMALLINT NOT NULL DEFAULT 0,
    goals_for_home        SMALLINT NOT NULL DEFAULT 0,
    goals_for_away        SMALLINT NOT NULL DEFAULT 0,
    goals_against_home    SMALLINT NOT NULL DEFAULT 0,
    goals_against_away    SMALLINT NOT NULL DEFAULT 0,
    clean_sheets          SMALLINT NOT NULL DEFAULT 0,
    failed_to_score_count SMALLINT NOT NULL DEFAULT 0,
    -- Last 5 results ordered by match_date DESC, NOT by matchday (§4.5.14).
    form_last_5           VARCHAR(15),
    xg_for_per_match      REAL GENERATED ALWAYS AS
                              (xg_for / NULLIF(matches_played, 0)) STORED,
    xg_against_per_match  REAL GENERATED ALWAYS AS
                              (xg_against / NULLIF(matches_played, 0)) STORED,
    points_per_match      REAL GENERATED ALWAYS AS
                              (points::real / NULLIF(matches_played, 0)) STORED,
    -- A ranking, not an aggregate: depends on every other team's row, so it is
    -- written by a second UPDATE pass once the season's rows exist. NULL for
    -- cup competition_seasons, where it has no meaning (§4.5.14).
    league_position       SMALLINT,

    -- ═══ §4.5.1 Result & goals ═════════════════════════════════════════════
    goals_for             SMALLINT NOT NULL DEFAULT 0,
    goals_against         SMALLINT NOT NULL DEFAULT 0,
    goal_difference       SMALLINT GENERATED ALWAYS AS (goals_for - goals_against) STORED,
    goals_for_ht          SMALLINT NOT NULL DEFAULT 0,
    goals_against_ht      SMALLINT NOT NULL DEFAULT 0,
    goals_for_p1          SMALLINT NOT NULL DEFAULT 0,
    goals_for_p2          SMALLINT NOT NULL DEFAULT 0,
    goals_against_p1      SMALLINT NOT NULL DEFAULT 0,
    goals_against_p2      SMALLINT NOT NULL DEFAULT 0,
    own_goals_for         SMALLINT NOT NULL DEFAULT 0,
    own_goals_against     SMALLINT NOT NULL DEFAULT 0,

    -- ═══ §4.5.2 Shots ══════════════════════════════════════════════════════
    shots                    SMALLINT NOT NULL DEFAULT 0,
    shots_on_target          SMALLINT NOT NULL DEFAULT 0,
    shots_blocked            SMALLINT NOT NULL DEFAULT 0,
    shots_off_target         SMALLINT NOT NULL DEFAULT 0,
    shots_woodwork           SMALLINT NOT NULL DEFAULT 0,
    shots_inside_box         SMALLINT NOT NULL DEFAULT 0,
    shots_outside_box        SMALLINT NOT NULL DEFAULT 0,
    shots_first_time         SMALLINT NOT NULL DEFAULT 0,
    shots_individual_play    SMALLINT NOT NULL DEFAULT 0,
    big_chances              SMALLINT NOT NULL DEFAULT 0,
    big_chances_scored       SMALLINT NOT NULL DEFAULT 0,
    penalties_taken          SMALLINT NOT NULL DEFAULT 0,
    penalties_scored         SMALLINT NOT NULL DEFAULT 0,
    shots_against            SMALLINT NOT NULL DEFAULT 0,
    shots_on_target_against  SMALLINT NOT NULL DEFAULT 0,
    shots_inside_box_against SMALLINT NOT NULL DEFAULT 0,
    big_chances_against      SMALLINT NOT NULL DEFAULT 0,
    shot_accuracy            REAL GENERATED ALWAYS AS
                                 (shots_on_target::real / NULLIF(shots, 0)) STORED,
    shot_conversion_rate     REAL GENERATED ALWAYS AS
                                 (goals_for::real / NULLIF(shots, 0)) STORED,

    -- ═══ §4.5.3 Expected goals ═════════════════════════════════════════════
    xg_for                REAL NOT NULL DEFAULT 0,
    xg_against            REAL NOT NULL DEFAULT 0,
    xg_difference         REAL GENERATED ALWAYS AS (xg_for - xg_against) STORED,
    npxg_for              REAL NOT NULL DEFAULT 0,
    npxg_against          REAL NOT NULL DEFAULT 0,
    xg_overperformance    REAL GENERATED ALWAYS AS (goals_for::real - xg_for) STORED,
    xg_open_play          REAL NOT NULL DEFAULT 0,
    xg_set_piece          REAL NOT NULL DEFAULT 0,
    xg_from_corner        REAL NOT NULL DEFAULT 0,
    xg_free_kick          REAL NOT NULL DEFAULT 0,
    xg_fast_break         REAL NOT NULL DEFAULT 0,
    xg_penalty            REAL NOT NULL DEFAULT 0,
    goals_open_play       SMALLINT NOT NULL DEFAULT 0,
    goals_set_piece       SMALLINT NOT NULL DEFAULT 0,
    goals_fast_break      SMALLINT NOT NULL DEFAULT 0,
    xg_per_shot           REAL GENERATED ALWAYS AS
                              (xg_for / NULLIF(shots, 0)) STORED,

    -- ═══ §4.5.4 Passing ════════════════════════════════════════════════════
    passes                        INT      NOT NULL DEFAULT 0,
    passes_completed              INT      NOT NULL DEFAULT 0,
    passes_short                  INT      NOT NULL DEFAULT 0,
    passes_medium                 INT      NOT NULL DEFAULT 0,
    passes_long                   INT      NOT NULL DEFAULT 0,
    passes_forward                INT      NOT NULL DEFAULT 0,
    passes_backward               INT      NOT NULL DEFAULT 0,
    passes_sideways               INT      NOT NULL DEFAULT 0,
    progressive_passes            INT      NOT NULL DEFAULT 0,
    progressive_passes_completed  INT      NOT NULL DEFAULT 0,
    passes_into_final_third       INT      NOT NULL DEFAULT 0,
    passes_into_penalty_area      INT      NOT NULL DEFAULT 0,
    crosses                       SMALLINT NOT NULL DEFAULT 0,
    crosses_completed             SMALLINT NOT NULL DEFAULT 0,
    through_balls                 SMALLINT NOT NULL DEFAULT 0,
    long_balls                    SMALLINT NOT NULL DEFAULT 0,
    key_passes                    SMALLINT NOT NULL DEFAULT 0,
    assists                       SMALLINT NOT NULL DEFAULT 0,
    second_assists                SMALLINT NOT NULL DEFAULT 0,
    xa                            REAL     NOT NULL DEFAULT 0,
    offside_passes                SMALLINT NOT NULL DEFAULT 0,
    passes_against                INT      NOT NULL DEFAULT 0,
    passes_def_third              INT      NOT NULL DEFAULT 0,
    passes_mid_third              INT      NOT NULL DEFAULT 0,
    passes_att_third              INT      NOT NULL DEFAULT 0,
    pass_completion_rate          REAL GENERATED ALWAYS AS
                                      (passes_completed::real / NULLIF(passes, 0)) STORED,
    cross_completion_rate         REAL GENERATED ALWAYS AS
                                      (crosses_completed::real / NULLIF(crosses, 0)) STORED,

    -- ═══ §4.5.5 Carrying & dribbling ═══════════════════════════════════════
    carries                     INT      NOT NULL DEFAULT 0,
    carry_distance_total        REAL     NOT NULL DEFAULT 0,
    progressive_carries         INT      NOT NULL DEFAULT 0,
    carries_into_final_third    INT      NOT NULL DEFAULT 0,
    carries_into_penalty_area   INT      NOT NULL DEFAULT 0,
    take_ons                    SMALLINT NOT NULL DEFAULT 0,
    take_ons_won                SMALLINT NOT NULL DEFAULT 0,
    dispossessed                SMALLINT NOT NULL DEFAULT 0,
    errors                      SMALLINT NOT NULL DEFAULT 0,
    take_on_success_rate        REAL GENERATED ALWAYS AS
                                    (take_ons_won::real / NULLIF(take_ons, 0)) STORED,

    -- ═══ §4.5.6 Model values ═══════════════════════════════════════════════
    xt_for              REAL NOT NULL DEFAULT 0,
    xt_against          REAL NOT NULL DEFAULT 0,
    xt_net              REAL GENERATED ALWAYS AS (xt_for - xt_against) STORED,
    xt_open_play        REAL NOT NULL DEFAULT 0,
    vaep_for            REAL NOT NULL DEFAULT 0,
    vaep_against        REAL NOT NULL DEFAULT 0,
    vaep_offensive_for  REAL NOT NULL DEFAULT 0,
    vaep_defensive_for  REAL NOT NULL DEFAULT 0,

    -- ═══ §4.5.7 Defensive actions ══════════════════════════════════════════
    tackles              SMALLINT NOT NULL DEFAULT 0,
    tackles_won          SMALLINT NOT NULL DEFAULT 0,
    interceptions        SMALLINT NOT NULL DEFAULT 0,
    clearances           SMALLINT NOT NULL DEFAULT 0,
    blocked_passes       SMALLINT NOT NULL DEFAULT 0,
    ball_recoveries      INT      NOT NULL DEFAULT 0,
    aerials_won          SMALLINT NOT NULL DEFAULT 0,
    aerials_lost         SMALLINT NOT NULL DEFAULT 0,
    duels_won            SMALLINT NOT NULL DEFAULT 0,
    duels_lost           SMALLINT NOT NULL DEFAULT 0,
    fouls_committed      SMALLINT NOT NULL DEFAULT 0,
    fouls_won            SMALLINT NOT NULL DEFAULT 0,
    offsides_provoked    SMALLINT NOT NULL DEFAULT 0,
    caught_offside       SMALLINT NOT NULL DEFAULT 0,
    defensive_actions    INT      NOT NULL DEFAULT 0,
    aerial_win_rate      REAL GENERATED ALWAYS AS
                             (aerials_won::real / NULLIF(aerials_won + aerials_lost, 0)) STORED,
    tackle_success_rate  REAL GENERATED ALWAYS AS
                             (tackles_won::real / NULLIF(tackles, 0)) STORED,

    -- ═══ §4.5.8 Goalkeeping ════════════════════════════════════════════════
    saves                   SMALLINT NOT NULL DEFAULT 0,
    claims                  SMALLINT NOT NULL DEFAULT 0,
    punches                 SMALLINT NOT NULL DEFAULT 0,
    smothers                SMALLINT NOT NULL DEFAULT 0,
    crosses_not_claimed     SMALLINT NOT NULL DEFAULT 0,
    keeper_sweeper_actions  SMALLINT NOT NULL DEFAULT 0,
    keeper_pickups          SMALLINT NOT NULL DEFAULT 0,
    goals_prevented         REAL GENERATED ALWAYS AS
                                (xg_against - goals_against::real) STORED,
    save_pct                REAL GENERATED ALWAYS AS
                                (saves::real / NULLIF(shots_on_target_against, 0)) STORED,

    -- ═══ §4.5.9 Possession & territory ═════════════════════════════════════
    possession_duration_sec      REAL NOT NULL DEFAULT 0,
    possession_duration_opp_sec  REAL NOT NULL DEFAULT 0,
    touches                      INT  NOT NULL DEFAULT 0,
    touches_def_third            INT  NOT NULL DEFAULT 0,
    touches_mid_third            INT  NOT NULL DEFAULT 0,
    touches_final_third          INT  NOT NULL DEFAULT 0,
    touches_in_box               INT  NOT NULL DEFAULT 0,
    touches_final_third_opp      INT  NOT NULL DEFAULT 0,
    -- Weighted by touches, not by match count (§4.5.13, means over
    -- sub-entities): SUM(avg_action_x * touches) / SUM(touches).
    avg_action_x                 REAL,
    possession_pct               REAL GENERATED ALWAYS AS
        (possession_duration_sec
           / NULLIF(possession_duration_sec + possession_duration_opp_sec, 0)) STORED,
    field_tilt                   REAL GENERATED ALWAYS AS
        (touches_final_third::real
           / NULLIF(touches_final_third + touches_final_third_opp, 0)) STORED,
    pass_share_def_third         REAL GENERATED ALWAYS AS
                                     (passes_def_third::real / NULLIF(passes, 0)) STORED,
    pass_share_mid_third         REAL GENERATED ALWAYS AS
                                     (passes_mid_third::real / NULLIF(passes, 0)) STORED,
    pass_share_att_third         REAL GENERATED ALWAYS AS
                                     (passes_att_third::real / NULLIF(passes, 0)) STORED,

    -- ═══ §4.5.10 Sequences ═════════════════════════════════════════════════
    sequences                    INT  NOT NULL DEFAULT 0,
    sequence_passes_total        INT  NOT NULL DEFAULT 0,
    sequence_duration_total      REAL NOT NULL DEFAULT 0,
    long_sequences               INT  NOT NULL DEFAULT 0,
    sequences_ending_in_shot     INT  NOT NULL DEFAULT 0,
    sequences_ending_in_goal     SMALLINT NOT NULL DEFAULT 0,
    final_third_entries          INT  NOT NULL DEFAULT 0,
    penalty_box_entries          INT  NOT NULL DEFAULT 0,
    sequences_started_def_third  INT  NOT NULL DEFAULT 0,
    sequences_started_mid_third  INT  NOT NULL DEFAULT 0,
    sequences_started_att_third  INT  NOT NULL DEFAULT 0,
    buildup_sequences_from_gk    INT  NOT NULL DEFAULT 0,
    counter_attack_sequences     INT  NOT NULL DEFAULT 0,
    high_transition_sequences    INT  NOT NULL DEFAULT 0,
    set_piece_sequences          INT  NOT NULL DEFAULT 0,
    sequence_xt_total            REAL NOT NULL DEFAULT 0,
    sequence_distance_total      REAL NOT NULL DEFAULT 0,
    field_progression_total      REAL NOT NULL DEFAULT 0,
    directness                   REAL GENERATED ALWAYS AS
        (field_progression_total / NULLIF(sequence_distance_total, 0)) STORED,
    avg_sequence_passes          REAL GENERATED ALWAYS AS
        (sequence_passes_total::real / NULLIF(sequences, 0)) STORED,
    avg_sequence_duration_sec    REAL GENERATED ALWAYS AS
        (sequence_duration_total / NULLIF(sequences, 0)) STORED,
    direct_speed                 REAL GENERATED ALWAYS AS
        (field_progression_total / NULLIF(sequence_duration_total, 0)) STORED,
    shot_ending_sequence_rate    REAL GENERATED ALWAYS AS
        (sequences_ending_in_shot::real / NULLIF(sequences, 0)) STORED,

    -- ═══ §4.5.11 Pressing & transition ═════════════════════════════════════
    -- ppda is the ratio of the SUMs, which is why the two inputs are stored.
    -- The mean of per-match ratios is materially different and biased toward
    -- low-event matches — see the worked example in §4.5.13.
    ppda_opp_passes                INT  NOT NULL DEFAULT 0,
    ppda_def_actions               INT  NOT NULL DEFAULT 0,
    ppda_opp_passes_against        INT  NOT NULL DEFAULT 0,
    ppda_def_actions_against       INT  NOT NULL DEFAULT 0,
    high_turnovers                 INT  NOT NULL DEFAULT 0,
    high_turnover_shots            SMALLINT NOT NULL DEFAULT 0,
    counterpress_recoveries        INT  NOT NULL DEFAULT 0,
    counterattack_sequences        INT  NOT NULL DEFAULT 0,
    opp_sequences_ended_own_third  INT  NOT NULL DEFAULT 0,
    defensive_line_height          REAL,
    ppda                           REAL GENERATED ALWAYS AS
        (ppda_opp_passes::real / NULLIF(ppda_def_actions, 0)) STORED,
    ppda_against                   REAL GENERATED ALWAYS AS
        (ppda_opp_passes_against::real / NULLIF(ppda_def_actions_against, 0)) STORED,

    -- ═══ §4.5.12 Set pieces & discipline ═══════════════════════════════════
    corners_for              SMALLINT NOT NULL DEFAULT 0,
    corners_against          SMALLINT NOT NULL DEFAULT 0,
    free_kicks_taken         SMALLINT NOT NULL DEFAULT 0,
    throw_ins_taken          SMALLINT NOT NULL DEFAULT 0,
    set_piece_goals_for      SMALLINT NOT NULL DEFAULT 0,
    set_piece_goals_against  SMALLINT NOT NULL DEFAULT 0,
    yellow_cards             SMALLINT NOT NULL DEFAULT 0,
    second_yellows           SMALLINT NOT NULL DEFAULT 0,
    red_cards                SMALLINT NOT NULL DEFAULT 0,

    computed_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (competition_season_id, team_id)
);

CREATE INDEX IF NOT EXISTS team_season_stats_season_points_idx
    ON gold.team_season_stats (competition_season_id, points DESC);
