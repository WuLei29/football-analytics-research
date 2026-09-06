-- ═══════════════════════════════════════════════════════════════════════════
-- gold_players.sql — DDL for the player layer
-- ═══════════════════════════════════════════════════════════════════════════
--
-- Spec: md/GOLD_LAYER.md §4.2 (tables), §4.6 (metric columns, defined once at
--       match grain), §4.6.14 (normalisation), §4.6.15 (rollup rules).
--
-- Creates:
--   gold.player_match_stats        one row per (match, player) who PLAYED
--   gold.player_season_stats       one row per (competition_season, player, team)
--   gold.player_season_percentiles long format, one row per metric
--   gold.percentile_metrics        seeded: which metric is ranked for which
--                                  position group, and which way is "good"
--   gold.player_season_totals      view — the transfer-safe season rollup
--
-- gold.formation_slot_positions is seeded separately in
-- sql/seed/formation_slot_positions.sql and must exist before the build runs.
--
-- As on the team tables, every ratio marked ‡ in §4.6 and every per-90 is a
-- STORED GENERATED column over the summed inputs on the same row. §4.6.15's
-- rule -- "per-90 is SUM(metric) / (SUM(minutes) / 90), never the average of
-- per-match rates" -- is therefore enforced by the schema rather than trusted
-- to the build. The padj_* columns are the deliberate exception: §4.6.14
-- requires them adjusted per match and then summed, so they are generated at
-- match grain and plain summed columns at season grain.
--
-- Idempotent: safe to run repeatedly. Drops nothing.
-- ═══════════════════════════════════════════════════════════════════════════

CREATE SCHEMA IF NOT EXISTS gold;


-- ───────────────────────────────────────────────────────────────────────────
-- gold.player_match_stats — §4.2.2
--
-- Rows exist only for players who actually played (minute_in IS NOT NULL).
-- Unused substitutes get no row: they have no minutes, no slot and no actions,
-- and including them makes every COUNT(*) wrong. Measured, 458 matches:
-- 14,394 lineup rows played and have a formation slot; 6,025 never played and
-- have none. There is no third case.
-- ───────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS gold.player_match_stats (
    -- ── grain ──────────────────────────────────────────────────────────────
    match_id              INT NOT NULL REFERENCES silver.matches(match_id),
    player_id             INT NOT NULL REFERENCES silver.players(player_id),

    -- ── context ────────────────────────────────────────────────────────────
    team_id               INT      NOT NULL REFERENCES silver.teams(team_id),
    opponent_team_id      INT      NOT NULL REFERENCES silver.teams(team_id),
    competition_season_id INT      NOT NULL
                              REFERENCES silver.competition_seasons(competition_season_id),
    matchday              SMALLINT NOT NULL,
    match_date            DATE     NOT NULL,
    is_home               BOOLEAN  NOT NULL,
    result                CHAR(1)  NOT NULL CHECK (result IN ('W','D','L')),

    -- ── role in this match (§4.6.0) ────────────────────────────────────────
    -- The slot is the STARTING assignment. team_formation comes from the
    -- opening Team set up event only, and the 1,529 Formation change events
    -- are not reflected in it; Opta's typeId 35 (Player changed position) is
    -- absent from this dataset entirely. A side that starts 4-4-2 and shifts
    -- to 3-5-2 keeps its opening labels for the whole match.
    position              VARCHAR(3)  NOT NULL
        CHECK (position IN ('GK','CB','RB','LB','DM','CM','AM','RW','LW','ST')),
    position_group        VARCHAR(3)  NOT NULL
        CHECK (position_group IN ('GK','CB','RB','LB','MID','W','ST')),
    formation_position    SMALLINT    NOT NULL,
    team_formation        VARCHAR(15),
    started               BOOLEAN     NOT NULL,
    subbed_on             BOOLEAN     NOT NULL,
    subbed_off            BOOLEAN     NOT NULL,
    played_full_match     BOOLEAN     NOT NULL,
    minutes_played        SMALLINT    NOT NULL,
    is_captain            BOOLEAN     NOT NULL,

    -- ═══ §4.6.4 Scoring & finishing ════════════════════════════════════════
    goals                 SMALLINT NOT NULL DEFAULT 0,
    -- Stored so xg_overperformance can be computed the honest way. xg is
    -- missing on every penalty goal, so raw `goals - xg` systematically
    -- OVERSTATES overperformance for penalty takers -- the players the metric
    -- is most often used to judge (§4.6.4).
    goals_non_penalty     SMALLINT NOT NULL DEFAULT 0,
    shots                 SMALLINT NOT NULL DEFAULT 0,
    shots_on_target       SMALLINT NOT NULL DEFAULT 0,
    shots_in_box          SMALLINT NOT NULL DEFAULT 0,
    shots_outside_box     SMALLINT NOT NULL DEFAULT 0,
    shots_first_time      SMALLINT NOT NULL DEFAULT 0,
    headed_goals          SMALLINT NOT NULL DEFAULT 0,
    xg                    REAL     NOT NULL DEFAULT 0,
    npxg                  REAL     NOT NULL DEFAULT 0,
    xg_overperformance    REAL GENERATED ALWAYS AS
                              (goals_non_penalty::real - npxg) STORED,
    big_chances           SMALLINT NOT NULL DEFAULT 0,
    big_chances_scored    SMALLINT NOT NULL DEFAULT 0,
    big_chances_missed    SMALLINT NOT NULL DEFAULT 0,
    penalties_taken       SMALLINT NOT NULL DEFAULT 0,
    penalties_scored      SMALLINT NOT NULL DEFAULT 0,
    xg_per_shot           REAL GENERATED ALWAYS AS (xg / NULLIF(shots, 0)) STORED,
    shot_conversion_rate  REAL GENERATED ALWAYS AS
                              (goals::real / NULLIF(shots, 0)) STORED,
    shot_accuracy         REAL GENERATED ALWAYS AS
                              (shots_on_target::real / NULLIF(shots, 0)) STORED,

    -- ═══ §4.6.5 Chance creation ════════════════════════════════════════════
    assists                  SMALLINT NOT NULL DEFAULT 0,
    key_passes               SMALLINT NOT NULL DEFAULT 0,
    chances_created          SMALLINT GENERATED ALWAYS AS (assists + key_passes) STORED,
    xa                       REAL     NOT NULL DEFAULT 0,
    xa_overperformance       REAL GENERATED ALWAYS AS (assists::real - xa) STORED,
    second_assists           SMALLINT NOT NULL DEFAULT 0,
    crosses                  SMALLINT NOT NULL DEFAULT 0,
    crosses_completed        SMALLINT NOT NULL DEFAULT 0,
    crosses_from_open_play   SMALLINT NOT NULL DEFAULT 0,
    cross_completion_rate    REAL GENERATED ALWAYS AS
                                 (crosses_completed::real / NULLIF(crosses, 0)) STORED,
    through_balls            SMALLINT NOT NULL DEFAULT 0,
    passes_into_penalty_area SMALLINT NOT NULL DEFAULT 0,
    shot_creating_actions    SMALLINT NOT NULL DEFAULT 0,

    -- ═══ §4.6.6 Passing & progression ══════════════════════════════════════
    passes                       SMALLINT NOT NULL DEFAULT 0,
    passes_completed             SMALLINT NOT NULL DEFAULT 0,
    pass_completion_rate         REAL GENERATED ALWAYS AS
                                     (passes_completed::real / NULLIF(passes, 0)) STORED,
    passes_received              SMALLINT NOT NULL DEFAULT 0,
    passes_short                 SMALLINT NOT NULL DEFAULT 0,
    passes_medium                SMALLINT NOT NULL DEFAULT 0,
    passes_long                  SMALLINT NOT NULL DEFAULT 0,
    passes_forward               SMALLINT NOT NULL DEFAULT 0,
    passes_backward              SMALLINT NOT NULL DEFAULT 0,
    progressive_passes           SMALLINT NOT NULL DEFAULT 0,
    progressive_passes_completed SMALLINT NOT NULL DEFAULT 0,
    progressive_pass_rate        REAL GENERATED ALWAYS AS
                                     (progressive_passes::real / NULLIF(passes, 0)) STORED,
    passes_into_final_third      SMALLINT NOT NULL DEFAULT 0,
    long_balls                   SMALLINT NOT NULL DEFAULT 0,
    long_balls_completed         SMALLINT NOT NULL DEFAULT 0,
    long_ball_accuracy           REAL GENERATED ALWAYS AS
                                     (long_balls_completed::real / NULLIF(long_balls, 0)) STORED,
    switches                     SMALLINT NOT NULL DEFAULT 0,
    offside_passes               SMALLINT NOT NULL DEFAULT 0,
    xt_pass                      REAL NOT NULL DEFAULT 0,

    -- ═══ §4.6.7 Carrying, dribbling & retention ════════════════════════════
    carries                   SMALLINT NOT NULL DEFAULT 0,
    carry_distance_total      REAL     NOT NULL DEFAULT 0,
    progressive_carries       SMALLINT NOT NULL DEFAULT 0,
    carries_into_final_third  SMALLINT NOT NULL DEFAULT 0,
    carries_into_penalty_area SMALLINT NOT NULL DEFAULT 0,
    take_ons                  SMALLINT NOT NULL DEFAULT 0,
    take_ons_won              SMALLINT NOT NULL DEFAULT 0,
    take_on_success_rate      REAL GENERATED ALWAYS AS
                                  (take_ons_won::real / NULLIF(take_ons, 0)) STORED,
    dispossessed              SMALLINT NOT NULL DEFAULT 0,
    touches                   SMALLINT NOT NULL DEFAULT 0,
    touches_in_box            SMALLINT NOT NULL DEFAULT 0,
    touches_final_third       SMALLINT NOT NULL DEFAULT 0,
    xt_carry                  REAL     NOT NULL DEFAULT 0,

    -- ═══ §4.6.8 Defending ══════════════════════════════════════════════════
    -- Three semantics confirmed by counting rows, not assumed:
    --   Challenge (6,998) means THIS PLAYER WAS DRIBBLED PAST -- it pairs 1:1
    --     with the 6,998 successful Take Ons. Cleanest dribbled_past there is.
    --   Foul records both sides: 'failure' committed, 'success' won.
    --   Aerial records both contestants, so won + lost is the true duel count.
    -- Attempted Tackle (typeId 83, 12,403 rows, all failures) is a SEPARATE
    -- event from Tackle. §4.5.7's team-grain tackle_success_rate ignores it and
    -- so flatters every defender; at player grain that is not tolerable, so it
    -- is in the denominator here.
    tackles                 SMALLINT NOT NULL DEFAULT 0,
    tackles_won             SMALLINT NOT NULL DEFAULT 0,
    tackles_missed          SMALLINT NOT NULL DEFAULT 0,
    tackle_success_rate     REAL GENERATED ALWAYS AS
        (tackles_won::real / NULLIF(tackles + tackles_missed, 0)) STORED,
    dribbled_past           SMALLINT NOT NULL DEFAULT 0,
    interceptions           SMALLINT NOT NULL DEFAULT 0,
    clearances              SMALLINT NOT NULL DEFAULT 0,
    blocked_passes          SMALLINT NOT NULL DEFAULT 0,
    ball_recoveries         SMALLINT NOT NULL DEFAULT 0,
    defensive_actions       SMALLINT NOT NULL DEFAULT 0,
    def_actions_final_third SMALLINT NOT NULL DEFAULT 0,
    def_action_avg_x        REAL,
    fouls_committed         SMALLINT NOT NULL DEFAULT 0,
    fouls_won               SMALLINT NOT NULL DEFAULT 0,
    errors                  SMALLINT NOT NULL DEFAULT 0,
    offsides_provoked       SMALLINT NOT NULL DEFAULT 0,
    caught_offside          SMALLINT NOT NULL DEFAULT 0,
    shield_ball_opp         SMALLINT NOT NULL DEFAULT 0,

    -- ═══ §4.6.9 Aerial duels ═══════════════════════════════════════════════
    aerials_won           SMALLINT NOT NULL DEFAULT 0,
    aerials_lost          SMALLINT NOT NULL DEFAULT 0,
    aerial_win_rate       REAL GENERATED ALWAYS AS
        (aerials_won::real / NULLIF(aerials_won + aerials_lost, 0)) STORED,
    aerials_won_att_third SMALLINT NOT NULL DEFAULT 0,
    aerials_won_def_third SMALLINT NOT NULL DEFAULT 0,

    -- ═══ §4.6.10 Goalkeeping — NULL for outfield players ═══════════════════
    -- The one place in §4.6 the table is deliberately sparse.
    -- goals_conceded uses the ON-PITCH WINDOW, not the match score: a keeper
    -- subbed at 60' owns only what went in before then.
    saves                   SMALLINT,
    goals_conceded          SMALLINT,
    clean_sheet             BOOLEAN GENERATED ALWAYS AS
                                (played_full_match AND goals_conceded = 0) STORED,
    save_pct                REAL GENERATED ALWAYS AS
                                (saves::real / NULLIF(saves + goals_conceded, 0)) STORED,
    xg_faced                REAL,
    goals_prevented         REAL GENERATED ALWAYS AS
                                (xg_faced - goals_conceded::real) STORED,
    penalties_faced         SMALLINT,
    penalties_saved         SMALLINT,
    claims                  SMALLINT,
    claims_successful       SMALLINT,
    punches                 SMALLINT,
    crosses_not_claimed     SMALLINT,
    smothers                SMALLINT,
    keeper_sweeper_actions  SMALLINT,
    sweeper_avg_x           REAL,
    keeper_pickups          SMALLINT,
    gk_passes               SMALLINT,
    gk_passes_completed     SMALLINT,
    gk_long_balls           SMALLINT,
    gk_pass_completion_rate REAL GENERATED ALWAYS AS
                                (gk_passes_completed::real / NULLIF(gk_passes, 0)) STORED,
    gk_long_ball_pct        REAL GENERATED ALWAYS AS
                                (gk_long_balls::real / NULLIF(gk_passes, 0)) STORED,

    -- ═══ §4.6.11 Model values ══════════════════════════════════════════════
    -- vaep_actions is COUNT(vaep_value), NOT COUNT(*): only SPADL-valid
    -- actions carry a value and the first action of each match is NULL. Using
    -- COUNT(*) divides by ball touches and set-up events that carry no value,
    -- deflating centre backs and keepers hardest.
    xt              REAL NOT NULL DEFAULT 0,
    vaep            REAL NOT NULL DEFAULT 0,
    vaep_offensive  REAL NOT NULL DEFAULT 0,
    vaep_defensive  REAL NOT NULL DEFAULT 0,
    vaep_actions    SMALLINT NOT NULL DEFAULT 0,
    vaep_per_action REAL GENERATED ALWAYS AS (vaep / NULLIF(vaep_actions, 0)) STORED,

    -- ═══ §4.6.12 Discipline ════════════════════════════════════════════════
    yellow_cards     SMALLINT NOT NULL DEFAULT 0,
    second_yellows   SMALLINT NOT NULL DEFAULT 0,
    red_cards        SMALLINT NOT NULL DEFAULT 0,
    fouls_per_tackle REAL GENERATED ALWAYS AS
        (fouls_committed::real / NULLIF(tackles + tackles_missed, 0)) STORED,

    -- ═══ §4.6.13 Set pieces ════════════════════════════════════════════════
    corners_taken          SMALLINT NOT NULL DEFAULT 0,
    free_kicks_taken       SMALLINT NOT NULL DEFAULT 0,
    direct_free_kick_shots SMALLINT NOT NULL DEFAULT 0,
    throw_ins_taken        SMALLINT NOT NULL DEFAULT 0,
    set_piece_goals        SMALLINT NOT NULL DEFAULT 0,
    set_piece_xg           REAL     NOT NULL DEFAULT 0,

    -- ═══ §4.6.14 Possession adjustment ═════════════════════════════════════
    -- Adjusted HERE, at match grain, and summed at season grain -- never
    -- adjusted once using a season possession figure. A player whose team
    -- dominated eight matches and was overrun in two gets a materially
    -- different number, and the match-grain version is the correct one.
    -- The input is the OPPONENT's possession share from gold.team_match_stats.
    opponent_possession_pct REAL,
    padj_tackles            REAL GENERATED ALWAYS AS
        (tackles * 0.50 / NULLIF(opponent_possession_pct, 0)) STORED,
    padj_interceptions      REAL GENERATED ALWAYS AS
        (interceptions * 0.50 / NULLIF(opponent_possession_pct, 0)) STORED,
    padj_clearances         REAL GENERATED ALWAYS AS
        (clearances * 0.50 / NULLIF(opponent_possession_pct, 0)) STORED,
    padj_ball_recoveries    REAL GENERATED ALWAYS AS
        (ball_recoveries * 0.50 / NULLIF(opponent_possession_pct, 0)) STORED,
    padj_blocked_passes     REAL GENERATED ALWAYS AS
        (blocked_passes * 0.50 / NULLIF(opponent_possession_pct, 0)) STORED,
    padj_defensive_actions  REAL GENERATED ALWAYS AS
        (defensive_actions * 0.50 / NULLIF(opponent_possession_pct, 0)) STORED,

    computed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (match_id, player_id)
);

CREATE INDEX IF NOT EXISTS player_match_stats_season_player_date_idx
    ON gold.player_match_stats (competition_season_id, player_id, match_date);
CREATE INDEX IF NOT EXISTS player_match_stats_season_posgroup_idx
    ON gold.player_match_stats (competition_season_id, position_group);
CREATE INDEX IF NOT EXISTS player_match_stats_match_team_idx
    ON gold.player_match_stats (match_id, team_id);


-- ───────────────────────────────────────────────────────────────────────────
-- gold.player_season_stats — §4.2.3
--
-- GRAIN IS THREE COLUMNS: (competition_season_id, player_id, team_id).
-- The two-column grain is a silent data-loss bug -- a player who scores 8 for
-- Girona and moves to Sevilla in January produces one row attributing all 8 to
-- Sevilla, and the transfer is exactly the case an analyst wants to look at.
-- Use gold.player_season_totals (below) for the player-season rollup.
-- ───────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS gold.player_season_stats (
    competition_season_id INT NOT NULL
                              REFERENCES silver.competition_seasons(competition_season_id),
    player_id             INT NOT NULL REFERENCES silver.players(player_id),
    team_id               INT NOT NULL REFERENCES silver.teams(team_id),

    -- ── denormalised context — DISPLAY ONLY, never join or filter on these ─
    player_name           VARCHAR(150) NOT NULL,
    team_name             VARCHAR(100) NOT NULL,
    team_abbreviation     VARCHAR(5),
    competition_code      VARCHAR(10)  NOT NULL,
    tier_level            SMALLINT     NOT NULL,
    season_label          VARCHAR(10)  NOT NULL,
    nationality           VARCHAR(60),
    date_of_birth         DATE,
    age_at_season_start   SMALLINT,
    preferred_foot        VARCHAR(5),
    shirt_number          SMALLINT,

    -- ── position profile (§4.6.2) — weighted by MINUTES, not match count ───
    primary_position       VARCHAR(3),
    primary_position_group VARCHAR(3),
    primary_position_pct   REAL,
    minutes_by_position    JSONB,
    positions_played       SMALLINT,
    -- A WARNING FLAG for the reader of a percentile, not a compliment: a
    -- player at 55 pct CB / 45 pct DM is ranked against centre backs on
    -- metrics half their minutes did not come from.
    is_utility             BOOLEAN GENERATED ALWAYS AS
                               (primary_position_pct < 0.60) STORED,

    -- ── coverage (§4.2.3) ──────────────────────────────────────────────────
    matches_played        SMALLINT NOT NULL DEFAULT 0,
    matches_started       SMALLINT NOT NULL DEFAULT 0,
    matches_sub_on        SMALLINT NOT NULL DEFAULT 0,
    matches_full_90       SMALLINT NOT NULL DEFAULT 0,
    matches_as_captain    SMALLINT NOT NULL DEFAULT 0,
    minutes_played        SMALLINT NOT NULL DEFAULT 0,
    -- From gold.team_match_stats, so player and team coverage cannot disagree.
    team_matches          SMALLINT NOT NULL DEFAULT 0,
    -- Denominator is the CLUB's matches, not the league's, so it stays correct
    -- for a January signing and for a team with games in hand.
    minutes_share         REAL GENERATED ALWAYS AS
                              (minutes_played::real / NULLIF(team_matches * 90, 0)) STORED,
    first_match_date      DATE,
    through_match_date    DATE,
    -- 450 minutes = 5 full matches. Stored rather than applied as a WHERE so
    -- the row still exists and the totals stay queryable -- the player is
    -- excluded from RANKING, not from the table.
    meets_min_minutes     BOOLEAN GENERATED ALWAYS AS (minutes_played >= 450) STORED,

    -- ═══ §4.6.4 Scoring & finishing ════════════════════════════════════════
    goals                 SMALLINT NOT NULL DEFAULT 0,
    goals_non_penalty     SMALLINT NOT NULL DEFAULT 0,
    shots                 SMALLINT NOT NULL DEFAULT 0,
    shots_on_target       SMALLINT NOT NULL DEFAULT 0,
    shots_in_box          SMALLINT NOT NULL DEFAULT 0,
    shots_outside_box     SMALLINT NOT NULL DEFAULT 0,
    shots_first_time      SMALLINT NOT NULL DEFAULT 0,
    headed_goals          SMALLINT NOT NULL DEFAULT 0,
    xg                    REAL     NOT NULL DEFAULT 0,
    npxg                  REAL     NOT NULL DEFAULT 0,
    xg_overperformance    REAL GENERATED ALWAYS AS
                              (goals_non_penalty::real - npxg) STORED,
    big_chances           SMALLINT NOT NULL DEFAULT 0,
    big_chances_scored    SMALLINT NOT NULL DEFAULT 0,
    big_chances_missed    SMALLINT NOT NULL DEFAULT 0,
    penalties_taken       SMALLINT NOT NULL DEFAULT 0,
    penalties_scored      SMALLINT NOT NULL DEFAULT 0,
    xg_per_shot           REAL GENERATED ALWAYS AS (xg / NULLIF(shots, 0)) STORED,
    shot_conversion_rate  REAL GENERATED ALWAYS AS
                              (goals::real / NULLIF(shots, 0)) STORED,
    shot_accuracy         REAL GENERATED ALWAYS AS
                              (shots_on_target::real / NULLIF(shots, 0)) STORED,

    -- ═══ §4.6.5 Chance creation ════════════════════════════════════════════
    assists                  SMALLINT NOT NULL DEFAULT 0,
    key_passes               SMALLINT NOT NULL DEFAULT 0,
    chances_created          SMALLINT GENERATED ALWAYS AS (assists + key_passes) STORED,
    xa                       REAL     NOT NULL DEFAULT 0,
    xa_overperformance       REAL GENERATED ALWAYS AS (assists::real - xa) STORED,
    second_assists           SMALLINT NOT NULL DEFAULT 0,
    crosses                  SMALLINT NOT NULL DEFAULT 0,
    crosses_completed        SMALLINT NOT NULL DEFAULT 0,
    crosses_from_open_play   SMALLINT NOT NULL DEFAULT 0,
    cross_completion_rate    REAL GENERATED ALWAYS AS
                                 (crosses_completed::real / NULLIF(crosses, 0)) STORED,
    through_balls            SMALLINT NOT NULL DEFAULT 0,
    passes_into_penalty_area SMALLINT NOT NULL DEFAULT 0,
    shot_creating_actions    SMALLINT NOT NULL DEFAULT 0,

    -- ═══ §4.6.6 Passing & progression ══════════════════════════════════════
    passes                       INT      NOT NULL DEFAULT 0,
    passes_completed             INT      NOT NULL DEFAULT 0,
    pass_completion_rate         REAL GENERATED ALWAYS AS
                                     (passes_completed::real / NULLIF(passes, 0)) STORED,
    passes_received              INT      NOT NULL DEFAULT 0,
    passes_short                 INT      NOT NULL DEFAULT 0,
    passes_medium                INT      NOT NULL DEFAULT 0,
    passes_long                  INT      NOT NULL DEFAULT 0,
    passes_forward               INT      NOT NULL DEFAULT 0,
    passes_backward              INT      NOT NULL DEFAULT 0,
    progressive_passes           INT      NOT NULL DEFAULT 0,
    progressive_passes_completed INT      NOT NULL DEFAULT 0,
    progressive_pass_rate        REAL GENERATED ALWAYS AS
                                     (progressive_passes::real / NULLIF(passes, 0)) STORED,
    passes_into_final_third      INT      NOT NULL DEFAULT 0,
    long_balls                   SMALLINT NOT NULL DEFAULT 0,
    long_balls_completed         SMALLINT NOT NULL DEFAULT 0,
    long_ball_accuracy           REAL GENERATED ALWAYS AS
                                     (long_balls_completed::real / NULLIF(long_balls, 0)) STORED,
    switches                     SMALLINT NOT NULL DEFAULT 0,
    offside_passes               SMALLINT NOT NULL DEFAULT 0,
    xt_pass                      REAL     NOT NULL DEFAULT 0,

    -- ═══ §4.6.7 Carrying, dribbling & retention ════════════════════════════
    carries                   INT      NOT NULL DEFAULT 0,
    carry_distance_total      REAL     NOT NULL DEFAULT 0,
    progressive_carries       INT      NOT NULL DEFAULT 0,
    carries_into_final_third  INT      NOT NULL DEFAULT 0,
    carries_into_penalty_area SMALLINT NOT NULL DEFAULT 0,
    take_ons                  SMALLINT NOT NULL DEFAULT 0,
    take_ons_won              SMALLINT NOT NULL DEFAULT 0,
    take_on_success_rate      REAL GENERATED ALWAYS AS
                                  (take_ons_won::real / NULLIF(take_ons, 0)) STORED,
    dispossessed              SMALLINT NOT NULL DEFAULT 0,
    touches                   INT      NOT NULL DEFAULT 0,
    touches_in_box            SMALLINT NOT NULL DEFAULT 0,
    touches_final_third       INT      NOT NULL DEFAULT 0,
    xt_carry                  REAL     NOT NULL DEFAULT 0,

    -- ═══ §4.6.8 Defending ══════════════════════════════════════════════════
    tackles                 SMALLINT NOT NULL DEFAULT 0,
    tackles_won             SMALLINT NOT NULL DEFAULT 0,
    tackles_missed          SMALLINT NOT NULL DEFAULT 0,
    tackle_success_rate     REAL GENERATED ALWAYS AS
        (tackles_won::real / NULLIF(tackles + tackles_missed, 0)) STORED,
    dribbled_past           SMALLINT NOT NULL DEFAULT 0,
    interceptions           SMALLINT NOT NULL DEFAULT 0,
    clearances              SMALLINT NOT NULL DEFAULT 0,
    blocked_passes          SMALLINT NOT NULL DEFAULT 0,
    ball_recoveries         SMALLINT NOT NULL DEFAULT 0,
    defensive_actions       INT      NOT NULL DEFAULT 0,
    def_actions_final_third SMALLINT NOT NULL DEFAULT 0,
    def_action_avg_x        REAL,
    fouls_committed         SMALLINT NOT NULL DEFAULT 0,
    fouls_won               SMALLINT NOT NULL DEFAULT 0,
    errors                  SMALLINT NOT NULL DEFAULT 0,
    offsides_provoked       SMALLINT NOT NULL DEFAULT 0,
    caught_offside          SMALLINT NOT NULL DEFAULT 0,
    shield_ball_opp         SMALLINT NOT NULL DEFAULT 0,

    -- ═══ §4.6.9 Aerial duels ═══════════════════════════════════════════════
    aerials_won           SMALLINT NOT NULL DEFAULT 0,
    aerials_lost          SMALLINT NOT NULL DEFAULT 0,
    aerial_win_rate       REAL GENERATED ALWAYS AS
        (aerials_won::real / NULLIF(aerials_won + aerials_lost, 0)) STORED,
    aerials_won_att_third SMALLINT NOT NULL DEFAULT 0,
    aerials_won_def_third SMALLINT NOT NULL DEFAULT 0,

    -- ═══ §4.6.10 Goalkeeping — NULL for outfield players ═══════════════════
    saves                   SMALLINT,
    goals_conceded          SMALLINT,
    clean_sheets            SMALLINT,
    save_pct                REAL GENERATED ALWAYS AS
                                (saves::real / NULLIF(saves + goals_conceded, 0)) STORED,
    xg_faced                REAL,
    goals_prevented         REAL GENERATED ALWAYS AS
                                (xg_faced - goals_conceded::real) STORED,
    penalties_faced         SMALLINT,
    penalties_saved         SMALLINT,
    claims                  SMALLINT,
    claims_successful       SMALLINT,
    punches                 SMALLINT,
    crosses_not_claimed     SMALLINT,
    smothers                SMALLINT,
    keeper_sweeper_actions  SMALLINT,
    sweeper_avg_x           REAL,
    keeper_pickups          SMALLINT,
    gk_passes               SMALLINT,
    gk_passes_completed     SMALLINT,
    gk_long_balls           SMALLINT,
    gk_pass_completion_rate REAL GENERATED ALWAYS AS
                                (gk_passes_completed::real / NULLIF(gk_passes, 0)) STORED,
    gk_long_ball_pct        REAL GENERATED ALWAYS AS
                                (gk_long_balls::real / NULLIF(gk_passes, 0)) STORED,

    -- ═══ §4.6.11 Model values ══════════════════════════════════════════════
    xt              REAL NOT NULL DEFAULT 0,
    vaep            REAL NOT NULL DEFAULT 0,
    vaep_offensive  REAL NOT NULL DEFAULT 0,
    vaep_defensive  REAL NOT NULL DEFAULT 0,
    vaep_actions    INT  NOT NULL DEFAULT 0,
    vaep_per_action REAL GENERATED ALWAYS AS (vaep / NULLIF(vaep_actions, 0)) STORED,
    -- Rank by vaep_per_90 within the team-season; written by a second pass.
    vaep_rank_in_team SMALLINT,

    -- ═══ §4.6.12 Discipline ════════════════════════════════════════════════
    yellow_cards     SMALLINT NOT NULL DEFAULT 0,
    second_yellows   SMALLINT NOT NULL DEFAULT 0,
    red_cards        SMALLINT NOT NULL DEFAULT 0,
    fouls_per_tackle REAL GENERATED ALWAYS AS
        (fouls_committed::real / NULLIF(tackles + tackles_missed, 0)) STORED,

    -- ═══ §4.6.13 Set pieces ════════════════════════════════════════════════
    corners_taken          SMALLINT NOT NULL DEFAULT 0,
    free_kicks_taken       SMALLINT NOT NULL DEFAULT 0,
    direct_free_kick_shots SMALLINT NOT NULL DEFAULT 0,
    throw_ins_taken        SMALLINT NOT NULL DEFAULT 0,
    set_piece_goals        SMALLINT NOT NULL DEFAULT 0,
    set_piece_xg           REAL     NOT NULL DEFAULT 0,

    -- ═══ §4.6.14 padj — SUMMED from match grain, never re-adjusted here ════
    padj_tackles           REAL NOT NULL DEFAULT 0,
    padj_interceptions     REAL NOT NULL DEFAULT 0,
    padj_clearances        REAL NOT NULL DEFAULT 0,
    padj_ball_recoveries   REAL NOT NULL DEFAULT 0,
    padj_blocked_passes    REAL NOT NULL DEFAULT 0,
    padj_defensive_actions REAL NOT NULL DEFAULT 0,

    -- ═══ §4.6.16 season-only ═══════════════════════════════════════════════
    goal_contributions SMALLINT GENERATED ALWAYS AS (goals + assists) STORED,
    -- team_goal_share needs the club's season goals; stored as the input.
    team_season_goals  SMALLINT,
    team_goal_share    REAL GENERATED ALWAYS AS
                           (goals::real / NULLIF(team_season_goals, 0)) STORED,

    -- ═══ §4.6.14 per-90 — SUM(metric) / (SUM(minutes)/90), never averaged ══
    -- Generated, so the per-match-average form is not expressible. Only the
    -- volume metrics that are genuinely compared across players get one;
    -- ratios do not, because a rate is already normalised.
    goals_per_90                REAL GENERATED ALWAYS AS
        (goals * 90.0 / NULLIF(minutes_played, 0)) STORED,
    npxg_per_90                 REAL GENERATED ALWAYS AS
        (npxg * 90.0 / NULLIF(minutes_played, 0)) STORED,
    xa_per_90                   REAL GENERATED ALWAYS AS
        (xa * 90.0 / NULLIF(minutes_played, 0)) STORED,
    key_passes_per_90           REAL GENERATED ALWAYS AS
        (key_passes * 90.0 / NULLIF(minutes_played, 0)) STORED,
    shots_per_90                REAL GENERATED ALWAYS AS
        (shots * 90.0 / NULLIF(minutes_played, 0)) STORED,
    progressive_passes_per_90   REAL GENERATED ALWAYS AS
        (progressive_passes * 90.0 / NULLIF(minutes_played, 0)) STORED,
    progressive_carries_per_90  REAL GENERATED ALWAYS AS
        (progressive_carries * 90.0 / NULLIF(minutes_played, 0)) STORED,
    tackles_per_90              REAL GENERATED ALWAYS AS
        (tackles * 90.0 / NULLIF(minutes_played, 0)) STORED,
    interceptions_per_90        REAL GENERATED ALWAYS AS
        (interceptions * 90.0 / NULLIF(minutes_played, 0)) STORED,
    ball_recoveries_per_90      REAL GENERATED ALWAYS AS
        (ball_recoveries * 90.0 / NULLIF(minutes_played, 0)) STORED,
    clearances_per_90           REAL GENERATED ALWAYS AS
        (clearances * 90.0 / NULLIF(minutes_played, 0)) STORED,
    aerials_won_per_90          REAL GENERATED ALWAYS AS
        (aerials_won * 90.0 / NULLIF(minutes_played, 0)) STORED,
    xt_per_90                   REAL GENERATED ALWAYS AS
        (xt * 90.0 / NULLIF(minutes_played, 0)) STORED,
    vaep_per_90                 REAL GENERATED ALWAYS AS
        (vaep * 90.0 / NULLIF(minutes_played, 0)) STORED,
    goal_contributions_per_90   REAL GENERATED ALWAYS AS
        ((goals + assists) * 90.0 / NULLIF(minutes_played, 0)) STORED,
    padj_tackles_per_90         REAL GENERATED ALWAYS AS
        (padj_tackles * 90.0 / NULLIF(minutes_played, 0)) STORED,
    padj_interceptions_per_90   REAL GENERATED ALWAYS AS
        (padj_interceptions * 90.0 / NULLIF(minutes_played, 0)) STORED,
    padj_ball_recoveries_per_90 REAL GENERATED ALWAYS AS
        (padj_ball_recoveries * 90.0 / NULLIF(minutes_played, 0)) STORED,
    padj_clearances_per_90      REAL GENERATED ALWAYS AS
        (padj_clearances * 90.0 / NULLIF(minutes_played, 0)) STORED,
    padj_defensive_actions_per_90 REAL GENERATED ALWAYS AS
        (padj_defensive_actions * 90.0 / NULLIF(minutes_played, 0)) STORED,
    dispossessed_per_90         REAL GENERATED ALWAYS AS
        (dispossessed * 90.0 / NULLIF(minutes_played, 0)) STORED,
    dribbled_past_per_90        REAL GENERATED ALWAYS AS
        (dribbled_past * 90.0 / NULLIF(minutes_played, 0)) STORED,
    fouls_committed_per_90      REAL GENERATED ALWAYS AS
        (fouls_committed * 90.0 / NULLIF(minutes_played, 0)) STORED,
    errors_per_90               REAL GENERATED ALWAYS AS
        (errors * 90.0 / NULLIF(minutes_played, 0)) STORED,
    touches_per_90              REAL GENERATED ALWAYS AS
        (touches * 90.0 / NULLIF(minutes_played, 0)) STORED,

    computed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (competition_season_id, player_id, team_id)
);

CREATE INDEX IF NOT EXISTS player_season_stats_season_posgroup_idx
    ON gold.player_season_stats (competition_season_id, primary_position_group);
CREATE INDEX IF NOT EXISTS player_season_stats_player_idx
    ON gold.player_season_stats (player_id);
CREATE INDEX IF NOT EXISTS player_season_stats_season_team_idx
    ON gold.player_season_stats (competition_season_id, team_id);


-- ───────────────────────────────────────────────────────────────────────────
-- gold.player_season_totals — §4.2.1
--
-- The player-season rollup across clubs. Exposed as a VIEW rather than as
-- extra rows so SELECT * FROM gold.player_season_stats can never double-count
-- a transferred player.
-- ───────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW gold.player_season_totals AS
SELECT player_id,
       competition_season_id,
       max(player_name)              AS player_name,
       count(DISTINCT team_id)::smallint AS clubs,
       sum(matches_played)::smallint AS matches_played,
       sum(minutes_played)::int      AS minutes_played,
       sum(goals)::smallint          AS goals,
       sum(assists)::smallint        AS assists,
       sum(xg)::real                 AS xg,
       sum(npxg)::real               AS npxg,
       sum(xa)::real                 AS xa,
       sum(shots)::smallint          AS shots,
       sum(xt)::real                 AS xt,
       sum(vaep)::real               AS vaep,
       -- ratios recomputed from summed inputs, never AVG'd (§4.5.13)
       (sum(goals)::real / NULLIF(sum(shots), 0))          AS shot_conversion_rate,
       (sum(goals) * 90.0 / NULLIF(sum(minutes_played), 0)) AS goals_per_90,
       (sum(vaep) * 90.0 / NULLIF(sum(minutes_played), 0))  AS vaep_per_90
FROM gold.player_season_stats
GROUP BY player_id, competition_season_id;


-- ───────────────────────────────────────────────────────────────────────────
-- gold.percentile_metrics — seeded reference driving §4.6.14 step 3
--
-- Encodes the **Positions** column of §4.6: which metrics are ranked for which
-- position group, and which direction is good. Adding a metric to the
-- percentile set is then a one-row INSERT, not a rewrite of the build SQL.
--
-- Two rules from §4.6.14 are enforced by what is listed here:
--   * only per-90 or padj forms appear -- percentiling raw volume produces a
--     minutes ranking wearing a skill ranking's clothes;
--   * `higher_is_better = FALSE` for the metrics where it is not (dispossessed,
--     errors, dribbled_past, fouls_committed, caught_offside, goals_conceded).
-- ───────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS gold.percentile_metrics (
    metric           VARCHAR(60) NOT NULL,
    position_group   VARCHAR(3)  NOT NULL
        CHECK (position_group IN ('GK','CB','RB','LB','MID','W','ST')),
    higher_is_better BOOLEAN     NOT NULL DEFAULT TRUE,
    PRIMARY KEY (metric, position_group)
);

INSERT INTO gold.percentile_metrics (metric, position_group, higher_is_better)
SELECT m.metric, g.grp, m.hib
FROM (VALUES
    -- attacking output
    ('goals_per_90',                 'ST W MID',             TRUE),
    ('npxg_per_90',                  'ST W MID',             TRUE),
    ('shots_per_90',                 'ST W MID',             TRUE),
    ('xg_per_shot',                  'ST W',                 TRUE),
    ('shot_conversion_rate',         'ST W',                 TRUE),
    ('shot_accuracy',                'ST W MID',             TRUE),
    ('goal_contributions_per_90',    'ST W MID',             TRUE),
    -- creation
    ('xa_per_90',                    'W MID RB LB',          TRUE),
    ('key_passes_per_90',            'W MID RB LB ST',       TRUE),
    ('cross_completion_rate',        'RB LB W MID',          TRUE),
    -- passing and progression
    ('pass_completion_rate',         'CB MID GK RB LB W ST', TRUE),
    ('progressive_passes_per_90',    'MID CB RB LB W',       TRUE),
    ('progressive_pass_rate',        'CB MID RB LB',         TRUE),
    ('long_ball_accuracy',           'GK CB',                TRUE),
    ('xt_per_90',                    'MID W RB LB CB',       TRUE),
    -- carrying
    ('progressive_carries_per_90',   'W MID RB LB CB ST',    TRUE),
    ('take_on_success_rate',         'W ST RB LB',           TRUE),
    ('touches_per_90',               'CB MID RB LB',         TRUE),
    -- defending (possession-adjusted — raw volume ranks teams, not players)
    ('padj_tackles_per_90',          'CB RB LB MID W',       TRUE),
    ('padj_interceptions_per_90',    'CB MID RB LB',         TRUE),
    ('padj_ball_recoveries_per_90',  'MID CB RB LB W',       TRUE),
    ('padj_clearances_per_90',       'CB RB LB',             TRUE),
    ('padj_defensive_actions_per_90','CB RB LB MID W ST',    TRUE),
    ('tackle_success_rate',          'CB RB LB MID',         TRUE),
    ('aerial_win_rate',              'CB ST RB LB MID',      TRUE),
    ('aerials_won_per_90',           'CB ST',                TRUE),
    -- model values
    ('vaep_per_90',                  'GK CB RB LB MID W ST', TRUE),
    -- lower is better
    ('dispossessed_per_90',          'W ST MID',             FALSE),
    ('dribbled_past_per_90',         'RB LB CB MID W',       FALSE),
    ('fouls_committed_per_90',       'CB RB LB MID ST',      FALSE),
    ('errors_per_90',                'CB GK RB LB MID',      FALSE),
    ('fouls_per_tackle',             'CB RB LB MID',         FALSE),
    -- goalkeeping
    ('save_pct',                     'GK',                   TRUE),
    ('goals_prevented',              'GK',                   TRUE),
    ('gk_pass_completion_rate',      'GK',                   TRUE),
    ('gk_long_ball_pct',             'GK',                   TRUE)
) AS m(metric, groups, hib)
CROSS JOIN LATERAL unnest(string_to_array(m.groups, ' ')) AS g(grp)
ON CONFLICT (metric, position_group) DO NOTHING;


-- ───────────────────────────────────────────────────────────────────────────
-- gold.player_season_percentiles — §4.6.14 step 3
--
-- Long format, not one column per metric: the metric set will keep growing and
-- a wide table means a migration every time.
-- ───────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS gold.player_season_percentiles (
    competition_season_id INT         NOT NULL
                              REFERENCES silver.competition_seasons(competition_season_id),
    player_id             INT         NOT NULL REFERENCES silver.players(player_id),
    team_id               INT         NOT NULL REFERENCES silver.teams(team_id),
    position_group        VARCHAR(3)  NOT NULL,
    metric                VARCHAR(60) NOT NULL,
    value                 REAL,
    percentile            REAL,      -- 0-100 within (season, position_group)
    -- STORED because a percentile against 14 goalkeepers is a much weaker
    -- statement than one against 90 midfielders, and the reader cannot tell
    -- without it.
    peer_n                SMALLINT,
    -- TRUE when RB and LB were pooled into FB because either peer set was
    -- under 20 (§4.6.14). Recorded rather than done silently.
    pooled_fb             BOOLEAN NOT NULL DEFAULT FALSE,

    PRIMARY KEY (competition_season_id, player_id, team_id, metric)
);

CREATE INDEX IF NOT EXISTS player_season_percentiles_lookup_idx
    ON gold.player_season_percentiles (competition_season_id, position_group, metric);
