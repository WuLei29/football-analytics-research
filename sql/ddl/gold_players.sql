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
-- Amendment (15 Oct 2026): the screen-03 percentile spine (README-PLAYER §4).
--
-- Added as ALTERs because CREATE TABLE IF NOT EXISTS no-ops on an existing
-- database; on a fresh one they run straight after the CREATEs. Measured
-- league-wide values for every new column are in GOLD_LAYER §4.6.17.
--
-- Base counts, at match grain and summed at season grain:
--   shots_blocked          Save + Q94 by the player: an outfield block of a
--                          shot (3,194 rows). Keepers' Saves never carry Q94.
--   headed_shots           shots with spadl_bodypart_id = 1 (headed_goals
--                          counts only the goals).
--   progressive_carries_into_final_third
--                          gold.is_progressive AND x < 70 AND end_x >= 70.
--                          The non-progressive crossings are 7 m nudges over
--                          the line (median); the progressive ones are 23 m.
--   passes_long_completed  completed passes >= 30 m (passes_long's numerator).
--   receptions_final_third / receptions_in_box / progressive_receptions
--                          from gold.pass_receptions by receiver_id: the
--                          reception point is the pass end (rx_x >= 70,
--                          rx_in_box) and is_progressive is the pass's. Needs
--                          pass_receptions built first (build_gold STEP_ORDER).
--
-- Ground duels (Spanish: duelos defensivos en el suelo). Opta records no
-- 50/50; the defender's side of a ground contest is three events, all already
-- counted: Tackle (won -- both outcomes, since a failed Tackle still took the
-- ball off the attacker), Attempted Tackle and Challenge (lost: went in and
-- missed / was dribbled past). They overlap in 255 of 7,658 Challenges, so
-- the sum is a fair duel count.
-- ───────────────────────────────────────────────────────────────────────────
ALTER TABLE gold.player_match_stats
    ADD COLUMN IF NOT EXISTS shots_blocked                        SMALLINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS headed_shots                         SMALLINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS progressive_carries_into_final_third SMALLINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS passes_long_completed                SMALLINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS receptions_final_third               SMALLINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS receptions_in_box                    SMALLINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS progressive_receptions               SMALLINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS ground_duels SMALLINT GENERATED ALWAYS AS
        (tackles + tackles_missed + dribbled_past) STORED,
    ADD COLUMN IF NOT EXISTS ground_duel_win_rate REAL GENERATED ALWAYS AS
        (tackles::real / NULLIF(tackles + tackles_missed + dribbled_past, 0)) STORED;

ALTER TABLE gold.player_season_stats
    ADD COLUMN IF NOT EXISTS matches_subbed_off                   SMALLINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS shots_blocked                        SMALLINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS headed_shots                         SMALLINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS progressive_carries_into_final_third SMALLINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS passes_long_completed                INT      NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS receptions_final_third               INT      NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS receptions_in_box                    SMALLINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS progressive_receptions               INT      NOT NULL DEFAULT 0,

    -- ── role (§4.6.2a) ─────────────────────────────────────────────────────
    -- The percentile ROLE: the formation-slot group, except that a midfielder
    -- with >= 30% of his on-ball touches in the final third is an AM. The slot
    -- alone cannot see it: a 4-3-3's advanced 8 is a CM, and 15 of the 27 AMs
    -- of 2025/26 came from CM slots. Measured on the season so far, so it can
    -- move early in a season (86% of midfielders kept their role between the
    -- two halves of 2025/26).
    ADD COLUMN IF NOT EXISTS role_group VARCHAR(3) GENERATED ALWAYS AS (
        CASE WHEN primary_position_group = 'MID'
              AND touches > 0
              AND touches_final_third >= 0.30 * touches
             THEN 'AM' ELSE primary_position_group END) STORED,

    -- ── attempt counts: the denominators the percentile floors read ────────
    ADD COLUMN IF NOT EXISTS ground_duels INT GENERATED ALWAYS AS
        (tackles + tackles_missed + dribbled_past) STORED,
    ADD COLUMN IF NOT EXISTS aerial_duels INT GENERATED ALWAYS AS
        (aerials_won + aerials_lost) STORED,
    ADD COLUMN IF NOT EXISTS shots_non_penalty SMALLINT GENERATED ALWAYS AS
        (shots - penalties_taken) STORED,
    ADD COLUMN IF NOT EXISTS shots_on_target_faced SMALLINT GENERATED ALWAYS AS
        (saves + goals_conceded) STORED,

    -- ── rates ──────────────────────────────────────────────────────────────
    ADD COLUMN IF NOT EXISTS ground_duel_win_rate REAL GENERATED ALWAYS AS
        (tackles::real / NULLIF(tackles + tackles_missed + dribbled_past, 0)) STORED,
    ADD COLUMN IF NOT EXISTS claim_rate REAL GENERATED ALWAYS AS
        (claims_successful::real / NULLIF(claims, 0)) STORED,
    ADD COLUMN IF NOT EXISTS long_pass_completion_rate REAL GENERATED ALWAYS AS
        (passes_long_completed::real / NULLIF(passes_long, 0)) STORED,
    ADD COLUMN IF NOT EXISTS clean_sheet_rate REAL GENERATED ALWAYS AS
        (clean_sheets::real / NULLIF(matches_full_90, 0)) STORED,
    -- xg_per_shot counts every penalty at 0.79 and flatters the taker.
    ADD COLUMN IF NOT EXISTS npxg_per_shot REAL GENERATED ALWAYS AS
        (npxg / NULLIF(shots - penalties_taken, 0)) STORED,

    -- ── per-90 (same rule as the block above: generated, never averaged) ──
    ADD COLUMN IF NOT EXISTS goals_non_penalty_per_90 REAL GENERATED ALWAYS AS
        (goals_non_penalty * 90.0 / NULLIF(minutes_played, 0)) STORED,
    ADD COLUMN IF NOT EXISTS xg_overperformance_per_90 REAL GENERATED ALWAYS AS
        ((goals_non_penalty - npxg) * 90.0 / NULLIF(minutes_played, 0)) STORED,
    ADD COLUMN IF NOT EXISTS headed_shots_per_90 REAL GENERATED ALWAYS AS
        (headed_shots * 90.0 / NULLIF(minutes_played, 0)) STORED,
    ADD COLUMN IF NOT EXISTS touches_in_box_per_90 REAL GENERATED ALWAYS AS
        (touches_in_box * 90.0 / NULLIF(minutes_played, 0)) STORED,
    ADD COLUMN IF NOT EXISTS shot_creating_actions_per_90 REAL GENERATED ALWAYS AS
        (shot_creating_actions * 90.0 / NULLIF(minutes_played, 0)) STORED,
    ADD COLUMN IF NOT EXISTS crosses_per_90 REAL GENERATED ALWAYS AS
        (crosses * 90.0 / NULLIF(minutes_played, 0)) STORED,
    ADD COLUMN IF NOT EXISTS through_balls_per_90 REAL GENERATED ALWAYS AS
        (through_balls * 90.0 / NULLIF(minutes_played, 0)) STORED,
    ADD COLUMN IF NOT EXISTS passes_into_penalty_area_per_90 REAL GENERATED ALWAYS AS
        (passes_into_penalty_area * 90.0 / NULLIF(minutes_played, 0)) STORED,
    ADD COLUMN IF NOT EXISTS passes_per_90 REAL GENERATED ALWAYS AS
        (passes * 90.0 / NULLIF(minutes_played, 0)) STORED,
    ADD COLUMN IF NOT EXISTS passes_received_per_90 REAL GENERATED ALWAYS AS
        (passes_received * 90.0 / NULLIF(minutes_played, 0)) STORED,
    ADD COLUMN IF NOT EXISTS passes_into_final_third_per_90 REAL GENERATED ALWAYS AS
        (passes_into_final_third * 90.0 / NULLIF(minutes_played, 0)) STORED,
    ADD COLUMN IF NOT EXISTS long_balls_per_90 REAL GENERATED ALWAYS AS
        (long_balls * 90.0 / NULLIF(minutes_played, 0)) STORED,
    ADD COLUMN IF NOT EXISTS take_ons_per_90 REAL GENERATED ALWAYS AS
        (take_ons * 90.0 / NULLIF(minutes_played, 0)) STORED,
    ADD COLUMN IF NOT EXISTS carries_into_penalty_area_per_90 REAL GENERATED ALWAYS AS
        (carries_into_penalty_area * 90.0 / NULLIF(minutes_played, 0)) STORED,
    ADD COLUMN IF NOT EXISTS progressive_carries_into_final_third_per_90 REAL GENERATED ALWAYS AS
        (progressive_carries_into_final_third * 90.0 / NULLIF(minutes_played, 0)) STORED,
    ADD COLUMN IF NOT EXISTS receptions_final_third_per_90 REAL GENERATED ALWAYS AS
        (receptions_final_third * 90.0 / NULLIF(minutes_played, 0)) STORED,
    ADD COLUMN IF NOT EXISTS receptions_in_box_per_90 REAL GENERATED ALWAYS AS
        (receptions_in_box * 90.0 / NULLIF(minutes_played, 0)) STORED,
    ADD COLUMN IF NOT EXISTS progressive_receptions_per_90 REAL GENERATED ALWAYS AS
        (progressive_receptions * 90.0 / NULLIF(minutes_played, 0)) STORED,
    ADD COLUMN IF NOT EXISTS fouls_won_per_90 REAL GENERATED ALWAYS AS
        (fouls_won * 90.0 / NULLIF(minutes_played, 0)) STORED,
    ADD COLUMN IF NOT EXISTS def_actions_final_third_per_90 REAL GENERATED ALWAYS AS
        (def_actions_final_third * 90.0 / NULLIF(minutes_played, 0)) STORED,
    ADD COLUMN IF NOT EXISTS shots_blocked_per_90 REAL GENERATED ALWAYS AS
        (shots_blocked * 90.0 / NULLIF(minutes_played, 0)) STORED,
    ADD COLUMN IF NOT EXISTS padj_blocked_passes_per_90 REAL GENERATED ALWAYS AS
        (padj_blocked_passes * 90.0 / NULLIF(minutes_played, 0)) STORED,
    ADD COLUMN IF NOT EXISTS aerials_won_att_third_per_90 REAL GENERATED ALWAYS AS
        (aerials_won_att_third * 90.0 / NULLIF(minutes_played, 0)) STORED,
    ADD COLUMN IF NOT EXISTS vaep_offensive_per_90 REAL GENERATED ALWAYS AS
        (vaep_offensive * 90.0 / NULLIF(minutes_played, 0)) STORED,
    ADD COLUMN IF NOT EXISTS vaep_defensive_per_90 REAL GENERATED ALWAYS AS
        (vaep_defensive * 90.0 / NULLIF(minutes_played, 0)) STORED,
    -- goalkeeping: NULL for outfield players because their inputs are NULL.
    ADD COLUMN IF NOT EXISTS saves_per_90 REAL GENERATED ALWAYS AS
        (saves * 90.0 / NULLIF(minutes_played, 0)) STORED,
    ADD COLUMN IF NOT EXISTS goals_conceded_per_90 REAL GENERATED ALWAYS AS
        (goals_conceded * 90.0 / NULLIF(minutes_played, 0)) STORED,
    ADD COLUMN IF NOT EXISTS xg_faced_per_90 REAL GENERATED ALWAYS AS
        (xg_faced * 90.0 / NULLIF(minutes_played, 0)) STORED,
    ADD COLUMN IF NOT EXISTS goals_prevented_per_90 REAL GENERATED ALWAYS AS
        ((xg_faced - goals_conceded) * 90.0 / NULLIF(minutes_played, 0)) STORED,
    ADD COLUMN IF NOT EXISTS claims_per_90 REAL GENERATED ALWAYS AS
        (claims * 90.0 / NULLIF(minutes_played, 0)) STORED,
    ADD COLUMN IF NOT EXISTS punches_per_90 REAL GENERATED ALWAYS AS
        (punches * 90.0 / NULLIF(minutes_played, 0)) STORED,
    ADD COLUMN IF NOT EXISTS keeper_sweeper_actions_per_90 REAL GENERATED ALWAYS AS
        (keeper_sweeper_actions * 90.0 / NULLIF(minutes_played, 0)) STORED;

CREATE INDEX IF NOT EXISTS player_season_stats_season_role_idx
    ON gold.player_season_stats (competition_season_id, role_group);


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
-- Three rules from §4.6.14 are enforced by what is listed here:
--   * only per-90, padj or rate forms appear -- percentiling raw volume
--     produces a minutes ranking wearing a skill ranking's clothes;
--   * `higher_is_better = FALSE` for the metrics where it is not (dispossessed,
--     errors, dribbled_past, fouls_committed, goals_conceded);
--   * a rate is ranked only once its denominator reaches `min_attempts`
--     (`attempts_column` names that denominator on player_season_stats). Below
--     it the value is NULL for ranking: 4 of 5 crosses is not an 80% crosser.
--
-- position_group here is the player's ROLE (player_season_stats.role_group):
-- the seven formation-slot groups plus AM, the midfielders who play high
-- (§4.6.2a). The metric set is chosen by role; the peer set a role is ranked
-- in may be pooled (RB+LB -> FB, AM -> MID) when it is under 20 players.
-- ───────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS gold.percentile_metrics (
    metric           VARCHAR(60) NOT NULL,
    position_group   VARCHAR(3)  NOT NULL
        CHECK (position_group IN ('GK','CB','RB','LB','MID','AM','W','ST')),
    higher_is_better BOOLEAN     NOT NULL DEFAULT TRUE,
    attempts_column  VARCHAR(60),
    min_attempts     SMALLINT,
    PRIMARY KEY (metric, position_group)
);

-- Amendment (15 Oct 2026): the attempt floor and the AM role. CREATE TABLE IF
-- NOT EXISTS no-ops on an existing database, so add both explicitly.
ALTER TABLE gold.percentile_metrics
    ADD COLUMN IF NOT EXISTS attempts_column VARCHAR(60),
    ADD COLUMN IF NOT EXISTS min_attempts    SMALLINT;
ALTER TABLE gold.percentile_metrics
    DROP CONSTRAINT IF EXISTS percentile_metrics_position_group_check;
ALTER TABLE gold.percentile_metrics
    ADD CONSTRAINT percentile_metrics_position_group_check
    CHECK (position_group IN ('GK','CB','RB','LB','MID','AM','W','ST'));

-- The seed is REPLACED, not merged: it is reference data, and a row left over
-- from an earlier metric set would keep ranking a metric no card shows.
--
-- It is the screen-03 spine (README-PLAYER §4, src/export/config.py
-- PLAYER_SPINE) with the unranked rows left out: four cards of six per role,
-- three of six for keepers. A metric shared by several roles appears once with
-- all of them. Defensive volumes are possession-adjusted; "duelos en el suelo
-- ganados" is padj_tackles_per_90, since a won ground duel IS a Tackle.
DELETE FROM gold.percentile_metrics;

INSERT INTO gold.percentile_metrics
    (metric, position_group, higher_is_better, attempts_column, min_attempts)
SELECT m.metric, g.grp, m.hib, m.attempts_column, m.min_attempts
FROM (VALUES
    ('save_pct',                                    'GK',                    TRUE , 'shots_on_target_faced', 10),
    ('goals_prevented_per_90',                      'GK',                    TRUE , NULL, NULL),
    ('saves_per_90',                                'GK',                    TRUE , NULL, NULL),
    ('goals_conceded_per_90',                       'GK',                    FALSE, NULL, NULL),
    ('clean_sheet_rate',                            'GK',                    TRUE , 'matches_full_90', 5),
    ('claims_per_90',                               'GK',                    TRUE , NULL, NULL),
    ('claim_rate',                                  'GK',                    TRUE , 'claims', 10),
    ('punches_per_90',                              'GK',                    TRUE , NULL, NULL),
    ('keeper_sweeper_actions_per_90',               'GK',                    TRUE , NULL, NULL),
    ('sweeper_avg_x',                               'GK',                    TRUE , 'keeper_sweeper_actions', 10),
    ('padj_clearances_per_90',                      'GK CB RB LB',           TRUE , NULL, NULL),
    ('gk_pass_completion_rate',                     'GK',                    TRUE , 'gk_passes', 20),
    ('long_pass_completion_rate',                   'GK',                    TRUE , 'passes_long', 10),
    ('progressive_passes_per_90',                   'GK CB RB LB MID AM',    TRUE , NULL, NULL),
    ('vaep_per_90',                                 'GK CB RB LB MID AM W ST', TRUE , NULL, NULL),
    ('vaep_defensive_per_90',                       'GK CB RB LB MID AM W',  TRUE , NULL, NULL),
    ('vaep_offensive_per_90',                       'GK CB',                 TRUE , NULL, NULL),
    ('errors_per_90',                               'GK CB MID',             FALSE, NULL, NULL),
    ('padj_interceptions_per_90',                   'CB RB LB MID AM W',     TRUE , NULL, NULL),
    ('shots_blocked_per_90',                        'CB RB LB',              TRUE , NULL, NULL),
    ('padj_blocked_passes_per_90',                  'CB MID',                TRUE , NULL, NULL),
    ('padj_ball_recoveries_per_90',                 'CB RB LB MID AM W ST',  TRUE , NULL, NULL),
    ('padj_tackles_per_90',                         'CB RB LB MID AM W',     TRUE , NULL, NULL),
    ('ground_duel_win_rate',                        'CB RB LB MID AM W',     TRUE , 'ground_duels', 10),
    ('dribbled_past_per_90',                        'CB RB LB MID',          FALSE, NULL, NULL),
    ('aerials_won_per_90',                          'CB RB LB MID ST',       TRUE , NULL, NULL),
    ('aerial_win_rate',                             'CB RB LB MID ST',       TRUE , 'aerial_duels', 10),
    ('fouls_committed_per_90',                      'CB RB LB MID',          FALSE, NULL, NULL),
    ('passes_per_90',                               'CB MID AM',             TRUE , NULL, NULL),
    ('pass_completion_rate',                        'CB RB LB MID AM ST',    TRUE , 'passes', 20),
    ('long_balls_per_90',                           'CB',                    TRUE , NULL, NULL),
    ('long_ball_accuracy',                          'CB',                    TRUE , 'long_balls', 10),
    ('passes_into_final_third_per_90',              'CB MID AM',             TRUE , NULL, NULL),
    ('progressive_carries_per_90',                  'CB RB LB MID ST',       TRUE , NULL, NULL),
    ('headed_shots_per_90',                         'CB ST',                 TRUE , NULL, NULL),
    ('aerials_won_att_third_per_90',                'CB ST',                 TRUE , NULL, NULL),
    ('def_actions_final_third_per_90',              'RB LB MID AM W ST',     TRUE , NULL, NULL),
    ('progressive_carries_into_final_third_per_90', 'RB LB AM W',            TRUE , NULL, NULL),
    ('receptions_final_third_per_90',               'RB LB AM',              TRUE , NULL, NULL),
    ('take_on_success_rate',                        'RB LB AM W ST',         TRUE , 'take_ons', 10),
    ('crosses_per_90',                              'RB LB W',               TRUE , NULL, NULL),
    ('cross_completion_rate',                       'RB LB W',               TRUE , 'crosses', 10),
    ('passes_into_penalty_area_per_90',             'RB LB AM W',            TRUE , NULL, NULL),
    ('xa_per_90',                                   'RB LB MID AM W ST',     TRUE , NULL, NULL),
    ('shot_creating_actions_per_90',                'RB LB MID AM W ST',     TRUE , NULL, NULL),
    ('xt_per_90',                                   'MID AM',                TRUE , NULL, NULL),
    ('key_passes_per_90',                           'MID AM W ST',           TRUE , NULL, NULL),
    ('through_balls_per_90',                        'MID AM',                TRUE , NULL, NULL),
    ('npxg_per_90',                                 'MID AM W ST',           TRUE , NULL, NULL),
    ('take_ons_per_90',                             'AM W',                  TRUE , NULL, NULL),
    ('dispossessed_per_90',                         'AM W ST',               FALSE, NULL, NULL),
    ('goals_non_penalty_per_90',                    'AM W ST',               TRUE , NULL, NULL),
    ('carries_into_penalty_area_per_90',            'W',                     TRUE , NULL, NULL),
    ('progressive_receptions_per_90',               'W ST',                  TRUE , NULL, NULL),
    ('shots_per_90',                                'W ST',                  TRUE , NULL, NULL),
    ('touches_in_box_per_90',                       'W ST',                  TRUE , NULL, NULL),
    ('npxg_per_shot',                               'W ST',                  TRUE , 'shots_non_penalty', 10),
    ('fouls_won_per_90',                            'ST',                    TRUE , NULL, NULL),
    ('passes_received_per_90',                      'ST',                    TRUE , NULL, NULL),
    ('receptions_in_box_per_90',                    'ST',                    TRUE , NULL, NULL),
    ('xg_overperformance_per_90',                   'ST',                    TRUE , NULL, NULL)
) AS m(metric, groups, hib, attempts_column, min_attempts)
CROSS JOIN LATERAL unnest(string_to_array(m.groups, ' ')) AS g(grp);


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
    -- TRUE when the AM role had under 20 eligible players and was ranked
    -- among all midfielders instead (§4.6.2a).
    pooled_am             BOOLEAN NOT NULL DEFAULT FALSE,

    PRIMARY KEY (competition_season_id, player_id, team_id, metric)
);

ALTER TABLE gold.player_season_percentiles
    ADD COLUMN IF NOT EXISTS pooled_am BOOLEAN NOT NULL DEFAULT FALSE;

CREATE INDEX IF NOT EXISTS player_season_percentiles_lookup_idx
    ON gold.player_season_percentiles (competition_season_id, position_group, metric);
