-- ═══════════════════════════════════════════════════════════════════════════
-- gold_sequences.sql — DDL for the sequence layer
-- ═══════════════════════════════════════════════════════════════════════════
--
-- Spec: md/GOLD_LAYER.md §4.1
--
-- Creates, in dependency order:
--   gold.pitch_zones                (30 rows, seeded inline — §4.1.7)
--   gold.sequences                  (one row per possession sequence — §4.1.1)
--   gold.sequence_players           (sequence × player — §4.1.2)
--   gold.sequence_phase_segments    (sequence × contiguous phase — §4.1.6)
--
-- Upstream contract (src/silver/events/sequences.py, GOLD_LAYER.md §3):
--   silver.events.sequence_id is VARCHAR '{match_id}_{h_a}_{NNN}', NOT a UUID.
--
-- Idempotent: safe to run repeatedly. Drops nothing.
-- Run once:  psql "$FOOTBALL_DB_DSN" -f sql/ddl/gold_sequences.sql
-- ═══════════════════════════════════════════════════════════════════════════

CREATE SCHEMA IF NOT EXISTS gold;


-- ───────────────────────────────────────────────────────────────────────────
-- Guard: a prototype gold.sequences predates this spec
--
-- An earlier 28-column prototype (sequence_length, total_xt, num_passes,
-- had_shot, sequence_start_type, …) exists in the database with 0 rows. It is
-- superseded by GOLD_LAYER.md §4.1 and shares the table name, so
-- CREATE TABLE IF NOT EXISTS would silently no-op and the first index naming a
-- new column would then fail halfway through this file. Fail loudly instead.
-- ───────────────────────────────────────────────────────────────────────────
DO $$
DECLARE
    legacy_rows BIGINT;
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'gold'
          AND table_name   = 'sequences'
          AND column_name  = 'sequence_length'   -- prototype-only column
    ) THEN
        EXECUTE 'SELECT count(*) FROM gold.sequences' INTO legacy_rows;
        RAISE EXCEPTION
            'Prototype gold.sequences is present (% rows). It is superseded by '
            'GOLD_LAYER.md 4.1. Confirm the row count is 0, then run '
            'DROP TABLE gold.sequences; and re-run this file.', legacy_rows;
    END IF;
END $$;


-- ───────────────────────────────────────────────────────────────────────────
-- gold.pitch_zones — seeded reference (§4.1.7)
--
-- 6 longitudinal strips × 5 lateral channels = 30 zones, boundaries aligned to
-- real pitch markings (penalty-area and six-yard-box edges).
--
-- NOTE ON SIDES: low y is the RIGHT flank, high y the LEFT. Established from
-- data in GOLD_LAYER.md §4.6.0 — formation slot 2 (mean y ≈ 11.2) is 839
-- right-footed to 13 left-footed. The `pitch-guide` skill §5.2 was inverted and
-- was corrected on 16 Aug 2026; skill and seed now agree. This table remains
-- the authoritative materialisation.
-- ───────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS gold.pitch_zones (
    zone_id       SMALLINT    PRIMARY KEY,      -- x_strip * 10 + y_channel
    x_strip       SMALLINT    NOT NULL CHECK (x_strip   BETWEEN 1 AND 6),
    y_channel     SMALLINT    NOT NULL CHECK (y_channel BETWEEN 1 AND 5),
    zone_name     VARCHAR(24) NOT NULL UNIQUE,  -- 'X6_center'
    third         SMALLINT    NOT NULL CHECK (third BETWEEN 1 AND 3),
    channel_group VARCHAR(12) NOT NULL,         -- wide | half_space | center
    side          VARCHAR(6)  NOT NULL,         -- right | centre | left
    x_min         REAL        NOT NULL,
    x_max         REAL        NOT NULL,
    y_min         REAL        NOT NULL,
    y_max         REAL        NOT NULL,
    UNIQUE (x_strip, y_channel)
);

INSERT INTO gold.pitch_zones
    (zone_id, x_strip, y_channel, zone_name, third,
     channel_group, side, x_min, x_max, y_min, y_max)
SELECT
    (s.strip * 10 + c.channel)::smallint,
    s.strip, c.channel,
    'X' || s.strip || '_' || c.cname,
    ((s.strip + 1) / 2)::smallint,
    c.cgroup, c.side,
    s.x_min, s.x_max, c.y_min, c.y_max
FROM (VALUES
        (1::smallint, 0.00::real,  16.50::real),
        (2::smallint, 16.50::real, 35.00::real),
        (3::smallint, 35.00::real, 52.50::real),
        (4::smallint, 52.50::real, 70.00::real),
        (5::smallint, 70.00::real, 88.50::real),
        (6::smallint, 88.50::real, 105.00::real)
     ) AS s(strip, x_min, x_max)
CROSS JOIN (VALUES
        (1::smallint, 'wide_right',       'wide',       'right',  0.00::real,  13.84::real),
        (2::smallint, 'half_space_right', 'half_space', 'right',  13.84::real, 24.84::real),
        (3::smallint, 'center',           'center',     'centre', 24.84::real, 43.16::real),
        (4::smallint, 'half_space_left',  'half_space', 'left',   43.16::real, 54.16::real),
        (5::smallint, 'wide_left',        'wide',       'left',   54.16::real, 68.00::real)
     ) AS c(channel, cname, cgroup, side, y_min, y_max)
ON CONFLICT (zone_id) DO NOTHING;


-- ───────────────────────────────────────────────────────────────────────────
-- gold.sequences — one row per possession sequence (§4.1.1)
--
-- Serving table: read directly by the Sequence Laboratory under interactive
-- filters, so context is denormalised and the ranking keys are indexed.
-- ───────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS gold.sequences (
    -- ── grain ──────────────────────────────────────────────────────────────
    sequence_id             VARCHAR(24) PRIMARY KEY,

    -- ── context ────────────────────────────────────────────────────────────
    match_id                INT      NOT NULL REFERENCES silver.matches(match_id),
    team_id                 INT      NOT NULL REFERENCES silver.teams(team_id),
    opponent_team_id        INT      NOT NULL REFERENCES silver.teams(team_id),
    competition_season_id   INT      NOT NULL
                                     REFERENCES silver.competition_seasons(competition_season_id),
    match_date              DATE     NOT NULL,
    is_home                 BOOLEAN  NOT NULL,
    sequence_number         SMALLINT NOT NULL,
    period                  SMALLINT NOT NULL,
    start_minute            SMALLINT NOT NULL,
    start_second            SMALLINT NOT NULL,
    end_minute              SMALLINT NOT NULL,
    end_second              SMALLINT NOT NULL,
    -- Derived from minute*60+second. NEVER from silver.events.timestamp, which
    -- is NULL on all 229,662 synthesised carries (GOLD_LAYER.md §4.1.0 #1).
    duration_seconds        REAL     NOT NULL,
    start_event_id          BIGINT   NOT NULL REFERENCES silver.events(event_id),
    end_event_id            BIGINT   NOT NULL REFERENCES silver.events(event_id),

    -- ── size & integrity (§4.1.0 #2) ───────────────────────────────────────
    event_count             SMALLINT NOT NULL,   -- possessing team only
    total_event_count       SMALLINT NOT NULL,   -- every event with this sequence_id
    opponent_event_count    SMALLINT NOT NULL,
    -- TRUE when opponent_event_count >= 3, or when the majority of events
    -- belong to a team other than team_id. Exclude from clustering sets and
    -- from published leaderboards.
    is_suspect_segmentation BOOLEAN  NOT NULL DEFAULT FALSE,
    unique_players          SMALLINT NOT NULL,
    touches_per_player      REAL,

    -- ── start & end classification (§4.1.3) ────────────────────────────────
    sequence_type           VARCHAR(12) NOT NULL
        CHECK (sequence_type IN ('open_play','kick_off','throw_in','corner',
                                 'free_kick','goal_kick','penalty','drop_ball')),
    start_trigger           VARCHAR(14) NOT NULL
        CHECK (start_trigger IN ('pass','clearance','recovery','tackle',
                                 'interception','block','keeper','rebound',
                                 'take_on','drop_ball','other')),
    start_event_type        VARCHAR(40) NOT NULL,
    start_spadl_type_id     SMALLINT,            -- NULL on Ball recovery starts
    end_event_type          VARCHAR(40) NOT NULL,
    end_spadl_type_id       SMALLINT,
    -- First event after the sequence, in any team's possession and regardless
    -- of sequence membership. Recovers the 13,879 orphaned fouls and 5,673
    -- orphaned corner awards that terminate sequences (§4.1.0 #3).
    next_event_type         VARCHAR(40),
    next_event_team_id      INT REFERENCES silver.teams(team_id),
    outcome                 VARCHAR(18) NOT NULL
        CHECK (outcome IN ('goal','shot_saved','shot_blocked','shot_off_target',
                           'shot_woodwork','corner_won','foul_won','offside',
                           'ball_out','keeper_collected','period_end','turnover')),

    -- ── spatial (§4.1.4) ───────────────────────────────────────────────────
    start_x                 REAL,
    start_y                 REAL,
    end_x                   REAL,
    end_y                   REAL,
    max_x                   REAL,                -- deepest point reached
    avg_x                   REAL,
    avg_y                   REAL,
    start_zone              SMALLINT REFERENCES gold.pitch_zones(zone_id),
    end_zone                SMALLINT REFERENCES gold.pitch_zones(zone_id),
    start_x_strip           SMALLINT,
    start_y_channel         SMALLINT,
    end_x_strip             SMALLINT,
    end_y_channel           SMALLINT,
    start_third             SMALLINT GENERATED ALWAYS AS
                                (((start_x_strip + 1) / 2)::smallint) STORED,
    end_third               SMALLINT GENERATED ALWAYS AS
                                (((end_x_strip   + 1) / 2)::smallint) STORED,
    zone_path               SMALLINT[],          -- 30-zone path, runs collapsed
    field_progression       REAL,                -- end_x - start_x
    distance_covered_m      REAL,                -- Σ |pass/carry vector|
    directness              REAL,                -- field_progression / distance_covered_m
    direct_speed            REAL,                -- m/s; NULL when duration = 0
    width_used_m            REAL,                -- max(y) - min(y)
    channels_used           SMALLINT,
    final_third_entry       BOOLEAN  NOT NULL DEFAULT FALSE,
    penalty_box_entry       BOOLEAN  NOT NULL DEFAULT FALSE,
    box_touches             SMALLINT NOT NULL DEFAULT 0,

    -- ── passing ────────────────────────────────────────────────────────────
    pass_count                   SMALLINT NOT NULL DEFAULT 0,
    pass_completed               SMALLINT NOT NULL DEFAULT 0,
    pass_completion_rate         REAL,                       -- 0-1, not 0-100
    pass_short                   SMALLINT NOT NULL DEFAULT 0,  -- < 15 m
    pass_medium                  SMALLINT NOT NULL DEFAULT 0,  -- 15-30 m
    pass_long                    SMALLINT NOT NULL DEFAULT 0,  -- >= 30 m
    progressive_passes           SMALLINT NOT NULL DEFAULT 0,  -- §4.5.0 rule
    progressive_passes_completed SMALLINT NOT NULL DEFAULT 0,
    passes_into_final_third      SMALLINT NOT NULL DEFAULT 0,
    passes_into_box              SMALLINT NOT NULL DEFAULT 0,
    cross_count                  SMALLINT NOT NULL DEFAULT 0,
    cross_completed              SMALLINT NOT NULL DEFAULT 0,
    through_ball_count           SMALLINT NOT NULL DEFAULT 0,  -- Q4
    long_ball_count              SMALLINT NOT NULL DEFAULT 0,  -- Q157
    switch_count                 SMALLINT NOT NULL DEFAULT 0,  -- |Δy| >= 30 m

    -- ── carrying & dribbling ───────────────────────────────────────────────
    carry_count              SMALLINT NOT NULL DEFAULT 0,
    carry_distance_m         REAL     NOT NULL DEFAULT 0,
    progressive_carries      SMALLINT NOT NULL DEFAULT 0,
    carries_into_final_third SMALLINT NOT NULL DEFAULT 0,
    carries_into_box         SMALLINT NOT NULL DEFAULT 0,
    take_ons                 SMALLINT NOT NULL DEFAULT 0,
    take_ons_won             SMALLINT NOT NULL DEFAULT 0,

    -- ── shooting ───────────────────────────────────────────────────────────
    shot_count       SMALLINT NOT NULL DEFAULT 0,
    shots_on_target  SMALLINT NOT NULL DEFAULT 0,   -- Goal + Attempt Saved w/o Q82
    shots_in_box     SMALLINT NOT NULL DEFAULT 0,
    goal_count       SMALLINT NOT NULL DEFAULT 0,
    woodwork_count   SMALLINT NOT NULL DEFAULT 0,
    big_chance_count SMALLINT NOT NULL DEFAULT 0,   -- Q214

    -- ── set pieces & regains inside the sequence ───────────────────────────
    corner_count        SMALLINT NOT NULL DEFAULT 0,
    throw_in_count      SMALLINT NOT NULL DEFAULT 0,
    free_kick_count     SMALLINT NOT NULL DEFAULT 0,
    goal_kick_count     SMALLINT NOT NULL DEFAULT 0,
    ball_recovery_count SMALLINT NOT NULL DEFAULT 0,
    interception_count  SMALLINT NOT NULL DEFAULT 0,

    -- ── model values (§4.1.5) ──────────────────────────────────────────────
    xg              REAL,
    npxg            REAL,
    xg_max          REAL,
    xt              REAL,
    xt_max          REAL,
    xt_per_event    REAL,
    vaep            REAL,
    vaep_offensive  REAL,
    vaep_defensive  REAL,
    vaep_max        REAL,

    -- ── third-to-third flow, complete 3×3 (§4.1.4) ─────────────────────────
    pass_1_to_1  SMALLINT NOT NULL DEFAULT 0,
    pass_1_to_2  SMALLINT NOT NULL DEFAULT 0,
    pass_1_to_3  SMALLINT NOT NULL DEFAULT 0,
    pass_2_to_1  SMALLINT NOT NULL DEFAULT 0,
    pass_2_to_2  SMALLINT NOT NULL DEFAULT 0,
    pass_2_to_3  SMALLINT NOT NULL DEFAULT 0,
    pass_3_to_1  SMALLINT NOT NULL DEFAULT 0,
    pass_3_to_2  SMALLINT NOT NULL DEFAULT 0,
    pass_3_to_3  SMALLINT NOT NULL DEFAULT 0,
    carry_1_to_1 SMALLINT NOT NULL DEFAULT 0,
    carry_1_to_2 SMALLINT NOT NULL DEFAULT 0,
    carry_1_to_3 SMALLINT NOT NULL DEFAULT 0,
    carry_2_to_1 SMALLINT NOT NULL DEFAULT 0,
    carry_2_to_2 SMALLINT NOT NULL DEFAULT 0,
    carry_2_to_3 SMALLINT NOT NULL DEFAULT 0,
    carry_3_to_1 SMALLINT NOT NULL DEFAULT 0,
    carry_3_to_2 SMALLINT NOT NULL DEFAULT 0,
    carry_3_to_3 SMALLINT NOT NULL DEFAULT 0,

    -- ── channel-to-channel flow, sparse (§4.1.4) ───────────────────────────
    -- {"1_3": 2, "3_3": 5, "3_5": 1} — keys are 'from_to' channel ids.
    -- JSONB rather than 50 columns so the schema survives a change in the
    -- channel count (the open 5-vs-7 question in §4.1.4).
    pass_channel_flow  JSONB,
    carry_channel_flow JSONB,

    -- ── phases of play (§4.1.6, `phases-of-play` skill) ────────────────────
    -- Written by src/gold/sequence_phases.py AFTER the row is inserted.
    has_buildup         BOOLEAN NOT NULL DEFAULT FALSE,
    has_fast_buildup    BOOLEAN NOT NULL DEFAULT FALSE,
    has_midblock        BOOLEAN NOT NULL DEFAULT FALSE,
    has_fast_midblock   BOOLEAN NOT NULL DEFAULT FALSE,
    has_attacking       BOOLEAN NOT NULL DEFAULT FALSE,
    has_fast_attacking  BOOLEAN NOT NULL DEFAULT FALSE,
    has_set_piece       BOOLEAN NOT NULL DEFAULT FALSE,
    has_counter_attack  BOOLEAN NOT NULL DEFAULT FALSE,
    has_high_transition BOOLEAN NOT NULL DEFAULT FALSE,
    has_direct_long     BOOLEAN NOT NULL DEFAULT FALSE,
    has_direct_fk_pk    BOOLEAN NOT NULL DEFAULT FALSE,
    phase_count         SMALLINT,
    primary_phase       VARCHAR(18),
    phase_path          VARCHAR(120),   -- 'buildup>fast_midblock>attacking'

    computed_at         TIMESTAMPTZ NOT NULL DEFAULT now(),

    CHECK (event_count <= total_event_count),
    CHECK (opponent_event_count = total_event_count - event_count)
);

-- Laboratory: the default filter is always (season, team, date)
CREATE INDEX IF NOT EXISTS sequences_season_team_date_idx
    ON gold.sequences (competition_season_id, team_id, match_date);
CREATE INDEX IF NOT EXISTS sequences_match_team_idx
    ON gold.sequences (match_id, team_id);
-- Leaderboards: "top xG / xT / VAEP sequences"
CREATE INDEX IF NOT EXISTS sequences_season_xg_idx
    ON gold.sequences (competition_season_id, xg   DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS sequences_season_xt_idx
    ON gold.sequences (competition_season_id, xt   DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS sequences_season_vaep_idx
    ON gold.sequences (competition_season_id, vaep DESC NULLS LAST);
-- Laboratory facets
CREATE INDEX IF NOT EXISTS sequences_season_type_outcome_idx
    ON gold.sequences (competition_season_id, sequence_type, outcome);
CREATE INDEX IF NOT EXISTS sequences_season_zones_idx
    ON gold.sequences (competition_season_id, start_zone, end_zone);
-- Pattern mining over the zone path
CREATE INDEX IF NOT EXISTS sequences_zone_path_idx
    ON gold.sequences USING GIN (zone_path);


-- ───────────────────────────────────────────────────────────────────────────
-- gold.sequence_players — sequence × player (§4.1.2)
--
-- Involvement, NOT totals. Its denominator is sequences, and 13.3% of events
-- carry no sequence_id, so SUM(passes) here sits below a player's true match
-- total. Use gold.player_match_stats for totals.
-- ───────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS gold.sequence_players (
    sequence_id           VARCHAR(24) NOT NULL
                              REFERENCES gold.sequences(sequence_id) ON DELETE CASCADE,
    player_id             INT NOT NULL REFERENCES silver.players(player_id),

    -- denormalised so the common filter never joins
    match_id              INT NOT NULL REFERENCES silver.matches(match_id),
    team_id               INT NOT NULL REFERENCES silver.teams(team_id),
    competition_season_id INT NOT NULL
                              REFERENCES silver.competition_seasons(competition_season_id),
    -- FALSE on sandwich / deflection rows. Default every player-facing query
    -- to WHERE is_possessing_team (§4.1.0 #2).
    is_possessing_team    BOOLEAN NOT NULL,

    -- where in the sequence they appear
    first_event_number    SMALLINT NOT NULL,
    last_event_number     SMALLINT NOT NULL,
    is_sequence_starter   BOOLEAN  NOT NULL DEFAULT FALSE,
    is_sequence_finisher  BOOLEAN  NOT NULL DEFAULT FALSE,
    is_assister           BOOLEAN  NOT NULL DEFAULT FALSE,

    -- volume
    touches          SMALLINT NOT NULL DEFAULT 0,
    passes           SMALLINT NOT NULL DEFAULT 0,
    passes_completed SMALLINT NOT NULL DEFAULT 0,
    carries          SMALLINT NOT NULL DEFAULT 0,
    take_ons         SMALLINT NOT NULL DEFAULT 0,
    take_ons_won     SMALLINT NOT NULL DEFAULT 0,
    shots            SMALLINT NOT NULL DEFAULT 0,
    goals            SMALLINT NOT NULL DEFAULT 0,
    progression_m    REAL,     -- Σ (end_x - x) over their passes and carries

    -- model values
    xg             REAL,
    xt             REAL,
    vaep           REAL,
    vaep_offensive REAL,
    vaep_defensive REAL,

    PRIMARY KEY (sequence_id, player_id)
);

CREATE INDEX IF NOT EXISTS sequence_players_season_player_idx
    ON gold.sequence_players (competition_season_id, player_id);
CREATE INDEX IF NOT EXISTS sequence_players_player_xt_idx
    ON gold.sequence_players (player_id, xt   DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS sequence_players_player_vaep_idx
    ON gold.sequence_players (player_id, vaep DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS sequence_players_match_team_idx
    ON gold.sequence_players (match_id, team_id);


-- ───────────────────────────────────────────────────────────────────────────
-- gold.sequence_phase_segments — sequence × contiguous phase (§4.1.6)
--
-- Written by src/gold/sequence_phases.py. Enables the phase transition matrix
-- (`phases-of-play` skill §4.2) by self-joining phase_order = phase_order + 1.
-- ───────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS gold.sequence_phase_segments (
    sequence_id      VARCHAR(24) NOT NULL
                         REFERENCES gold.sequences(sequence_id) ON DELETE CASCADE,
    phase_order      SMALLINT NOT NULL,
    phase_type       VARCHAR(18) NOT NULL
        CHECK (phase_type IN ('buildup','fast_buildup','midblock','fast_midblock',
                              'attacking','fast_attacking','set_piece',
                              'counter_attack','high_transition','direct_long',
                              'direct_fk_pk','chaotic')),
    start_event_id   BIGINT NOT NULL REFERENCES silver.events(event_id),
    end_event_id     BIGINT NOT NULL REFERENCES silver.events(event_id),
    event_count      SMALLINT NOT NULL,
    pass_carry_count SMALLINT NOT NULL,   -- the tempo denominator (skill §5.3)
    start_x          REAL,
    end_x            REAL,
    start_third      SMALLINT,
    end_third        SMALLINT,
    duration_seconds REAL,
    xt               REAL,

    PRIMARY KEY (sequence_id, phase_order)
);

CREATE INDEX IF NOT EXISTS sequence_phase_segments_type_idx
    ON gold.sequence_phase_segments (phase_type);
