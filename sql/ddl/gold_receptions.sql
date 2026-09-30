-- ═══════════════════════════════════════════════════════════════════════════
-- gold_receptions.sql — DDL for gold.pass_receptions
-- ═══════════════════════════════════════════════════════════════════════════
--
-- Spec: md/GOLD_LAYER.md §4.7 (table, receiver rule, spell rule, measured
--       numbers), §9.5 gate G16.
--
-- Creates:
--   gold.pass_receptions   one row per completed pass whose receiver is known
--
-- Opta records no reception event and no receiver. The receiver is RESOLVED:
-- the next event of the passing team, in (json_index, event_id) order, with
-- opponent and administrative rows skipped (§4.7.1). The reception point is the
-- pass's own end_x/end_y, which sits within 0.21 m (mean) of the receiver's
-- first action on the 376k completed passes where both exist.
--
-- The row also describes the receiver's SPELL: their unbroken run of actions
-- from the reception until a teammate acts, the possession sequence changes or
-- the period ends (§4.7.2). That is "what did the player do after receiving".
--
-- Frame: coordinates are the passing team's frame, attacking towards x = 105,
-- like every other single-team gold table.
--
-- Idempotent: safe to run repeatedly. Drops nothing.
-- ═══════════════════════════════════════════════════════════════════════════

CREATE SCHEMA IF NOT EXISTS gold;


CREATE TABLE IF NOT EXISTS gold.pass_receptions (
    -- ── grain ──────────────────────────────────────────────────────────────
    pass_event_id         BIGINT   PRIMARY KEY REFERENCES silver.events(event_id),

    -- ── context (from gold.team_match_stats, so it cannot disagree) ────────
    match_id              INT      NOT NULL REFERENCES silver.matches(match_id),
    competition_season_id INT      NOT NULL
                              REFERENCES silver.competition_seasons(competition_season_id),
    team_id               INT      NOT NULL REFERENCES silver.teams(team_id),
    opponent_team_id      INT      NOT NULL REFERENCES silver.teams(team_id),
    match_date            DATE     NOT NULL,
    period                SMALLINT NOT NULL,
    minute                SMALLINT NOT NULL,
    second                SMALLINT,
    -- The PASS's sequence. NULL only if the classifier left the pass out.
    sequence_id           VARCHAR(24),

    -- ── who ────────────────────────────────────────────────────────────────
    passer_id             INT      NOT NULL REFERENCES silver.players(player_id),
    receiver_id           INT      NOT NULL REFERENCES silver.players(player_id),

    -- ── the pass ───────────────────────────────────────────────────────────
    pass_x                REAL     NOT NULL,
    pass_y                REAL     NOT NULL,
    pass_length_m         REAL     NOT NULL,
    is_progressive        BOOLEAN  NOT NULL,   -- gold.is_progressive (§4.5.0)
    is_cross              BOOLEAN  NOT NULL,   -- Q2
    is_through_ball       BOOLEAN  NOT NULL,   -- Q4
    is_long_ball          BOOLEAN  NOT NULL,   -- Q157 "launch", as gold's long_balls
    -- NULL = open play. Precedence as the export's start_kind: kick-off (Q279)
    -- first, then corner, throw-in, goal kick, free kick.
    restart               VARCHAR(10)
        CHECK (restart IN ('kick_off','corner','throw_in','goal_kick','free_kick')),
    pass_xt               REAL,

    -- ── the reception ──────────────────────────────────────────────────────
    -- rx_* is the pass's end point: where the ball arrived.
    rx_x                  REAL     NOT NULL,
    rx_y                  REAL     NOT NULL,
    rx_zone_id            SMALLINT NOT NULL REFERENCES gold.pitch_zones(zone_id),
    rx_in_box             BOOLEAN  NOT NULL,
    rx_event_id           BIGINT   NOT NULL REFERENCES silver.events(event_id),
    rx_event_type         VARCHAR(40) NOT NULL,  -- receiver's first action
    -- Distance from the pass end to the receiver's first action. Median 0 m,
    -- p99 ~9 m; large values are a first action taken after the ball moved
    -- (e.g. a sub-1 m carry that was not synthesised, then a pass). Filter on
    -- it when the location of the first touch matters more than of the pass.
    rx_gap_m              REAL     NOT NULL,

    -- ── the receiver's spell (§4.7.2) ──────────────────────────────────────
    spell_events          SMALLINT NOT NULL,
    spell_end_event_id    BIGINT   NOT NULL REFERENCES silver.events(event_id),
    spell_end             VARCHAR(16) NOT NULL
        CHECK (spell_end IN ('pass_completed','pass_failed','shot','take_on_failed',
                             'dispossessed','miscontrol','foul_won','other')),
    spell_end_x           REAL,
    spell_end_y           REAL,
    spell_carry_m         REAL     NOT NULL,   -- summed Carry length
    spell_progressive_carry BOOLEAN NOT NULL,  -- any carry passing gold.is_progressive
    spell_take_on         BOOLEAN  NOT NULL,   -- attempted a take-on in the spell
    -- An opponent duelled for the ball between the pass and the spell's end:
    -- Tackle, Attempted Tackle, Challenge, Aerial or a foul committed.
    spell_contested       BOOLEAN  NOT NULL,
    spell_xt              REAL     NOT NULL,   -- receiver's xT in the spell (failed actions earn 0)
    spell_vaep            REAL     NOT NULL,   -- receiver's vaep_value in the spell

    CHECK (receiver_id <> passer_id)
);

CREATE INDEX IF NOT EXISTS pass_receptions_receiver_season_idx
    ON gold.pass_receptions (competition_season_id, receiver_id);
CREATE INDEX IF NOT EXISTS pass_receptions_match_team_idx
    ON gold.pass_receptions (match_id, team_id);

COMMENT ON TABLE gold.pass_receptions IS
    'One row per completed pass with a resolved receiver: where it was received '
    'and what the receiver did before a teammate acted. Spec: GOLD_LAYER.md §4.7.';
