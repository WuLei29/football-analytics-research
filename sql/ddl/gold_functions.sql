-- ═══════════════════════════════════════════════════════════════════════════
-- gold_functions.sql — shared geometry helpers for the gold builds
-- ═══════════════════════════════════════════════════════════════════════════
--
-- Spec: md/GOLD_LAYER.md §4.1.4 (zones, thirds, channels), §4.5.0 (canonical
--       pitch regions and the progressive-action rule), `pitch-guide` skill.
--
-- Why these exist: the strip/channel CASE expression appears roughly a dozen
-- times across build_sequences.sql and build_team_match_stats.sql (start and
-- end coordinates, both action types). Written out each time it is a dozen
-- chances to fat-finger a boundary, and the resulting error — a zone that is
-- off by one for 3% of events — is invisible in every summary statistic.
--
-- gold.pitch_zones remains AUTHORITATIVE. These functions duplicate its
-- boundaries for speed, and the guide (§9) carries an assertion that proves
-- the two agree on all 30 zones. Run it after changing either.
--
-- All functions are IMMUTABLE so the planner can inline and constant-fold them.
--
-- Idempotent: CREATE OR REPLACE throughout.
-- ═══════════════════════════════════════════════════════════════════════════

CREATE SCHEMA IF NOT EXISTS gold;


-- ───────────────────────────────────────────────────────────────────────────
-- Longitudinal strip 1-6 from x. Boundaries align to real pitch markings:
-- 16.5 = penalty-area edge, 88.5 = the mirror, 52.5 = halfway.
-- x = 105.0 exactly belongs to strip 6, not to NULL.
-- ───────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION gold.x_strip(x double precision)
RETURNS SMALLINT
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
    SELECT CASE
        WHEN x IS NULL   THEN NULL
        WHEN x <  16.5   THEN 1
        WHEN x <  35.0   THEN 2
        WHEN x <  52.5   THEN 3
        WHEN x <  70.0   THEN 4
        WHEN x <  88.5   THEN 5
        ELSE                  6
    END::smallint
$$;


-- ───────────────────────────────────────────────────────────────────────────
-- Lateral channel 1-5 from y.
--
-- LOW y IS THE RIGHT FLANK. Settled from data in §4.6.0: formation slot 2
-- (mean y ~ 11.2) is 839 right-footed to 13 left-footed. Do not "fix" this.
--   Y1 wide_right | Y2 half_space_right | Y3 center | Y4 half_space_left
--   Y5 wide_left
-- ───────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION gold.y_channel(y double precision)
RETURNS SMALLINT
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
    SELECT CASE
        WHEN y IS NULL   THEN NULL
        WHEN y <  13.84  THEN 1
        WHEN y <  24.84  THEN 2
        WHEN y <  43.16  THEN 3
        WHEN y <  54.16  THEN 4
        ELSE                  5
    END::smallint
$$;


-- ───────────────────────────────────────────────────────────────────────────
-- zone_id = x_strip * 10 + y_channel, i.e. 11 … 65. Matches gold.pitch_zones.
-- ───────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION gold.zone_id(x double precision, y double precision)
RETURNS SMALLINT
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
    SELECT (gold.x_strip(x) * 10 + gold.y_channel(y))::smallint
$$;


-- ───────────────────────────────────────────────────────────────────────────
-- Third 1-3, DERIVED FROM THE STRIP so that third and strip can never
-- disagree (§4.1.4). Strips 1-2 -> 1, 3-4 -> 2, 5-6 -> 3.
--
-- NOTE the boundary difference from §4.5.0's touch/region thresholds: the
-- strip-derived third splits at x = 35 and x = 70, which is exactly the
-- §4.5.0 defensive/middle/final third. They agree by construction.
-- ───────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION gold.third(x double precision)
RETURNS SMALLINT
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
    SELECT ((gold.x_strip(x) + 1) / 2)::smallint
$$;


-- ───────────────────────────────────────────────────────────────────────────
-- Penalty area (§4.5.0): x >= 88.5 AND y BETWEEN 13.84 AND 54.16.
-- ───────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION gold.in_box(x double precision, y double precision)
RETURNS BOOLEAN
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
    SELECT x >= 88.5 AND y >= 13.84 AND y <= 54.16
$$;


-- ───────────────────────────────────────────────────────────────────────────
-- Progressive action (§4.5.0) — identical rule for passes and carries, so the
-- two are directly comparable. An action is progressive when it cuts the
-- straight-line distance to the centre of the opponent goal (105, 34) by at
-- least 25%.
--
-- Chosen over `end_x - x >= 10`, which credits sideways balls near the
-- touchline and penalises genuine progression through the centre.
-- ───────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION gold.is_progressive(x double precision, y double precision, end_x double precision, end_y double precision)
RETURNS BOOLEAN
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
    SELECT sqrt(power(105 - end_x, 2) + power(34 - end_y, 2))
           <= 0.75 * sqrt(power(105 - x, 2) + power(34 - y, 2))
$$;


-- ───────────────────────────────────────────────────────────────────────────
-- Euclidean length of an action vector, in metres. NULL-safe.
-- ───────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION gold.action_length(x double precision, y double precision, end_x double precision, end_y double precision)
RETURNS REAL
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
    SELECT sqrt(power(end_x - x, 2) + power(end_y - y, 2))::real
$$;
