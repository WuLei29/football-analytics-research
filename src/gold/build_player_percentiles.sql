-- ═══════════════════════════════════════════════════════════════════════════
-- build_player_percentiles.sql — gold.player_season_percentiles
-- ═══════════════════════════════════════════════════════════════════════════
--
-- Spec: md/GOLD_LAYER.md §4.6.14 step 3, §4.6.2a (roles), §6 Step 9.
--
-- Runs as:  python -m src.gold.build_gold --step player_percentiles
--           (the runner loops over every competition_season_id)
--
-- Parameter
--   %(cs_id)s :: int   the competition_season to rebuild.
--
-- Reads gold.player_season_stats and gold.percentile_metrics. Nothing else.
--
-- HOW THIS STAYS DATA-DRIVEN WITHOUT DYNAMIC SQL. The metric set lives in
-- gold.percentile_metrics as ROWS, but on gold.player_season_stats it lives as
-- COLUMNS. Rather than generate SQL, the season row is converted with
-- to_jsonb() and the metric plucked out by name: `to_jsonb(e) ->> m.metric`.
-- Adding a metric to the percentile set is then a one-row INSERT into
-- gold.percentile_metrics and nothing here changes. The cost is that a typo in
-- a metric name yields NULL rather than an error -- gate G13 in §9.5 catches
-- exactly that.
--
-- SIX RULES FROM §4.6.14, all enforced below:
--   1. the peer set is (competition_season_id, role), filtered to
--      meets_min_minutes. The role is player_season_stats.role_group: the
--      formation-slot group, with high-playing midfielders split out as AM.
--      A per-90 rate over 40 minutes tops every table forever, which is what
--      the 450-minute gate exists to prevent.
--   2. peer_n is STORED. A percentile against 14 goalkeepers is a much weaker
--      statement than one against 90 midfielders and the reader cannot tell
--      without it.
--   3. only the metrics whose row lists the player's ROLE are written --
--      percentiling crosses_completed for centre backs produces a
--      confident-looking number describing nothing.
--   4. RB and LB are separate peer groups by default and pooled into FB only
--      when either is under 20; AM is pooled back into MID when it is under
--      20. Both are RECORDED (pooled_fb, pooled_am).
--   5. a pooled peer set is ranked on the union of its roles' metrics, so an
--      AM pooled into MID is ranked on take_ons_per_90 against every
--      midfielder, not only against the other AMs. Rows are then written for
--      each player's own metric set only.
--   6. a rate whose denominator (`attempts_column`) is under `min_attempts`
--      is not ranked: 4 of 5 crosses is not an 80 percent crosser. (No
--      percent sign anywhere in this file: psycopg2 reads it as a parameter.)
--
-- Direction is handled by negating the sort key where higher_is_better is
-- FALSE (dispossessed, errors, dribbled_past, fouls_committed, ...), so a
-- percentile is always "higher is better" to the reader.
-- ═══════════════════════════════════════════════════════════════════════════

DELETE FROM gold.player_season_percentiles WHERE competition_season_id = %(cs_id)s;


WITH eligible AS (
    SELECT *
    FROM gold.player_season_stats
    WHERE competition_season_id = %(cs_id)s
      AND meets_min_minutes
      AND role_group IS NOT NULL
),

-- ── decide which small roles need pooling, per season ──────────────────────
role_n AS (
    SELECT role_group AS grp, count(*) AS n
    FROM eligible
    GROUP BY 1
),
pool AS (
    SELECT COALESCE(bool_or(n < 20) FILTER (WHERE grp IN ('RB','LB')), FALSE) AS pool_fb,
           COALESCE(bool_or(n < 20) FILTER (WHERE grp = 'AM'), FALSE)         AS pool_am
    FROM role_n
),
peers AS (
    SELECT e.*,
           CASE WHEN e.role_group IN ('RB','LB') AND pool.pool_fb THEN 'FB'
                WHEN e.role_group = 'AM'         AND pool.pool_am THEN 'MID'
                ELSE e.role_group END                                  AS peer_group,
           (e.role_group IN ('RB','LB') AND pool.pool_fb)              AS pooled_fb,
           (e.role_group = 'AM' AND pool.pool_am)                      AS pooled_am
    FROM eligible e
    CROSS JOIN pool
),

-- ── rule 5: every metric of every role that ranks in a peer group ──────────
-- One row per (peer_group, metric). The seed lists each metric once with its
-- direction and floor, so DISTINCT cannot fold two different specs together.
peer_metrics AS (
    SELECT DISTINCT p.peer_group, m.metric, m.higher_is_better,
           m.attempts_column, m.min_attempts
    FROM (SELECT DISTINCT role_group, peer_group FROM peers) p
    JOIN gold.percentile_metrics m ON m.position_group = p.role_group
),

-- ── unpivot: one row per (player, metric ranked in their peer group) ───────
long AS (
    SELECT
        p.competition_season_id, p.player_id, p.team_id, p.peer_group,
        p.pooled_fb, p.pooled_am,
        pm.metric, pm.higher_is_better,
        (own.metric IS NOT NULL) AS is_own,
        CASE WHEN pm.attempts_column IS NULL
               OR (to_jsonb(p) ->> pm.attempts_column)::real >= pm.min_attempts
             THEN (to_jsonb(p) ->> pm.metric)::real
        END AS value
    FROM peers p
    JOIN peer_metrics pm ON pm.peer_group = p.peer_group
    LEFT JOIN gold.percentile_metrics own
      ON own.metric = pm.metric AND own.position_group = p.role_group
),

ranked AS (
    SELECT
        competition_season_id, player_id, team_id, peer_group, pooled_fb,
        pooled_am, metric, value, is_own,
        (percent_rank() OVER (
            PARTITION BY competition_season_id, peer_group, metric
            ORDER BY CASE WHEN higher_is_better THEN value ELSE -value END
        ) * 100.0)::real AS percentile,
        count(*) OVER (
            PARTITION BY competition_season_id, peer_group, metric
        )::smallint AS peer_n
    FROM long
    -- A player with no value for a metric is not ranked last on it; they are
    -- not ranked at all. Otherwise every keeper who never attempted a long
    -- ball would sit at the bottom of long_ball_accuracy. The same holds for
    -- a value under its attempt floor (rule 6).
    WHERE value IS NOT NULL
)

INSERT INTO gold.player_season_percentiles (
    competition_season_id, player_id, team_id, position_group,
    metric, value, percentile, peer_n, pooled_fb, pooled_am
)
SELECT competition_season_id, player_id, team_id, peer_group,
       metric, value, percentile, peer_n, pooled_fb, pooled_am
FROM ranked
WHERE is_own;
