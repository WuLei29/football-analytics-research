-- ═══════════════════════════════════════════════════════════════════════════
-- build_player_percentiles.sql — gold.player_season_percentiles
-- ═══════════════════════════════════════════════════════════════════════════
--
-- Spec: md/GOLD_LAYER.md §4.6.14 step 3, §6 Step 9.
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
-- FOUR RULES FROM §4.6.14, all enforced below:
--   1. peer set is (competition_season_id, primary_position_group), filtered
--      to meets_min_minutes. A per-90 rate over 40 minutes tops every table
--      forever, which is what the 450-minute gate exists to prevent.
--   2. peer_n is STORED. A percentile against 14 goalkeepers is a much weaker
--      statement than one against 90 midfielders and the reader cannot tell
--      without it.
--   3. only the metrics whose Positions column includes that group are
--      populated -- percentiling crosses_completed for centre backs produces
--      a confident-looking number describing nothing.
--   4. RB and LB are separate peer groups by default and pooled into FB only
--      when either is under 20, WITH THE POOLING RECORDED in pooled_fb.
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
      AND primary_position_group IS NOT NULL
),

-- ── decide whether the full-back groups need pooling, per season ───────────
group_n AS (
    SELECT primary_position_group AS grp, count(*) AS n
    FROM eligible
    GROUP BY 1
),
pool AS (
    SELECT COALESCE(bool_or(n < 20) FILTER (WHERE grp IN ('RB','LB')), FALSE) AS pool_fb
    FROM group_n
),

-- ── unpivot: one row per (player, applicable metric) ───────────────────────
long AS (
    SELECT
        e.competition_season_id,
        e.player_id,
        e.team_id,
        -- The metric set is chosen by the player's REAL group; the peer set it
        -- is ranked within may be the pooled one.
        CASE WHEN e.primary_position_group IN ('RB','LB') AND pool.pool_fb
             THEN 'FB' ELSE e.primary_position_group END AS peer_group,
        (e.primary_position_group IN ('RB','LB') AND pool.pool_fb) AS pooled_fb,
        m.metric,
        m.higher_is_better,
        (to_jsonb(e) ->> m.metric)::real AS value
    FROM eligible e
    CROSS JOIN pool
    JOIN gold.percentile_metrics m
      ON m.position_group = e.primary_position_group
),

ranked AS (
    SELECT
        competition_season_id, player_id, team_id, peer_group, pooled_fb,
        metric, value,
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
    -- ball would sit at the bottom of long_ball_accuracy.
    WHERE value IS NOT NULL
)

INSERT INTO gold.player_season_percentiles (
    competition_season_id, player_id, team_id, position_group,
    metric, value, percentile, peer_n, pooled_fb
)
SELECT competition_season_id, player_id, team_id, peer_group,
       metric, value, percentile, peer_n, pooled_fb
FROM ranked;
