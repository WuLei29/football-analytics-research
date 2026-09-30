-- ═══════════════════════════════════════════════════════════════════════════
-- 20260929_match_lineups_exits.sql — repair minute_out / exit_reason
-- ═══════════════════════════════════════════════════════════════════════════
--
-- Until 29 Sep 2026, load_match_lineups.py pass 2c/d set minute_out on EVERY
-- Q32/Q33 card, and nothing handled typeId 20 (Player retired):
--
--   * 4 players substituted off and then sent off from the bench had
--     minute_out overwritten with the card minute (minutes inflated in gold);
--   * 10 unused substitutes sent off from the bench got minute_out and
--     exit_reason although they never played;
--   * 6 players who retired injured (no substitutes left) had minute_out NULL,
--     i.e. counted as playing to the final whistle.
--
-- The loader is fixed; it skips matches it has already loaded, so this script
-- repairs the stored rows. It recomputes every lineup row's exit from
-- silver.events and writes only the rows that differ. Idempotent: a second run
-- updates 0 rows.
--
-- Exit rule (the loader's, now): the FIRST of Player off (substitution),
-- Player retired (retired), or a Q33 / Q32 card (red_card / second_yellow)
-- while the player is on the pitch, i.e. in the starting XI or brought on by
-- an earlier Player on. Ordered by (json_index, event_id).
--
-- Run:  psql "$FOOTBALL_DB_DSN" -f sql/fixes/20260929_match_lineups_exits.sql
-- Then: python -m src.gold.build_gold --from-step player_match_stats --skip-ddl
-- ═══════════════════════════════════════════════════════════════════════════

-- 'retired' joins the allowed exit reasons.
ALTER TABLE silver.match_lineups DROP CONSTRAINT IF EXISTS match_lineups_exit_reason_check;
ALTER TABLE silver.match_lineups ADD CONSTRAINT match_lineups_exit_reason_check
    CHECK (exit_reason IN ('substitution', 'red_card', 'second_yellow', 'retired'));

WITH ev AS (
    SELECT e.match_id, e.player_id, e.minute, e.json_index, e.event_id,
           CASE
               WHEN e.event_type = 'Player off'     THEN 'substitution'
               WHEN e.event_type = 'Player retired' THEN 'retired'
               WHEN e.event_type = 'Card'
                AND e.raw_data->'qualifier' @> '[{"qualifierId": 33}]' THEN 'red_card'
               WHEN e.event_type = 'Card'
                AND e.raw_data->'qualifier' @> '[{"qualifierId": 32}]' THEN 'second_yellow'
           END AS reason
    FROM silver.events e
    WHERE e.player_id IS NOT NULL
      AND e.event_type IN ('Player off', 'Player retired', 'Card')
),
exits AS (
    SELECT DISTINCT ON (ml.lineup_id)
           ml.lineup_id, ev.minute, ev.reason
    FROM silver.match_lineups ml
    JOIN ev ON ev.match_id = ml.match_id AND ev.player_id = ml.player_id
    WHERE ev.reason IS NOT NULL
      AND (ml.starting_xi
           OR EXISTS (SELECT 1 FROM silver.events s
                      WHERE s.match_id = ml.match_id AND s.player_id = ml.player_id
                        AND s.event_type = 'Player on'
                        AND (s.json_index, s.event_id) < (ev.json_index, ev.event_id)))
    ORDER BY ml.lineup_id, ev.json_index, ev.event_id
),
target AS (
    SELECT ml.lineup_id, x.minute::smallint AS minute_out, x.reason AS exit_reason
    FROM silver.match_lineups ml
    LEFT JOIN exits x USING (lineup_id)
)
UPDATE silver.match_lineups ml
SET minute_out  = t.minute_out,
    exit_reason = t.exit_reason
FROM target t
WHERE ml.lineup_id = t.lineup_id
  AND (ml.minute_out, ml.exit_reason) IS DISTINCT FROM (t.minute_out, t.exit_reason);
