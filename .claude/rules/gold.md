---
paths:
  - "src/gold/**"
  - "sql/**"
---

# Gold layer rules

Operating manual: `md/GOLD_LAYER.md §9`. Metric definitions: the
`football-metrics` skill.

- **A full rebuild is the intended posture** (`python -m src.gold.build_gold`,
  ~5 min). It avoids mixing rows computed under old and new definitions.
  `--step` / `--match-ids` are for iterating on a query, not for the weekend
  run. Run it after `python -m src.silver.events.sequences`, since gold reads
  `sequence_id`.
- **Run the §9.5 gates after any rebuild that follows a code change.** Their
  hard-coded expected counts date from 458 matches. Trust the invariants
  (symmetry, reconciliation with silver, shares summing to 1, coverage
  equality), not the literal numbers.
- **The phase pass is a hard dependency of the team build.** Without it,
  `counter_attack_sequences` and `high_transition_sequences` are zero
  league-wide, which looks plausible and is wrong. `build_gold.py` refuses to
  run the team step if `gold.sequence_phase_segments` is empty.
- **`sequence_number` is per team, not per match.** Ordering a match by it ties
  every value and corrupts any `lag()` / `lead()`. Order by start clock or
  `(json_index, event_id)`. Gate G10.
- **Percentiles read metrics via `to_jsonb(row) ->> metric`**, so a typo in
  `gold.percentile_metrics` yields NULL, not an error. Gate G12.
- **Position comes from the formation slot** (`gold.formation_slot_positions`),
  not `match_lineups.position` (`SUB` for substitutes) or
  `players.position_raw`. An unmapped slot must fail the build, not emit NULL.
- **Geometry goes through `sql/ddl/gold_functions.sql`** (`x_strip`,
  `y_channel`, `zone_id`, `third`, `in_box`, `is_progressive`,
  `action_length`). Don't inline a CASE. `gold.pitch_zones` is authoritative.
  Low `y` is the right flank.
- **Ratios and per-90s are STORED GENERATED columns** over summed inputs. Never
  `AVG` a rate. `padj_*` is adjusted per match and then summed.
- `build_sequences.sql` opens with `SET LOCAL enable_nestloop = off`. Keep it,
  or the step can stall past 20 min after a VACUUM (§9.8).
- `possession_pct` is clock-based, start-to-next-start (§4.5.0). Don't
  reintroduce the duration sum with a one-second floor.
- DDL is idempotent (`CREATE … IF NOT EXISTS`, `ADD COLUMN IF NOT EXISTS`). One-off
  data repairs go in `sql/fixes/YYYYMMDD_*.sql`.
