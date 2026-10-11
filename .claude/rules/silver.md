---
paths:
  - "src/silver/**"
  - "src/bronze/**"
  - "src/scraper/**"
---

# Silver pipeline rules

- **Order event streams by `(json_index, event_id)`**, in SQL and in pandas.
  About 11.8k events share a `json_index` (mostly `Carry` and `Challenge`), so
  without the tiebreaker results change from run to run (GOLD_SEQUENCES §6, Fix 0).
- **Separation of concerns**: `parser.py`, `carries.py`, `xt.py` are pure
  transforms; all SQL goes in `db.py`; `processor.py` only orchestrates. New
  stages follow the same split.
- **The events pipeline has no delete step.** To re-run it over loaded
  matches, `TRUNCATE silver.events, gold.sequences, gold.sequence_players,
  gold.sequence_phase_segments`. Don't use `DELETE`; it ran for 15 minutes
  (GOLD_SEQUENCES §6.6). This is destructive, so ask first.
- **After a full events reload, run `VACUUM (FULL, ANALYZE) silver.events`.**
  The SPADL / VAEP / xG / foot / sequence UPDATEs leave the heap ~3× its real
  size, and gold's `sequences` step goes from 3 to 32 minutes. The vacuum takes
  ~90 s, an exclusive lock, and about the table's size in free disk.
- **VAEP backfill only finds `vaep_value IS NULL`.** To re-score, NULL the three
  `vaep_*` columns first. Read the `vaep-model` skill before touching `vaep.py`
  or features.
- **xG backfill** defaults to matches with NULL `xg`; `--match-ids` re-scores.
  Own goals feed game state and are never scored; penalties get 0.79.
- **Lineups**: exits apply only to a player on the pitch. Pass 2a runs before 2b.
  Run `load_match_lineups.py --reprocess` after loading squads late.
- **New-season gotchas** (`silver.teams.country`, NULL competition metadata,
  `status` set only at insert) are listed in CLAUDE.md. Read them before
  loading a new season.
- **`sequence_id` renumbers on every re-classification** (it's a per-team
  counter). When reporting a chain, quote match and kick-off time, not the id.
- `Ball touch` with `outcome='success'` is a deflection and correctly does
  **not** end a sequence. Don't "fix" it.
- Classifier fixes A and B are on by default; `--no-fix-a/b` exist only for A/B
  comparison, never for a production backfill.
