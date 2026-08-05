# Football Analytics — Pipeline Operational Guide

## Table of Contents
1. [Dependency Map](#1-dependency-map)
2. [From Scratch — Full Initial Load](#2-from-scratch--full-initial-load)
3. [Updating the Database — New Matches](#3-updating-the-database--new-matches)
4. [Updating the Database — Mid-Season Squad Changes](#4-updating-the-database--mid-season-squad-changes)
5. [Adding a New Competition or Season](#5-adding-a-new-competition-or-season)
6. [Extending the Gold Layer](#6-extending-the-gold-layer)
7. [Idempotency Reference](#7-idempotency-reference)
8. [Common Errors & Fixes](#8-common-errors--fixes)

---

## 1. Dependency Map

Every table has foreign key dependencies. You must always respect this load order — running a script out of sequence will silently skip rows or fail.

```
[MANUAL SEEDS]
    competitions
    seasons
    competition_seasons
         │
         ▼
    teams  ◄── parse_teams_bronze.py → load_teams.py
    team_competition_seasons  ◄── load_teams.py
         │
         ▼
    players  ◄─────────────────┐
    player_squads  ◄────────── python -m src.silver.squads
         │
         ▼
    matches  ◄──────────────── load_matches.py
         │
         ▼
    match_lineups  ◄──────────  load_match_lineups.py
         │
         ▼
    events  ◄───────────────── python -m src.silver.events
         │
         ▼
    players.preferred_foot  ◄── foot_preference.py                      [auto in Phase 6]
    events.is_weak_foot     ◄── foot_preference.py                      [auto in Phase 6]
    events.xg               ◄── python -m src.silver.events.xg          [auto in Phase 6]
         │
         ▼
    events (sequence cols)  ◄── python -m src.silver.events.sequences   [Phase 7]
         │
         ▼
    gold.sequences  ◄────────── NOT YET IMPLEMENTED                     [Phase 8, WIP]
```

**Hard rules:**
- `matches` requires `competition_seasons` and `teams`
- `match_lineups` requires `matches` and `players`
- `events` requires `matches`, `teams`, and `players`
- `team_competition_seasons` requires `teams` and `competition_seasons`
- **Phase 7** (sequence classification) requires `events` to be fully loaded for the target matches
- **Phase 8** (gold sequences) is not built yet — see the note in [§2 Phase 8](#phase-8--build-gold-sequences-wip)

---

## 2. From Scratch — Full Initial Load

Use this sequence when setting up the database for the first time for a new season.

### Phase 0 — Manual Seeds (once per season)

These three tables have no script — you populate them manually via SQL or a seed CSV.

**`competitions`** — one row per league, inserted once and never touched again:
```sql
INSERT INTO silver.competitions (source_competition_id, name, known_name, country, confederation, tier_level)
VALUES ('PRD', 'Primera División', 'Spanish La Liga', 'Spain', 'UEFA', 1),
       ('SD',  'Segunda División', NULL,              'Spain', 'UEFA', 2);
```

**`seasons`** — one row per season label, shared across all competitions:
```sql
INSERT INTO silver.seasons (label) VALUES ('2025/2026');
```

**`competition_seasons`** — one row per competition × season edition:
```sql
INSERT INTO silver.competition_seasons (
    source_season_id, source_stage_id, competition_id, season_id,
    stage_name, start_date, status
)
VALUES (
    '<tournamentCalendarId from provider>',
    '<stage.id from provider>',
    1, 1,  -- competition_id and season_id from the rows above
    'Regular Season', '2025-08-17', 'active'
);
```

> **Where to find provider IDs:** open any raw match file. `matchInfo.stage.id` is your `source_stage_id`. `matchInfo.tournamentCalendar.id` is your `source_season_id`. Both appear in every match file for that competition.

---

### Phase 1 — Bronze Team Extraction

Reads all squad files and produces one JSON per competition in `data/teams/`.

```bash
cd src/bronze
python parse_teams_bronze.py
```

**Output:** `data/teams/Primera División.json`, `data/teams/Segunda División.json`, etc.

**Check:** Each JSON should have a `team_count` matching the number of clubs in that division (e.g. 20 for La Liga).

---

### Phase 2 — Load Teams into Silver

Reads the bronze JSONs, writes seed CSVs, and inserts into `teams` + `team_competition_seasons`.

```bash
python src/silver/load_teams.py
```

**Output:** rows in `silver.teams` and `silver.team_competition_seasons`.

**After this step:** manually enrich any columns not available from the provider — `city`, `stadium_capacity`, `founded_year`, `kit_home_color`, `kit_away_color`, `badge_url` — either via SQL UPDATE or by editing the seed CSVs and re-running (safe due to `ON CONFLICT DO NOTHING`).

---

### Phase 3 — Load Players & Squads

Processes all squad snapshot files under `data/raw/`. Automatically detects initial vs. diff loads.

```bash
python -m src.silver.squads --raw-root data/raw
```

**Output:** rows in `silver.players` and `silver.player_squads`.

**After this step:** manually enrich player attributes not in the provider feed: `date_of_birth`, `preferred_position`, `preferred_foot`, `height_cm`.

---

### Phase 4 — Load Matches

Scans all `data/raw/{competition_code}/{season}/matches/` directories and inserts completed matches.

```bash
python src/silver/load_matches.py
```

**Output:** rows in `silver.matches`.

---

### Phase 5 — Load Match Lineups

5-pass ingestion that populates starting XIs, substitutions, and card-related exits.

```bash
python src/silver/load_match_lineups.py

python src/silver/load_match_lineups.py 2>&1 | tee lineups_run.log 
```

**Output:** rows in `silver.match_lineups`.

> **Important:** this script skips any match whose `source_match_id` is not yet in `silver.matches`. Always run Phase 4 first.

---

### Phase 6 — Load Events

Full event stream ingestion, including optional carry synthesis and xT calculation.

```bash
python -m src.silver.events \
    --raw-root data/raw \
    --events-map data/mapping/opta-events.js \
    --qualifiers-map data/mapping/opta-qualifiers.js
```

**Flags:**
| Flag | Effect |
|---|---|
| `--no-carries` | Skip carry synthesis (faster, useful for testing) |
| `--no-spadl` | Skip SPADL column mapping |
| `--no-xt` | Skip xT calculation |
| `--no-vaep` | Skip VAEP calculation |
| `--no-xg` | Skip post-processing (foot preference + xG backfill) |
| `--dry-run` | Parse and resolve IDs but write nothing to the DB |
| `--no-skip-existing` | Re-load matches already in `silver.events` |

**Output:** rows in `silver.events` with xT, VAEP, and xG values, plus synthesised carry events. Also enriches `silver.players.preferred_foot` and `silver.events.is_weak_foot`. The `sequence_*` columns are `NULL` at this point — they are populated in Phase 7.

**Post-processing (automatic):** after all match files are inserted, the pipeline runs foot preference enrichment (`preferred_foot` + `is_weak_foot`) then xG backfill. Skip with `--no-xg`. Standalone:

```bash
python src/silver/foot_preference.py               # foot preference only
python -m src.silver.events.xg                     # xG backfill (auto-discovers NULL xG)
python -m src.silver.events.xg --match-ids 1 2 3   # specific matches
```

---

### Phase 7 — Classify Possession Sequences

Reads events from `silver.events`, runs the possession-sequence classifier, and writes back the four sequence columns: `sequence_id`, `sequence_start`, `sequence_end`, `sequence_event_number`.

```bash
python -m src.silver.events.sequences
```

**Flags:**
| Flag | Effect |
|---|---|
| `--match-ids 42 43 44` | Process only specific match_ids (space-separated) |
| `--limit 100` | Cap the number of matches to process in one run |
| `--batch-size 50` | Matches loaded into memory at a time (default: 50) |

**What it does internally:**
1. Discovers matches with no sequence data (or uses explicit `--match-ids`)
2. Resets existing sequence columns for those matches (idempotency)
3. Pulls events into a DataFrame
4. Pre-processes: filters excluded rows (period 14/16, FormationChange, FormationSet), materialises qualifier flags (`is_set_piece_pass`, `is_dead_ball_goal`) from `raw_data`
5. Runs the state-machine classifier
6. Batch-UPDATEs `silver.events` with the results

**Output:** `silver.events` rows updated with `sequence_id`, `sequence_start`, `sequence_end`, `sequence_event_number`.

> **When to re-run:** after fixing classifier logic or adding new rules. The function resets all sequence columns for the target matches before re-classifying, so it is fully idempotent.

---

### Phase 8 — Build Gold Sequences (WIP)

**Not yet implemented.** The plan is to aggregate classified events from `silver.events` into `gold.sequences` — one row per possession sequence with derived metrics (length, duration, xT, pass/carry/shot counts, start/end zones, flags).
`src/gold/gold_sequences.py` currently holds a stray duplicate of the Phase 7 classifier (its own docstring says `Placement: src/silver/events/sequences.py`) rather than aggregation logic — there is no gold table DDL, no `_aggregate_sequences()`, and no CLI/`__main__` entry point yet. Track it as an open item (see `CLAUDE.md` → "Immediate Next").

---

## 3. Updating the Database — New Matches

When new match files arrive (weekly jornada update), you need to run Phases 4–7 (Phase 8 is not yet implemented — see note above). All scripts are idempotent — they skip anything already loaded.

### Step-by-step

**1. Drop new match files into the correct folder:**
```
data/raw/PRD/2025-26/matches/<new_match_file>
```

**2. Run matches loader:**
```bash
python src/silver/load_matches.py
```

**3. Run lineups loader:**
```bash
python src/silver/load_match_lineups.py
```

**4. Run events loader:**
```bash
python -m src.silver.events --raw-root data/raw \
    --events-map data/mapping/opta-events.js \
    --qualifiers-map data/mapping/opta-qualifiers.js
```

> This automatically runs foot preference enrichment and xG backfill as post-processing. Use `--no-xg` to skip.

**5. Run sequence classifier:**
```bash
python -m src.silver.events.sequences
```

If we need to truncate the sequence classifier:

```bash
UPDATE silver.events
SET sequence_id           = NULL,
    sequence_start        = FALSE,
    sequence_end          = FALSE,
    sequence_event_number = 0;
```

**6. Gold sequences builder — not yet implemented, skip for now.**

That's it. The idempotency guards in each script ensure already-loaded/classified matches are skipped automatically. Phase 7 only picks up matches that have new events but no sequence data yet.

---

## 4. Updating the Database — Mid-Season Squad Changes

When the January window opens or a player joins/leaves a club, drop the new squad snapshot files into the correct folder and re-run the squads loader.

**1. Drop new squad snapshot into:**
```
data/raw/PRD/2025-26/squads/2026-01-31/<source_team_id>.json
```

**2. Run squads loader:**
```bash
python -m src.silver.squads --raw-root data/raw
```

The processor's diff logic handles everything automatically:
- Players no longer in the snapshot → their `player_squads.end_date` is set to the snapshot date
- New arrivals → a new `player_squads` row is opened with `start_date = snapshot_date`
- Shirt number changes → updated in place (no new row)

No action needed for matches, lineups, events, or sequences — historical rows reference `player_id`, not squad membership, so they stay correctly attributed.

---

## 5. Adding a New Competition or Season

**New season (next year):**
1. Insert one row into `seasons`
2. Insert rows into `competition_seasons` for each participating league
3. Re-run Phase 1 (bronze teams) and Phase 2 (load teams) for new clubs from promotion/relegation
4. Run Phase 3 for the new squad snapshots
5. Run Phases 4–7 as matches arrive (Phase 8 not yet implemented)

**New competition (e.g. adding Copa del Rey):**
1. Insert one row into `competitions`
2. Insert rows into `competition_seasons` for each edition to track
3. Everything else is identical — same scripts, same commands

No schema changes are ever required.

---

## 6. Extending the Gold Layer

The gold layer is fully derived from silver and can be rebuilt at any time. This makes it safe to evolve iteratively.

> **Status:** `gold.sequences` and its builder are not built yet (see [Phase 8](#phase-8--build-gold-sequences-wip)). The plan below is the intended pattern once it exists.

### Adding new columns to `gold.sequences` (planned)

When you need new derived metrics (e.g. `progressive_passes`, `final_third_entries`, `ppda_contribution`):

1. **Add the column to the table:**
   ```sql
   ALTER TABLE gold.sequences ADD COLUMN progressive_passes INT DEFAULT 0;
   ```

2. **Update the gold builder** — add the aggregation logic and include the new column in the insert column list and UPSERT clause.

3. **Re-run Phase 8** for all matches to backfill, or truncate and rebuild all.

> **Nuclear option:** once the builder exists, a large schema change can `DROP TABLE gold.sequences` and re-run Phase 8 to recreate it from scratch — always safe because gold is never a source of truth.

### Adding new gold tables

Future gold tables (`team_season_stats`, `player_season_stats`, `match_summaries`) follow the same pattern:
- DDL lives inside the module (or in `sql/gold/`)
- A `build_*` function reads from silver, aggregates, and inserts
- Idempotency via DELETE + INSERT per match/season batch
- Can be fully rebuilt from silver at any time

---

## 7. Idempotency Reference

All silver scripts are safe to re-run. Here's how each one handles duplicates:

| Script | Deduplication key | Conflict behaviour |
|---|---|---|
| `load_teams.py` | `source_team_id` | `ON CONFLICT DO NOTHING` |
| `src.silver.squads` | `(source_team_id, source_season_id, snapshot_date)` in log table | Entire file skipped |
| `load_matches.py` | `source_match_id` | `ON CONFLICT DO NOTHING` |
| `load_match_lineups.py` | `match_id` presence in `match_lineups` | Entire match skipped |
| `src.silver.events` | `match_id` presence in `silver.events` | Entire match skipped |
| `foot_preference.py` | None — always re-derives from full event data | UPDATE (idempotent) |
| `src.silver.events.xg` | `match_id` — auto-discovers shots with NULL xG | UPDATE xg column |
| `src.silver.events.sequences` | `match_id` — resets sequence cols before re-classifying | UPDATE with fresh values |
| gold sequences builder | *(not yet implemented — planned: `match_id`, DELETE + INSERT)* | — |

---

## 8. Common Errors & Fixes

**`FOOTBALL_DB_DSN is not set`**
Your `.env` file is missing or in the wrong location. The scripts look for it at the project root. Create it:
```
FOOTBALL_DB_DSN=postgresql://user:password@localhost:5432/football
```

**`Match not in DB yet — load matches first`** (from lineups or events loader)
The match file exists but `load_matches.py` hasn't processed it yet, or the `competition_seasons` seed is missing its `source_stage_id`. Run Phase 4 first and verify Phase 0 is complete.

**`team not found in DB` / `player not found in DB`** (from lineups loader)
The team or player isn't in `silver.teams` / `silver.players`. For teams: re-run Phases 1 and 2. For players: re-run Phase 3 (squads). A player appearing in a lineup but not in any squad file is normal for new signings — their squad file needs to be present before the lineup loader runs.

**`0 rows inserted` for a lineup** (lineups loader)
The raw match file has no `typeId: 34` (Team set up) event, or all player IDs failed to resolve. Check the `coverage_level` field on the match — low-coverage matches may have incomplete event data.

**Squad diff closing every player on first run**
This happens when the `squad_snapshot_log` table is empty (or was cleared) and the script treats a snapshot as an initial load for a team that already has rows. Fix: ensure `squad_snapshot_log` has not been truncated independently of `player_squads`.

**`No sequences found for batch`** (Phase 7)
The classifier found no events matching start conditions. This usually means the events for those matches have unusual coverage. Check `coverage_level` on the match — low-coverage matches may lack pass/tackle events. You can inspect with:
```sql
SELECT event_type, COUNT(*) FROM silver.events
WHERE match_id = <id> GROUP BY event_type ORDER BY count DESC;
```

**`Column 'value_assist' not found`** (Phase 7 warning)
The classifier logs a warning but continues. Assist-based sequence guards will be inactive — assist passes may incorrectly break sequences. To fix, add the column:
```sql
ALTER TABLE silver.events ADD COLUMN value_assist FLOAT;
```
Then populate it from `raw_data` qualifier 210 and re-run Phase 7.

**xG values are NULL after events pipeline**
The xG model files are missing from `models/xg/`, or the pipeline was run with `--no-xg`. Ensure the four model artifacts are in place and run the standalone backfill:
```bash
python -m src.silver.events.xg
```

**Gold sequences out of sync after classifier fix** *(applies once Phase 8 is implemented)*
If you changed the classifier logic and re-ran Phase 7, Phase 8 won't automatically detect the change (the gold rows already exist for those matches). Force a rebuild via the gold builder's `--match-ids` flag, or truncate `gold.sequences` and rebuild all.