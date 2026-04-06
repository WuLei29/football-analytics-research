# Football Analytics — Pipeline Operational Guide

## Table of Contents
1. [Dependency Map](#1-dependency-map)
2. [From Scratch — Full Initial Load](#2-from-scratch--full-initial-load)
3. [Updating the Database — New Matches](#3-updating-the-database--new-matches)
4. [Updating the Database — Mid-Season Squad Changes](#4-updating-the-database--mid-season-squad-changes)
5. [Adding a New Competition or Season](#5-adding-a-new-competition-or-season)
6. [Idempotency Reference](#6-idempotency-reference)
7. [Common Errors & Fixes](#7-common-errors--fixes)

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
```

**Hard rules:**
- `matches` requires `competition_seasons` and `teams`
- `match_lineups` requires `matches` and `players`
- `events` requires `matches`, `teams`, and `players`
- `team_competition_seasons` requires `teams` and `competition_seasons`

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
| `--no-xt` | Skip xT calculation |
| `--dry-run` | Parse and resolve IDs but write nothing to the DB |
| `--no-skip-existing` | Re-load matches already in `silver.events` |

**Output:** rows in `silver.events` with xT values and synthesised carry events.

---

## 3. Updating the Database — New Matches

When new match files arrive (weekly jornada update), you only need to run Phases 4–6. All three scripts are idempotent — they skip anything already loaded.

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

That's it. The idempotency guards in each script ensure already-loaded matches are skipped automatically.

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

No action needed for matches, lineups, or events — historical rows reference `player_id`, not squad membership, so they stay correctly attributed.

---

## 5. Adding a New Competition or Season

**New season (next year):**
1. Insert one row into `seasons`
2. Insert rows into `competition_seasons` for each participating league
3. Re-run Phase 1 (bronze teams) and Phase 2 (load teams) for new clubs from promotion/relegation
4. Run Phase 3 for the new squad snapshots
5. Run Phases 4–6 as matches arrive

**New competition (e.g. adding Copa del Rey):**
1. Insert one row into `competitions`
2. Insert rows into `competition_seasons` for each edition to track
3. Everything else is identical — same scripts, same commands

No schema changes are ever required.

---

## 6. Idempotency Reference

All silver scripts are safe to re-run. Here's how each one handles duplicates:

| Script | Deduplication key | Conflict behaviour |
|---|---|---|
| `load_teams.py` | `source_team_id` | `ON CONFLICT DO NOTHING` |
| `src.silver.squads` | `(source_team_id, source_season_id, snapshot_date)` in log table | Entire file skipped |
| `load_matches.py` | `source_match_id` | `ON CONFLICT DO NOTHING` |
| `load_match_lineups.py` | `match_id` presence in `match_lineups` | Entire match skipped |
| `src.silver.events` | `match_id` presence in `silver.events` | Entire match skipped |

---

## 7. Common Errors & Fixes

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