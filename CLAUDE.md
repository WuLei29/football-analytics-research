# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Purpose

Football analytics data platform — collecting, structuring, and analysing La Liga and Segunda Division match data (season 2025/26 onwards). Three areas: data engineering (ingestion pipelines), data modelling (relational silver schema, raw bronze, derived gold), and analytics (xG, xT, PPDA, possession sequences).

---

## Stack & Environment

| Component | Choice |
|-----------|--------|
| Database | PostgreSQL (schemas: `bronze`, `silver`, `gold`) |
| Language | Python 3 |
| DB driver | `psycopg2` (raw SQL; no ORM) |
| Event data | Opta (JSONP files) |
| Config | `.env` file -> `FOOTBALL_DB_DSN` environment variable |
| Dependencies | `psycopg2-binary`, `python-dotenv`, `pandas`, `numpy` |
| Scraper (optional extra) | `playwright` — install via `pip install -e ".[scraper]"` |

---

## How to Run Pipelines

All scripts use `FOOTBALL_DB_DSN` from `.env` (loaded via `python-dotenv`). Data files live in `data/raw/{competition_code}/{season_code}/matches/` and `data/raw/{competition_code}/{season_code}/squads/` (gitignored).

```bash
# Load matches into silver.matches
python src/silver/load_matches.py

# Load teams
python src/silver/load_teams.py

# Load match lineups (5-pass strategy)
python src/silver/load_match_lineups.py

# Load squads (diff strategy)
python -m src.silver.squads --raw-root data/raw

# Load events (full pipeline: parse + carries + xT)
python -m src.silver.events --raw-root data/raw

# Events with options
python -m src.silver.events --raw-root data/raw --dry-run
python -m src.silver.events --raw-root data/raw --no-carries --no-xt

# Enrich preferred_foot (players) + is_weak_foot (events)
python src/silver/foot_preference.py
python src/silver/foot_preference.py --dry-run

# Backfill SPADL columns, then VAEP (VAEP requires SPADL to be populated first)
python -m src.silver.events.spadl
python -m src.silver.events.vaep
python -m src.silver.events.vaep --match-ids 1 2 3
# NOTE: vaep only discovers matches WHERE vaep_value IS NULL. To re-score after a
# model change, NULL the three vaep_* columns first or the run silently does nothing.

# Scrape raw Opta match event files from scoresway.com (interactive, Playwright)
python -m src.scraper
python -m src.scraper --headless
```



**Load order matters**: `competitions, seasons -> competition_seasons -> teams, players -> matches -> match_lineups, events`

---

## Medallion Architecture

```
Bronze -> Silver -> Gold
```

| Layer | Role | Rule |
|-------|------|------|
| **Bronze** | Raw JSONP files and JSONB dumps exactly as received | Never modified after ingestion |
| **Silver** | All 10 relational tables — the source of truth | Never bypassed; always query here |
| **Gold** | Pre-computed aggregates | Fully derived from silver; recomputable at any time |

**Never** write from Gold back to Silver, or query Bronze to bypass Silver.

---

## Code Architecture

### Source Layout

```
src/
  bronze/
    parse_teams_bronze.py         # Raw team data parsing
  silver/
    load_matches.py               # Match ingestion (standalone script)
    load_match_lineups.py         # 5-pass lineup ingestion (standalone script)
    load_teams.py                 # Team ingestion (standalone script)
    foot_preference.py            # Standalone enrichment: preferred_foot + is_weak_foot
    events/                       # Events pipeline (run as module: python -m src.silver.events)
      __main__.py                 #   CLI entry point
      processor.py                #   Orchestrator — no SQL, no transforms
      parser.py                   #   JSONP -> event dicts (pure transform)
      carries.py                  #   Synthesised carry detection (pure transform)
      xt.py                       #   Expected Threat calculation (pure transform)
      spadl.py                    #   SPADL mapping + backfill (python -m src.silver.events.spadl)
      vaep.py                     #   VAEP valuation + backfill (python -m src.silver.events.vaep)
      db.py                       #   All SQL operations for events
      sequences.py                #   Possession sequence classifier (state machine)
    squads/                       # Squad pipeline (run as module: python -m src.silver.squads)
      __main__.py                 #   CLI entry point
      squad_processor.py          #   Orchestrator
      parser.py                   #   Squad JSON parsing
      scanner.py                  #   File discovery
      validator.py                #   Data validation
      db.py                       #   SQL operations
      models.py                   #   Data models
      preflight_squads.py         #   Pre-load checks
  gold/
    gold_sequences.py             #   Possession sequence aggregation (WIP)
  scraper/                        # Opta match file scraper (run as module: python -m src.scraper)
    __main__.py                   #   CLI entry point (--headless flag)
    scraper.py                    #   Playwright browser automation: navigates scoresway.com,
                                   #   discovers matchdays/matches, intercepts api.performfeeds.com
                                   #   responses, saves raw JSONP to data/raw/{competition}/{season}/matches/
```

### Architectural Patterns

- **Separation of concerns in events pipeline**: `parser.py` (pure transform, no DB), `carries.py` (pure transform), `xt.py` (pure transform), `db.py` (only SQL), `processor.py` (orchestration only). Follow this pattern when adding new pipeline stages.
- **FK resolution happens in SQL, not Python**: source IDs (e.g. `source_team_id`, `source_stage_id`) are resolved to internal IDs via SQL JOINs in INSERT statements. Exception: events pipeline resolves `team_id`/`player_id` via in-memory caches for performance.
- **Idempotency**: every loader uses `ON CONFLICT DO NOTHING` or `DO UPDATE`, or guards with a pre-check (events use `is_match_already_loaded`). Safe to re-run any pipeline.

---

## Silver Schema — 10 Tables

### Tier 1 — Competition Framework
- `competitions` — timeless competition identity, inserted once
- `seasons` — label table (`2025/2026`), shared across competitions
- `competition_seasons` — **central FK anchor** for all season-specific data; resolved via `source_stage_id`

### Tier 2 — Participants
- `teams` — stable club identity, inserted once
- `team_competition_seasons` — season enrollment; promotions/relegations add new rows
- `players` — stable player identity; `full_name` is `GENERATED ALWAYS AS` — **never include in INSERT**. `preferred_foot` derived from event qualifiers via `foot_preference.py`
- `player_squads` — squad membership per team per season; mid-season transfers = two rows

### Tier 3 — Match Data
- `matches` — one row per match; resolved via `source_match_id`
- `match_lineups` — one row per player per match; built via **5-pass ingestion strategy**
- `events` — granular event stream; includes synthesised carries (`type_id = -1`); guarded by `is_match_already_loaded`. `is_weak_foot` derived by `foot_preference.py` (not part of main events pipeline)

### Idempotency Patterns

| Table | Conflict Strategy |
|-------|-----------------|
| competitions, seasons, competition_seasons, teams | `DO NOTHING` |
| team_competition_seasons, matches | `DO NOTHING` |
| players | `DO UPDATE` (provider fields only) |
| match_lineups | `DO UPDATE` on `(match_id, player_id)` |
| events | No conflict key; guarded by match-level pre-check |
| player_squads | Diff strategy — close departed, insert arrivals |

---

## Opta Event Data — Key Facts

- **JSONP format**: strip hash prefix, parse JSON from first `{`, strip trailing `)`
- **Coordinate system**: provider 0-100 scale -> real pitch metres (X: 0-105, Y: 0-68) in silver
- **Key typeIds**: `34`=Team set up, `1`=Pass, `16`=Shot, `3`=Take on, `4`=Foul, `17`=Card, `18`=Player off, `19`=Player on, `-1`=Synthesised carry
- **5-pass lineup strategy**: Pass 1 (34) inserts all; Pass 2a (18) sets `minute_out`; Pass 2b (19) sets `minute_in`; Pass 2c/d (17 + Q33/Q32) sets red card exits. **2a must run before 2b.**
- **Shot play pattern** (Q22/23/24/25/26/160/9 — mutually exclusive): `regular_play`, `fast_break`, `set_piece`, `from_corner`, `free_kick`, `throw_in_set_piece`, `penalty`. Extracted to `shot_play_pattern` column; NULL for non-shots.
- **First-time shot** (Q328): boolean `first_time` column; NULL for non-shots.
- **Foot qualifiers** (Q20=Right foot, Q72=Left foot): appear on passes (typeId 1, 2) and shots (typeId 13, 14, 15, 16). Used by `foot_preference.py` to derive `preferred_foot` (players) and `is_weak_foot` (events).

### Common SQL Patterns

```sql
-- Minutes played (no event rescan needed)
COALESCE(ml.minute_out, m.match_length_min) - COALESCE(ml.minute_in, 0) AS minutes_played
FROM match_lineups ml JOIN matches m USING (match_id)

-- Temporal join: player's team on a match date
JOIN player_squads ps ON ps.player_id = e.player_id
WHERE match_date BETWEEN ps.start_date AND COALESCE(ps.end_date, '9999-12-31')

-- FK resolution via source IDs (never in app code)
WHERE cs.source_stage_id = :source_stage_id
```

---

## Skills Available

These project-specific skills are registered and should be consulted automatically:

- **`football-silver-schema`** — full table DDL, FK resolution, idempotency patterns, ingestion strategies. Use when writing or debugging ingestion scripts, silver SQL, or gold table design.
- **`opta-events-reference`** — full event typeId list, qualifier IDs, JSONP format, coordinate conversion, carry synthesis, sequence columns. Use when working on event pipeline files or event-level SQL.
- **`phases-of-play`** — possession sequence phase definitions and classification conditions. Use when working on sequence/phase logic.
- **`pitch-guide`** — language-agnostic pitch drawing primitives (all markings with exact coordinates and angles), the 30-zone grid (6x5), zone assignment logic in Python/SQL/JS. Use when drawing pitches, implementing zone assignment, or building spatial visualisations.
- **`spadl-mapping`** — full SPADL mapping reference: the three columns added to `silver.events` (`spadl_type_id`, `spadl_result_id`, `spadl_bodypart_id`), Opta event_type → action type decision tree, qualifier-based sub-type detection, result/body-part mapping, always-success/always-fail sets, backfill API, and VAEP downstream contract. Use when working on `spadl.py`, writing SPADL-filtered queries, or building gold-layer VAEP features.
- **`vaep-model`** — the retrained VAEP model: why the previous production weights were degenerate, the artifact contract (`vaep_model.json` + `metrics.json`, including the load-bearing Platt calibrator), the 148-feature set and the four semantics that are easy to get backwards, the frame convention, the formula and its four guards, the ten acceptance gates with measured values, and a symptom → cause → fix table. Use when working on `vaep.py`, `_build_features`, the VAEP backfill, `models/vaep/*.json`, or debugging the `vaep_*` columns — and **before any feature-set change**, since it must be mirrored in the training repo.
- **`karpathy-guidelines`** - Behavioral guidelines to reduce common LLM coding mistakes. Use when writing, reviewing, or refactoring code to avoid overcomplication, make surgical changes, surface assumptions, and define verifiable success criteria.

---

## Working Conventions

- All SQL is raw; no ORM.
- Match existing psycopg2 patterns and idempotency guards.
- Do not invent column names or table structures — derive from the documented schema or use skills.
- The `full_name` column on `players` is `GENERATED ALWAYS AS` — never include it in `INSERT` statements.
- Data files (`data/`) are gitignored; only `.gitkeep` placeholders are tracked.
- Every time you work with event_data and you want to work with game states or sequental actions, order by json_index column

---

## Current State

> Last updated: 11 August 2026

### Done
- Silver schema fully designed and operational for La Liga 2025/26 — all 10 tables
- Data ingestion pipelines built for matches, lineups, squads, teams, players, events (carries + xT + SPADL mapping)
- SPADL columns (`spadl_type_id`, `spadl_result_id`, `spadl_bodypart_id`) added to `silver.events`; backfill via `python -m src.silver.events.spadl`
- **VAEP is live.** `vaep_value` / `vaep_offensive` / `vaep_defensive` are populated across all 427 matches / 780,395 events. The previously shipped model was degenerate (`scores` AUC excluding goal actions **0.5222** — a coin flip; it had learned "was this a successful shot?"). It was retrained from scratch in a companion repo, Platt-calibrated on held-out Opta, and integrated on 11 Aug 2026: **0.7465** AUC excluding goal actions, `corr(vaep_offensive, xt)` **−0.1942 → +0.2605**. Backfill via `python -m src.silver.events.vaep`; consult the **`vaep-model`** skill before touching `vaep.py`, `_build_features` or `models/vaep/*.json`
- Possession sequence classifier implemented (`sequence_id`, `sequence_start`, `sequence_end`, `sequence_event_number`)
- Squad diff strategy live with `squad_snapshot_log` audit table
- Shot enrichment columns: `shot_play_pattern` (7-value enum from Q22/23/24/25/26/160/9) and `first_time` (bool from Q328)
- Foot preference enrichment: `preferred_foot` on `silver.players` (derived from Q20/Q72 counts) and `is_weak_foot` on `silver.events` (standalone script, not part of events pipeline)

### Immediate Next — Gold Layer
1. **Sequences & phases of play table** — derive from `silver.events.sequence_id`; classify by phase
2. **Player aggregation** (`player_season_stats`) — goals, assists, minutes, xG, xA, progressive passes
3. **Team aggregation** (`team_season_stats`) — W/D/L, GF/GA, xG, PPDA, per-phase breakdowns
4. **Match summaries** (`match_summaries`) — denormalised match rows for dashboards

### Future Scope
- Segunda Division (zero schema changes needed)
- Web visualisation on top of gold layer
- Sequence & phase clustering (unsupervised ML)
