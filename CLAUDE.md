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

### Full run order — new season, or new matchdays of an existing one

Every step below is idempotent and scans the whole `data/raw` tree, so re-running
the sequence over already-loaded seasons is safe and cheap. **Run them in this
order** — each resolves FKs written by the one before it.

```bash
# 0. Scrape raw Opta match files (interactive, Playwright).
#    Prompts for the results URL, league code (PRD) and season code (2026-2027);
#    creates data/raw/{league}/{season}/matches/ itself. Squad JSON is downloaded
#    by hand into data/raw/{league}/{season}/squads/{YYYY-MM-DD}/{team_id}.json
python -m src.scraper
python -m src.scraper --headless

# 1. Seed silver.seasons + silver.competition_seasons from the raw match files.
#    MUST run before load_matches for a new season — every other loader resolves
#    competition_season_id via source_stage_id, and if the anchor row is missing
#    load_matches inserts nothing and reports it as "skipped", with no error.
python src/silver/load_competition_seasons.py
python src/silver/load_competition_seasons.py --dry-run

# 2. Teams. parse_teams_bronze reads the EARLIEST squad snapshot of each season
#    and writes data/teams/{COMP}_{season}.json; load_teams then loads
#    teams + team_competition_seasons.
python src/bronze/parse_teams_bronze.py
python src/silver/load_teams.py

# 3. Squads (diff strategy) — creates silver.players, which match_lineups needs.
python -m src.silver.squads --raw-root data/raw

# 4. Matches, then lineups (5-pass strategy).
python src/silver/load_matches.py
python src/silver/load_match_lineups.py

# 5. Events. This is the FULL pipeline: parse + carries + xT + SPADL + VAEP,
#    then foot-preference enrichment and xG as post-processing. For a newly
#    loaded match nothing else needs running — see the note below.
python -m src.silver.events --raw-root data/raw

# Events with options
python -m src.silver.events --raw-root data/raw --dry-run
python -m src.silver.events --raw-root data/raw --no-carries --no-xt
python -m src.silver.events --raw-root data/raw --no-spadl --no-vaep

# 6. Possession sequences. NOT part of the events pipeline — always a separate
#    step. Auto-discovers matches with no sequence_id.
python -m src.silver.events.sequences
python -m src.silver.events.sequences --match-ids 1 2 3

# 7. Gold layer. Full rebuild of all four gold tables in dependency order,
#    ~5 minutes. MUST run after step 6 — it reads sequence_id. Operating
#    manual, validation gates and troubleshooting: md/GOLD_LAYER.md §9.
python -m src.gold.build_gold
python -m src.gold.build_gold --step team_match_stats
```

### Backfill-only tools

These re-derive columns on rows that are **already loaded**. They are not part of
the new-season path — step 5 above already populates all of them.

```bash
# Enrich preferred_foot (players) + is_weak_foot (events)
python src/silver/foot_preference.py
python src/silver/foot_preference.py --dry-run

# Backfill SPADL columns, then VAEP (VAEP requires SPADL to be populated first)
python -m src.silver.events.spadl
python -m src.silver.events.vaep
python -m src.silver.events.vaep --match-ids 1 2 3
# NOTE: vaep only discovers matches WHERE vaep_value IS NULL. To re-score after a
# model change, NULL the three vaep_* columns first or the run silently does nothing.
```

**Load order matters**: `competitions -> seasons, competition_seasons -> teams, players -> matches -> match_lineups, events -> sequences`

### New-season gotchas

- **`silver.teams.country` is `NOT NULL` and is not in the provider squad feed.**
  `load_teams.py` therefore refuses to insert a new club until `country` is
  supplied by hand (`city` too — nullable, but filled for every existing row).
  It raises a named error listing the offending clubs. Insert them directly into
  `silver.teams`, then re-run. Do **not** try to lean on `ON CONFLICT DO NOTHING`
  here: a `NOT NULL` violation is raised while the tuple is formed, *before* the
  unique index is consulted, so `DO NOTHING` never sees it and the whole batch
  aborts — including on clubs that were already loaded and enriched.
- **`num_teams` / `total_matchdays` / `promo_spots` / `relegation_spots`** are not
  in the feed either. `load_competition_seasons.py` leaves them NULL; set them by
  hand if you need them.
- **`status` is derived from the date window at insert time only.** An existing
  row is never updated, so a season will not flip to `completed` on its own.

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
    load_competition_seasons.py   # Seeds seasons + competition_seasons (run FIRST for a new season)
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
  gold/                           # Gold layer — spec in md/GOLD_LAYER.md, MANUAL in §9
    build_gold.py                 #   Runner; enforces the §5 dependency order (python -m src.gold.build_gold)
    build_sequences.sql           #   gold.sequences + gold.sequence_players
    phases.py                     #   Zone x tempo state machine (pure transform, no SQL)
    sequence_phases.py            #   Phase segments + the eleven has_* flags
    build_team_match_stats.sql    #   gold.team_match_stats
    build_team_season_stats.sql   #   gold.team_season_stats (+ league_position pass)
    build_player_match_stats.sql  #   gold.player_match_stats
    build_player_season_stats.sql #   gold.player_season_stats (+ vaep_rank_in_team pass)
    build_player_percentiles.sql  #   gold.player_season_percentiles
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
- **`espanyol-viz-design`** — chart craft carried over from the v1 Streamlit visualisations: the mark vocabulary (filled = success, hollow = failure, shape = event family, size = volume), the z-order ladder as SVG element order, marker-size conversion (matplotlib `s` is area in points²), heatmap ramp construction, per-chart geometry for the shot map, passing network, sequence traces, radar, time series and match summary, and a list of v1 inconsistencies to fix rather than port. Use when building or porting any chart component in `web/` (Phase 4 onwards, `md/WEB_PLAN.md` §5). **Not the visual authority**: colour, typography, spacing, radii and layout come from `web/design_handoff_espanyol_analytics/README.md`, and zones from `pitch-guide` / `gold.pitch_zones` — where they disagree with the skill, they win. Its §0 records the split.
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

> Last updated: 5 September 2026

### Done
- Silver schema fully designed and operational for La Liga — all 10 tables
- **Seasons loaded: 2024/25 (47 matches), 2025/26 (380), 2026/27 (31 and counting) — 458 total.** 2026/27 was loaded on 5 Sep 2026 through matchday 3 plus one early matchday-6 fixture; re-run the full order in "How to Run Pipelines" to pick up later matchdays
- Data ingestion pipelines built for matches, lineups, squads, teams, players, events (carries + xT + SPADL mapping)
- SPADL columns (`spadl_type_id`, `spadl_result_id`, `spadl_bodypart_id`) added to `silver.events`; backfill via `python -m src.silver.events.spadl`
- **VAEP is live.** `vaep_value` / `vaep_offensive` / `vaep_defensive` are populated across all 458 matches (840,540 of 970,623 events; the rest are action types SPADL does not map). The previously shipped model was degenerate (`scores` AUC excluding goal actions **0.5222** — a coin flip; it had learned "was this a successful shot?"). It was retrained from scratch in a companion repo, Platt-calibrated on held-out Opta, and integrated on 11 Aug 2026: **0.7465** AUC excluding goal actions, `corr(vaep_offensive, xt)` **−0.1942 → +0.2605**. Backfill via `python -m src.silver.events.vaep`; consult the **`vaep-model`** skill before touching `vaep.py`, `_build_features` or `models/vaep/*.json`
- Possession sequence classifier implemented (`sequence_id`, `sequence_start`, `sequence_end`, `sequence_event_number`)
- Squad diff strategy live with `squad_snapshot_log` audit table
- Shot enrichment columns: `shot_play_pattern` (7-value enum from Q22/23/24/25/26/160/9) and `first_time` (bool from Q328)
- Foot preference enrichment: `preferred_foot` on `silver.players` (derived from Q20/Q72 counts) and `is_weak_foot` on `silver.events`. Runs automatically as post-processing inside `python -m src.silver.events`, alongside xG; `foot_preference.py` is the standalone backfill
- SPADL and VAEP are computed **inline** by the events pipeline (`include_spadl` / `include_vaep`, both default True). `src.silver.events.spadl` and `.vaep` are backfill tools for already-loaded rows, not part of the new-season path

### Gold Layer — COMPLETE: sequences, teams and players (5 Sep 2026)

The full gold layer is designed in `md/GOLD_LAYER.md`. **The operating manual is
`GOLD_LAYER.md §9`** — run order, validation gates, documented deviations and
troubleshooting. Read that before touching anything in `src/gold/`.

Built and populated over all 458 matches:

| Table | Rows |
|---|---|
| `gold.sequences` | 155,050 |
| `gold.sequence_players` | 450,833 |
| `gold.sequence_phase_segments` | 234,952 |
| `gold.team_match_stats` | 916 |
| `gold.team_season_stats` | 60 |
| `gold.player_match_stats` | 14,394 |
| `gold.player_season_stats` | 1,464 |
| `gold.player_season_percentiles` | 9,735 |
| `gold.formation_slot_positions` | 209 (seeded, generated) |

```bash
# Append to the silver run order every weekend. Full rebuild, ~5 minutes.
python -m src.silver.events.sequences
python -m src.gold.build_gold
```

**Full rebuild is the intended posture, not a fallback** (§9.2): at this volume
it costs ~5 min and cannot leave rows computed under an old definition beside
rows computed under a new one. `--match-ids` is for fast iteration while
developing a query, not for the weekend run.

**The phase pass is a hard dependency of the team build**, not an enrichment:
without it `counter_attack_sequences` and `high_transition_sequences` are zero
league-wide, which looks plausible and is wrong. `build_gold.py` refuses to run
the team step if `gold.sequence_phase_segments` is empty.

**Player position comes from the Opta formation slot, not from
`match_lineups.position`** (which is `SUB` for every substitute) or
`players.position_raw` (4 values only). `gold.formation_slot_positions` maps
`(team_formation, formation_position) -> position, position_group` and is
**generated** from the measured mean `(x, y)` of each slot — regeneration
procedure and validation assertions in `GOLD_LAYER.md §9.6 deviation 10` and
gate G14. The build fails loudly on an unmapped slot rather than emitting a
NULL position.

**Two traps that will bite anyone extending this** (both in §9.5):
`sequence_number` is per **team**, not per match — ordering a match by it ties
every value and silently corrupts any `lag()`/`lead()`; and the percentile
build reads metrics via `to_jsonb(row) ->> metric`, so a typo in
`gold.percentile_metrics` yields NULL rather than an error. Gates G10 and G12
exist to catch exactly these.

**Pitch geometry is settled:** 6 strips × 5 lateral channels = 30 zones, and
**low `y` is the RIGHT flank** (839:13 on foot preference — GOLD_LAYER §4.6.0).
The `pitch-guide` skill was inverted and was corrected on 16 Aug 2026; skill,
GOLD_LAYER and `gold.pitch_zones` now agree. `gold.pitch_zones` is authoritative.

**`sequences.py` — Fix 0 + Fix A + Fix B applied 16 Aug 2026, all 427 matches
re-classified.** Diagnosis, worked examples and full before/after in
`md/GOLD_SEQUENCES.md §6`. The before/after table below is that 427-match
snapshot and is kept as the historical record; the 2026/27 matches loaded on
5 Sep 2026 were classified with the same fixes, bringing the current totals to
**458 matches / 884,330 events with a `sequence_id` / 155,050 sequences**.

- **Fix 0 — determinism.** The classifier was **non-deterministic**: 11,766
  events share a `json_index` with another event in the same match (6,026
  `Carry`, 5,445 `Challenge`) and neither the SQL `ORDER BY` nor the pandas sort
  had a tiebreaker, so a re-run reproduced only 92.7% of stored `sequence_id`s
  with no code change. `event_id` is now the tiebreaker in both. Verified: a
  shuffled re-run now reproduces the stored columns exactly.
- **Fix A** — possession change is detected against the team that owns the
  running sequence, not the previous event.
- **Fix B** — gap closer: any on-ball action opens a sequence when none is
  running (contested events gated on `outcome = 'success'`, so a *failed* tackle
  never opens one).

Fixes A and B are **ON by default**; `--no-fix-a` / `--no-fix-b` reproduce the
old behaviour for A/B comparison only — never for a production backfill.

| | before | after |
|---|---|---|
| Events with a `sequence_id` | 782,251 | **821,278** |
| Sequences | 121,850 | **144,763** |
| Orphaned passes | 22,036 | **301** |
| Majority-flip sequences | 3,738 | **1,374** |
| Sequences with 6+ opponent events | 604 | **5** |
| Goals inside a sequence | — | **1,140 / 1,140** |
| Single-event sequences | 22,440 | 43,243 |
| Sequences per team-match | 142.7 | 169.5 |

The last two rows are the intended price of Fix B — recording brief possessions
that were previously invisible. Two consequences downstream: `possession_pct`
**must** use the one-second floor (`GOLD_LAYER.md §4.5.0`), and style clustering
must filter `event_count >= 3` (§4.1.9), which now drops ~40% of rows.

Pre-fix snapshot kept in `silver.sequences_backup_20260816` (901,805 rows) —
drop it once you are satisfied.

Note `Ball touch` with `outcome = 'success'` is a *deflection* (Opta: "ball
simply hit the player unintentionally") and correctly does **not** end a
sequence; do not "fix" that.

### Future Scope
- Segunda Division (zero schema changes needed)
- Web visualisation on top of gold layer
- Sequence & phase clustering (unsupervised ML)
