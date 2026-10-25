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
python src/silver/load_match_lineups.py --reprocess   # re-run loaded matches; adds missing rows only

# 5. Events. This is the FULL pipeline: parse + carries + xT + SPADL + VAEP,
#    then game state, foot-preference enrichment and xG as post-processing. For a newly
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

# Game state (scoreline + players on the pitch before each event). Default:
# matches with NULL state only. --all recomputes and writes only changed rows.
python -m src.silver.events.game_state
python -m src.silver.events.game_state --all
python -m src.silver.events.game_state --match-ids 1 2 3

# xG. Default: matches with a NULL xg; --match-ids re-scores those matches.
python -m src.silver.events.xg --match-ids 1 2 3
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
      xg.py                       #   xG model scoring + backfill (python -m src.silver.events.xg)
      game_state.py               #   Scoreline + players on pitch before each event (GOLD_LAYER §4.8)
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
    build_pass_receptions.sql     #   gold.pass_receptions (receiver, zone, what they did next — §4.7)
    build_game_states.sql         #   gold.match_game_states (segments of constant score/numbers — §4.8)
  scraper/                        # Opta match file scraper (run as module: python -m src.scraper)
    __main__.py                   #   CLI entry point (--headless flag)
    scraper.py                    #   Playwright browser automation: navigates scoresway.com,
                                   #   discovers matchdays/matches, intercepts api.performfeeds.com
                                   #   responses, saves raw JSONP to data/raw/{competition}/{season}/matches/
  export/                         # Gold -> static JSON for the site (python -m src.export) — md/WEB_DATA.md
    config.py                     #   teams, seasons, metric registries, percentile gate, caps
    db.py                         #   every export query (read-only)
    manifest.py league_table.py overview.py match.py   # one module per published file
    squad.py player.py            #   screen 03: squad.json + players/{id}.json (per match, filterable)
    player_percentiles.py         #   screen-03 percentile ranking (pure; site gate)
sql/
  ddl/                            # gold DDL + gold_functions.sql (geometry helpers: zone_id, in_box, is_progressive…)
  seed/                           # formation_slot_positions
  fixes/                          # dated one-off data repairs
web/                              # Next.js 16 static site — web/README.md, md/WEB_PLAN.md
  app/                            #   Pizarra world (DESIGN.md): page.tsx = landing, every [equipo]/ page; (handoff)/ = /demo only
  components/landing/             #   landing blocks: SeasonHighlight, DepthIndex, ChainPitch, Thumbs, XgStroke
  components/matches/             #   match list blocks: MatchIndex (rows), MatchShots (static shot pitch)
  components/shell/               #   Pizarra shell: PillNav, Arrow, Exits, BoardFooter (+ legacy handoff Masthead/Footer)
  components/viz/                 #   pitch + chart layers (inventory: espanyol-viz-design §2)
  lib/                            #   data loaders + types, labels (Spanish), format, viz geometry
  public/data/                    #   exported JSON (committed; the deploy artifact)
  design_handoff_espanyol_analytics/  # visual authority for app/(handoff)/ pages only, being superseded by DESIGN.md
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
- **5-pass lineup strategy**: Pass 1 (34) inserts all; Pass 2a (18) sets `minute_out`; Pass 2b (19) sets `minute_in`; Pass 2c/d (17 + Q33/Q32) sets red card exits; Pass 2e (20) sets `retired`. Exits apply only to a player on the pitch. **2a must run before 2b.**
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

Project skills in `.claude/skills/`. Consult them before working in their area:

| Skill | Use when |
|---|---|
| **`football-metrics`** | Any metric name, in a request, brief, legend, percentile card or query. The glossary: one-line definition + where it's computed + what Opta doesn't have. **Design briefs aren't the authority on definitions**; its §8 lists the open conflicts with `README-PLAYER.md`. |
| **`espanyol-viz-design`** | Building or changing any chart in `web/`. The v2 **chart catalog** (question, data, component to reuse, marks, traps for every chart type) + component inventory. The v1 matplotlib language is in its `references/`. Visual authority stays with the design handoff. |
| **`new-viz`** | Adding a chart, pitch mode, block or page end to end: definitions → export → fixture/demo → component → page → screenshot check → docs. |
| **`football-silver-schema`** | Ingestion scripts, silver SQL, FK resolution, idempotency, gold table design. |
| **`opta-events-reference`** | Event pipeline files, event-level SQL, typeIds, qualifiers, JSONP, carries. |
| **`phases-of-play`** | Sequence phase classification (`phases.py`, `sequence_phases.py`, phase flags). |
| **`pitch-guide`** | Pitch drawing primitives, the 30-zone grid, zone assignment in Python/SQL/JS. |
| **`spadl-mapping`** | `spadl.py`, SPADL-filtered queries, VAEP features. |
| **`vaep-model`** | `vaep.py`, `_build_features`, `models/vaep/*.json`, `vaep_*` columns. **Read before any feature-set change** (it's mirrored in the training repo). |
| **`karpathy-guidelines`** | Writing, reviewing or refactoring code: surgical changes, stated assumptions, verifiable success criteria. |
| **`grill-me`** | The user asks to stress-test a plan or design. |
| **`impeccable`** | Redesigning, critiquing, auditing or polishing the look of `web/` (third-party, v4.7.0). Its launcher downloads a binary on first run; it reads/writes `PRODUCT.md` and `DESIGN.md` at the repo root. |
| **`better-accessibility`** | Keyboard, focus, ARIA, contrast and screen-reader checks on `web/` (third-party). |
| **`better-colors`** | Palette, token naming and contrast measurement for `web/` (third-party). |

**Precedence in `web/`:** the third-party design skills above (`impeccable`,
`better-*`) are for critique, redesign and review. Until the handoff is
formally superseded, `web/design_handoff_espanyol_analytics/` stays the visual
authority and `espanyol-viz-design` the chart authority; a redesign that
replaces the handoff must update or mark it superseded in the same change.
Metric definitions always come from `football-metrics`.

Path-scoped rules in `.claude/rules/` load automatically when you work in
`src/silver`, `src/gold` + `sql`, `src/export` or `web/`. They hold the
traps for those areas. The harness plan and its status: `md/HARNESS_PLAN.md`.

---

## Working Conventions

- All SQL is raw; no ORM.
- Match existing psycopg2 patterns and idempotency guards.
- Do not invent column names or table structures — derive from the documented schema or use skills.
- The `full_name` column on `players` is `GENERATED ALWAYS AS` — never include it in `INSERT` statements.
- Data files (`data/`) are gitignored; only `.gitkeep` placeholders are tracked.
- **Order event streams by `(json_index, event_id)`**, never `json_index` alone (ties exist), whenever sequence, game state or "next action" matters.
- Propose changes before making them; for data-affecting changes, a read-only before/after comparison is how to earn approval.

### Traps that apply everywhere

- **Low `y` is the RIGHT flank** (839:13 right-footers in slot 2). `gold.pitch_zones` is authoritative; the `pitch-guide` skill agrees with it.
- **`sequence_number` is per team, not per match**, and **`sequence_id` renumbers on every rebuild**. Quote match + kick-off time when reporting a chain.
- **Every event is in its own team's attacking frame.** Mirror opponent events (`105 − x`, `68 − y`) to compare.
- **`possession_pct` is clock-based** (start-to-next-start, `GOLD_LAYER.md §4.5.0`).
- **Style analysis filters `event_count >= 3`.** About 30% of sequences are single-event, by design (Fix B).
- **`Ball touch` with `outcome = 'success'` is a deflection** and correctly does not end a sequence.

---

## Current State

> Last updated: 10 October 2026. **History, measured before/after numbers and
> the reasons behind every fix: `md/CHANGELOG.md`** (newest first). Add new
> dated entries there, and keep this section to what is true now.

- **Data**: La Liga 2024/25 (47 matches, partial, not published), 2025/26
  (380), 2026/27 (in progress): **496 matches** at the 29 Sep 2026 rebuild.
  Re-run the full order above to pick up new matchdays.
- **Silver**: all 10 tables. Events carry synthesised carries, xT (failed
  actions earn 0), SPADL, **VAEP** (retrained, calibrated: `vaep-model`),
  **xG** (XGBoost, penalties 0.79), game state before each event, foot
  preference. Possession sequences from `sequences.py` (Fix 0/A/B and the
  owner-relative rules: `md/GOLD_SEQUENCES.md §6`).
- **Gold**: complete. `sequences`, `sequence_players`,
  `sequence_phase_segments`, `team_match_stats`, `team_season_stats`,
  `player_match_stats`, `player_season_stats`, `player_season_percentiles`,
  `formation_slot_positions` (seeded), `pass_receptions`,
  `match_game_states` + view `team_game_states`. Manual and row counts:
  `md/GOLD_LAYER.md §9` (§9.4).
- **Visual redesign (24 Oct 2026, branch `redesign/visual-world`)**: new
  "Pizarra" world (`DESIGN.md`), landing `/`, screen 01 (season), the
  match list, screen 02 (match) and screen 03 (squad + player) rebuilt in
  it; only `/demo` is left in the handoff; the other pages still use the handoff under `web/app/(handoff)/` and
  move over one by one
  (`md/REDESIGN_HANDOFF.md`). Pre-redesign snapshot: tag `pre-redesign-2026-10-23`.
- **Web**: Next.js 16 static site in `web/`, reading `web/public/data` written
  by `python -m src.export`. Built: screen 01 (season), screen 02 (match),
  match list, `/demo`, and **screen 03 (player, 16 Oct 2026)**:
  `/{team}/jugador/{season}/{player_id}` from `squad.json` +
  `players/{id}.json` (`src/export/squad.py`, `player.py`), components in
  `web/components/player/`, fixture page `/demo/jugador`; the Plantilla tab
  (`/{team}/plantilla/{season}`) opens it on the first squad player. A match
  filter (`?partidos=`) narrows card, map and sequences, not percentiles;
  filtered sequences fetch their match files, so **export player and match
  files in the same run** (they share `sequence_id`). Every map mark has a
  hover card, and a click opens its chain in the sequence block (map rows
  carry `s`, `schema_version` 2); Toques / Recepciones / Defensivas switch
  between heat and events. README-PLAYER "Review of 16 Oct 2026" and
  "Review of 17 Oct 2026".
  **Screen 02 has the same map -> sequence click (22 Oct 2026)**: shots,
  progression and defensive actions carry `s` (index into the match file's
  `sequences[]`, null for an opponent's event or a chain under 3 actions);
  since 24 Oct 2026 (Pizarra) `components/match/MatchMap` (keyboard-reachable,
  labelled marks) + `SequenceLink` + `MatchSequences` (the spot panel).
  Brief `web/design_handoff_espanyol_analytics/README-PLAYER.md`. Its percentile
  spine is **settled (15 Oct 2026)**: per-role cards (README-PLAYER §4), roles
  `GK CB RB LB MID AM W ST` (`role_group`, AM = midfielder with ≥ 30% of
  touches in the final third), ranked by `src/export/player_percentiles.py`
  from `PLAYER_SPINE` + `gold.percentile_metrics`. Its other open decisions
  are in README-PLAYER "Open decisions" (defaults applied). `src/export/sequences.py`
  is specified (WEB_DATA §10), not implemented. Plan: `md/WEB_PLAN.md`;
  contract: `md/WEB_DATA.md`.
- **Weekend run**: full order above → `python -m src.silver.events.sequences` →
  `python -m src.gold.build_gold` → gates (`GOLD_LAYER.md §9.5`) →
  `python -m src.export` → commit `web/public/data`.
- **Snapshots to drop once satisfied**: `silver.events_backup_20260921`,
  `silver.sequences_backup_20260816`.

### Future Scope
- Segunda Division (zero schema changes needed)
- Screens 03–05 of the site (player, sequence lab, comparison) and methodology
- Sequence & phase clustering (unsupervised ML): proposal and roadmaps in `md/SEQUENCE_CLUSTERING.md`
