# Football Analytics — Data Model Documentation
**La Liga & Segunda División — Season 2025/26 onwards**

---

## Table of Contents
1. [Overview](#1-overview)
2. [Entity Relationship Overview](#2-entity-relationship-overview)
3. [Table Specifications](#3-table-specifications)
   - [3.1 competitions](#31-competitions)
   - [3.2 seasons](#32-seasons)
   - [3.3 competition_seasons](#33-competition_seasons)
   - [3.4 teams](#34-teams)
   - [3.5 team_competition_seasons](#35-team_competition_seasons)
   - [3.6 players](#36-players)
   - [3.7 player_squads](#37-player_squads)
   - [3.8 matches](#38-matches)
   - [3.9 match_lineups](#39-match_lineups)
   - [3.10 events](#310-events)
4. [Medallion Architecture](#4-medallion-architecture)
   - [4.1 Bronze Layer](#41-bronze-layer--raw-ingestion)
   - [4.2 Silver Layer](#42-silver-layer--clean--structured)
   - [4.3 Gold Layer](#43-gold-layer--aggregates--analytics)
   - [4.4 Layer Summary](#44-layer-summary)
5. [Scalability Notes](#5-scalability-notes)

---

## 1. Overview

This document defines the relational data model for the football analytics project. The model has been designed with three core principles in mind:

- **Reliability** — clean separation of concerns, no redundancy, referential integrity throughout.
- **Scalability** — adding new leagues, new seasons, promotions/relegations and mid-season transfers requires only `INSERT` operations, never schema changes.
- **Agility** — the model is optimised for the most common analytical queries: per-team season aggregates, per-player career stats, match event lookup, and head-to-head records.

---

## 2. Entity Relationship Overview

The model is structured in three logical tiers:

- **Tier 1 — Competition framework:** `competitions`, `seasons`, `competition_seasons`
- **Tier 2 — Participants:** `teams`, `team_competition_seasons`, `players`, `player_squads`
- **Tier 3 — Match data:** `matches`, `match_lineups`, `events`

> **Key design principle:** `competition_seasons.id` is the central foreign key that anchors all season-specific entities. Everything — teams, matches, stats — hangs off a specific edition of a competition.

```
competitions ──┐
               ├──► competition_seasons ◄── seasons
               │           │
               │    ┌──────┴───────┐
               │    │              │
               │  matches   team_competition_seasons
               │    │              │
               │  match_lineups  player_squads
               │    │              │
               │  events        players
               │
              teams
```

---

## 3. Table Specifications

### 3.1 `competitions`

The stable, timeless identity of a competition. One row per competition, never changes.

| Column | Type | Nullable | Description |
|---|---|---|---|
| `competition_id` | INT (PK) | No | Surrogate primary key |
| `source_competition_id` | VARCHAR(50) | No | Provider's `competition.id` — unique, used for ingestion lookups |
| `name` | VARCHAR(100) | No | `competition.name` — e.g. `Primera División` |
| `known_name` | VARCHAR(100) | Yes | `competition.knownName` — e.g. `Spanish La Liga` |
| `sponsor_name` | VARCHAR(100) | Yes | `competition.sponsorName` — nullable, may change over time |
| `competition_code` | VARCHAR(10) | Yes | `competition.competitionCode` — e.g. `PRD` |
| `competition_format` | VARCHAR(50) | Yes | `competition.competitionFormat` — e.g. `Domestic league` |
| `country` | VARCHAR(60) | No | `competition.country.name` — e.g. `Spain` |
| `confederation` | VARCHAR(10) | No | Manual — e.g. `UEFA`, `CONMEBOL` |
| `tier_level` | INT | No | Manual — `1` = top flight, `2` = second division, etc. |
| `created_at` | TIMESTAMP | No | Record creation timestamp |

---

### 3.2 `seasons`

A pure label table. One row per season, shared across all competitions. Reusing the same season row for La Liga and Segunda División avoids duplication.

Dates are intentionally absent here — each competition runs on its own calendar, so start and end dates belong in `competition_seasons`, not here. The `label` is the only field truly shared across competitions and the deduplication key used during ingestion.

| Column | Type | Nullable | Description |
|---|---|---|---|
| `season_id` | INT (PK) | No | Surrogate primary key |
| `label` | VARCHAR(10) | No | Human label — e.g. `2025/2026`. Unique, used as ingestion lookup key |
| `created_at` | TIMESTAMP | No | Record creation timestamp |

> **Ingestion pattern:** seasons are seeded manually. The label (`tournamentCalendar.name`) is used to resolve `season_id` when processing any match file.

---

### 3.3 `competition_seasons`

The central anchor of the model. Represents one specific edition of a competition — e.g. *La Liga 2025/26*. This is the foreign key referenced by all season-specific entities.

Dates live here rather than in `seasons` because different competitions run on different calendars — La Liga and Segunda División share the same season label but have different start, end, and stage dates.

| Column | Type | Nullable | Description |
|---|---|---|---|
| `competition_season_id` | INT (PK) | No | Surrogate primary key — the master FK |
| `source_season_id` | VARCHAR(50) | No | Provider's `tournamentCalendar.id` — competition-specific, unique |
| `source_stage_id` | VARCHAR(50) | No | Provider's `stage.id` — primary ingestion lookup key, unique |
| `competition_id` | INT (FK) | No | Reference to `competitions` |
| `season_id` | INT (FK) | No | Reference to `seasons` |
| `stage_name` | VARCHAR(50) | Yes | `stage.name` — e.g. `Regular Season` |
| `start_date` | DATE | No | `tournamentCalendar.startDate` — competition calendar start |
| `end_date` | DATE | Yes | `tournamentCalendar.endDate` — competition calendar end, null if ongoing |
| `stage_start_date` | DATE | Yes | `stage.startDate` — stage start, may differ from calendar start |
| `stage_end_date` | DATE | Yes | `stage.endDate` — stage end, may differ from calendar end |
| `num_teams` | INT | Yes | Number of teams in this edition — filled manually |
| `total_matchdays` | INT | Yes | Total rounds/matchdays in the season — filled manually |
| `promo_spots` | INT | Yes | Number of promotion places |
| `relegation_spots` | INT | Yes | Number of relegation places |
| `status` | VARCHAR(20) | No | `upcoming` / `active` / `completed` |

> **Ingestion pattern:** every match file carries `stage.id`, so resolving to `competition_season_id` during ingestion is a single indexed lookup on `source_stage_id`.
>
> `num_teams`, `total_matchdays`, `promo_spots` and `relegation_spots` are not available from the provider and must be filled manually. They are nullable to avoid blocking initial inserts.

---

### 3.4 `teams`

The stable identity of a club. Contains attributes that belong to the club regardless of which competition or season it is in. Dynamic facts (current league, badge) live downstream.

| Column | Type | Nullable | Description |
|---|---|---|---|
| `team_id` | INT (PK) | No | Surrogate primary key |
| `source_team_id` | VARCHAR(50) | No | Provider's `contestant.id` — unique, used for ingestion lookups and deduplication |
| `source_venue_id` | VARCHAR(50) | Yes | Provider's `venue.id` — stored for future venue-level enrichment |
| `name` | VARCHAR(100) | No | Full official name |
| `short_name` | VARCHAR(30) | No | Common short name — e.g. `Barça` |
| `abbreviation` | VARCHAR(5) | Yes | 3-letter code — e.g. `FCB` |
| `city` | VARCHAR(60) | Yes | City of the club |
| `stadium_name` | VARCHAR(100) | Yes | Main home stadium name |
| `stadium_capacity` | INT | Yes | Seating capacity |
| `founded_year` | INT | Yes | Year of founding |
| `country` | VARCHAR(60) | No | Country — useful when adding international competitions |
| `created_at` | TIMESTAMP | No | Record creation timestamp |

> **Ingestion pattern:** `source_team_id` is the deduplication key used during ingestion. `ON CONFLICT (source_team_id) DO NOTHING` ensures the table is safe to re-populate without creating duplicates. `city`, `stadium_capacity`, and `founded_year` are not available from the provider and are filled manually in the bronze seed file before loading.

---

### 3.5 `team_competition_seasons`

The enrollment of a team in a specific competition season. A new row is inserted each season — promotions and relegations are handled naturally. No existing rows are ever modified.

| Column | Type | Nullable | Description |
|---|---|---|---|
| `team_cs_id` | INT (PK) | No | Surrogate primary key |
| `team_id` | INT (FK) | No | Reference to `teams` |
| `competition_season_id` | INT (FK) | No | Reference to `competition_seasons` — resolved from `source_season_id` during ingestion |
| `kit_home_color` | VARCHAR(20) | Yes | Primary home kit color (hex) — filled manually in bronze seed file |
| `kit_away_color` | VARCHAR(20) | Yes | Primary away kit color (hex) — filled manually in bronze seed file |
| `badge_url` | VARCHAR(255) | Yes | Season-specific badge/crest URL |

> **Unique constraint** on `(team_id, competition_season_id)` ensures no duplicate enrollments. A team can appear in multiple `competition_season_ids` in the same season if Copa del Rey or similar is added later.
>
> **Ingestion pattern:** `team_id` and `competition_season_id` are resolved inside PostgreSQL via a `JOIN` on `source_team_id` and `source_season_id` respectively — no application-side ID lookup required. `competition_seasons` must be populated before this table can be loaded.

---

### 3.6 `players`

The stable identity of a player. Contains biographical attributes that do not change over time. Current team, shirt number, and role live in `player_squads`. Only `type: "player"` entries from the provider feed are loaded — coaching staff are excluded.

| Column | Type | Nullable | Source | Description |
|---|---|---|---|---|
| `player_id` | SERIAL (PK) | No | — | Surrogate primary key |
| `source_player_id` | VARCHAR(50) | No | `person.id` | Provider's player ID — unique, deduplication key |
| `first_name` | VARCHAR(100) | No | `person.firstName` | Legal first name |
| `last_name` | VARCHAR(150) | No | `person.lastName` | Legal last name |
| `short_first_name` | VARCHAR(50) | Yes | `person.shortFirstName` | Abbreviated first name used by provider |
| `short_last_name` | VARCHAR(50) | Yes | `person.shortLastName` | Abbreviated last name used by provider |
| `full_name` | VARCHAR(200) | **Generated** | Derived | `first_name \|\| ' ' \|\| last_name` — stored computed column, not writable |
| `known_name` | VARCHAR(100) | Yes | `person.knownName` | Public alias when different from legal name — e.g. `Jonny`, `Vinicius Jr.` |
| `match_name` | VARCHAR(150) | Yes | `person.matchName` | Name string used in the provider's match event feed |
| `gender` | VARCHAR(10) | Yes | `person.gender` | `Male` / `Female` |
| `date_of_birth` | DATE | Yes | — | Not in provider feed — enriched manually |
| `place_of_birth` | VARCHAR(100) | Yes | `person.placeOfBirth` | City of birth |
| `nationality` | VARCHAR(60) | Yes | `person.nationality` | Primary nationality name |
| `nationality_source_id` | VARCHAR(50) | Yes | `person.nationalityId` | Provider's nationality ID — useful for future nationality table |
| `second_nationality` | VARCHAR(60) | Yes | `person.secondNationality` | Second nationality name, if applicable |
| `second_nationality_source_id` | VARCHAR(50) | Yes | `person.secondNationalityId` | Provider's ID for the second nationality |
| `position_raw` | VARCHAR(30) | Yes | `person.position` | Broad position from provider: `Goalkeeper` / `Defender` / `Midfielder` / `Attacker` |
| `preferred_position` | VARCHAR(10) | Yes | — | Granular position — mapped manually: `GK` / `CB` / `LB` / `RB` / `DM` / `CM` / `AM` / `LW` / `RW` / `ST` |
| `preferred_foot` | VARCHAR(5) | Yes | — | Not in provider feed — enriched manually: `left` / `right` / `both` |
| `height_cm` | INT | Yes | — | Not in provider feed — enriched manually |
| `created_at` | TIMESTAMP | No | — | Record creation timestamp |

> **Ingestion pattern:** `source_player_id` is the deduplication key. `INSERT ... ON CONFLICT (source_player_id) DO NOTHING` makes the load safe to re-run. `full_name` is a `GENERATED ALWAYS AS` stored column — never include it in `INSERT` statements.
>
> **Two-tier position model:** `position_raw` is populated automatically from the provider's broad classification. `preferred_position` requires a manual mapping pass and is intentionally left nullable until then.
>
> **Coaching staff:** `type: "coach"` and `type: "assistant coach"` entries in the squad feed are filtered out during ingestion and not stored in this table.

---

### 3.7 `player_squads`

Records a player's membership of a team during a specific competition season, with date ranges to support mid-season transfers. A player who transfers in January gets **two rows** for the same season — the first spell is closed (`end_date` set), a new one opened.

| Column | Type | Nullable | Source | Description |
|---|---|---|---|---|
| `squad_id` | SERIAL (PK) | No | — | Surrogate primary key |
| `player_id` | INT (FK) | No | Resolved | Reference to `players` |
| `team_id` | INT (FK) | No | `contestantId` → resolved | Reference to `teams` via `source_team_id` |
| `competition_season_id` | INT (FK) | No | `tournamentCalendarId` → resolved | Reference to `competition_seasons` via `source_season_id` |
| `start_date` | DATE | No | `person.startDate` / snapshot date | On initial load: `person.startDate`. On diff loads: snapshot date when arrival was detected |
| `end_date` | DATE | Yes | `person.endDate` / snapshot date | On initial load: `person.endDate` (null if still active). On diff loads: snapshot date when departure was detected | 
| `shirt_number` | INT | Yes | `person.shirtNumber` | Squad number for this spell |
| `squad_role` | VARCHAR(20) | No | Default: `first_team` | `first_team` / `loan` / `youth` — not in provider feed, defaults to `first_team`, enriched manually |
| `transfer_type` | VARCHAR(20) | Yes | — | `permanent` / `loan` / `free` / `youth` — not in provider feed, enriched manually |
| `created_at` | TIMESTAMP | No | — | Record creation timestamp |

> **Temporal join pattern** — to find which team a player belonged to on a given match date:
> ```sql
> JOIN player_squads ps ON ps.player_id = e.player_id
> WHERE match_date BETWEEN ps.start_date AND COALESCE(ps.end_date, '9999-12-31')
> ```
>
> **Diff strategy across snapshot loads:** when processing a new squad snapshot, compare it against rows where `end_date IS NULL` for that `(team_id, competition_season_id)`. Players present in the DB but absent from the new snapshot have their `end_date` set to the snapshot date. Players appearing for the first time get a new row with `start_date = snapshot_date`.

---

### 3.8 `matches`

One row per match. Home and away teams are stored as direct columns — home/away is a first-class football concept and does not benefit from a secondary junction table in a two-team sport.

| Column | Type | Nullable | Description |
|---|---|---|---|
| `match_id` | SERIAL (PK) | No | Surrogate primary key |
| `source_match_id` | VARCHAR(50) | No | Provider's `matchInfo.id` — unique, deduplication key |
| `competition_season_id` | INT (FK) | No | Reference to `competition_seasons` — resolved from `matchInfo.stage.id` |
| `home_team_id` | INT (FK) | No | Reference to `teams` — resolved from `contestant[position=home].id` |
| `away_team_id` | INT (FK) | No | Reference to `teams` — resolved from `contestant[position=away].id` |
| `description` | VARCHAR(100) | Yes | `matchInfo.description` — e.g. `Girona vs Rayo Vallecano` |
| `matchday` | INT | Yes | Round / jornada number — `matchInfo.week` |
| `match_date` | TIMESTAMP | Yes | Local kick-off datetime (Spain time, no timezone) — `matchInfo.localDate` + `localTime` |
| `status` | VARCHAR(20) | No | Hardcoded `completed` at ingestion — all loaded files are post-match |
| `winner` | VARCHAR(10) | Yes | `matchDetails.winner` — `home` / `away` / `draw` |
| `home_score` | INT | Yes | Full-time goals by home team — `scores.ft.home` |
| `away_score` | INT | Yes | Full-time goals by away team — `scores.ft.away` |
| `home_score_ht` | INT | Yes | Half-time goals by home team — `scores.ht.home` |
| `away_score_ht` | INT | Yes | Half-time goals by away team — `scores.ht.away` |
| `match_length_min` | INT | Yes | Actual match duration in minutes including stoppages — `matchDetails.matchLengthMin` |
| `match_length_sec` | INT | Yes | Seconds component of actual match duration — `matchDetails.matchLengthSec` |
| `ht_injury_time_sec` | INT | Yes | Announced added time at end of first half, in seconds — `period[id=1].announcedInjuryTime` |
| `ft_injury_time_sec` | INT | Yes | Announced added time at end of second half, in seconds — `period[id=2].announcedInjuryTime` |
| `var_used` | BOOL | Yes | Whether VAR was active in this match — `matchInfo.var` (`"1"` = true) |
| `venue` | VARCHAR(100) | Yes | Stadium where match was played — `matchInfo.venue.longName` |
| `source_venue_id` | VARCHAR(50) | Yes | Provider's venue ID — stored for future `venues` table enrichment |
| `venue_latitude` | FLOAT | Yes | Venue latitude coordinate — `matchInfo.venue.latitude` |
| `venue_longitude` | FLOAT | Yes | Venue longitude coordinate — `matchInfo.venue.longitude` |
| `neutral_venue` | BOOL | Yes | Whether match was played at a neutral ground — `matchInfo.venue.neutral` |
| `coverage_level` | VARCHAR(10) | Yes | Provider data coverage level — data quality signal for event completeness |
| `last_updated_at` | TIMESTAMPTZ | Yes | Timestamp of last provider update — `matchInfo.lastUpdated` |
| `created_at` | TIMESTAMP | No | Record creation timestamp |

> **Ingestion pattern:** `source_match_id` is the deduplication key. `ON CONFLICT (source_match_id) DO NOTHING` makes the load safe to re-run. `competition_season_id`, `home_team_id`, and `away_team_id` are resolved inside PostgreSQL via joins on `source_stage_id` and `source_team_id` respectively — no application-side ID lookup required.
>
> **Common queries:**
> - All matches for a team → `WHERE home_team_id = X OR away_team_id = X`
> - All matches in a season → `WHERE competition_season_id = X`
> - Winner lookup → `WHERE winner = 'home'` (more ergonomic than comparing score columns)
>
> `home_team_id`, `away_team_id`, and `competition_season_id` all carry an index.

---

### 3.9 `match_lineups`

Records which players participated in each match, whether they started, at what position, and their minutes on the pitch. Kept separate from `events` because a lineup is a squad fact *about* a match, not an event that occurred *during* it.

**Ingestion strategy — 5-pass processing** of each raw match file (see `load_match_lineups.py`):

| Pass | Source event | Condition | Action |
|---|---|---|---|
| 1 | `typeId: 34` (Team set up) | — | INSERT all rows for both teams — starters get `minute_in = 0`, bench get `minute_in = NULL` |
| 2a | `typeId: 18` (Player off) | — | UPDATE → `minute_out = timeMin`, `exit_reason = 'substitution'` |
| 2b | `typeId: 19` (Player on) | — | UPDATE → `minute_in = timeMin`, `formation_position = Q145` |
| 2c | `typeId: 17` (Card) | Q33 present | UPDATE → `minute_out = timeMin`, `exit_reason = 'red_card'` |
| 2d | `typeId: 17` (Card) | Q32 present | UPDATE → `minute_out = timeMin`, `exit_reason = 'second_yellow'` |

Pass order matters — Pass 2a (player off) always runs before Pass 2b (player on) so that the outgoing player's row is closed before the incoming substitute's row is opened.

| Column | Type | Nullable | Source | Description |
|---|---|---|---|---|
| `lineup_id` | SERIAL (PK) | No | — | Surrogate primary key |
| `match_id` | INT (FK) | No | Resolved | Reference to `matches` |
| `player_id` | INT (FK) | No | Q30 → resolved | Reference to `players` via `source_player_id` |
| `team_id` | INT (FK) | No | `contestantId` → resolved | Reference to `teams` via `source_team_id` |
| `starting_xi` | BOOL | No | Q131 | `true` if formation position > 0; `false` for bench |
| `formation_position` | SMALLINT | Yes | Q131 / Q145 | 1–11 for starters (from typeId 34 Q131); updated to Q145 value for subs who enter; `NULL` for unused subs |
| `position_code` | SMALLINT | No | Q44 (typeId 34) | Raw provider code: `1`=GK `2`=DEF `3`=MID `4`=FWD `5`=SUB |
| `position` | VARCHAR(5) | No | Q44 → mapped | Human label: `GK` / `DEF` / `MID` / `FWD` / `SUB`. For starters: mapped from typeId 34 numeric code. For subs: text value from typeId 18 Q44 |
| `shirt_number` | SMALLINT | Yes | Q59 (typeId 34) | Shirt number on the day |
| `is_captain` | BOOL | No | Q194 (typeId 34) | `true` if this player's `source_player_id` matches qualifier 194 |
| `team_formation` | VARCHAR(15) | Yes | Q130 (typeId 34) | Raw formation ID from provider. No decoding applied — appendix 14 mapping not yet available. Denormalised: repeated for all rows of the same team in the match |
| `minute_in` | SMALLINT | Yes | Pass 1 / 2b | `0` for starters; `timeMin` of typeId 19 for subs who play; `NULL` for unused subs |
| `minute_out` | SMALLINT | Yes | Pass 2a / 2c / 2d | `timeMin` of the exit event; `NULL` if played to the end or never entered |
| `exit_reason` | VARCHAR(15) | Yes | Event type + qualifier | `substitution` / `red_card` / `second_yellow` / `NULL`. Disambiguates why `minute_out` was set — analytically critical for disciplinary records and minutes-played calculations |
| `raw_status_flag` | SMALLINT | Yes | Q227 (typeId 34) | Raw qualifier 227 value — meaning unconfirmed (all-zero in observed data); preserved for future decoding |
| `raw_sub_flag` | SMALLINT | Yes | Q293 (typeId 19) | Raw qualifier 293 value — meaning unconfirmed; preserved for future decoding |

> **Unique constraint** on `(match_id, player_id)` — one row per player per match. Unlike most other tables where `ON CONFLICT DO NOTHING` is used, lineup corrections use `ON CONFLICT (match_id, player_id) DO UPDATE` because providers do issue lineup amendments post-match.

> **Prerequisite:** the `matches` table must contain the corresponding `source_match_id` before a lineup file can be loaded. Files for unresolved matches are skipped and logged — they are not errors.

> **`position` vs `preferred_position`:** `position` here is the match-specific role assigned by the manager. `players.preferred_position` is the player's general role. They frequently differ (e.g. a natural CM deployed as a DM) and must never be conflated.

> **Minutes played calculation** (for gold layer `player_season_stats`) is a single expression that requires no re-scan of the `events` table:
> ```sql
> COALESCE(ml.minute_out, m.match_length_min) - COALESCE(ml.minute_in, 0) AS minutes_played
> FROM match_lineups ml
> JOIN matches m USING (match_id)
> ```

> **`team_formation` denormalisation:** this is a team×match level fact, repeated across all rows for that team (2 teams × up to 23 players = up to 46 rows per match). The repetition cost is negligible; the benefit is avoiding a join for the most common formation query.

---

### 3.10 `events`

The granular event stream for each match. One row per event. This is the primary raw data table fed by the event data provider.

| Column | Type | Nullable | Description |
|---|---|---|---|
| `event_id` | BIGINT (PK) | No | Surrogate primary key |
| `match_id` | INT (FK) | No | Reference to `matches` |
| `period` | INT | No | 1 = first half, 2 = second half, 3/4 = extra time |
| `minute` | INT | No | Match minute of the event |
| `second` | INT | Yes | Second within the minute |
| `event_type` | VARCHAR(50) | No | `pass` / `shot` / `dribble` / `tackle` / `foul` / etc. |
| `player_id` | INT (FK) | Yes | Primary player involved — null for some events |
| `team_id` | INT (FK) | No | Team that produced the event |
| `x` | FLOAT | Yes | Pitch x-coordinate (0–100 scale) |
| `y` | FLOAT | Yes | Pitch y-coordinate (0–100 scale) |
| `end_x` | FLOAT | Yes | Destination x — for passes, shots, carries |
| `end_y` | FLOAT | Yes | Destination y |
| `outcome` | VARCHAR(20) | Yes | `success` / `failure` / `blocked` / etc. |
| `secondary_player_id` | INT (FK) | Yes | Recipient / fouled player / assist player |
| `xg` | FLOAT | Yes | Expected goal value — for shots only |
| `raw_data` | JSONB | Yes | Full raw payload from provider for future-proofing |

> `raw_data` (JSONB) stores the full provider payload. As the model evolves and new event attributes are needed, they can be promoted from `raw_data` to proper columns without losing historical data.

---

## 4. Medallion Architecture

The data pipeline follows a three-layer medallion structure. Each layer has a clear contract and direction of data flow: **Bronze → Silver → Gold**. Layers never write backwards.

### 4.1 Bronze Layer — Raw Ingestion

The bronze layer is the unmodified record of what was received from the data provider. Nothing is cleaned, validated, or transformed. Its purpose is to be the immutable source of truth.

- Raw event payloads exactly as received from the API, stored as JSONB or flat files.
- Raw match metadata (scores, dates, teams) as returned by the source.
- Ingestion metadata: `received_at` timestamp, provider name, batch/request ID.
- **Never modified after ingestion** — corrections happen downstream.

> If a data provider sends incorrect data and later corrects it, the bronze layer retains both versions. Reruns of the silver layer process the corrected version cleanly.

---

### 4.2 Silver Layer — Clean & Structured

The silver layer contains the validated, deduplicated, and entity-resolved data in the relational schema defined in Section 3. This is the **primary layer for all analytical work**.

- All 10 relational tables defined in this document live here.
- Provider IDs are resolved to internal IDs (team names, player names normalised).
- Duplicate events and matches are deduplicated using `source_match_id` and event fingerprints.
- Data quality flags may be added (e.g. `manually_reviewed`, `event_quality_score`).
- **This layer is the source of truth** — never bypass it to query bronze directly.

---

### 4.3 Gold Layer — Aggregates & Analytics

The gold layer contains pre-computed aggregates, derived metrics, and model-ready datasets. It is entirely derived from silver — **never a source of truth** — and can be fully recomputed at any time.

| Table | Contents |
|---|---|
| `team_season_stats` | Points, W/D/L, GF/GA, xG for/against, possession avg, PPDA |
| `player_season_stats` | Goals, assists, minutes, xG, xA, progressive passes, pressures |
| `match_summaries` | Pre-joined match row with team names, scores, and top-level metrics |
| `model_features` | Flattened, feature-engineered tables ready for ML model input |

> **Do not populate the gold layer during initial data collection.** Build it once the silver layer is stable and you have a concrete analytical need. Premature aggregation creates technical debt — recomputing from silver is always cheap.

---

### 4.4 Layer Summary

| Table | Layer | Notes |
|---|---|---|
| `competitions` | Silver | Timeless entity, inserted once |
| `seasons` | Silver | Inserted once per season |
| `competition_seasons` | Silver | Inserted once per edition |
| `teams` | Silver | Inserted once per club |
| `team_competition_seasons` | Silver | Inserted each season enrollment |
| `players` | Silver | Inserted once per player |
| `player_squads` | Silver | Inserted per squad spell — transfers create new rows |
| `matches` | Silver | Resolved from bronze match metadata |
| `match_lineups` | Silver | Resolved from bronze lineup data |
| `events` | Silver | Resolved from bronze event stream |
| Raw API payloads | Bronze | Stored as JSONB / flat files, never modified |
| `team_season_stats` | Gold | Derived from events + matches |
| `player_season_stats` | Gold | Derived from events + lineups |
| `match_summaries` | Gold | Denormalised view for dashboards |

---

## 5. Scalability Notes

The model was designed with the following expansion paths in mind:

- **Adding a new league** (e.g. Premier League) — insert one row into `competitions`, reuse existing `seasons` rows, insert team and player records. Zero schema changes.
- **New season** — insert one row into `seasons`, one row per competition into `competition_seasons`, new rows into `team_competition_seasons` for promoted/relegated teams. Existing rows untouched.
- **Mid-season transfers** — close the current `player_squads` row (set `end_date`), insert a new row for the new team. The `events` table always references `player_id`, so historical events remain correctly attributed.
- **New event types** — add new `event_type` values. Raw payload is preserved in `raw_data` (JSONB) so no historical data is ever lost when new attributes are promoted to columns.
- **Cup competitions** — treated as a new `competition_id` with its own `competition_season` rows. Matches and events follow the same schema.