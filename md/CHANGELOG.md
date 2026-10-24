# Changelog — project state history

Moved out of `CLAUDE.md` on 10 Oct 2026 so that file stays a set of rules and
pointers rather than a diary. Newest first. Everything below is the verbatim
"Current State" section of `CLAUDE.md` as it stood on 29 Sep 2026; add new
entries above it, dated, with measured before/after numbers.

The evergreen traps from these entries were also copied into `CLAUDE.md`
("Traps") and the path-scoped `.claude/rules/*.md`.

---

## 24 Oct 2026 — Pizarra revised after the author's review (landing + season)

- **Highlight panel**: the dark `night` panel (#16191F, lit inks) read as too
  heavy. It is now a light Espanyol-blue `spot` (#DFE7F4, 1.14:1 against the
  board) with the ordinary inks, on the landing (last match, chain pitch) and
  the season page ("Partido a partido"). No more black fills: primary buttons
  are team blue, hovers go to `team-deep` (#0C2D68).
- **Verdict colours back**: green / amber / red (`good` #17703F, `fair`
  #8A6414 as text and `fair-fill` #E0B33A as a fill, `bad` #B3261E) for
  percentiles (70 / 45 thresholds unchanged), positive / negative deltas,
  results and KPI rank chips. Blue stays for Espanyol, `rival` for the
  opponent's marks. DESIGN.md: "Identity / Verdict Rule" replaces "Two Inks".
- **Nav**: frosted and raised (white 78 %, backdrop blur, hairline ring,
  `shadow-float`), with a small team disc mark; it no longer vanishes over
  the white board. Active section on spot in team blue.
- **Season page**: the five figures are separate raised white cards with
  green / neutral / red rank chips (top six / bottom six); the reading has its
  own full-width card with a drawn quotation mark and two text columns from
  xl; "Partido a partido" holds two white sub-cards. The form strip gained an
  xG for / against bar pair per match. The two static rolling charts became
  `SeasonTrend` (client island): one bar per match plus the 5-match line, an
  xG / xT switch, and a readout of the selected match (hover, tap, or arrow
  keys; `aria-live`). `RollingArea` is back to its handoff-only form.
- **Checks**: typecheck and build pass; detect clean on the landing, and on
  both seasons except one advisory "shape-assembled illustration" on the
  trend chart (a false positive: those shapes are the per-match data bars);
  new text pairs 5.2–9.0:1; no text under 12 px; no horizontal scroll at
  390 px.

## 24 Oct 2026 — screen 01 (season) moved to Pizarra

- **Structure** agreed with the author through `/impeccable shape` ("reading
  first"): h1 with two facts (table position and points; xG-difference rank in
  team blue), the record line and season pills; a lead panel with the five
  figures and the authored reading (moved up from last place); the league
  table beside the page's one night panel, "Partido a partido" (form strip +
  the two rolling trends); team profile; leaders; numbered exits; footer.
  The page moved to `web/app/[equipo]/temporada/[season]/` (URL unchanged).
- **Same data and encodings**; colours remapped per `DESIGN.md`: zone bars
  team (Europe, 5th at 45 %) / rival (relegation); signed differences team /
  rival (ink on the club's own band, where rival on panel-deep is 4.2:1);
  percentile tiers team / slate / rival at the old 70 / 45 thresholds; form
  results as a team-lit disc (G), night-text ring (E), rival-lit disc (P),
  Spanish letters instead of W/D/L.
- **Shared parts**: `Arrow`, `PillNav` (new `active` prop) and `BoardFooter`
  (new `photoCredit` prop; off away from the photo) moved from
  `components/landing/` to `components/shell/`; new `components/shell/Exits`.
  `RollingArea` gained `tone="night"` (HTML labels at a real 12 px, fixed CSS
  height); `/demo` keeps the handoff tone. `PercentileCard`/`PercentileRow`
  in `TeamProfile.tsx` stay handoff for screen 03 until it moves.
- **Checks**: typecheck and build pass (103 pages); `impeccable detect` clean
  on the sources and on both rendered seasons; one `h1`; no text under 12 px;
  no horizontal scroll at 390 px; 45 tab stops, each form cell named in full
  ("Jornada 7, en casa ante Elche: derrota 1–3, xG neto +0.1…").
- **Open**: the 2026/27 reading (written at matchday 3) is shown as written,
  with its provenance line; the author rewrites it.

## 24 Oct 2026 — new visual world "Pizarra", landing rebuilt

- **Why**: the handoff look (cream paper, mono eyebrows, bordered cards) read as
  an AI template; critique of 22 Oct scored the site 24/40 with a P0 on the
  landing (a pipeline-status page, author unnamed). The author approved
  replacing the visual world. Branch `redesign/visual-world`; snapshot of
  everything before it: tag `pre-redesign-2026-10-23`, plus
  `.impeccable/archive/2026-10-23-pre-redesign/`.
- **World**: "Pizarra", the coach's tactics board. White enamel board, flat
  panels one tone step apart (no borders, no shadows), two inks (Espanyol blue
  `team`, marker red `rival`), one dark `night` panel per screen with lit inks,
  Hanken Grotesk, data drawn as round-capped strokes, floating pill nav, real
  stadium photo (Commons, Elemaki, CC BY 3.0, credited in the footer).
  Tokens in `web/app/globals.css`; system in `DESIGN.md`; landing contract in
  `.impeccable/surfaces/web-app-page-tsx.md`.
- **Landing `/`**: season highlight (headline = table position next to the
  xG-difference rank, both from data; figures; per-match xG stroke; "Ver la
  temporada"; byline with LinkedIn/GitHub) beside a numbered depth index
  (Temporada → Partido → Secuencia → Jugador → Método) with real thumbnails;
  the latest match on the dark panel, where hovering/focusing/tapping a shot
  draws its possession (vertical pitch on phones); author block; footer.
  Components in `web/components/landing/`. The old coverage cards became one
  status line.
- **Structure**: the pages still in the handoff world moved into the route
  group `web/app/(handoff)/` (URLs unchanged), which now owns the handoff page
  padding; the root layout lost it. `npm run build`: 103 static pages, same
  routes as before.
- **Accessibility on the landing**: one `h1`; shots are keyboard buttons with
  labels and `aria-pressed`; visible focus rings; text ≥ 12 px; all text pairs
  ≥ 4.5:1 (measured: slate on panel-deep 5.2, rival on panel 4.6, lit inks on
  night 7.8–8.0).
- **Open**: the 2026/27 season reading in `content/narratives.ts` was written
  at matchday 3 and is stale (now 7); not shown on the landing. Number locale
  still en-GB by choice. The other pages move over next (`md/REDESIGN_HANDOFF.md`).

## 22 Oct 2026 — screen 02 map → sequence link

- **Feature**: on `/{team}/partido/{match_id}` a click on a shot, a progression arrow or a
  defensive action opens its chain in the sequence block (selected, scrolled into the list,
  page scrolled to the block), as on screen 03. Progression arrows gained a hover card.
  `components/match/SequenceLink` + `MatchMaps`. Built in a cloud session (commit `5858a1a`).
- **Export**: `shots[].s`, `progression[].s` / `.minute` / `.player`, `defence.actions[].s`
  (index into the file's `sequences[]`, null for opponent shots and chains under three
  actions). `PROGRESSION_SQL` rewritten with aliases + `silver.players` join. `schema_version`
  not bumped (additive, optional on the site). WEB_DATA §7.1.
- **First real run** (22 Oct): 99 files, 11.8 s, no errors. Links over 45 match files: own
  shots 384/577 (67 %), progression 1,230/1,350 (91 %), defensive actions 2,720/5,998 (45 %).
  Match 70: 15/20, 29/30, 52/138 (the cloud estimate by coordinate matching was ~54).
- Checks: every non-null `s` in range; no opponent shot links; every linked row has an
  action at the same x/y (and minute, for shots and defence) in `sequences[s].actions`.
  Structural diff vs the previous export: only the new keys plus `generated_at`, no row
  reordered. `pytest tests/test_export_match.py tests/test_export_player.py` 15 passed.
  Browser on match 70: shot, arrow and diamond clicks open the right chain; xT ↔ VAEP clears
  the pin; player page link still works; no console errors.

## 17 Oct 2026 — screen 03 event map: hover on every mark, map → sequence link, heat or events

- **Hover**: every mark on the player's event map names its match (matchday, opponent, venue)
  and minute, plus its own facts (completed / failed and xT, xG, in the box). Heat zones give
  their count and share. New `viz/HoverCard`; delegated listeners via `data-i` / `data-zone`.
- **Link**: a click on a mark whose chain the page can draw pins that chain in the sequence
  block and scrolls to it (bar + "Volver a las 10 mejores"). Share of 2025/26 map rows that
  link: carries 100 %, receptions 97 %, passes 88 %, touches 87 %, shots 67 %, defensive
  actions 45 % (most are in the opponent's possession; shots miss on penalties and direct free
  kicks, chains under three events).
- **Heat / events switch** on Toques, Recepciones, Defensivas. Defensive heat is binned on the
  site with `zoneId`, a copy of `gold.zone_id`.
- **Export, `schema_version` 1 → 2**: every map row gains `minute` and `s` (index into
  `sequences.index`); `maps.touches` + `touch_types` added; `maps.progressive_receptions`
  replaced by `maps.receptions` (every reception, flags progressive / in box). Player files
  2025/26: 2.6 MB → 3.5 MB. Full re-export: 99 files, all at version 2.
- Side fix: the `DefensiveActions` tooltip sat lower than its mark by the chip row's height
  (positioned against a wrapper that included the chips); it now has a pitch-only wrapper.
  Affects the match page too.
- Checks: `tests/test_export_player.py` 12 passed; `npm run typecheck` and `npm run build`
  (103 pages) clean. **Not yet looked at in a browser.** README-PLAYER "Review of 17 Oct 2026".

## 16 Oct 2026 — screen 03 review: Plantilla tab, match filter, picker, match-page visuals

- Plantilla nav item live; `/{team}/plantilla/{season}` = first squad player's page.
- Player file is per match: `matches[]` (bio counts + `kpi_parts`), map rows tagged with `m`
  and uncapped, `sequences.index` + season top `entries` in match-file format. Written compact
  (no indent): 3.1 MB → 3.7 MB for 47 files (indented it was 9.8 MB).
- Check in the export: per-match KPI and bio sums reproduce gold's season values for all 47
  players. Cross-check: 14,525 index rows found in their match files with equal xT and VAEP.
- Visuals moved to the match page's components and weights; sequences use `SequenceBrowser`
  with a gold focus node. README-PLAYER "Review of 16 Oct 2026".

## 15 Oct 2026 — screen-03 percentile spine, AM role, ground duels

The user's per-position sheet (`metricas_web.xlsx`) was reviewed metric by
metric and settled as four cards of six per role (README-PLAYER §4). Record:
`GOLD_LAYER §4.6.2a`, `§4.6.17`, gates G18 and G19.

- **Role `AM`** (`player_season_stats.role_group`): a midfielder with ≥ 30% of
  his touches in the final third. Slot-AM was 15 eligible players (2025/26),
  9 (2026/27); the role gives 27 / 23 (MID 70 / 64). Pools into MID under 20.
- **Ground duels**: won `Tackle`, lost `Attempted Tackle` + `Challenge` (no
  50/50 in Opta). `ground_duel_win_rate`: CB 55%, MID 43%, W/ST 36%.
- **New gold columns**: `shots_blocked` (3,194), `headed_shots` (2,108),
  `progressive_carries_into_final_third` (4,483), `passes_long_completed`,
  three reception counts from `gold.pass_receptions`, `matches_subbed_off`,
  and 32 generated per-90s / 5 rates on the season table.
- **`gold.percentile_metrics` reseeded** to the ranked spine (36 → 61
  metrics), with an attempt floor per rate (10; 20 for pass %). Percentile
  rows 12,412 → 13,936.
- **Build order**: `pass_receptions` now runs before `player_match_stats`.
- **Export**: `PLAYER_SPINE` (config), `player_percentiles.py` (ranking, 25%
  gate). Run with a 450 gate it reproduces gold 13,394 / 13,394 rows (G19).
  Not yet called by an exporter: `players/{id}.json` lands with screen 03.
- **Unchanged**: every pre-existing `player_match_stats` column (content hash
  identical before/after). The published export re-generated byte-identical
  apart from `generated_at`. The season table's old columns did hash
  differently from the 29 Sep build; the step is deterministic (rebuilt twice,
  identical), so the cause is silver edits since then (bio/label fields), not
  this change.

---

## State as of 29 Sep 2026

> Last updated: 29 September 2026

### Game state + xG own-goal fix (27–29 Sep 2026)

Silver columns, one gold table + view, four sequence columns, and a fix to xG.
Record: `md/GOLD_LAYER.md §4.8` and gate G17.

- **`silver.events.{home,away}_{goals,players}_before`**: the scoreline and
  players on the pitch *before* each event, by `(json_index, event_id)`.
  `src/silver/events/game_state.py`, run as events post-processing (NULL-state
  matches only; `--all` is a 30 s no-op when nothing changed). Own goals (Q28)
  count for the opponent. A Q32/Q33 card or a `Player retired` removes a player
  only if they were on the pitch (115 of 136 red cards count). The module
  refuses to commit unless every match ends on its `silver.matches` score
  (496/496, and half time 496/496).
- **`gold.match_game_states`** (2,463 segments of constant state, durations tile
  every period exactly) and the view **`gold.team_game_states`** (per team,
  with `score_state` / `numbers_state` labels). `gold.sequences` gained
  `start_goals_for/against` and `start_players_for/against`. Build step
  `game_states`, right after `ddl`.
- **xG fix** (`xg.py`): own goals were filtered out of the shot query, so
  `score_diff_at_shot` ignored them (451 shots in 37 matches off by 1–2). They
  now feed the game state and are dropped before scoring; the sort is
  `json_index, event_id`. The 38 own-goal matches were re-scored (max |Δxg| 0.077,
  league total 1164.52 → 1164.09) and gold rebuilt.
- **Gold `sequences` step planner guard**: `build_sequences.sql` opens with
  `SET LOCAL enable_nestloop = off`. After a `VACUUM (FULL, ANALYZE)` the
  planner re-scanned the underestimated `poss` CTE inside nested loops, which
  is quadratic (full build stalled past 20 min; now 184 s). `GOLD_LAYER §9.8`.
- **Lineup exits fixed (29 Sep 2026)**: pass 2c/d set `minute_out` on every
  red card, and `Player retired` was ignored. Every exit now applies only to a
  player on the pitch, and pass **2e** (typeId 20) records `exit_reason =
  'retired'` (CHECK widened). 20 stored rows repaired by
  `sql/fixes/20260929_match_lineups_exits.sql`: 4 subbed-then-sent-off, 6
  retired, 10 bench reds cleared. Gold minutes for the 10 who played: −255 in
  total. Verified: re-loading all 496 matches with the fixed loader reproduces
  every repaired row exactly. Gold rebuilt from `player_match_stats`; web
  export re-run.
- **48 missing lineup rows added (29 Sep 2026)**: players not yet in
  `silver.players` when their match was first loaded (pass 1 skips unknown
  players). New flag `load_match_lineups.py --reprocess` re-runs loaded matches
  and inserts only missing rows (48 in 35 matches, 13 of them played; 0 existing
  rows changed). Re-run it after loading squads late.
- **Export determinism**: four `src/export/db.py` sorts had ties and reshuffled
  between runs (progression by `xt`, pass-network edges by count, defensive
  actions by `json_index`, sequences by `xt`); each now ends on a unique key.
- Full re-run after all of the above: lineups → game state (unchanged) → full
  gold rebuild → gates → web export.

### Sequence view, third pass (20 Sep 2026)

Export and renderer only; silver and gold untouched. Record: `md/WEB_DATA.md
§7.5`. Every match file re-exported.

- **`start_kind`**: a `pass` start refined into `corner` / `throw_in` /
  `goal_kick` / `kick_off` / `free_kick` from the first pass's qualifiers —
  28% of all sequences were labelled "Pase". **`end_action`**: how a
  `turnover` (63% of sequences) lost the ball, from our last action, with
  failed passes split by sub-type. Both from qualifiers in the export; the
  gold CASE is the natural next step.
- **Shots end at `goal_mouth_y`** (present on every shot, off target too),
  not the goal centre. **`simplify()` is RDP** under the same 6-point budget:
  mean shape error 9.3 m → 4.1 m.
- `SequenceDetail`: start halo, end ring, regain diamond, HTML hover card
  (`ShotMap`'s pattern) with dimming; nodes 1.9 → 1.5.

### Sequence review, second pass (19 Sep 2026)

Seven chains flagged by eye on the deployed match pages were traced to the raw
events; full record in **`md/GOLD_SEQUENCES.md §6.6`**. Two data defects, both
fixed and rebuilt over the 458 matches (events → sequences → gold → export):

- **`carries.py` matched event names this pipeline never emits** (`BallRecovery`,
  `Shot`, `MissedShot`…), so no carry was ever synthesised after a ball
  recovery or keeper pick-up, nor before a shot. Also fixed: the carry clock
  averaged minute and second separately; and carries under **1 m** (21% of
  them, coordinate rounding) are no longer created (`MIN_CARRY_DISTANCE_M`).
  255,795 → **227,047** carries; non-carry rows unchanged.
- **Set-piece passes now open a sequence before the sandwich guard is
  consulted**, and **Q124 (goal kick)** joins Q5/Q6/Q107. 1,018 throw-ins and
  209 goal kicks had been absorbed into the possession the ball had left.
  150,990 → **152,217** sequences; every other event partitions identically.
- Gold: `ball_out` no longer requires the opponent to take the restart.
  Export/web: `Clearance` is its own action kind with a destination; every
  action ships a `type` key that the sequence view names on hover.
- **`sequence_id`s renumber on every full rebuild** (per-team counter). Quote
  kick-off time, not the id, when reporting a chain.
- Pre-rebuild snapshot: `silver.events_backup_20260921` (941,875 rows) — drop
  it once satisfied. The events pipeline has **no delete step**: to re-run it
  over loaded matches, `TRUNCATE silver.events, gold.sequences,
  gold.sequence_players, gold.sequence_phase_segments` first (not `DELETE` —
  see §6.6 on why that ran for 15 minutes).
- **After a full rebuild, `VACUUM (FULL, ANALYZE) silver.events`.** The reload
  UPDATEs every row several times (SPADL, VAEP, xG, foot preference) and the
  sequence pass once more, which leaves the heap ~3× its real size (2,525 MB
  vs 935 MB on 21 Sep 2026) and turned gold's `sequences` step from ~3 minutes
  into 32. The vacuum takes ~90 s, needs an exclusive lock and roughly the
  table's size in free disk.

### Sequence review applied (16 Sep 2026)

A full review of the sequence path (classifier → gold → phases → export → web)
produced one rule set and four gold changes, all applied and rebuilt over the
458 matches. Full record with before/after tables: **`md/GOLD_SEQUENCES.md §6.5`**.

- **Classifier** (`sequences.py`, always on, no flag): the sandwich exemptions,
  the regain start rules and the assist guard are now **owner-relative** (a team
  cannot regain the ball from itself; an opponent's mirror record between two of
  our actions is not a team change); a **failed take-on ends the possession only
  when the next on-ball action is the opponent's** (Opta: a failed paired tackle
  means "won the tackle but not the ball"; Q211 overrun always ends). 155,050 →
  150,990 sequences; opponent on-ball events inside a sequence 1,147 → 0;
  every assist now in its own team's chain. `--all` re-classifies every match.
- **xT** (`xt.py`): a failed pass or carry earns **0**. It used to get the full
  zone gain of its intended destination, which decided the "best sequences"
  ranking on the match pages (64% of the top 12 drew most of their xT from a
  pass that never arrived).
- **`gold.sequences`**: `end_x`/`end_y` from the possessing team's last action
  (the last event's coordinates are in the opponent's frame when it is theirs);
  four new `outcome` values — `retained`, `corner_conceded`, `foul_committed`,
  `interrupted` — split out of `turnover`; DDL constraint updated in
  `sql/ddl/gold_sequences.sql` (idempotent ALTER).
- **Phases** (`phases.py`): the zone a sequence ends in confirms itself, so a
  chain ending "pass into the final third, shot" gets its attacking segment.
- **Possession** (`build_team_match_stats.sql`): start-to-next-start clock
  attribution replaces the one-second-floor duration sum (`GOLD_LAYER.md §4.5.0`).

### Done
- Silver schema fully designed and operational for La Liga — all 10 tables
- **Seasons loaded: 2024/25 (47 matches), 2025/26 (380), 2026/27 (31 and counting) — 458 total.** 2026/27 was loaded on 5 Sep 2026 through matchday 3 plus one early matchday-6 fixture; re-run the full order in "How to Run Pipelines" to pick up later matchdays
- Data ingestion pipelines built for matches, lineups, squads, teams, players, events (carries + xT + SPADL mapping)
- SPADL columns (`spadl_type_id`, `spadl_result_id`, `spadl_bodypart_id`) added to `silver.events`; backfill via `python -m src.silver.events.spadl`
- **VAEP is live.** `vaep_value` / `vaep_offensive` / `vaep_defensive` are populated across all 458 matches (811,792 of 941,875 events; the rest are action types SPADL does not map). The previously shipped model was degenerate (`scores` AUC excluding goal actions **0.5222** — a coin flip; it had learned "was this a successful shot?"). It was retrained from scratch in a companion repo, Platt-calibrated on held-out Opta, and integrated on 11 Aug 2026: **0.7465** AUC excluding goal actions, `corr(vaep_offensive, xt)` **−0.1942 → +0.2605**. Backfill via `python -m src.silver.events.vaep`; consult the **`vaep-model`** skill before touching `vaep.py`, `_build_features` or `models/vaep/*.json`
- Possession sequence classifier implemented (`sequence_id`, `sequence_start`, `sequence_end`, `sequence_event_number`)
- Squad diff strategy live with `squad_snapshot_log` audit table
- Shot enrichment columns: `shot_play_pattern` (7-value enum from Q22/23/24/25/26/160/9) and `first_time` (bool from Q328)
- Foot preference enrichment: `preferred_foot` on `silver.players` (derived from Q20/Q72 counts) and `is_weak_foot` on `silver.events`. Runs automatically as post-processing inside `python -m src.silver.events`, alongside xG; `foot_preference.py` is the standalone backfill
- SPADL and VAEP are computed **inline** by the events pipeline (`include_spadl` / `include_vaep`, both default True). `src.silver.events.spadl` and `.vaep` are backfill tools for already-loaded rows, not part of the new-season path

### Gold Layer — COMPLETE: sequences, teams and players (5 Sep 2026)

The full gold layer is designed in `md/GOLD_LAYER.md`. **The operating manual is
`GOLD_LAYER.md §9`** — run order, validation gates, documented deviations and
troubleshooting. Read that before touching anything in `src/gold/`.

Built and populated over all 458 matches (counts below are the 458-match
snapshot; the 29 Sep 2026 rebuild over 496 matches is in `GOLD_LAYER.md §9.4`):

| Table | Rows |
|---|---|
| `gold.sequences` | 152,217 |
| `gold.sequence_players` | 449,415 |
| `gold.sequence_phase_segments` | 244,417 |
| `gold.team_match_stats` | 916 |
| `gold.team_season_stats` | 60 |
| `gold.player_match_stats` | 14,394 |
| `gold.player_season_stats` | 1,464 |
| `gold.player_season_percentiles` | 9,735 |
| `gold.formation_slot_positions` | 209 (seeded, generated) |
| `gold.pass_receptions` | 386,452 (22 Sep 2026, 496 matches) |
| `gold.match_game_states` | 2,463 (29 Sep 2026, 496 matches) + view `gold.team_game_states` |

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

**`gold.pass_receptions` (added 22 Sep 2026)** is one row per completed pass
with a resolved receiver: the reception point (pass end, `rx_zone_id`) and the
receiver's *spell* — their unbroken run of actions until a teammate acts or
the sequence ends — classified by `spell_end`. Opta records no receiver; the
rule is the next same-team event by `(json_index, event_id)`, skipping opponent
and administrative rows. Built by the last `build_gold` step
(`--step pass_receptions`, ~80 s). Definition, measured numbers and gate G16:
`GOLD_LAYER.md §4.7`.

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
**458 matches / 858,297 events with a `sequence_id` / 152,217 sequences** after the
19 Sep 2026 pass (§6.6).

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
is clock-based, start-to-next-start, since 16 Sep 2026 (`GOLD_LAYER.md §4.5.0`;
the one-second floor it replaced is no longer needed), and style clustering
must filter `event_count >= 3` (§4.1.9), which now drops ~40% of rows.

Pre-fix snapshot kept in `silver.sequences_backup_20260816` (901,805 rows) —
drop it once you are satisfied.

Note `Ball touch` with `outcome = 'success'` is a *deflection* (Opta: "ball
simply hit the player unintentionally") and correctly does **not** end a
sequence; do not "fix" that.

### Future Scope
- Segunda Division (zero schema changes needed)
- Web visualisation on top of gold layer
- Sequence & phase clustering (unsupervised ML) — proposal and roadmaps in `md/SEQUENCE_CLUSTERING.md`
