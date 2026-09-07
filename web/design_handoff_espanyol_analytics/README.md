# Handoff: RCD Espanyol Analytics Platform

## Overview
A five-page football analytics web app built on Opta event data for RCD Espanyol. It is a portfolio-grade
analytics product: season overview, match analysis, player profile, sequence laboratory, and match-group
comparison. Pages 01 (Season overview) and 02 (Match analysis) are fully specified and pixel-final.
Pages 03–05 exist as complete but *first-pass* layouts and should be treated as lofi until iterated.

## About the Design Files
The files in this bundle are **design references authored in HTML** — prototypes that show intended look,
layout, data shapes, and interaction behavior. They are **not production code to copy**.

The task is to **recreate these designs in the target codebase's own environment** (React + Vite,
Next.js, Vue, etc.) using its established patterns, component library, and charting approach. If no
frontend exists yet, pick the stack that fits the project — the natural choice here is React + TypeScript
with **D3 for scales/geometry and plain SVG (React-rendered) for marks**, which is exactly how the
prototype is structured: all chart geometry is computed in JS, then emitted as SVG elements.

The prototype's own runtime (a small template/logic component format) is an authoring convenience.
Ignore it. What matters is: the markup structure, the inline style values, the geometry math, and the
state model documented below.

## Fidelity
**High-fidelity for pages 01 and 02.** Colors, typography, spacing, radii, chart geometry, and
interaction behavior are final — recreate them precisely.
**Mid/low-fidelity for pages 03 (player), 04 (sequence lab), 05 (comparison).** Layout and component
inventory are indicative; styling tokens still apply, but expect design iteration.

---

## Design Tokens

### Colors (CSS custom properties, defined on `:root`)
| Token | Hex | Use |
|---|---|---|
| `--paper` | `#FAEDE1` | Page background, inner chips/rows |
| `--card` | `#FFFBF5` | Card surface |
| `--line` | `#E4D6C5` | Card borders, dotted table rules |
| `--ink` | `#17181C` | Primary text, hard rules, tooltip surface |
| `--mid` | `#6B6257` | Secondary text, opponent values |
| `--faint` | `#9A8D7E` | Meta text, axis labels, "away" mark colour |
| `--blue` | `#0B4C9E` | Primary accent (Espanyol), positive series |
| `--blue2` | `#2C82D2` | Secondary blue (mid-tier chain colour, dark-nav active) |
| `--pos` | `#1E7A4D` | Positive deltas, top-quartile percentile |
| `--neg` | `#A83A2C` | Negative deltas, bottom-half percentile, defensive line |
| `--gold` | `#B0872F` | Carries, group B in comparison |

Supporting literals used inline: `#EFE3D4` (bar track), `#C9B9A6` / `#8E8172` / `#D9C9B6` (opponent
fills, pitch lines), `#EADCCB` (momentum gridlines), `#F6EFE6` (text on dark), `rgba(11,76,158,.10)`
(selected row tint), `rgba(11,76,158,.20)` / `rgba(168,58,44,.18)` (area fills).

### Typography (Google Fonts)
- **Outfit** 400/500/600/700 — display + numerics. H1 46px/1.0/-0.035em 700; H2-card 19px/-0.015em 700;
  H3-small 16px 700; KPI value 44px/0.86/-0.02em 700; stat value 16–17px 700.
- **Figtree** 400/500/600 + italic 400/500 — body and names. Body 12–13.5px; narrative 17–20px/1.42–1.45;
  italic 12.5–14.5px for deks and notes.
- **DM Mono** 400/500 — all labels, meta, axis text, numeric table cells. 8–11px with
  `letter-spacing:.1em–.22em` and `text-transform:uppercase` for labels; 9–12px plain for values.

### Spacing / shape
- Page padding: `34px 40px 60px`. Card padding: 16–18px. Card gap: 14–16px. Section bottom margin: 16px.
- Radii: cards **14px**, inner chips/rows **9–11px**, pills/bars **999px**, hero **22px**, crest slot 12px.
- Borders: `1px solid var(--line)`; emphasis rules `1px solid var(--ink)`; narrative block `1px solid var(--ink)`.
- No shadows anywhere. Flat, print-adjacent surfaces.

---

## Global Shell

- **Hero header**: 238px min-height, radius 22px, dark ground `#0A2340`, full-bleed photo behind a
  `linear-gradient(100deg, rgba(7,26,48,.94) 0%, rgba(7,26,48,.86) 34%, rgba(7,26,48,.46) 70%, rgba(7,26,48,.30) 100%)`
  scrim, title + filter chips over it. Two user-supplied image slots: hero photo (`esp-hero`) and club
  crest (`esp-badge`, 54×54, `object-fit:contain`, sits left of the page-01 H1).
- **Nav**: three variants exist behind a `navStyle` prop — `masthead` (default), `rail`, `topbar`.
  Ship `masthead` unless told otherwise; the others are exploration.
- **Season switcher**: `2025/26` (32 matchdays played) and `2024/25` (38, final). Every page-01 number
  is season-scoped.
- **Configurable at build time** (were prototype "tweaks"): `primaryBlue` (default `#0B4C9E`, writes
  `--blue`), `paperTone` (default `#FAEDE1`, writes `--paper`), `navStyle` enum.

---

## Screen 01 · Season overview

**Purpose**: read the season at a glance — where the club sits, how it is trending, what kind of team it is.

**Layout** (top to bottom, all full-width unless stated):
1. **Page header** — flex row, `border-bottom:1px solid var(--ink)`, `padding-bottom:12px`.
   Left: eyebrow `01 · SEASON OVERVIEW` (DM Mono 9.5px, .22em, blue) then a row of [crest 54×54] +
   H1 "The season, **measured**" (46px, "measured" in `--blue`). Right: italic dek, `max-width:400px`.
2. **KPI strip** — `grid-template-columns:repeat(5,1fr); gap:14px`. Each card: label (mono 9px, .16em,
   uppercase) + rank chip (mono 9px, white on `--pos` when top-6, else `--mid`); value 44px Outfit 700
   with unit suffix 16px in `--faint`; italic note 12.5px `--mid`.
   Metrics: Points, xG difference, Possession share, PPDA, Set-piece goals.
3. **Table + trend column** — `grid-template-columns:minmax(0,1.05fr) minmax(0,1fr); gap:16px`.
   - **La Liga table** (left): 20 rows, columns `# / Club / Pl / W / D / L / GD / xGD / Pts`. Header
     rule `1px solid var(--ink)`, rows `1px dotted var(--line)`. Position cell carries a 3px left
     border zone marker: rows 1–4 `--pos`, row 5 `--blue2`, rows 18–20 `--neg`, else transparent.
     Espanyol row: background `rgba(11,76,158,.10)`, text `--blue`, weight 700. GD/xGD coloured
     `--pos`/`--neg` by sign. Numerics in DM Mono 11px; Pts in Outfit 700 14px.
   - **Right column** (flex, gap 16): *Form · last 10* card (10 equal cells: opponent code, 23×23
     result badge — W `--pos`, D `--faint`, L `--neg` — score, xGD delta), then two identical
     **rolling charts** that flex to fill the table's height.
4. **Rolling charts** (xGD and xT) — SVG `viewBox="0 0 600 182"`, plot area x 58→590, zero line y=100,
   value scale: xGD 60px per 1.0 (ticks ±1.0/±0.5/0 at y 40/70/100/130/160), xT 150px per 1.0
   (ticks ±0.4/±0.2/0 at the same y). Positive area `rgba(11,76,158,.20)`, negative `rgba(168,58,44,.18)`,
   line `--blue` 2px, zero rule `--ink` 1px, gridlines `--line`. Y tick labels are **static SVG text**
   at x=52, size 15 (viewBox units), `text-anchor:end`. Rotated y-axis title at x=14, size 13,
   letter-spacing 2.4. X axis rule at y=176. **Matchday tick labels are HTML**, not SVG: a
   `position:relative` strip under the svg with absolutely positioned spans at `left:(x/6)%`,
   `translateX(-50%)`, DM Mono 9.5px, plus a centered "MATCHDAY" caption. Series = 5-match rolling
   mean, one point per matchday from MD5.
5. **Team profile** — section head ("Team profile" + mono caption "Per-match value · bar = percentile
   among the 20 La Liga sides, higher is better"), then `repeat(4,1fr)` cards: **Defensive,
   Possession, Progression, Finishing**. Each card: title 15px Outfit 700 + "VS LA LIGA" mono 8.5px,
   `border-bottom:1px solid var(--ink)`, then 6 metric rows (gap 10). Row = label (Figtree 12px 500
   `--mid`) + value (DM Mono 12px 600 `--ink`) on line one; 5px percentile track (`--paper` bed,
   radius 3) + 2-digit percentile on line two. Fill colour by percentile: ≥70 `--pos`, ≥45 `--blue`,
   else `--neg`.
   Metric sets (exact, in order):
   - *Defensive*: Duels won, Aerials won, Aerial win rate, Tackles won, Tackle win rate, Fouls committed
     (fouls percentile is inverted — higher percentile = fewer fouls).
   - *Possession*: Possession, Pass share · own 3rd, Pass share · mid 3rd, Pass share · final 3rd,
     Touches in box, Field tilt.
   - *Progression*: Progressive passes, Prog. passes completed, Crosses, Progressive carries, Take-ons,
     Take-on success.
   - *Finishing*: Shot accuracy, npxG for, xG per shot, xG open play, xG set piece, xG fast break.
6. **Player leaders** — section head + `repeat(4,1fr)` cards: Goals, xT per 90, Prog. passes, Carries
   (into final third). Six rows each, sorted desc; rank-1 row inverted (`--blue` background, white
   text), others on `--paper`. **Each row is a button → opens screen 03 for that player.**
7. **Footer row** — `grid-template-columns:minmax(0,1fr) minmax(0,1.4fr); gap:16px`: *Next fixtures*
   (5 rows: MD, opponent, H/A chip — H `--blue`, A `--faint` — date) and *Reading of the season*
   (bordered `--ink` block on `--paper`, eyebrow + 20px/1.42 narrative + provenance line).

---

## Screen 02 · Match analysis

**Purpose**: read one match — momentum, structure, chances, progression, defensive shape, best sequences,
individual standouts. Reference match in the mock: Espanyol 2–1 Valencia, MD31, 2.11 xG – 0.94 xG.

**Layout** (top to bottom):
1. **Page header** — eyebrow `02 · MATCH ANALYSIS`, H1 "Espanyol **2–1** Valencia" (score in `--blue`),
   right-aligned mono meta (competition · matchday · venue / date · xG line).
2. **Momentum · 5-min rolling xT** — full-width card, SVG `viewBox="0 0 1180 260"`, zero line y=120,
   scale 200px per 1.0 xT, 96 samples (one per minute + stoppage). Espanyol area
   `rgba(11,76,158,.28)` above, opponent `rgba(154,141,126,.35)` below. Event markers: vertical line
   y 8→232 with per-event colour and dash (goals solid, subs `3 3`, HT `2 3`) plus a mono 9.5px label
   flipped to `text-anchor:end` past x=900. Minute ticks at 15/30/45/60/75/90. Team names as static
   SVG text at both extremes.
3. **Pass network + Match totals** — `grid-template-columns:minmax(0,306px) minmax(0,1fr); gap:16px`.
   - **Pass network (vertical)**: SVG `viewBox="-2.5 -2.5 73 110"`, pitch drawn in **transposed**
     coordinates (see *Pitch geometry*), attack **upward**. Edges `--blue` at 0.28 opacity, width
     0.35–1.85 by combination count; nodes `--blue` filled circles r 1.7–2.96 (∝ touches) with
     `--card` 0.5 stroke; player surname above each node in DM Mono 3px.
   - **Match totals**: card header rule `1px solid var(--ink)`, then a `1fr 1fr` grid (gap 14/28,
     `align-content:space-around`) of **14 stat rows**. Row = [home value Outfit 17px `--blue`]
     [centered mono 9px uppercase label `--mid`] [away value Outfit 17px `--mid`] over a 7px
     rounded track (`#EFE3D4`) filled `--blue` to the home share.
     Order: Possession, xG, Shots, Shots on target, Big chances, Passes completed, Final-third entries,
     xT created, Yellow cards, Red cards, Fouls committed, Corners, xG open play, xG set piece.
4. **Shot map + xT surface** — `grid-template-columns:minmax(0,1fr) minmax(0,306px); gap:16px;
   align-items:start`.
   - **Shot map**: horizontal full pitch `viewBox="-2 -2 109 72"`, **both teams** — Espanyol shooting
     right (x 79–103), Valencia shooting left (x 2–26). Marker r = `1.4 + sqrt(xG)*5.2`; goals filled
     (`--blue` / `#8E8172`), non-goals translucent with team stroke. **Hover interaction**: on
     mouse-enter the hovered shot keeps opacity 1 and every other shot drops to 0.3; an HTML tooltip
     (`--ink` surface, radius 10, `translate(-50%,-124%)`, `pointer-events:none`) is positioned at
     `left:(x+2)/109*100%`, `top:(y+2)/72*100%` of the pitch wrapper and shows player, team,
     minute · body part, xG · outcome (Goal / Saved / Off target). 4-item legend below.
   - **xT surface (vertical)**: 12×8 zone grid transposed onto the vertical pitch, cells `--blue`
     with `fill-opacity` 0.06→0.92, pitch lines redrawn over the heat in `--card` at 0.6 opacity,
     LOW→HIGH gradient legend.
5. **Progression + Defensive actions** — `1fr 1fr`, both horizontal pitches `viewBox="-2 -2 109 72"`.
   Progression: 30 arrows, `--blue` 0.45px at 0.55 opacity for progressive passes, `--gold` 0.7px at
   0.9 for carries, arrowhead markers. Defensive: 34 diamonds (rotated 45°, side 1.9) — `--blue` for
   regains, `#C9B9A6` for lost duels — plus a dashed `--neg` average-defensive-line at x=41.6 with a
   mono 3.2px label.
6. **Top sequences by xT** — `grid-template-columns:minmax(0,1fr) minmax(0,318px); gap:16px`.
   Big horizontal pitch with 12 chain traces (`polyline`, `stroke-linejoin:round`), start dot r 0.9,
   end ring r 1.7 when the chain ends in a shot else 0.8. **Selection**: the selected chain renders
   `--blue` at 1.5 width / 0.95 opacity, all others `--faint` at 0.5 / 0.2. Right card is the
   sequence list (max-height 392px, scroll): 12 buttons, each `SEQ-id` + minute, description
   ("High regain → shot"), then passes · duration · xT. Selected row: `rgba(11,76,158,.10)`
   background + `--blue` border. Clicking a row selects that chain. No filters on this page (filtering
   lives on screen 04).
7. **Top players in the match** — section head + `repeat(4,1fr)` cards, **both teams**: Recoveries,
   Passes completed, Passes into final third, xG + xA. Six rows each: team chip (ESP `--blue`,
   VAL `--faint`, mono 8px, radius 4) + name + value; row 1 tinted `rgba(11,76,158,.10)`.
8. **Match read** — bordered `--ink` banner on `--paper`: eyebrow, 17px narrative, and a pill button
   "Open in sequence lab →" that navigates to screen 04 (hover inverts to `--blue` fill, white text).

---

## Screens 03–05 (first-pass)
- **03 · Player profile**: shirt-number block + first/last name display header; player tab strip
  (6 players, selected = `--blue`); radar/percentile blocks, shot map, per-90 metric rows, minutes
  timeline. Entered from page-01 player leader rows.
- **04 · Sequence laboratory**: 3-column grid `236px / 1fr / 268px` — filter rail (start zone, chain
  ending, match group, min-passes range input), chain-trace pitch + 4 aggregate figures, sequence list.
- **05 · Match-group comparison**: two group builder cards (A `--blue`, B `--gold`), a "metric spine"
  slope/dumbbell chart, and per-90 comparison rows.

---

## Pitch geometry (both orientations)

Real-world metres, drawn 1:1 in user units. **Horizontal**: 105×68, `viewBox="-2 -2 109 72"`.
- Outline `0,0,105,68`; penalty areas `0,13.85,16.5,40.3` and `88.5,13.85,16.5,40.3`;
  six-yard boxes `0,24.85,5.5,18.3` and `99.5,24.85,5.5,18.3`; halfway line x=52.5; centre circle
  r 9.15 at `52.5,34`. Lines `#D9C9B6`, 0.35 width.
**Vertical**: 68×105, `viewBox="-2.5 -2.5 73 110"`, attack upward — transpose `(x,y) → (y, 105-x)`.
- Outline `0,0,68,105`; boxes `13.85,0,40.3,16.5` and `13.85,88.5,40.3,16.5`; six-yard
  `24.85,0,18.3,5.5` and `24.85,99.5,18.3,5.5`; halfway y=52.5; circle r 9.15 at `34,52.5`; width 0.4.

**Opta → pitch conversion**: Opta x/y are 0–100 percentages. Multiply x by 1.05 and y by 0.68 for the
horizontal pitch; for the vertical pitch apply the transpose above afterwards. Opta y=0 is the
right-hand touchline looking up-pitch — normalise per team so the analysed side always attacks
right (horizontal) / up (vertical).

---

## Interactions & Behavior
| Trigger | Result |
|---|---|
| Nav item click | Switch page (`page` state) |
| Season button click | Rebuild every page-01 figure for that season |
| Page-01 player leader row click | `page → 'player'`, `player → name` |
| Shot hover (enter/leave) | `hoverShot → index` / `null`; non-hovered shots to 0.3 opacity; tooltip shows |
| Sequence row click (page 02) | `matchSeq → index`; trace highlight + row selection follow |
| Sequence lab filter chip / range | Refilter chains and aggregates |
| Comparison group chip | Reassign matches to group A/B |
| "Open in sequence lab" | `page → 'lab'` |
| Card row / button hover | `border-color: var(--blue)` on bordered rows; `opacity:.85` on filled rows; pill buttons invert to `--blue` fill + white text |

No entry animations, no transitions beyond hover colour changes. Everything is instant — deliberate.

## State Management
```ts
type PageKey = 'overview' | 'match' | 'player' | 'lab' | 'compare';

interface AppState {
  page: PageKey;              // 'overview'
  season: '2025/26' | '2024/25';
  player: string;             // 'Javi Puado'
  hoverShot: number | null;   // shot-map hover index (page 02)
  matchSeq: number;           // selected sequence index (page 02), 0
  labZone: 'Own third' | 'Middle third' | 'Final third';
  labEnd: 'Shot' | 'Box entry' | 'Loss' | 'Any';
  labLen: number;             // min passes, 1–12, default 4
  ga: string; gb: string;     // comparison group definitions
}
```
All figures in the prototype are produced by a **seeded PRNG** so the mock is stable across renders.
In production every block below becomes a query. Season-scoped data should be cached per season;
match data per match id.

## Data contracts (Opta pipeline)

The design assumes the existing medallion warehouse: raw JSONP → `bronze` → `silver` (relational:
`matches`, `teams`, `players`, `match_lineups`, `player_squads`, `events`) → `gold` (aggregates
serving the UI). Build one gold table per block; the UI should never aggregate raw events client-side.

**Screen 01**
- `gold.team_season_kpis`: `season_id, team_id, points, position, xgd, npxg_for, npxg_against,
  possession_pct, ppda, set_piece_goals, goals_total` → KPI strip.
- `gold.league_table`: `season_id, position, team_id, team_name, played, won, drawn, lost, goals_for,
  goals_against, xg_for, xg_against, xgd, points` → table.
- `gold.team_match_form`: `match_id, matchday, opponent_code, venue, result, goals_for, goals_against,
  xg_for, xg_against` → form strip **and** both rolling charts (5-match trailing mean of
  `xg_for - xg_against` and `xt_for - xt_against`; needs `xt_for/xt_against` on the same grain).
- `gold.team_season_metrics`: one row per `(season_id, team_id)` with the 24 team-profile metrics
  **plus** a league percentile per metric — `{metric_key, value, percentile}` long-form is easier to
  render. Percentiles computed across the 20 clubs; `fouls_committed` percentile must be inverted.
- `gold.player_season_totals`: `player_id, name, position, minutes, goals, npxg, xt_per_90,
  progressive_passes, carries_into_final_third` → player leader boxes.
- `gold.fixtures`: `matchday, opponent_name, venue, kickoff_utc`.

**Screen 02** (all keyed by `match_id`)
- `gold.match_momentum`: `minute, xt_home, xt_away` at 1-minute grain (5-minute rolling applied in the
  query or the client) + `gold.match_events_timeline`: `minute, type ('goal'|'sub'|'card'|'period'),
  team, label`.
- `gold.match_pass_network`: nodes `player_id, shirt, surname, avg_x, avg_y, touches`; edges
  `from_player_id, to_player_id, passes` (threshold ≥4). Positions in Opta 0–100 space; convert at render.
- `gold.match_totals`: the 14 stats above as `metric_key, home_value, away_value` — including
  `xg_open_play`, `xg_set_piece`, `big_chances`, `final_third_entries`, `xt_created`.
- `gold.match_shots`: `shot_id, team_id, player_name, minute, x, y, xg, body_part, outcome,
  is_penalty, is_own_goal` → shot map + tooltip. One row per shot; do not pre-bucket.
- `gold.match_xt_grid`: `zone_x (0–11), zone_y (0–7), xt_sum` → xT surface.
- `gold.match_progression`: `event_id, kind ('pass'|'carry'), x, y, end_x, end_y, xt_delta`.
- `gold.match_defensive_actions`: `event_id, type, x, y, outcome ('won'|'lost')`, plus
  `avg_defensive_line_x` and `ppda`, `high_regains` on `gold.match_totals`.
- `gold.match_sequences`: `sequence_id, start_minute, start_type, end_type, pass_count,
  duration_s, xt_added, points (ordered [x,y] array or a child table of touches)` — top 12 by
  `xt_added`. VAEP can replace xT here; keep the column name generic (`value_added`) if both are wanted.
- `gold.match_player_stats`: `player_id, name, team_code, recoveries, passes_completed,
  passes_into_final_third, xg, xa` → the four match player boxes.

**Screen 03–05** will need per-player match series, a filterable sequence query
(`start_zone, end_type, min_passes, match_group`), and a match-group aggregate with the same metric
spine on both sides.

## Assets
- **Google Fonts**: Outfit, Figtree, DM Mono — loaded via `fonts.googleapis.com` `css2` link.
- **Two user-supplied images**, both empty placeholders in the prototype (drag-and-drop slots):
  `esp-hero` (hero background photo, ~1600×600, dark-ground scrim applied over it) and `esp-badge`
  (club crest, square, transparent PNG/SVG, rendered 54×54 `object-fit:contain`).
  Supply real licensed assets in the app — RCD Espanyol marks belong to the club; use them only with
  permission and keep them in the app's own asset pipeline.
- **No icon set**: every mark is either type, an SVG primitive, or a coloured chip. Keep it that way.

## Files
- `screenshots/01–04` — screen 01 (Season overview), top to bottom.
  `screenshots/05–09` — screen 02 (Match analysis), top to bottom. Reference renders at ~900px
  preview width; use them to check proportion and reading order, and the spec above for exact values.
- `Espanyol Analytics.dc.html` — the whole design: all five screens, the shell, all chart geometry
  and mock data generators. Open it directly in a browser.
- `image-slot.js` — the drag-and-drop image placeholder used by the hero and crest slots. Design-time
  only; replace with real `<img>` elements in production.
- `support.js` — the prototype's template/logic runtime. **Not part of the design**; do not port.

## Implementation notes
- Recreate the charts as **React components over D3 scales**, not a charting library: every figure here
  is bespoke geometry (transposed pitches, dual-orientation heat grid, diverging areas with a hard zero
  rule, chain polylines). A chart library will fight all of it.
- Keep the **HTML-over-SVG rule** for axis and tick text: the prototype paints SVG `<text>` only for
  static labels and uses positioned HTML for anything data-driven, which keeps type at real CSS pixel
  sizes regardless of viewBox scaling. Worth preserving — it is why the axes read cleanly.
- Type minimums observed throughout: 8px mono for meta only, 12px+ for anything a user reads as content.
- Layout is desktop-first and fluid: `minmax(0,…)` grid tracks everywhere, no fixed pixel widths on
  text containers. Below ~1100px the 4- and 5-column grids should collapse to 2, then 1.
