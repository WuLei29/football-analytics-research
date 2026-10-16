---
name: espanyol-viz-design
description: >
  The chart catalog for the RCD Espanyol v2 site (Next.js, web/): for every
  chart type the project draws or plans (shot map, pass map, progressive
  passes and carries, key passes and assists, touch heatmap, reception
  heatmap, defensive actions, duels, take-ons, crosses, xT surface, pass
  network, sequence traces and detail, percentile cards, rolling area,
  momentum, stat bars), it gives the question the chart answers, its data
  (export key and gold/silver source), the existing component to reuse, the
  marks and encoding, variants, empty state and traps. Also covers the
  inventory of web/components/viz with props, the v2 mark grammar and z-order,
  and (in references/) the v1 matplotlib design language it descends from.

  Use this skill whenever building, porting or changing any chart in web/,
  adding a visual mode to a page, or when the user names a chart without
  specifying it ("add a progressive-passes map", "a reception heatmap"). Read
  the catalog entry first and the user will not have to explain the chart.

  NOT the visual authority for colour, type, spacing or layout: that is
  web/design_handoff_espanyol_analytics/README.md (+ README-PLAYER.md for
  screen 03). Metric definitions come from the football-metrics skill; pitch
  zones from pitch-guide.
---

# Espanyol Analytics: chart catalog (v2)

The user should be able to say "add a reception heatmap to the player page"
and nothing more. This file is what makes that work: every chart has an entry
(§4), and every entry says what to reuse.

## 0. Who owns what

| Concern | Authority |
|---|---|
| Colour tokens, typography, spacing, radii, page layout, interaction states | `web/design_handoff_espanyol_analytics/README.md`; screen 03: `README-PLAYER.md` |
| What a metric means (progressive, key pass, touch…) | `football-metrics` skill. **Handoff definitions are placeholders** (football-metrics §8). |
| Pitch zones, zone assignment, pitch geometry | `pitch-guide` skill, `gold.pitch_zones`, `web/lib/viz/pitch.ts` |
| Data shape on the site | `md/WEB_DATA.md`, `web/lib/data/types.ts` |
| **Mark grammar, encoding, which component, chart-specific rules** | **this skill** |
| How v1 drew it (matplotlib values, Spanish copy, legends) | `references/v1-design-language.md`, `references/viz-catalog.md`, `references/web-translation.md` |

Where the handoff and this skill disagree on a visual, the handoff wins.
Where the handoff and the repo disagree on a **definition**, ask the user.

> **Old section numbers.** Comments in `web/components/viz/*.tsx` cite
> "espanyol-viz-design §1 / §5 / §11". Those are the pre-10-Oct-2026 sections,
> now in `references/v1-design-language.md` under the same numbers. In this
> file they correspond to §1 rule 1 (one accent), rule 4 (z-order) and rule 5
> (furniture is HTML).

## 1. The v2 grammar: seven rules every chart follows

1. **One accent per pitch figure.** The analysed team or player gets `--blue`.
   Opponents and neutral actions get `--faint` / `--mid` / `#C9B9A6`. Inside a
   pitch, colour means "this is what you're reading", not category.
2. **Semantic tokens aren't decoration.** `--gold` = carries (and assists on
   screen 03's key-pass mode only), `--pos`/`--neg` = good/bad bands and deltas,
   `--blue2` = the middle tier of a chain ranking.
3. **Filled = succeeded / on target. Hollow or dashed = failed. Shape = event
   family. Size (area) = volume or value.** A failed action is drawn dashed or
   hollow, not dropped, when the chart is a list of attempts.
4. **Z-order is element order.** `<Pitch underlay>` holds heat fills and zone
   rects. Pitch lines come next. `children` holds arrows, then dots, then nodes,
   then the selected item last. Heat layers redraw lines in the card colour
   (`lineColor`, `cased`) so markings read through.
5. **Furniture is HTML, not SVG.** Title, note, legend (`<Legend>`), tooltip
   and axis labels all sit around the SVG at real pixel sizes. Only things
   placed in pitch coordinates go inside it.
6. **No Spanish in components.** Every string comes from `web/lib/labels.ts`;
   every number goes through `web/lib/format.ts`.
7. **Never flip in a component.** The export puts every action in the analysed
   team's attacking frame, attacking right. Low `y` = right flank = bottom of a
   horizontal pitch (`lib/viz/pitch.ts` handles it). Sanity check: a right-back's
   heat belongs in the lower half.

Empty state: the pitch is still drawn, with a centred mono message from
`labels.ts` (existing examples: `defenceEmpty`, the sequence `empty`). A chart
never disappears.

## 2. Component inventory (`web/components/viz/`)

Every chart composes on `<Pitch>`. Read the component's header comment before
changing it, since each one records its deviations from the handoff.

| Component | Draws | Key props | Client? | Used on |
|---|---|---|---|---|
| `Pitch` / `PitchLines` | Markings, `underlay` + `children` slots | `orientation`, `underlay`, `lineColor`, `cased`, `title` | no | everything |
| `VizDefs` | Shared `<defs>` arrowheads (`viz-arrow-*`) | none | no | once per page using arrows |
| `ShotMap` | Circles, r = R0 + √xG·K; solid goal / half on target / hollow off; hover card with goal frame | `shots: Shot[]`, `teamSide`, `abbr` | yes | 02, 03 (`Tiros`) |
| `ActionArrows` / `ArrowLayer` | Arrows with a per-arrow tone (`blue`/`gold`/`faint`), width, opacity, `head`; failed dashed. `ArrowLayer` is the bare `<g>` | `actions: ArrowMark[]` | no | 02 (via wrapper), 03 |
| `ProgressionArrows` | Thin wrapper on `ActionArrows`: pass `--blue` thin, carry `--gold` thicker | `actions: {kind, x,y,end_x,end_y, completed}[]` | no | 02 |
| `HeatSurface` | 12×8 **xT** grid cells | `cells: {cx, cy, xt}[]` | no | 02 |
| `ZoneHeatmap` | 30-zone fills, optional grid, `children` above the lines (reception dots) | `cells: {zone_id, value}[]`, `showGrid`, `children` | no | 02, 03 |
| `DefensiveActions` | Diamonds, blue won / grey lost, type chips, dashed line height, hover | `actions: {type,x,y,outcome,minute?,player?}[]`, `lineX`, `lineLabel` | yes | 02, 03 |
| `PassNetwork` | Nodes at mean position (r by touches), edges (width by count, ≥4) | `nodes`, `edges` | no | 02 |
| `SequenceTraces` | Polylines, start dot, end ring (big if shot), selection; `variant="player"`: width by `value`, gold dot on `touch` | `sequences: {sequence_id, points, ends_in_shot, value?, touch?}[]`, `selectedId`, `onSelect`, `variant` | no (page owns state) | 02, 03, 04 |
| `SequenceDetail` | One chain action by action: pass arrow, curved cross, dotted carry, take-on dot, shot to goal-mouth y | `actions`, `describe` | yes | 02 |
| `SequenceBrowser` | `SequenceDetail` + clickable list | `sequences`, `title` | yes | 02 |
| `Momentum` | Per-minute xT bars, team above / opponent below, goal markers | `bins`, `markers` | yes | 02 |
| `RollingArea` | Diverging area around 0 (5-match rolling xGD / xTD) | `points` | no | 01 |
| `Legend`, `Ramp` | HTML legend with real marks; LOW→HIGH ramp | `items` or `rows` | no | all |
| `MetricToggle` | Two-option segmented pill (xT / VAEP) | `value`, `onChange` | yes | 02, 03 |

Non-pitch blocks live in `components/season` and `components/match`:
`TeamProfile` (**the percentile cards**; its `PercentileCard` / `PercentileRow`
are what screen 03's `components/player/PlayerPercentiles` is built from),
`KpiStrip`, `PlayerLeaders`, `MatchTotals` (mirrored stat bars), `FormStrip`,
`LeagueTable`.

Helpers: `lib/viz/pitch.ts` (`project`, `zoneRect`, `xtCellRect`,
`ZONE_*_BOUNDARIES`, `PITCH_VIEWBOX`), `lib/viz/scale.ts` (`linear`,
`heatOpacity` with `HEAT_OPACITY = [0.06, 0.92]`, `rollingMean`, paths),
`lib/viz/fixtures.ts` (seeded demo data for `/demo`).

## 3. Reuse rules

- **Extend before you add.** A new mark style on an existing layer is a prop
  (`emphasis`, `variant`), not a new component. Follow WEB_PLAN §3.2:
  duplicate twice, extract on the third use.
- **One pitch, many modes** (screen 03): keep `<Pitch>` mounted and swap only
  the children. Title, note and legend change with the mode.
- **Fixture first.** Every new layer gets a seeded fixture in
  `lib/viz/fixtures.ts` and a card on `/demo` before it touches a real page.
- Arrow layers need `<VizDefs />` on the page. A new arrow colour means a new
  marker in `VizDefs`, not an inline `<marker>`.

## 4. Catalog

Format: **Q** the question it answers · **Data** export key ← source ·
**Use** component · **Marks** · **Variants** · **Traps**. "Planned" means the
export field doesn't exist yet. Add it to the contract (WEB_DATA) first.

### Shots and chances

**Shot map**
- Q: where does he/they shoot from, and how good were the chances?
- Data: `matches/{id}.json → shots[]` (exists); `players/{id}.json → shots[]`
  (WEB_DATA §9, planned) ← `silver.events` shot set, `xg`, `shot_play_pattern`,
  `goal_mouth_y/z`.
- Use: `ShotMap`. For a single player, pass every shot with `side = teamSide`.
  A separate player variant is only needed if the tooltip changes.
- Marks: circle area ∝ xG; fill steps goal / on target / off-or-blocked; hover
  shows the goal frame with the ball at `goal_mouth_y/z`.
- Variants: filter by `shot_play_pattern` (open play / set piece / penalty),
  vertical half pitch.
- Traps: own goals belong to the opponent; penalties are xG 0.79 (say so in the
  tooltip); `Attempt Saved` + Q82 is blocked, not on target.
- v1: `viz-catalog.md` shot_map, goalmouth_map, combined_shotmap.

**Key passes and assists** (screen 03 mode `Key passes`)
- Q: where does he create chances from, and where do they arrive?
- Data (planned): player events with `value_assist` → `kind: 'assist' | 'key_pass'`,
  origin and end ← `silver.events`. Optionally `xa` per pass (the xG of the
  created shot).
- Use: arrow layer (generalised `ProgressionArrows`, §5).
- Marks (handoff): assists `--gold` w 0.85, key passes `--blue` w 0.55, opacity
  0.85. This is the one place gold doesn't mean "carry". No carries are on this
  pitch, so there's no ambiguity.
- Traps: key passes **exclude** assists (football-metrics §3). Second assists
  (Q218) aren't key passes.

### Passing and progression

**Pass map** (screen 03 mode `Passes`)
- Q: what does his passing look like: volume, direction, risk?
- Data (planned): every pass by the player for the season, with
  `is_progressive`, `completed` ← `silver.events` + `gold.is_progressive`.
  A season can hold 1,000+ passes; the export may need to cap or sample, so
  decide with the user (WEB_PLAN §6 sets exposure limits).
- Use: generalised arrow layer, with progressive passes in `--blue` (w 0.6, op 0.9)
  and the rest in `--faint` (w 0.3, op 0.38). Failed passes dashed.
- Variants: completed only; one match.
- Traps: arrowheads dominate at volume, so draw non-progressive passes without
  heads.

**Progressive passes / carries** (screen 02 block 5; screen 03 modes
`Progressive`, `Carries`)
- Q: how does he move the ball towards goal, and from where?
- Data: `matches/{id}.json → progression` (top 30 by xT, exists); player
  version planned ← `gold.is_progressive` (**25% closer to goal**, same rule
  for both).
- Use: `ProgressionArrows`. Pass blue, carry gold, failed dashed.
- Variants: origin heatmap of progressive passes (`ZoneHeatmap` on start
  zone); passes only; carries only.
- Traps: legend copy must state the repo rule, not "≥10 m" (football-metrics §8).
  Carries are synthesised and ≥1 m.

**Pass network** (screen 02 block 4)
- Q: who combines with whom, and where does each player stand on average?
- Data: `matches/{id}.json → network` ← on-ball events while on pitch; edges ≥ 4.
- Use: `PassNetwork` (vertical).
- Traps: v1 cut at first substitution; v2 uses time on pitch. Edge count is
  per unordered pair.

**Crosses** (planned)
- Q: from where does he cross, and where do the crosses land?
- Data: passes with Q2 (`is_cross` in `gold.pass_receptions` for completed
  ones; the full set needs `silver.events`).
- Use: arrow layer, curved (reuse `SequenceDetail`'s cross curve), or two
  `ZoneHeatmap`s (origins / destinations).
- v1: `viz-catalog.md` player_cross_analysis (two ramps).

### Receiving and touching

**Touch heatmap** (screen 03 mode `Touch heat`)
- Q: where on the pitch does he play?
- Data: `players/{id}.json → heatmap.cells` (30 zones, WEB_DATA §9) ← on-ball
  **touch set** (football-metrics §2), `gold.zone_id`.
- Use: `ZoneHeatmap`, alpha via `heatOpacity`. A `Ramp` legend underneath.
- Traps: the handoff mock draws 12×8. Use the **30 zones**; 12×8 is the xT
  grid. Check the right flank is at the bottom.

**Reception heatmap / progressive receptions** (planned)
- Q: where does he receive the ball, and how far forward?
- Data: `gold.pass_receptions` where `receiver_id = player`: `rx_zone_id` for
  the heatmap, and `rx_x, rx_y, is_progressive, spell_end` for dots. Drop
  `rx_gap_m > 5` when the exact point matters.
- Use: `ZoneHeatmap` (density) or a dot layer (progressive receptions: `--blue`
  filled dots, a ring when `rx_in_box`).
- Variants: colour by `spell_end` (what he did next: shot, pass, lost it). Use
  one accent for "progressed" and `--faint` for the rest, never a seven-colour key.
- Traps: Opta records no receiver; this is derived (football-metrics §5).
- v1: player_progressive_receptions.

### Without the ball

**Defensive actions** (screen 02 block 8; screen 03 mode `Defensive`)
- Q: where does he/the team win or contest the ball?
- Data: `matches/{id}.json → defence` (exists); player version planned.
- Use: `DefensiveActions`. Blue = won, grey = lost, type chips, optional `lineX`.
  For a player, omit `lineX` or pass his `def_action_avg_x`.
- Marks: README-PLAYER asks for **colour by type** (tackle blue r1.1,
  interception gold r0.9, recovery faint r0.8). That conflicts with this
  component's "type via chips, colour = outcome" rule. Ask the user which one
  applies on screen 03, and don't silently mix them.
- Traps: `Challenge` = dribbled past (a loss). Clearances and recoveries
  dominate the cloud, which is why the chips exist.

**Duels: aerial / ground** (planned, v1 had them)
- Data: `Aerial` (both contestants), `Tackle` + `Attempted Tackle` +
  `Challenge`.
- Use: `DefensiveActions` with a duel filter, or the v1 shapes (triangle aerial,
  square ground; filled won, hollow lost) if the user wants them on one pitch.
- v1: defensive_duelsmap, player_defensive_duels.

**Take-ons** (planned)
- Data: `Take On` by outcome.
- Marks: filled `--blue` circle won, hollow lost.
- v1: player_takeons.

### Threat and sequences

**xT surface** (screen 02)
- Data: `matches/{id}.json → xt_grid` ← Σ xt per 12×8 cell.
- Use: `HeatSurface`. Never re-bin to 30 zones.

**Sequence traces** (screen 02, 03 block 5, 04)
- Q: which possessions mattered, and what shape did they take?
- Data: `sequences[]` with `points` (start, end, up to 6 RDP waypoints),
  `ends_in_shot` ← `gold.sequences`.
- Use: `SequenceTraces` (page owns `selectedId`) + a list (`SequenceBrowser`
  pattern). Screen 03 adds a `--gold` r1.5 dot on the player's touch
  (`player_touch_index`, planned), selected chain only, and inverts the selected
  row instead of tinting it.
- Traps: rank by `xt` or `vaep` sum; `sequence_id` renumbers on rebuild; filter
  `event_count ≥ 3` for style views; failed actions earn 0 xT.

**Sequence detail** (screen 02)
- Use: `SequenceDetail`. Its vocabulary (pass arrow, curved cross, dotted carry,
  take-on dot, shot to `goal_mouth_y`, start halo, end ring, regain diamond) is
  the house language for any action-by-action drawing.

### Not on a pitch

**Percentile cards** (screen 01 block 5; screen 03 block 4)
- Use: `TeamProfile` with a different metric source. Bar colour ≥70 `--pos`,
  ≥45 `--blue`, else `--neg`. Show `peer_n` and the peer group.
- Data: `players/{id}.json → percentiles` (export gate, 25% of club minutes).
- Traps: groups are `GK CB RB LB MID W ST`, not the handoff's three. The
  metrics come from `gold.percentile_metrics` for that group. Surface the
  `is_utility` flag.

**Radar**: v1 only (PyPizza). v2 replaces it with percentile cards. Don't
reintroduce it unless asked.

**Rolling area** (`RollingArea`), **Momentum** (`Momentum`), **Mirrored stat
bars** (`MatchTotals`), **KPI tiles** (`KpiStrip`, the screen 03 profile KPIs
follow the same type scale): reuse as they are.

## 5. Known gaps to close as charts are built

- ~~Generalise the arrow layer~~ — **done 16 Oct 2026**: `ActionArrows`
  (per-arrow tone / width / opacity / head), `ProgressionArrows` is a wrapper.
- **Dot layer.** The reception dots live in `components/player/EventMap.tsx`
  (`ReceptionDots`, filled dot + ring in the box). On the next caller
  (take-ons) lift it into `components/viz/`.
- ~~Player export~~ — **done 16 Oct 2026**: `src/export/player.py` writes
  `players/{id}.json` (README-PLAYER "Data contract", with `passes` split into
  `passes` / `progressive_passes` / `key_passes` because the three modes have
  different caps). Screen 03 is `components/player/*`; `/demo/jugador` draws it
  from `demoPlayerFile`. The catalog entries above marked "planned" for the
  player map are those fields.
