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
  DESIGN.md at the repo root (the Pizarra world, which superseded the design
  handoff on 24 Oct 2026). Metric definitions come from the football-metrics
  skill; pitch zones from pitch-guide.
---

# Espanyol Analytics: chart catalog (v2)

The user should be able to say "add a reception heatmap to the player page"
and nothing more. This file is what makes that work: every chart has an entry
(§4), and every entry says what to reuse.

## 0. Who owns what

| Concern | Authority |
|---|---|
| Colour tokens, typography, spacing, radii, page layout, interaction states | `DESIGN.md` at the repo root (Pizarra; its "Chart colour mapping" says how every mark is coloured). The design handoff in `web/design_handoff_espanyol_analytics/` is superseded (24 Oct 2026); `README-PLAYER.md` keeps its data contract and decisions, not its look. |
| What a metric means (progressive, key pass, touch…) | `football-metrics` skill. **Handoff definitions are placeholders** (football-metrics §8). |
| Pitch zones, zone assignment, pitch geometry | `pitch-guide` skill, `gold.pitch_zones`, `web/lib/viz/pitch.ts` |
| Data shape on the site | `md/WEB_DATA.md`, `web/lib/data/types.ts` |
| **Mark grammar, encoding, which component, chart-specific rules** | **this skill** |
| How v1 drew it (matplotlib values, Spanish copy, legends) | `references/v1-design-language.md`, `references/viz-catalog.md`, `references/web-translation.md` |

Where DESIGN.md and this skill disagree on a visual, DESIGN.md wins.
Where a brief and the repo disagree on a **definition**, ask the user.

> **Old section numbers.** Comments in `web/components/viz/*.tsx` cite
> "espanyol-viz-design §1 / §5 / §11". Those are the pre-10-Oct-2026 sections,
> now in `references/v1-design-language.md` under the same numbers. In this
> file they correspond to §1 rule 1 (one accent), rule 4 (z-order) and rule 5
> (furniture is HTML).

## 1. The v2 grammar: eight rules every chart follows

1. **Identity or verdict, never both** (DESIGN.md "Identity / Verdict
   Rule"). The analysed team or player is `--color-team`; the opponent is
   `--color-rival` (outline, solid only for goals); neutral or secondary
   marks are `--color-slate`; markings `--color-chalk` (`--color-spot-line`
   on the spot panel). Good / middling / bad are `--color-good` /
   `--color-fair-fill` (text `--color-fair`) / `--color-bad`.
2. **No other data hues.** Distinguish within a colour by fill, opacity,
   dash or size: carries are a **dotted** team line, assists a thicker team
   line ending in a dot, a failed action is faint (opacity ~0.3). The
   handoff's gold, blue2 and grey-brown are gone.
3. **Filled = succeeded / on target. Hollow or dashed = failed. Shape = event
   family. Size (area) = volume or value.** A failed action is drawn dashed or
   hollow, not dropped, when the chart is a list of attempts.
4. **Z-order is element order.** `<Pitch underlay>` holds heat fills and zone
   rects. Pitch lines come next. `children` holds arrows, then dots, then nodes,
   then the selected item last. Heat layers redraw lines cased in the ground
   colour (`--viz-ground`, set by the panel classes) so markings read through.
5. **Furniture is HTML, not SVG.** Title, note, key (real marks in slate
   text), the readout card and axis labels all sit around the SVG at real
   pixel sizes. Only things placed in pitch coordinates go inside it. No
   hover cards: a white `aria-live` readout under the map names the mark.
8. **Every interactive mark is reachable by keyboard and labelled.** A mark
   is a `role="button"` with an `aria-label`; the map is one roving tab stop,
   the arrow keys walk the marks in match order, Enter opens (`MatchMap`,
   `player/EventMap`). Delegate handlers to the SVG when marks number in
   the thousands.
6. **No Spanish in components.** Every string comes from `web/lib/labels.ts`;
   every number goes through `web/lib/format.ts`.
7. **Never flip in a component.** The export puts every action in the analysed
   team's attacking frame, attacking right. Low `y` = right flank = bottom of a
   horizontal pitch (`lib/viz/pitch.ts` handles it). Sanity check: a right-back's
   heat belongs in the lower half.

Empty state: the pitch is still drawn, with a message from
`labels.ts` (existing examples: `defenceEmpty`, the sequence `empty`). A chart
never disappears.

## 2. Component inventory

Every chart composes on `<Pitch>` (or draws `PitchLines` in its own SVG when
its marks must be focusable: `<Pitch>` is `role="img"`, which hides its
children). Read the component's header comment before changing it. Rebuilt
on 24 Oct 2026 when the site moved to Pizarra; the handoff-only layers
(`ShotMap`, `DefensiveActions`, `ActionArrows` / `ProgressionArrows`,
`HoverCard`, `HeatSurface`, `SequenceTraces`, `SequenceBrowser`,
`SequenceLegend`, `RollingArea`, `Legend`, `MetricToggle`) were deleted; the
tag `pre-redesign-2026-10-23` has them.

`web/components/viz/` (shared primitives):

| Component | Draws | Key props | Client? | Used on |
|---|---|---|---|---|
| `Pitch` / `PitchLines` | Markings, `underlay` + `children` slots | `orientation`, `underlay`, `lineColor`, `cased`, `title` | no | everything |
| `VizDefs` | Shared `<defs>` arrowheads (`viz-arrow-team`, `viz-arrow-slate`) | none | no | once per page drawing a `SequenceDetail` |
| `ZoneHeatmap` | 30-zone fills panel → team, optional grid, `children` above the lines | `cells: {zone_id, value}[]`, `showGrid`, `highlight` | no | 02 (xT surface), /demo |
| `PassNetwork` | Nodes at mean position (r by touches), edges (width by count, ≥4) | `nodes`, `edges` | no | 02 |
| `SequenceDetail` | One chain action by action: pass arrow, curved cross, dotted carry, take-on dot, shot to goal-mouth y; the page's player in spot-line | `actions`, `focusPlayerId`, `orientation` | yes | 02, 03 (via `ChainCard`) |
| `Momentum` | Diverging rolling xT, team above / rival below, goal and sub markers | `bins`, `markers`, `teamSide` | no | 02 |

Interactive maps and blocks (keyboard model of rule 8):

| Component | Draws | Used on |
|---|---|---|
| `match/MatchMap` | One event map: `mode` = `shots` (circles, area = xG, fill by outcome, rival outline) / `progression` (pass solid, carry dotted, failed faint) / `defence` (diamonds filled won / outline lost, type pills, average line); readout card; opens the chain through `SequenceLink` | 02, /demo |
| `player/EventMap` | Screen 03's eight modes in one SVG (passes, progressive, carries, touches, receptions, shots, defensive, key passes), heat / events switch, every mark and heat zone a button | 03 |
| `landing/ChainPitch` | Latest match's shots on the spot; a shot draws its possession | landing |
| `sequences/SequenceParts` | `ChainCard` (chain large + key + numbers + actions in words), `SequenceList`, `MetricSwitch` | 02, 03, /demo |
| `matches/MatchShots` | Static shot pitch (one labelled image) | match list |
| `landing/Thumbs`, `landing/XgStroke` | Small real drawings for index rows | landing, match list |

Non-pitch blocks: `season/TeamProfile` (percentile cards; its `PercentileCard`
/ `PercentileRow` also build screen 03's `PlayerPercentiles`), `KpiStrip`,
`PlayerLeaders`, `FormStrip`, `LeagueTable`, `SeasonTrend` (interactive trend
chart, replaced `RollingArea`); `match/MatchTotals` (two-ink split bars),
`MatchPlayers`.

**Map -> sequence link** (both pages): a click or Enter on a mark whose row
has `s` (index into the file's chain list, null when undrawable) opens that
chain in the sequence block (the spot panel) and scrolls to it. Screen 03:
`PlayerBody` holds the pin. Screen 02: `match/SequenceLink` (context wrapping
the maps and `MatchSequences`). Only the published team's events link; the
match file ships its chains only.

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
- Use: `MatchMap mode="shots"` (match), `player/EventMap` mode `tiros`
  (player: every shot in team ink).
- Marks: circle area ∝ xG; fill steps goal / on target / off-or-blocked; the
  readout names player, minute, xG, outcome, body part. (The handoff's goal
  frame inset went with its hover card; `goal_mouth_y/z` are still exported.)
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
- Use: `player/EventMap` mode `pases-clave`.
- Marks: key passes team arrows; assists thicker team lines ending in a dot
  (they were gold in the handoff; Pizarra has no gold).
- Traps: key passes **exclude** assists (football-metrics §3). Second assists
  (Q218) aren't key passes.

### Passing and progression

**Pass map** (screen 03 mode `Passes`)
- Q: what does his passing look like: volume, direction, risk?
- Data (planned): every pass by the player for the season, with
  `is_progressive`, `completed` ← `silver.events` + `gold.is_progressive`.
  A season can hold 1,000+ passes; the export may need to cap or sample, so
  decide with the user (WEB_PLAN §6 sets exposure limits).
- Use: `player/EventMap` mode `pases`: progressive passes team arrows, the
  rest thin headless slate; failed faint.
- Variants: completed only; one match.
- Traps: arrowheads dominate at volume, so draw non-progressive passes without
  heads.

**Progressive passes / carries** (screen 02 block 5; screen 03 modes
`Progressive`, `Carries`)
- Q: how does he move the ball towards goal, and from where?
- Data: `matches/{id}.json → progression` (top 30 by xT, exists); player
  version planned ← `gold.is_progressive` (**25% closer to goal**, same rule
  for both).
- Use: `MatchMap mode="progression"` (match), `player/EventMap` modes
  `progresivos` / `conducciones`. Pass solid team, carry dotted team, failed
  faint.
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
- Use: `player/EventMap` mode `toques`, heat display (zones are buttons;
  alpha via `heatOpacity`, panel → team), a ramp in the key.
- Traps: the handoff mock draws 12×8. Use the **30 zones**; 12×8 is the xT
  grid. Check the right flank is at the bottom.

**Reception heatmap / progressive receptions** (planned)
- Q: where does he receive the ball, and how far forward?
- Data: `gold.pass_receptions` where `receiver_id = player`: `rx_zone_id` for
  the heatmap, and `rx_x, rx_y, is_progressive, spell_end` for dots. Drop
  `rx_gap_m > 5` when the exact point matters.
- Use: `player/EventMap` mode `recepciones` (heat zones + progressive dots,
  or every reception: progressive team dots, the rest slate, a ring in the
  box).
- Variants: by `spell_end` (what he did next: shot, pass, lost it). One ink
  for "progressed" and slate for the rest, never a seven-colour key.
- Traps: Opta records no receiver; this is derived (football-metrics §5).
- v1: player_progressive_receptions.

### Without the ball

**Defensive actions** (screen 02 block 8; screen 03 mode `Defensive`)
- Q: where does he/the team win or contest the ball?
- Data: `matches/{id}.json → defence` (exists); player version planned.
- Use: `MatchMap mode="defence"` (match: type pills, `lineX`),
  `player/EventMap` mode `defensivas` (type pills, heat / events).
- Marks: team diamonds, filled when won, outline when lost; type by the
  pills, never by colour (README-PLAYER's colour-by-type is superseded).
- Traps: `Challenge` = dribbled past (a loss). Clearances and recoveries
  dominate the cloud, which is why the chips exist.

**Duels: aerial / ground** (planned, v1 had them)
- Data: `Aerial` (both contestants), `Tackle` + `Attempted Tackle` +
  `Challenge`.
- Use: the defence modes' type pills, or the v1 shapes (triangle aerial,
  square ground; filled won, hollow lost) if the user wants them on one pitch.
- v1: defensive_duelsmap, player_defensive_duels.

**Take-ons** (planned)
- Data: `Take On` by outcome.
- Marks: filled team circle won, hollow lost.
- v1: player_takeons.

### Threat and sequences

**xT surface** (screen 02)
- Data: `matches/{id}.json → xt_zones` (30 zones, SQL) and `xt_grid` (12×8).
- Use: `ZoneHeatmap` on `xt_zones` (screen 02 since 22 Sep 2026). `HeatSurface`
  (12×8) was deleted on 24 Oct 2026; `xtCellsToZones` re-bins old files.

**Sequence traces** (screen 02, 03 block 5, 04)
- Q: which possessions mattered, and what shape did they take?
- Data: `sequences[]` with `points` (start, end, up to 6 RDP waypoints),
  `ends_in_shot` ← `gold.sequences`.
- Use: `sequences/SequenceParts` (`ChainCard` + `SequenceList`) on the spot
  panel. A many-traces overlay (`SequenceTraces`) existed and was deleted
  unused on 24 Oct 2026; rebuild it from the tag if screen 04 needs it.
- Traps: rank by `xt` or `vaep` sum; `sequence_id` renumbers on rebuild; filter
  `event_count ≥ 3` for style views; failed actions earn 0 xT.

**Sequence detail** (screen 02)
- Use: `SequenceDetail`. Its vocabulary (pass arrow, curved cross, dotted carry,
  take-on dot, shot to `goal_mouth_y`, start halo, end ring, regain diamond) is
  the house language for any action-by-action drawing.

### Not on a pitch

**Percentile cards** (screen 01 block 5; screen 03 block 4)
- Use: `TeamProfile` / `PercentileCard` + `PercentileRow`. Tier ≥70 `good`,
  ≥45 `fair-fill` (number in `fair`), else `bad`. Show `peer_n` and the peer
  group.
- Data: `players/{id}.json → percentiles` (export gate, 25% of club minutes).
- Traps: groups are `GK CB RB LB MID W ST`, not the handoff's three. The
  metrics come from `gold.percentile_metrics` for that group. Surface the
  `is_utility` flag.

**Radar**: v1 only (PyPizza). v2 replaces it with percentile cards. Don't
reintroduce it unless asked.

**Season trend** (`SeasonTrend`, replaced `RollingArea`), **Momentum**
(`Momentum`), **Split stat bars** (`MatchTotals`), **KPI figures**
(`KpiStrip`, raised cards; screen 03's KPIs are white cards on the profile
panel): reuse as they are.

## 5. Known gaps to close as charts are built

- **One mark layer.** `MatchMap` and `player/EventMap` each draw their own
  marks with the same keyboard model. On a third caller, lift the mark layer
  (geometry, roving tab stop, readout) into `components/viz/`.
- ~~Player export~~ — **done 16 Oct 2026**: `src/export/player.py` writes
  `players/{id}.json` (README-PLAYER "Data contract", with `passes` split into
  `passes` / `progressive_passes` / `key_passes` because the three modes have
  different caps). Screen 03 is `components/player/*`; `/demo/jugador` draws it
  from `demoPlayerFile`. The catalog entries above marked "planned" for the
  player map are those fields.
