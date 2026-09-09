---
name: espanyol-viz-design
description: >
  Chart craft for the RCD Espanyol football-analytics visualizations: the mark
  vocabulary (shots, duels, take-ons, passes, sequences, heatmaps, radars, time
  series, stat bars), the z-order ladder, pitch and marker geometry, the
  matplotlib-to-SVG conversion math, the layout grammar, and the v1 mistakes not
  to repeat.

  Use this skill whenever building or porting an Espanyol analytics chart —
  including rewriting the v1 matplotlib/mplsoccer charts as TypeScript/React/
  D3/SVG components for the v2 Next.js site, or adding a new chart type that
  must match the existing encodings.

  NOT the visual authority for the v2 site. Colour, typography, spacing, radii
  and layout come from web/design_handoff_espanyol_analytics/README.md; pitch
  zones come from the pitch-guide skill and gold.pitch_zones. See section 0.
---

# Espanyol Analytics — Chart Craft (v1 design language)

The extracted description of the design language implemented in the **v1
Streamlit project**'s matplotlib/mplsoccer charts. Every value here was read out
of that shipped code, not invented.

Two reference files carry the detail:

- `references/viz-catalog.md` — one spec per visualization: canvas, marks,
  anchors, legend, summary line.
- `references/web-translation.md` — how each matplotlib primitive maps to
  React/SVG/CSS for the v2 site, plus the conversion math.

---

## 0. What this skill owns, and what it does not

The code described here lives in the **v1 Streamlit repo**, in its
`src/match_viz/`, `src/player_viz/` and `src/lab_viz/`. Those directories — and
the `config/config.yaml`, `config/radar_mappings.json` and
`config/player_mappings.json` the catalog cites — are **not in this repository**.
Do not go looking for them here.

For the v2 Next.js site the authority is split:

| Concern | Authority |
|---|---|
| Colour, typography, spacing, radii, layout, interaction | `web/design_handoff_espanyol_analytics/README.md` (`md/WEB_PLAN.md` §3.1) |
| Pitch zones and zone assignment | the `pitch-guide` skill and `gold.pitch_zones` |
| Pitch line colour and width, `viewBox`, orientation transforms | the handoff's *Pitch geometry* section |
| **Mark vocabulary, encoding rules, z-order, marker-size math, chart-specific geometry, and the v1 mistakes not to repeat** | **this skill** |

Where this skill and the handoff disagree, **the handoff wins**; the
disagreements are reconciled in §1 below. This skill is kept for the part the
handoff does not specify: what a shot, a duel, a carry, a take-on, a network node
or a sequence trace actually looks like, and why.

---

## 1. The three v1 non-negotiables — and what replaces each in v2

These three rules generated the whole v1 look. Each is superseded on the v2
site; the *reason* behind each survives, and that is what to carry across.

**1. The canvas was cream `#FFF1E0`** — figure background *and* axes background
on every chart, the colour "zero" fades to in every continuous ramp, and the
cell `edgecolor` separating bins. There was no white anywhere.
→ **v2: `--card #FFFBF5` for the chart surface, `--paper #FAEDE1` for the page.**
The rule that survives is that *a chart never sits on white and never sits
directly on the page* — it keeps its own warm card. Wherever this document says
cream is the zero point of a ramp or the colour of a cell edge, read `--card`.

**2. The typeface was Arvo**, a slab serif, on essentially every text call. The
one deliberate exception was the player radar, which used `monospace` throughout
(a PyPizza convention that was kept).
→ **v2: Outfit (display and numerics), Figtree (body and names), DM Mono
(labels, meta, axis text, numeric cells).** Arvo does not ship. The rule that
survives is the *split*: the radar's deliberately different face becomes DM Mono,
which the handoff already uses for all label and numeric text — so the
percentile charts still read as their own family without a fourth font.

**3. Colour carried exactly one meaning: team identity.** The accent was
`config.yaml → teams_color[<team>]` (Espanyol `#2388C4`); everything that was
not "this team" was greyscale. No categorical palette, no second accent. The
only two-colour charts were the two-team comparisons (field tilt, cumulative xT,
match summary) and the cross map (`#2388C4` origins, `#1C339A` destinations).
→ **v2: team colour for team-owned marks, semantic tokens for everything else.**
The handoff's `--pos` / `--neg` / `--gold` / `--blue2` are not decoration — they
are deltas, percentile bands, carries and chain tiers, and flattening them to
grey would delete real information from screens 01 and 02. The rule that
survives, narrowed: *inside a pitch figure, the analysed team is the only thing
that gets the accent; opponents and neutral actions stay in `--mid`, `--faint`
and `--line`.*

**On the accent hex.** The v2 primary is `--blue #0B4C9E`, not the v1
`#2388C4`. It was chosen against `--paper` and holds contrast for thin marks and
small text where `#2388C4` does not; `#2388C4` is close to the handoff's
secondary `--blue2 #2C82D2`. Wherever this document reads *TC* or "team colour",
resolve it through the handoff token, not through the v1 hex.

---

## 2. Colour tokens

**v1 values, kept for reading the shipped charts.** For anything new, map each
row onto its handoff token (§1): canvas → `--card`, canvas highlight →
`--paper`, text primary → `--ink`, text secondary → `--mid`, rules → `--line`,
pitch lines → `#D9C9B6`, neutral action → `--faint`. Only the *ramp
construction* below the table is still normative.

| Token | Value | Used for |
|---|---|---|
| Canvas | `#FFF1E0` | Every figure and axes background; continuous-heatmap zero point; heatmap cell edges; the line separating the two fills in field tilt |
| Canvas highlight | `#FFE7C9` | The pill behind each metric name in the match summary |
| Accent | team colour (`#2388C4` for Espanyol) | All "this team" marks, heatmap ramp top, radar slices |
| Cross destination | `#1C339A` | Second ramp in the cross-analysis map only |
| Text primary | `black` / `#333333` | Titles, goal annotations, stat values, legend labels under the pitch |
| Text secondary | `grey` / `#666666` | Subtitles, summary lines, legend text beside icons, player names in the passing network |
| Rules | `#CCCCCC` | Row separators in the match summary |
| Pitch lines | `#7c7c7c` | mplsoccer `line_color`, always at `linewidth=0.5` |
| Neutral action | `gray` / `grey` | Passes, carries, cross arcs, network links, goal frame |
| Marker outline | `black` (match viz) / team colour (player viz) | Scatter `edgecolors` |
| Empty state | `red` (no data at all), `orange` (data exists but none of this kind), `grey` (newer charts) | Centred message text |

The team-colour ramp used by every zone/bin heatmap is
`LinearSegmentedColormap.from_list('team_heatmap', [low, team_color], N=100)`
where `low` is `#FFFFFF` for the discrete zone heatmaps and `#FFF1E0` for the
continuous binned cross map. Intensity is deliberately compressed to
`0.1 + 0.7 * (count / max_count)` so no cell is ever pure white or fully
saturated, and the whole layer is drawn at `alpha=0.6`.

---

## 3. Type scale

All Arvo in v1. Bold carried the hierarchy; there was no italic and no
letter-spacing. **This table is a hierarchy, not a set of v2 sizes** — map each
role onto the handoff's scale (`references/web-translation.md` §1). The handoff
does use italic Figtree for deks and notes, and tracked uppercase DM Mono for
labels; neither existed here, and both are correct on v2.

| Size | Weight | Role |
|---|---|---|
| 30 | bold | Match summary scoreline |
| 18 | bold | Match summary title; combined shot map title; dual-pitch suptitle |
| 16 | bold | Sequence-grid `suptitle`; cross-analysis title |
| 15 | bold | Standard single-pitch chart title |
| 14 | bold | Time-series figure title (field tilt, cumulative xT) |
| 13 | bold / normal | Section subtitle inside a stacked chart; summary line on defensive maps |
| 12 | bold / normal | Axis labels; "KEY STATS"; stat names and values; empty-state message |
| 11 | normal | Player detail line (`#7 · MC · RCD Espanyol`) |
| 10 | normal | Jersey number in a large network node; player info line; boxed `ax.legend` |
| 9 | normal | Legend labels; goal annotation on the shot map; small-multiple pitch title |
| 8 | bold / normal | Jersey number in a small node; player name beside a node; goal label on a time series; right-aligned summary; explanatory footnote |
| 6 | bold | Jersey number inside a sequence pitch |

---

## 4. Pitch geometry and styling

Every pitch is the same object:

```python
Pitch(pitch_type='custom', pitch_width=68, pitch_length=105,
      line_color='#7c7c7c', linewidth=0.5)
```

**v2 line styling comes from the handoff, not from here**: `#D9C9B6` at width
0.35 on the horizontal pitch (`viewBox="-2 -2 109 72"`) and 0.4 on the vertical
(`viewBox="-2.5 -2.5 73 110"`, transpose `(x,y) → (y, 105-x)`, attacking up).
The `#7c7c7c` / `linewidth=0.5` above is the v1 value. The geometry itself —
metric 105×68, goal centre `(105, 34)` — is identical in both.

Three v1 variations, and nothing else:

- `line_zorder=4` whenever a heatmap or zone fill sits underneath, so the pitch
  lines stay readable through the fill.
- `VerticalPitch(..., half=True)` for shot maps and goal-mouth work — attacking
  half only, attacking upward.
- `VerticalPitch(...)` full, for the passing network.

Real-world metric coordinates throughout: x ∈ [0, 105], y ∈ [0, 68], goal centre
at `(105, 34)`. The goal-mouth charts use their own frame: half-width 3.66,
height 2.44, drawn as grey `linewidth=4` posts and crossbar with round caps plus
a `linewidth=2` goal line.

### The 20-zone grid — v1 only, do not port

Several v1 charts binned events into a hand-named 20-zone grid. Its x cuts are
real landmarks (penalty-area lines at 16.5/88.5, thirds at 35/70, halfway at
52.5), but its y cuts are **rounded approximations**: 13.54/54.35 for the
penalty-area width (true 13.84/54.16) and 24.54/43.35 for the six-yard-box width
(true 24.84/43.16). They were kept so the shipped PNGs stayed reproducible, not
because they were right.

**Nothing on the v2 site uses this grid, and no exported file carries it.** The
project's zone authority is the `pitch-guide` skill and `gold.pitch_zones` —
**30 zones, 6 longitudinal strips × 5 lateral channels**, with boundaries on the
real markings — plus the separate **12×8 xT surface** that `xt.py` scores on and
the match export ships (`md/WEB_PLAN.md` §9.1). Bin to those.

The v1 boundaries are recorded here only so an old chart can be read:

```
B1–B6  bottom band   y 0–13.54,      x cuts 0/16.5/35/52.5/70/88.5/105
T1–T6  top band      y 54.35–68,     same x cuts
F1, F2 flanks        y 13.54–54.35,  x 0–16.5 and 88.5–105
C1–C3  own-half core x 16.5–52.5,    y 13.54–24.54 / 24.54–43.35 / 43.35–54.35
C4–C6  opp-half core x 52.5–88.5,    same y cuts
```

When those boundaries were drawn (`draw_zone_lines`) they were grey,
`linewidth=0.5`, `alpha=0.7`, `zorder=3` — below the pitch lines, above the
fill. Most charts had the call commented out; the grid was a binning device, not
decoration.

---

## 5. Z-order ladder

Consistent across the whole system. Respect it when porting.

| z | Layer |
|---|---|
| 1 | Heatmap fills, zone rectangles |
| 2 | Passing-network links, sequence pass/cross lines |
| 3 | Zone boundary lines, sequence start/end dots |
| 4 | Pitch lines (`line_zorder=4`) |
| 5 | Event markers (shots, duels, take-ons), goal frame, progressive-pass arrows |
| 6 | Player nodes, legend glyphs |
| 7 | Titles, logos, summary text |
| 10–15 | Goal-mouth overlays and shot-map legends drawn outside the pitch |

---

## 6. Layout grammar

The four chart families each have a fixed furniture arrangement, expressed in
**data coordinates**, not figure fractions. This is what makes the set feel like
one product.

### Horizontal full pitch (105 × 68)

Used by heatmaps, duel maps, take-ons, crosses, progressive passes.

```
logo (1, 71) zoom 0.15      title x=4, y=70–71, 15pt bold black, ha=left
                            summary x=53–90, y=69, 8–13pt grey, ha=left
context line x=4, y=69, 10–11pt grey
┌──────────────────────── pitch ────────────────────────┐
└───────────────────────────────────────────────────────┘
        legend row centred on x=52.5 at y=-2
        labels 2–2.5 below each glyph, 9pt, #333333
```

### Vertical half pitch (shot map, goal mouth)

Title at `x=63, y=107` (15pt bold, `ha='left'`), logo at `(65, 108)` zoom 0.15,
custom legend laid out along `y=107`. The passing network uses the same idea on
a full vertical pitch: title `(63, 107)`, logo `(66, 108.5)`, and a single
explanatory line at `(68, -2)` in 8pt grey.

### Time series (field tilt, cumulative xT)

`figsize=(12, 6)`, `subplots_adjust(left=0.05, right=0.95, top=0.85, bottom=0.15)`.
Furniture lives in figure coordinates: title row at `y=0.98`, summary line at
`y=0.93` (10pt), footnote at `y=0.02` (8pt, `alpha=0.7`). Left margin `x=0.05`.

The title is **composed**, not a single string: `"Field Tilt: "` in black, then
the home team name in the home colour, then `" vs "` in black, then the away team
name in the away colour — each segment measured with `get_window_extent()` and
placed at the running x offset. On the web this is just inline `<span>`s, which
is far easier; keep the colour semantics.

Axes: grid `alpha=0.3`; x ticks every 15 minutes up to 90; spines removed
entirely (field tilt) or top+right only (cumulative xT); y ticks fixed at
±25/50/75/100 for tilt, dynamic 0.2 steps with a 1.4 floor for xT.

### Small-multiple grid (important sequences, laboratory)

Always **3 columns**, rows computed from the count. Figure is `3 × 5` wide by
`rows × (3.75 + 0.8)` tall. `suptitle` is 16pt bold, left-aligned at
`x=0.03–0.05, y=0.90`, with the team logo at figure fraction `(0.03, ~0.82)`.
Per-pitch title is 9pt bold, left-aligned inside the axes at `x=0.03, y=0.975`,
formatted `"{player} | {outcome} | ({mm}:{ss})"`. Leftover cells are **not**
hidden — they are drawn as empty cream pitches with an empty title, so the grid
stays rectangular. Close with `tight_layout()` then
`subplots_adjust(top=0.88, bottom=0.05)`.

---

## 7. Mark vocabulary

The shape/fill grammar is consistent and load-bearing — the same rules apply
whichever chart you are in.

**Filled = succeeded / on target. Hollow = failed / off target. Shape = event
family. Size = volume.**

| Event family | Shape | Success | Failure |
|---|---|---|---|
| Shot (on pitch) | circle `s=400` | on target: team colour `alpha=0.6`; goal: same + ball icon, `linewidth=2` | off target: team colour `alpha=0.3`, `linewidth=1` |
| Shot (goal mouth) | circle `s=200` | on target `alpha=0.6`; goal `alpha=1` | — |
| Aerial duel | triangle `^` `s=120` | filled team colour | `facecolors='none'`, team-colour edge |
| Ground duel / tackle | square `s` `s=100` | filled team colour | `facecolors='none'`, team-colour edge |
| Take-on | circle `o` `s=120` | filled team colour | `facecolors='white'`, team-colour edge |
| Progressive pass | arrow `->` team colour `lw=2 alpha=0.7` + origin dot `s=25`, white edge | — | — |
| Progressive reception | dot `s=80` team colour, white edge | — | — |

Marker outlines are `black` at `linewidth=1–1.5` in the match visualizations and
the **team colour** in the player visualizations — a real inconsistency in the
source. For v2, standardise on the team colour; it reads cleaner on cream.

Goals are always marked with the football icon (`OffsetImage`, `zoom=0.015`
inline in a legend or beside a node, `zoom=0.02` on a time series), offset up and
right of the mark, followed by an `x2`-style multiplier when a player scored more
than once, and annotated `"{shortName} {minute}'"` in 8–9pt bold black.

### Passing network

- Node radius: `s = pass_count * 30`, filled team colour, `linewidth=2`.
- Jersey number centred in white, size 10 if the player made ≥10 passes else 8.
- Player surname beside the node in grey 8pt bold, offset `±5` in x depending on
  which half of the pitch the node sits in.
- Links: grey arrows, `linewidth = pass_count / max_passes * 3`, `alpha=0.85`,
  drawn only for pairs with ≥3 passes, and shrunk by `1.2` units at the receiving
  end so the line stops short of the node.
- Network is cut at the first substitution.
- Four dotted guide lines (`ls=':' dashes=(1,3)`, grey, `lw=0.4`) mark the
  half-space and channel boundaries.

### Sequence pitches

A tiny visual language of its own, used by both the match sequences and the lab:

- **Pass** — straight gray line `lw=1.5 alpha=0.8` with a small arrowhead placed
  just past the midpoint; grey origin dot `s=50`.
- **Cross** — `FancyArrowPatch` with `connectionstyle="arc3,rad=0.3"` in grey,
  and an **orange** origin dot (the only orange in the system).
- **Carry** — grey dotted line `lw=2 marker='.'` with dots at both ends.
- **Take-on** — team-colour dot `s=50 alpha=0.7 linewidth=2`.
- **Shot** — arrow from the shot location to the goal centre `(105, 34)` in the
  team colour.
- **Players** — `s=200` circles at the last action of each consecutive block by
  that player. Every player is a white circle with a team-colour ring and a
  team-colour number; the **final actor** inverts to a filled team-colour circle
  with a white number. Numbers are 6pt bold.

Its legend (4 items, `element_spacing=15`, centred on `x=52.5` at `y=-3`, labels
1.5 **above** the glyph rather than below) reads: Participantes (hollow circle),
Finalización (filled circle), Pase (line + arrow), Conducción (dotted line).

---

## 8. Legends

There is no `ax.legend()` in the finished look — every legend is hand-placed, so
that it sits in the cream margin outside the pitch and uses the real marks rather
than proxy artists. Two idioms:

**Centred row** (duels, take-ons, crosses, sequences): compute
`start_x = 52.5 - ((n - 1) * spacing / 2)` with `spacing` 15 (sequences), 20
(duels) or 25 (crosses); draw each glyph at `y=-2`; put the 9pt `#333333` label
2–2.5 units below it. Items are built conditionally — a legend entry only exists
if that data is present.

**Right-to-left inline row** (shot maps): items are ordered goals → on target →
off target, laid out along `y=107` with a `|` separator between them, 9pt grey
text, `s=100` glyphs, and counts baked into the label (`Tiros a puerta (7)`). The
goals glyph is the ball image, falling back to a white circle with a black
`linewidth=2` edge.

Labels are Spanish: *Tiros fuera, Tiros a puerta, Goles, Duelos aéreos
ganados/perdidos, Duelos ganados/perdidos, Regates, Pases progresivos, Origen /
Destino de centros, Participantes, Finalización, Pase, Conducción*.

---

## 9. Text patterns

Three lines of copy recur and should be preserved verbatim in structure:

- **Title** — `"{Chart name} - {Team or player display name}"`, 15pt bold black,
  left-aligned, logo immediately to its left.
- **Context line** — `"#{shirt} - {position} - RCD Espanyol"` or
  `"#{shirt} | {position} | {team}"`, 10–11pt grey, directly under the title.
  Team charts use `"{Home} - {Away}"` plus a `KEY STATS` eyebrow instead.
- **Summary line** — a single `|`-separated string of totals, right side of the
  header band or under the pitch, 8–13pt grey. E.g.
  `"Acciones defensivas: 42 | Duelos aéreos: 6/11 | Duelos: 9/14"`,
  `"Regates: 4/7 (57.1% éxito)"`,
  `"Total: 23 centros | Desde banda izq: 14 | Desde banda der: 9"`.

Copy is Spanish. A handful of older charts still carry English titles (`Passing
Network`, `Field Tilt`, `Cumulative xT`) — v2 should translate these for
consistency.

**Empty states** are always a centred message at the pitch centre `(52.5, 34)`
(or `(0.5, 0.5)` in axes fraction for non-pitch charts) at 12pt, on a fully drawn
cream figure — never a blank or missing chart.

---

## 10. Specialist chart rules

**Match summary** (`figsize=(10, 12)`): mirrored horizontal bars from a central
gutter, `bar_height=0.04`, `spacing=0.1` per row, `xlim=(-2.8, 2.8)`. Bars run
outward from ±0.5 in each team's colour at `alpha=0.8`. The metric name sits in
the centre on a `#FFE7C9` pill (`0.9 × 0.08`), values in fixed columns at `x=±2`,
both 12pt bold `#333333`. Rows are separated by `#CCCCCC` hairlines drawn as two
segments (−2.5→−0.5 and 0.5→2.5) so they never cross the pill. Percentages are
normalised against 100, everything else against the row max. Logos at axes
fraction `(0.25, 1.08)` and `(0.75, 1.08)`, score at `(0.5, 1.05)` in 30pt. All
ticks and spines removed.

**Player radar** (`PyPizza`): `background_color='#FFF1E0'`,
`straight_line_color='#000000'` at `lw=1`, `last_circle_lw=1`,
`other_circle_lw=0`. Slices are the team colour with a black `lw=1` edge; value
labels are white on a `round,pad=0.2` box filled with the team colour and
outlined black. Everything is `monospace` here. Three radars per position —
*Defensa*, *Creación*, *Finalización* (or *Destrucción* etc. per position),
defined in `config/radar_mappings.json`. Values are percentiles vs same-position
La Liga players with 1000+ minutes. Fixed credit block bottom-left, 9pt:
comparison basis, `datos: Fbref`, `@jaumeblanco`.

**Time series**: field tilt fills between the smoothed curve and zero in each
team's colour at `alpha=0.6`, separates them with a `#FFF1E0` `lw=2` line drawn
on top of the curve, and puts a black dashed `lw=2 alpha=0.7 zorder=10` axis at
y=0. Goals drop a dashed grey vertical from the curve to a ball icon at ±90 with
the scorer label at ±96/−100. Cumulative xT uses `drawstyle='steps-post'`,
`lw=2 alpha=0.6` per team, ball icons placed directly on the curve.

**Cross analysis**: two `bin_statistic` heatmaps on one pitch — origins in
`#FFF1E0 → #2388C4`, destinations in `#FFF1E0 → #1C339A` — both `bins=(18, 15)`,
`edgecolor='#FFF1E0'`, `alpha=0.6`, `zorder=1`. Legend is two filled `1.5 × 1.5`
squares.

**Combined shot map** (`figsize=(12, 14)`): `subplot2grid((3,1))` with the goal
mouth taking 1 row and the pitch 2. Only the top panel carries the main 18pt
title and logo; each panel gets its own 13pt grey subtitle (*Tiros en portería*,
*Mapa de tiros en el campo*); the legend appears only under the pitch panel.
`tight_layout(h_pad=0.01)`.

**Dual-pitch defensive** (`figsize=(24, 10)`): two horizontal pitches sharing the
same zone heatmap, aerial duels left (*Duelos Aéreos*) and ground duels right
(*Duelos Terrestres*), 14pt bold panel titles with `pad=20`. The 18pt `suptitle`
at `y=0.95`, player detail centred at `y=0.91`, logo at figure fraction
`(0.05, 0.92)` zoom 0.12. This is the one chart that keeps a boxed
`ax.legend(loc='upper right', frameon=True, fancybox=True, shadow=True)` —
deliberately, because the two panels differ.

---

## 11. Porting to v2

Read `references/web-translation.md` before writing components. The short
version:

- The cream canvas becomes a surface token, not a `<body>` background — charts
  keep their own cream card so they read the same on any page.
- Draw the pitch once as an SVG component in metric coordinates, using the
  handoff's `viewBox` values (`"-2 -2 109 72"` horizontal, `"-2.5 -2.5 73 110"`
  vertical), and let every chart compose on top of it. Do not re-derive pitch
  lines per chart, and do not pad the `viewBox` for furniture — furniture is DOM
  (see the bullet below).
- Marker `s` values are matplotlib **areas in points²** — radius is
  `sqrt(s) / 2` points. `s=400 → r≈10pt`, `s=200 → r≈7pt`, `s=120 → r≈5.5pt`,
  `s=50 → r≈3.5pt`. Convert against your rendered scale rather than hard-coding
  pixels, so marks scale with the container.
- Everything in `y ∈ [-3, -2]` and `y ∈ [69, 71]` is furniture that belongs in
  HTML around the SVG, not inside it — header, context line, summary line and
  legend are all easier, more accessible and more responsive as DOM.
- The one thing that must stay in the SVG is anything positioned relative to
  pitch coordinates: marks, arrows, heatmap cells, player nodes, goal frame.
