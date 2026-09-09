# Translating the v1 charts to Next.js / React / TypeScript

**Read `SKILL.md` §0 first.** The v2 site's colour, typography, spacing and
pitch styling come from `web/design_handoff_espanyol_analytics/README.md`. This
file's job is the *conversion math* — ramps, marker sizes, mark mechanics,
chart-by-chart geometry — and the record of what v1 got wrong. Where it shows a
v1 value, that value is an archive, not an instruction.

The matplotlib code should not be transliterated — several of its patterns exist
only because matplotlib made the easy thing hard.

---

## 1. Design tokens — v1 archive

Do **not** create `lib/design/tokens.ts` from this block. The site's tokens are
the eleven handoff custom properties, already live in `web/app/globals.css` as
an `@theme` block (`md/WEB_PLAN.md` §3.4). What is kept below is the reading key
for v1 charts, plus the one thing the handoff does not carry: **the 19 La Liga
team colours**, which the two-team charts (field tilt, cumulative xT, match
summary) and any future non-Espanyol page still need.

```ts
// v1 values. Archive — see note above.
export const canvas = {
  base:      '#FFF1E0', // every chart surface, and the "zero" end of continuous ramps
  highlight: '#FFE7C9', // metric-name pill in the match summary
} as const;

export const ink = {
  primary:   '#000000',
  strong:    '#333333', // stat values, legend labels under a pitch
  muted:     '#666666', // subtitles
  secondary: '#808080', // 'grey' in matplotlib — summary lines, captions
  rule:      '#CCCCCC', // row separators
  pitchLine: '#7c7c7c',
} as const;

export const state = {
  noData:      '#FF0000', // legacy charts
  noDataSoft:  '#808080', // newer convention — prefer this
  noneOfKind:  '#FFA500',
} as const;

// Cross-analysis map only.
export const crossRamp = { origin: '#2388C4', destination: '#1C339A' } as const;

export const teamColor: Record<string, string> = {
  espanyol: '#2388C4', sevilla: '#BF2A37', valladolid: '#62247A',
  realsociedad: '#133C8B', atleticomadrid: '#E81422', rayo: '#EB3C34',
  alaves: '#1C339A', madrid: '#532c53', villarreal: '#f9e163',
  betis: '#047c24', mallorca: '#ed1b24', athletic: '#ff0302',
  barcelona: '#a50044', girona: '#cf0c29', celta: '#8ac3ee',
  osasuna: '#d30a29', getafe: '#174883', valencia: '#e96c08',
  laspalmas: '#f3d31d', leganes: '#00760a',
};
```

Three of these are light enough to fail against the warm grounds — Villarreal
`#f9e163`, Celta `#8ac3ee`, Las Palmas `#f3d31d`. Matplotlib got away with it
because marks carry a dark outline. On the web, keep the outline, and never use
those colours for text on `--paper` or `--card`.

### Typography

**Arvo does not ship on the v2 site.** The faces are Outfit, Figtree and DM Mono,
loaded via `next/font/google` (`md/WEB_PLAN.md` §3). Do not add an Arvo
`@font-face`.

What is still worth having is the *ratio* reading of the v1 point sizes, since
the matplotlib values were tuned against fixed `figsize` inches — they are
ratios, not pixels. At a nominal 12in-wide figure ≈ 1152px container:

| pt | ≈ px | Role |
|---|---|---|
| 30 | 30 | scoreline |
| 18 | 18 | composite chart title |
| 16 | 16 | grid title |
| 15 | 15 | standard chart title |
| 13 | 13 | panel subtitle |
| 12 | 12 | stat values, empty state |
| 11 | 11 | player context line |
| 10 | 10 | secondary context |
| 9  | 9  | legend, annotations |
| 8  | 8  | summary, footnote |

Map these onto the handoff's scale rather than shipping them as-is: chart title
→ H2-card 19px Outfit 700; panel subtitle → H3-small 16px 700; stat values →
16–17px 700; context and summary lines → Figtree 12–13.5px; legend, axis and
meta labels → DM Mono 9–11px uppercase with `.1em–.22em` tracking. The handoff's
floor is deliberate: anything a reader consumes is 12px or larger, and only
DM Mono meta goes below (`WEB_PLAN` §3.2). Use `clamp()` on the title sizes so a
chart card stays legible on mobile.

---

## 2. The pitch as one component

Write it once. Every chart composes on top of it.

Geometry, `viewBox` and line styling all come from the handoff's *Pitch
geometry* section. The values below are that section, not v1's.

```tsx
// components/pitch/Pitch.tsx
// Metric coordinates, drawn 1:1 in user units. x 0→105 (attacking right), y 0→68.
// No furniture margin in the viewBox — header, summary and legend are DOM (§6).
<svg viewBox="-2 -2 109 72" preserveAspectRatio="xMidYMid meet" role="img">
  <rect x="-2" y="-2" width="109" height="72" fill="var(--card)" />
  <g stroke="#D9C9B6" strokeWidth={0.35} fill="none">
    {/* outline 0,0,105,68; halfway x=52.5; centre circle r=9.15 at 52.5,34;
        penalty areas 0,13.85,16.5,40.3 and 88.5,13.85,16.5,40.3;
        six-yard boxes 0,24.85,5.5,18.3 and 99.5,24.85,5.5,18.3; spots, arcs */}
  </g>
  {children}
</svg>
```

Notes that matter:

- `strokeWidth={0.35}` is in *pitch units*, so it scales with the container —
  which is what you want, and matches how mplsoccer behaved at a fixed figsize.
- Set `vectorEffect="non-scaling-stroke"` only if you find lines vanishing at
  small sizes.
- The vertical variant is the same geometry transposed, `(x,y) → (y, 105-x)`,
  attacking upward, on `viewBox="-2.5 -2.5 73 110"` at stroke width 0.4. Half
  pitches are that vertical frame clipped to the attacking half.
- The handoff's Opta 0–100 conversion note does not apply here: `silver.events`
  already stores metres and the export ships metres (`WEB_PLAN` §9.1).
- `line_zorder=4` becomes render order: draw heatmap `<g>` first, pitch lines
  after it, marks after that. SVG has no z-index for shapes, so the z ladder in
  SKILL §5 is literally your element order.

### Zone grids

**Do not port the v1 20-zone grid** (`SKILL.md` §4). The site's zones are the
project's canonical **30-zone grid** — 6 longitudinal strips × 5 lateral
channels, `gold.pitch_zones` authoritative, boundaries and assignment logic in
the `pitch-guide` skill — and the **12×8 xT surface** the match export ships.

Take the boundaries from `pitch-guide` (or from `gold.pitch_zones` at export
time) as data, not as drawing code, and render each zone as one `<rect>` at
`fillOpacity={0.6}` with the ramp in §3.

Two things carry over unchanged from v1 and are worth keeping: **low `y` is the
RIGHT flank** (CLAUDE.md, `GOLD_LAYER.md` §4.6.0), and the fill alone carries
the value — v1 had per-zone count labels and a colorbar in the code and left
both commented out, which is why the maps read as calm.

---

## 3. Colour ramps

`LinearSegmentedColormap.from_list([low, teamColor], N=100)` with intensity
`0.1 + 0.7 * count / max` is just a clamped linear interpolation:

```ts
import { interpolateRgb } from 'd3-interpolate';

export function zoneFill(count: number, max: number, teamColor: string, low = '#FFFFFF') {
  const t = max > 0 ? 0.1 + 0.7 * (count / max) : 0.1;
  return interpolateRgb(low, teamColor)(t);
}
```

`low` is white for the discrete zone heatmaps and the **card ground** for the
continuous binned map — on v2 that is `var(--card)`, not v1's `#FFF1E0`; pass it
in rather than hard-coding either. `teamColor` resolves through the handoff
token (`--blue` for Espanyol). Keep the 0.1 floor and 0.8 ceiling — that
compression is why the maps read as calm rather than blown out. Always pair with
`fillOpacity={0.6}`.

Continuous heatmaps used `bins=(18, 15)` over 105×68, i.e. cells of
5.83 × 4.53 units, with the cell edge in the card ground — so give each cell a
thin `var(--card)` stroke, which is what makes the grid legible without drawing
gridlines. The site's own binned surface is the 12×8 xT grid the match export
ships; use its edges, not `(18, 15)`, wherever the data comes from that file.

---

## 4. Marker sizes

Matplotlib `s` is **area in points²**; radius is `sqrt(s) / 2` points.

| `s` | radius (pt) | radius in pitch units at a 12in / 105-unit pitch | Where |
|---|---|---|---|
| 400 | 10.0 | ≈ 1.2 | shot on the pitch |
| 200 | 7.1  | ≈ 0.86 | goal-mouth shot, sequence player node |
| 150 | 6.1  | ≈ 0.74 | sequence legend glyph |
| 120 | 5.5  | ≈ 0.66 | duel, take-on |
| 100 | 5.0  | ≈ 0.60 | ground duel, legend glyph |
| 80  | 4.5  | ≈ 0.54 | progressive reception |
| 50  | 3.5  | ≈ 0.43 | sequence event dot |
| 25  | 2.5  | ≈ 0.30 | progressive-pass origin dot |

The passing-network node is the one dynamic case: `s = passCount * 30`, so
`r = sqrt(passCount * 30) / 2` pt. Expressed in pitch units against a 12in
figure, `r ≈ 0.0725 * sqrt(passCount * 30)` — a player with 60 passes gets
`r ≈ 3.1`, one with 10 gets `r ≈ 1.3`. Keep the square-root scaling: area
encodes volume, radius does not.

Define these once as pitch-unit radii and let the SVG scale handle the rest.

---

## 5. Marks

```tsx
// Fill/stroke rules from SKILL §7. Standardise edges on the team colour.
const markStyle = (successful: boolean, teamColor: string) =>
  successful
    ? { fill: teamColor, fillOpacity: 0.8, stroke: teamColor, strokeWidth: 0.15 }
    : { fill: 'none',    stroke: teamColor, strokeWidth: 0.15 };
```

- **Circle** → `<circle>`. **Triangle** (aerial duel) → `<path>` or a
  `<polygon>` sized to the same visual area. **Square** (ground duel) → `<rect>`
  centred on the point.
- **Arrows** — matplotlib `arrowstyle='->'` with `head_width=0.3, head_length=0.5`
  becomes an SVG `<marker>` with `markerWidth/Height` tuned to those ratios,
  reused via `markerEnd`. Define one marker per colour you need, or use
  `context-stroke` where browser support allows.
- **Curved cross** — `connectionstyle="arc3,rad=0.3"` is a quadratic Bézier whose
  control point sits at the segment midpoint displaced perpendicular by
  `0.3 * length`. In SVG: `M x1 y1 Q cx cy x2 y2` with
  `cx = mx - 0.3 * dy, cy = my + 0.3 * dx` (flip the sign to bend the other way).
- **Dotted carry** — `strokeDasharray="0.6 1.2"` with round caps, plus the two
  end dots.
- **Network link shrink** — the 1.2-unit pullback at the receiving end: shorten
  the segment before drawing, don't rely on the marker offset.
- **Ball icon for goals** — an `<image>` or inline SVG glyph, offset up-right of
  the mark, with a `×n` label when the player scored more than once.

The three-way shot encoding (`alpha` 0.3 / 0.6 / 0.6+ball) is doing double duty
as both a legend key and a data encoding. It works, but on the web you now have
hover and focus available — use them for the detail (`xG`, minute, player) and
let the static mark stay simple.

---

## 6. What leaves the chart

Everything the matplotlib version drew at `y ∈ [69, 71]` or `y ∈ [-3, -2]` was
furniture squeezed into the axes because matplotlib had nowhere else to put it.
On the web it belongs in the DOM around the SVG:

```tsx
<figure className="chart-card">          {/* background: canvas.base */}
  <header>
    <TeamCrest team={team} />            {/* was AnnotationBbox zoom=0.15 at (1,71) */}
    <h3>Mapa de regates — {playerName}</h3>
    <p className="context">#{shirt} · {position} · {teamName}</p>
    <p className="summary">Regates: {ok}/{total} ({rate}% éxito)</p>
  </header>
  <Pitch>{marks}</Pitch>
  <figcaption><Legend items={legendItems} /></figcaption>
</figure>
```

This gets you real text selection, translation, screen-reader access, and
responsive reflow — none of which the rendered PNG had. It also deletes the
ugliest code in the repo: the `get_window_extent()` text-measuring loop used to
build the two-colour title in field tilt and cumulative xT becomes

```tsx
<h3>Field Tilt: <span style={{color: homeColor}}>{home}</span> vs <span style={{color: awayColor}}>{away}</span></h3>
```

Same for the shot-map legend, which measured character counts to lay itself out
right-to-left — that is a flex row.

Keep in the SVG only what is positioned in pitch coordinates: marks, arrows,
heatmap cells, player nodes, the goal frame, and any label anchored to an event
(the `"{player} {minute}'"` goal annotations).

---

## 7. Charts that are not pitches

- **Time series** (field tilt, cumulative xT) — the fills, the cream separator
  line, the dashed zero axis, `alpha=0.6`, grid at `alpha=0.3`, 15-minute ticks
  and the hidden spines all carry over directly to d3/visx or Recharts. Field
  tilt hides every spine; cumulative xT hides top and right only. Keep the
  `steps-post` interpolation on xT (`curveStepAfter`) — it is factually correct,
  since xT accrues at discrete events.
- **Match summary** — this is a table, not a chart. Build it as CSS grid: a
  centre column of metric-name pills on `canvas.highlight`, mirrored bars either
  side as `<div>`s with a percentage width, `#CCCCCC` row borders that stop short
  of the pill. No SVG needed.
- **Radar** — PyPizza has no React equivalent, so this is the one chart you will
  write from scratch. Keep: cream background, black spokes at `lw=1`, one filled
  outer circle, no intermediate circles, team-colour wedges with a black edge,
  and white value labels on rounded team-colour chips. Keep the monospace
  typeface too — it was a deliberate choice and it distinguishes the percentile
  charts from everything else. Percentile domain is 0–100, six metrics per radar,
  three radars per position from `config/radar_mappings.json`.

---

## 8. Things to fix rather than port

Faithfully reproducing these would carry bugs across:

1. **Edge colour inconsistency** — match charts outline marks in black, player
   charts in the team colour. Pick the team colour everywhere.
2. **English titles on three charts** — `Passing Network`, `Field Tilt`,
   `Cumulative xT` while everything else is Spanish. Translate them.
3. **Empty-state colours** — `red` in the older charts, `grey` in the newer ones.
   Standardise on the muted grey; a missing chart is not an error.
4. **`x{n}` goal multiplier colour** — grey in the passing network, red in the
   shot map. Pick one.
5. **Hard-coded furniture anchors** — `(90, 69)`, `(81, 69)`, `(73, 69)` for the
   summary line are eyeballed to the length of each specific string. In HTML this
   is `justify-content: space-between` and the problem disappears.
6. **`print()` calls in render paths** and the `try/except` blocks that swallow
   errors and return `(None, None)`. Let the component fail into an error
   boundary instead.
7. **The absolute Windows `path_event_data` in `config.yaml`** — must not survive
   into anything deployed.
8. **Ball icon as a raster PNG** — replace with an inline SVG so it stays crisp
   and can inherit colour.
