# Visualization catalog — v1 charts, as implemented

One entry per chart in the **v1 Streamlit repo**. Its `src/match_viz/`,
`src/player_viz/`, `src/lab_viz/` and `config/` are in *that* project, not this
one — see `SKILL.md` §0.

Values are as implemented there. `TC` = team colour from
`config.yaml → teams_color`. Every chart has `fig.patch` and `ax` set to
`#FFF1E0` and uses Arvo unless stated.

**Reading this for v2 work:** take the *structure* — which marks, at what sizes,
in what order, with which summary line — and resolve every colour, face and size
through the design handoff (`SKILL.md` §0 and §1). Cream reads as `--card`,
Arvo as Outfit/Figtree/DM Mono, TC as the handoff's `--blue` for Espanyol.

---

## Match visualizations (`src/match_viz/`)

### `field_tilt` — FieldTilt.plot_tilt
**Type** time series · **figsize** (12, 6) · **kwargs** `window_size=10`, `smoothing_sigma=0.8`

Share of final-third passes over time, smoothed with `gaussian_filter1d`.
Positive area = home, negative = away.

- `fill_between(minute, smooth_y, 0)` in home TC where `> 0`, away TC where
  `< 0`, both `alpha=0.6`.
- The curve itself is redrawn in `#FFF1E0` at `lw=2` — it reads as the gap
  between the two fills, not as a line.
- `axhline(0)` black dashed `lw=2 alpha=0.7 zorder=10`.
- Goals: dashed grey vertical (`lw=1 alpha=0.7 zorder=15`) from the centre line
  out to `ymin/ymax` 0.50→0.94 (home) or 0.50→0.08 (away); ball icon `zoom=0.02`
  at y ±90; label `"{player} {minute}'"` at y ±96 / −100, 8pt bold black.
- Composed title at fig `(0.05, 0.98)` 14pt bold: `"Field Tilt:"` + home name in
  home TC + `" vs "` + away name in away TC.
- Summary at `(0.05, 0.93)` 10pt: total final-third passes and each team's count
  and percentage.
- Footnote at `(0.05, 0.02)` 8pt `alpha=0.7` explaining the metric and window.
- Grid `alpha=0.3`; **all** spines hidden; x ticks every 15′ to 90′; y ticks
  fixed `[-100,-75,-50,-25,25,50,75,100]`.

### `cumulative_xt` — CumulativeXT.plot_cumulative_xt
**Type** time series · **figsize** (12, 6)

Running xT per team.

- One `plot` per team, `drawstyle='steps-post'`, TC, `lw=2 alpha=0.6`.
- `axhline(0)` black dashed `lw=1`.
- Goals: ball icon `zoom=0.02` placed on the curve at the goal minute; label
  `"{player} {minute}'"` 0.08 above, 8pt bold.
- Same composed title pattern as field tilt, prefix `"Cumulative xT: "`.
- Summary at `(0.05, 0.93)`: `"{Home}: 1.42 | {Away}: 0.87"`.
- Grid `alpha=0.3`; only top and right spines hidden; x ticks every 15′;
  y ticks in 0.2 steps, ceiling rounded up to the next 0.2 with a 1.4 floor.

### `match_summary` — MatchSummary.plot_match_summary
**Type** mirrored bar sheet · **figsize** (10, 12)

Left/right team assignment follows venue (home on the left).

- Header: title `"{Left} - {Right}"` 18pt bold `#333333` at axes `(0.5, 1.15)`;
  `KEY STATS` 12pt bold `#666666` at `(0.5, 1.1)`; score 30pt bold at
  `(0.5, 1.05)`; logos at `(0.25, 1.08)` and `(0.75, 1.08)`, zoom 0.30 (0.25 for
  the Espanyol crest, which is visually heavier).
- 11 rows: Possession %, Field Tilt %, Passes, Pass Acc. %, Passes into Box, xG,
  Shots, Shots OT, Ball Recoveries, Tackles, Corner Kicks.
- Row pitch `spacing=0.1`; bars `height=0.04` growing outward from `x=±0.5` in
  each TC at `alpha=0.8`; `xlim=(-2.8, 2.8)`.
- Metric name centred on a `#FFE7C9` pill `0.9 × 0.08`, 12pt bold `#333333`.
- Values in fixed columns at `x=±2`, 12pt bold `#333333`. Percentages `.0f%`,
  xG `.2f`, everything else integer.
- Between rows: two `#CCCCCC` hairlines `lw=0.5 alpha=0.7`, from −2.5 to −0.5 and
  0.5 to 2.5, so they never run under the pill.
- Percentages normalise against 100; other metrics against the row max.
- All ticks and spines removed. `subplots_adjust(left=.1, right=.9, top=.85, bottom=.3)`.

### `passing_network` — PassingNetwork.create_passing_network_plot
**Type** full VerticalPitch · **figsize** (12, 8) · **kwargs** `team='home'|'away'|'espanyol'|'opponent'`
**Returns** `(fig, ax, average_locs_and_count)` — note the third element, unlike every other chart.

Completed passes up to the first substitution.

- Nodes `s = count * 30`, TC fill and TC edge, `lw=2`, `zorder=2`.
- Jersey number centred white, size 10 when `count ≥ 10` else 8, `fontweight='medium'`.
- Surname beside the node, grey 8pt bold, x offset `+5` if `x > 52.5` else `−5`.
- Links: grey `annotate` arrows, `lw = pass_count / max_passes * 3`,
  `alpha=0.85`, `zorder=1`, endpoint pulled back 1.2 units. Minimum 3 passes
  between a pair.
- Goalscorers get a ball icon `zoom=0.015` offset by `sqrt(node_size) * 0.09`
  up-right, plus `x{n}` in grey 8pt bold when they scored more than once.
- Guide lines: verticals at 14, 54, 25, 43 and horizontals at 88.5, 70.5, 16.5,
  34.5, all `ls=':' dashes=(1,3)`, grey, `lw=0.4`, inset from the edges.
- Title `(63, 107)` 15pt bold, logo `(66, 108.5)` zoom 0.15.
- Footer at `(68, -2)` 8pt grey:
  `"Tamaño punto = Total de pases | Tamaño línea = Total de pases entre jugadores"`.

### `shot_map` — ShotMap.create_shot_map_plot
**Type** half VerticalPitch · **figsize** (10, 8) · **kwargs** `team=`

- Off target: `s=400` TC `alpha=0.3`, black edge `lw=1`, `zorder=1`.
- On target: `s=400` TC `alpha=0.6`, black edge `lw=1`, `zorder=2`.
- Goals: `s=400` TC `alpha=0.6`, black edge `lw=2`, `zorder=2`, plus ball icon
  `zoom=0.020` offset 0.8 units up-right and `x{n}` in red 8pt when repeated.
- Goal annotation `"{shortName} {minute}'"` 9pt bold black, 2 units above the mark.
- Title `(63, 107)` 15pt bold `"Mapa de tiros - {team}"`, logo `(65, 108)` zoom 0.15.
- Custom inline legend along `y=107` — see SKILL §8.

### `goalmouth_map` — GoalMouthMap.create_goal_mouth_plot
**Type** goal-frame scatter · **figsize** (12, 8)

Coordinate frame: x = `goal_mouth_y_centered` ∈ [−3.66, 3.66], y =
`goal_mouth_height` ∈ [0, 2.44]. `set_aspect('equal')`, margin 1.0, all ticks and
spines removed.

- Frame: posts and crossbar grey `lw=4 solid_capstyle='round' zorder=5`; goal
  line grey `lw=2`.
- On target `s=200` TC `alpha=0.6` black edge `lw=0.5` `zorder=12`.
- Goals `s=200` TC `alpha=1` `zorder=15` + ball icon.

### `combined_shotmap` — CombinedShotMap.create_combined_shot_visualization
**Type** stacked composite · **figsize** (12, 14)

`subplot2grid((3,1))`: goal mouth row 0 (1 row), half pitch rows 1–2 (2 rows).

- Main title 18pt bold at `(-3.85, 2.94)` in goal-mouth coordinates; logo at
  `(-4.05, 2.99)` zoom 0.15.
- Panel subtitles 13pt bold grey: `"Tiros en portería"` at `(-3.66, 2.54)`;
  `"Mapa de tiros en el campo"` at `(68, 106.5)` on the pitch panel.
- Legend only under the pitch panel, at `y=106.5`.
- `tight_layout(h_pad=0.01)`.
- Empty states here use **grey**, not red — this is the newer convention.

### `important_sequences` — ImportantSequences.create_sequences_grid_plot
**Type** 3-column small multiples · **figsize** `(15, rows * 4.55)` · **kwargs** `team=`

See SKILL §7 "Sequence pitches" for the mark language and §6 for the grid rules.
Per-pitch title 9pt bold `"{shortName} | {outcome} | ({mm}:{ss})"` at axes
`(0.03, 0.975)`, `loc='left'`, `pad=2`. `suptitle` 16pt bold at `(0.05, 0.90)`
`ha='left'`; logo at figure fraction `(0.03, y)` where y is 0.800/0.815/0.820/
0.830/0.835 for 1–5 rows.

### `defensive_heatmap` — DefensiveHeatMap.create_defensive_heatmap_plot
**Type** zone heatmap, horizontal pitch · **figsize** (15, 10) · **kwargs** `team=`

Blocked passes, interceptions, ball recoveries and successful tackles, binned
into the 20-zone grid. Each zone is a `Rectangle` filled with
`cmap(0.1 + 0.7 * count / max)` at `alpha=0.6 zorder=1`, ramp `#FFFFFF → TC`.
Pitch drawn with `line_zorder=4`. Title `(4, 70)` 15pt bold, logo `(1, 71)`,
summary `(1, −3)` 13pt grey listing the total and which action types are
included. Per-zone count labels and the colorbar exist in the code but are
commented out — the fill alone carries the value.

### `pass_heatmap` / `reception_heatmap`
Same construction as `defensive_heatmap`, over successful passes (origin) and
pass receptions (destination) respectively. Identical layout, ramp, alpha and
anchors.

### `defensive_duelsmap` — DefensiveDuelsMap.create_duels_plot
**Type** horizontal pitch · **figsize** (12, 8) · **kwargs** `team=`

- Aerial won `^ s=120` TC filled; aerial lost `^ s=120` hollow. Ground: `s`
  markers, `s=100`, successful tackles filled, challenges hollow. All black
  edges `lw=1.5 alpha=0.8 zorder=5` in the match version.
- Title `(4, 70)` 15pt bold, logo `(1, 71)`.
- Summary at `(53, 69)` 13pt grey:
  `"Acciones defensivas: {n} | Duelos aéreos: {won}/{total} | Duelos: {won}/{total}"`.
- Centred legend at `y=-2`, `element_spacing=20`, labels 2 below, 9pt `#333333`;
  legend glyphs use TC edges (not black) at `alpha=0.9 zorder=6`.

---

## Player visualizations (`src/player_viz/`)

All take a player display name, resolve the team through
`config/player_mappings.json` + `team_mappings.json`, and share the horizontal
header block: title `(4, 71)` 15pt bold, logo `(1, 71)` zoom 0.15, context line
`(4, 69)` 10–11pt grey, summary right-aligned block at `(73–90, 69)` 8pt grey.

### `player_radar` — PlayerRadar.generate_individual_radars
**Type** PyPizza percentile radar · **figsize** (8, 8) each, three per player

`background_color='#FFF1E0'`, `straight_line_color='#000000'` `lw=1`,
`last_circle_lw=1`, `other_circle_lw=0`. Slices TC with black `lw=1` edge; param
labels black 10pt monospace; values white 15pt monospace on a
`round,pad=0.2` box filled TC with a black `lw=1` edge.
Title at fig `(0.515, 0.98)` 18pt; subtitle `"{team} | La Liga 2024-25"` at
`(0.515, 0.952)` 11pt; credits at `(0.075, 0.005)` 9pt left-aligned, three lines:
comparison basis, `datos: Fbref`, `@jaumeblanco`. All monospace — the deliberate
exception to Arvo.
Radar sets per position come from `config/radar_mappings.json`, three per
position (e.g. DC: Defensa / Creación / Finalización; DFC: Destrucción / …), six
metrics each, all with `color: "#2388C4"`.

### `player_combined_shotmap`
Player-scoped clone of `combined_shotmap`: goal mouth over half pitch, same
sizes, alphas and anchors.

### `player_defensive_heatmap`
Zone heatmap of the player's defensive actions on a horizontal pitch, identical
ramp and alpha to the team version.

### `player_defensive_duels` — `player_defensiveduels_heatmap.create_dual_pitch_defensive_plot`
**figsize** (24, 10) · two horizontal pitches side by side

Both panels carry the same zone heatmap of all defensive actions; the left panel
overlays aerial duels (`^`), the right ground duels (`s`). Panel titles 14pt bold
`pad=20`: *Duelos Aéreos*, *Duelos Terrestres*. `suptitle` 18pt bold at
`y=0.95`; player detail centred at `y=0.91` 12pt grey; logo at figure fraction
`(0.05, 0.92)` zoom 0.12. Markers here use TC edges at `alpha=0.9 zorder=6`.
Uniquely, this chart keeps a boxed `ax.legend(loc='upper right', frameon=True,
fancybox=True, shadow=True)` per panel. A `_vertical` variant exists with the
same rules on `VerticalPitch`.

### `player_takeons` — create_takeons_plot
**figsize** (12, 8)

Successful: `o s=120` filled TC, TC edge `lw=1.5 alpha=0.8 zorder=5`.
Unsuccessful: `o s=120` `facecolors='white'`, TC edge.
Title `"Mapa de regates - {player}"`; summary at `(90, 69)` 8pt grey
`"Regates: {succ}/{total} ({rate}% éxito)"`.

### `player_cross_analysis` — create_cross_analysis_plot
**figsize** (12, 8)

Two continuous `bin_statistic` heatmaps on one pitch, `bins=(18, 15)`,
`edgecolor='#FFF1E0'`, `alpha=0.6`, `zorder=1`: origins `#FFF1E0 → #2388C4`,
destinations `#FFF1E0 → #1C339A`. Title 16pt bold at `(4, 71)`; detail
`"#{n} | {pos} | {team}"` 11pt grey at `(4, 69)`; summary at `(73, 69)` 8pt
`"Total: {n} centros | Desde banda izq: {n} | Desde banda der: {n}"`.
Legend: two `1.5 × 1.5` filled squares, `element_spacing=25`, at `y=-2` with
labels 2.5 below — *Origen de centros*, *Destino de centros*.

### `player_progressive_passes` — create_progressive_passes_plot
**figsize** (12, 8)

Each progressive pass is an `annotate` arrow `arrowstyle='->'` in TC,
`lw=2 alpha=0.7 zorder=5`, with an origin dot `s=25` TC, white edge `lw=0.5`,
`alpha=0.8 zorder=6`. Title `"Pases progresivos último 1/3 - {player}"`; summary
at `(81, 69)` 8pt `"Pases progresivos último 1/3 | {prog}/{succ} | ({rate}%)"`.
Empty-of-this-kind message is **orange**, not red.

### `player_progressive_receptions`
Reception points `s=80` TC with white edge, plus an emphasis ring `s=200`; this
is the one player chart that uses a real `ax.legend(loc='upper right')` built
from proxy artists.

### `player_progressive_heatmap`
Zone heatmap of progressive-pass origins, same construction as the other zone
heatmaps.

### `player_important_sequences`
Player-scoped clone of the sequence grid; `suptitle` becomes
`"Secuencias Importantes - {player}"`.

---

## Lab visualization (`src/lab_viz/`)

### `SequenceLaboratory`
`apply_filters(**kwargs) -> (filtered_sequences, sequence_events, filter_summary)`
then `create_sequences_grid_visualization(...)`. The grid, per-pitch marks,
titles and empty-cell handling are identical to `important_sequences`; only the
`suptitle` differs: `"{title_prefix} - {n} Secuencias Encontradas"`, 16pt bold at
`(0.03, 0.90)`. The Espanyol logo is built but its `fig.add_artist` call is
commented out.
