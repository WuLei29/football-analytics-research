# Handoff: Screen 03 · Player profile

Companion to `README.md` (RCD Espanyol Analytics Platform). Its design tokens, pitch geometry, the
HTML-over-SVG axis rule and the implementation notes apply unchanged. This file specifies
**screen 03** at the same fidelity as screens 01 and 02, and supersedes the "first-pass" note for
page 03 in `README.md`.

> **v2 — reconciled with the repo on 10 Oct 2026.** The first version of this brief came from the
> design prototype: mock data, placeholder metric definitions, hypothetical gold tables. This version
> keeps the **layout and visual spec** as designed and replaces everything that touches data with what
> the project actually has, the way `md/WEB_PLAN.md` §9.2 and §3.7 did for screens 01–02.
> - Definitions come from the `football-metrics` skill.
> - Chart reuse comes from the `espanyol-viz-design` catalog.
> - Data comes from `players/{player_id}.json`, written by `src/export` (`md/WEB_DATA.md` §9). No gold
>   tables are created for this page.
>
> Every departure from the mock is listed in **"What changed from the mock"** at the end. Items marked
> **(proposed)** are defaults chosen on 10 Oct 2026 that still need the owner's OK. They're collected
> under **"Open decisions"**.

## Purpose
Read one player's season: who he is, where he touches the ball, how he ranks inside his position
group, and which possession chains he is part of. Entered from the page-01 player leader rows
(flip `LINK_TO_PLAYER` in `PlayerLeaders.tsx`), from the squad table (`/plantilla`) and from the tab
strip on this page.

**Route** (proposed): `/{team}/jugador/{season}/{player_id}`, e.g. `/espanyol/jugador/2025-26/5678`.
The current `routes.player(team, playerId)` in `web/lib/routes.ts` has no season. A player page is a
season page (minutes, percentiles and maps are all season-scoped), so add the season when this page
is built. Static export: one page per `(season, player)` in `squad.json`, via `generateStaticParams`.

**Copy is Spanish.** The English titles below are the design's. The shipped strings live in
`web/lib/labels.ts`; suggested Spanish is given next to each one.

---

## Page structure (top to bottom)

```
┌─ Page header ─────────────────────────────────────────────────────────────┐
├─ Player tab strip (squad, links) ─────────────────────────────────────────┤
├─ Profile card (340px)        │ Event map card (selector + pitch)          │
├─ Percentile vs La Liga <group> — 4 cards ─────────────────────────────────┤
├─ Sequences involving <player>  ·  rank-by toggle ─────────────────────────┤
└─ Chain traces + aggregates    │ Top-10 sequence list (320px)              ┘
```

### 1 · Page header
`display:flex; align-items:flex-end; justify-content:space-between; gap:24px;`
`border-bottom:1px solid var(--ink); padding-bottom:12px; margin-bottom:22px`.

- **Left**: eyebrow `03 · PERFIL DE JUGADOR` (DM Mono 9.5px, `.22em`, `--blue`, `margin-bottom:6px`),
  then a `flex, align-items:flex-end, gap:16px` row:
  - **Shirt block**: `border:2px solid var(--ink)`, radius 11px, `padding:6px 12px 4px`, background
    `--card`, centred. Number Outfit 700 34px `line-height:.86`; caption `DORSAL` DM Mono 7px `.16em`
    `--faint`.
  - **Name block**: first name DM Mono 10px `.22em` uppercase `--blue`; surname Outfit 700 **52px**
    `line-height:.95; letter-spacing:-.04em`. Use the Opta `known_name` when set (e.g. a one-word
    name), split on the last space otherwise.
- **Right**: mono 10px `.12em` uppercase `--mid`, `line-height:1.7`, two lines:
  `{position} · {age} AÑOS · {minutes}'`, then `LALIGA {season} · HASTA J{matchday}`. Age is shown
  only when `date_of_birth` is known.

### 2 · Player tab strip
`flex, gap:6px, wrap, margin-bottom:16px`. One pill per squad player **who has played this season**,
ordered by position group (GK → CB → FB → MID → W → ST) and then by minutes. That's ~20–25 pills,
wrapping to two or three rows (the mock showed 6). Pill: DM Mono 10px `.1em` uppercase surname,
`padding:6px 12px`, radius 999px, `1px solid var(--line)`. Selected = `--blue` fill + white text;
others `--paper` on `--mid`. Hover → `border-color:var(--blue)`.

Each pill is a **link** to that player's page (static site, no client re-query). It carries the
current map mode, so the mode stays sticky across players (§ State).

### 3 · Profile card + event map
`grid-template-columns: minmax(0,340px) minmax(0,1fr); gap:16px; align-items:stretch;
margin-bottom:16px`. The left card uses `height:100%` and its photo block `flex:1`, so **the photo
grows to match the pitch card's height** and the two columns always bottom-align. Don't fix the
photo height; let it flex.

#### 3a · Profile card (left, 340px)
`background:var(--card); border:1px solid var(--line); border-radius:14px; padding:2px;
display:flex; flex-direction:column`. Four stacked blocks:

1. **Photo**: `position:relative; flex:1; min-height:206px; margin:-2px -2px 0;
   border-radius:14px 14px 0 0; overflow:hidden; background:#EFE3D4`. Image `object-fit:cover`.
   **No photo source exists in the data.** Photos are hand-supplied assets
   (`web/public/players/{player_id}.jpg`, licensed, same posture as the hero and crest in `README.md`
   § Assets). Without one, the block shows the shirt number as a large Outfit 700 `--line` numeral on
   `#EFE3D4` (proposed). Overlays (both `pointer-events:none`):
   - name plate bottom-left (`left:14px; bottom:11px`): inline-block, `background:rgba(23,24,28,.82)`,
     radius 9px, `padding:5px 10px 6px`; first name DM Mono 9px `.18em` `rgba(255,255,255,.8)`,
     surname Outfit 700 24px `line-height:1.05; letter-spacing:-.03em`, white.
   - shirt chip top-right (`right:12px; top:12px`): `--ink` surface, radius 9px, `padding:4px 9px 3px`,
     number Outfit 700 20px white.
   The plate and chip surfaces exist so the type passes contrast over **any** photo. Keep them.
2. **Bio grid**: `grid-template-columns:1fr 1fr; gap:0 14px; padding:12px 16px;
   border-bottom:1px solid var(--line)`. 8 rows, each a baseline-aligned `space-between` pair,
   `padding:3px 0`: key DM Mono 8.5px `.12em` uppercase `--faint`; value Figtree 12px 600 `--ink`.
   Keys, in order: **Edad, Nacimiento, Altura, Pie, Nacionalidad, Posición, Partidos,
   Titularidades**. *Weight* and *Contract* from the mock are out because neither exists in the data.
   `height_cm` and `date_of_birth` are hand-filled in `silver.players` and may be NULL; render `—`.
   *Pie* is `preferred_foot`, derived from event foot qualifiers (`izquierdo / derecho / ambos`).
   *Posición* is the **primary position** from the formation slot (`CB`, `RB`, `DM`… mapped to Spanish
   in `labels.ts`), not `position_raw`.
3. **Four KPIs**: `grid-template-columns:1fr 1fr`, each cell `padding:14px 16px`,
   `border:1px solid var(--line); margin:-0.5px` (collapsed hairline grid). Label DM Mono 9px `.14em`
   uppercase `--mid`; value Outfit 700 **42px** `line-height:.86` (`--blue` for the first, headline
   KPI, `--ink` otherwise); sub DM Mono 9px `--faint`, `margin-top:3px` (per-90 or rank-in-squad line).
   **The set depends on the position group** (proposed), so a centre back's card isn't four zeros:

   | Group | KPI 1 (headline) | KPI 2 | KPI 3 | KPI 4 |
   |---|---|---|---|---|
   | ST, W | Goles | Asistencias | npxG | xT / 90 |
   | MID | xT / 90 | Asistencias | xA | Pases progresivos |
   | CB, RB, LB | Acciones def. (aj. posesión) / 90 | Pases progresivos | Duelos aéreos % | Pases % |
   | GK | Goles evitados | Paradas % | Pases % | Porterías a cero |

   The sets live in `src/export/config.py` (`PLAYER_KPIS` by group), not in the component.
4. **Secondary stat row**: `flex; wrap; gap:14px; padding:14px 16px`. Each item is value
   (Outfit 700 19px) + key (DM Mono 8.5px `.1em` uppercase `--mid`), baseline-aligned, `gap:5px`.
   6 items: **Tiros, Pases clave, Regates ganados, Recuperaciones, Pases %, Minutos** (GK: Paradas,
   Blocajes, Salidas, Pases largos %, Pases %, Minutos).

#### 3b · Event map card (right)
`background:var(--card); border:1px solid var(--line); radius 14px; padding:16px`.

- **Header**: `flex; align-items:baseline; space-between; margin-bottom:8px`. Title Outfit 700 17px
  `-.015em` (**the title changes with the selected mode**); the right-hand note is DM Mono 8.5px
  `.1em` uppercase `--faint`, and states the **count shown of the total** whenever a cap applies
  (`150 DE 1.284 PASES`).
- **Mode selector**: `flex; wrap; gap:5px; margin-bottom:10px`. Pills: DM Mono 9.5px `.08em`
  uppercase, `padding:5px 11px`, radius 999px, `1px solid var(--line)`; active `--blue` + white,
  inactive `--paper` + `--mid`; hover `border-color:var(--blue)`.
- **Pitch**: horizontal full pitch, `viewBox="-2 -2 109 72"`, attack right, `<Pitch>` from
  `components/viz`. The `ATAQUE →` cue is HTML furniture beside the title, not SVG text (house rule:
  no type inside the SVG). Draw order: **heat cells → pitch lines → arrows → dots** (`underlay`, then
  `children`). Below the pitch is an HTML `<Legend>` with real marks, taken from the mode.

**Eight modes** (`vista`). The mock had six; **Recepciones** and **Tiros** are added, because the
player page is where a reception heatmap and a shot map belong, and the old separate shot-map pitch
stays out (one pitch, mode-switched). Definitions are the repo's (football-metrics):

| Mode (pill) | Title | Marks | Note / legend | Component |
|---|---|---|---|---|
| `Pases` | Mapa de pases | Arrows. Progressive: `--blue` w 0.6, op 0.9. Other: `--faint` w 0.3, op 0.38, **no arrowhead**. Failed: dashed | Azul = progresivo · gris = resto | `ActionArrows` (new, §Reuse) |
| `Progresivos` | Pases progresivos | `--blue` w 0.7, op 0.88; failed dashed | **Acercan el balón ≥ 25 % a la portería** | `ProgressionArrows` / `ActionArrows` |
| `Conducciones` | Conducciones progresivas | `--gold` w 0.75, op 0.9 | Progresivas · ≥ 25 % más cerca de portería | `ProgressionArrows` |
| `Toques` | Mapa de toques | **30 zones** (`gold.pitch_zones`), `--blue` fill, `heatOpacity` (0.06→0.92) | Densidad de toques · LOW→HIGH ramp | `ZoneHeatmap` |
| `Recepciones` | Recepciones | 30-zone heat of reception points, plus `--blue` dots r 0.9 for **progressive** receptions (ring r 1.4 when the reception is in the box) | Zonas = recepciones · punto = recepción progresiva | `ZoneHeatmap` + dot layer (new) |
| `Tiros` | Mapa de tiros | Circles r = `1.4 + sqrt(xG)*5.2`; goal filled / on target half / off or blocked hollow; hover card with goal frame | Tamaño = xG · relleno = resultado | `ShotMap` (all shots `side = teamSide`) |
| `Defensivas` | Acciones defensivas | Diamonds, `--blue` = ball won, `#C9B9A6` = lost; chips by type (proposed, see Open decisions #3) | Azul = recupera · gris = pierde | `DefensiveActions` |
| `Pases clave` | Pases clave y asistencias | Arrows: assists `--gold` w 0.85, key passes `--blue` w 0.55, op 0.85 | Dorado = asistencia · azul = pase clave | `ActionArrows` |

Arrowheads come from the shared `<VizDefs />` markers (`viz-arrow-*`); a new colour is a new marker
there, not an inline `<marker>`. Coordinates are real event coordinates in the player's attacking
frame, already set by the export. No component flips anything. Low `y` is the right flank and sits
at the bottom: a right-back's touches belong in the lower half. That's the first thing to check on
a real file.

**Definitions behind the modes** (`football-metrics` §2–§5):
- *Progressive* = `gold.is_progressive`: the action ends at least 25% closer to the centre of the goal
  (105, 34) than it started. Same rule for passes and carries. **Not** "≥10 m upfield, ends in opp.
  half" (the mock's note). That rule rewards sideways balls near the touchline.
- *Carry* = a synthesised carry (`type_id = -1`), ≥ 1 m. The mode shows **progressive** carries only.
  All carries are hundreds of short arrows, and "≥5 m under control" has no basis in the data.
- *Touch* = the on-ball set: pass, carry, take-on, ball touch, clearance, recovery, keeper actions,
  shots. Defensive duels aren't touches.
- *Reception* = a row of `gold.pass_receptions` with this player as `receiver_id`. Opta records no
  receiver; it's the next same-team action after a completed pass. Rows with `rx_gap_m > 5` are
  dropped from the dots, not from the heat.
- *Key pass* = a pass whose shot was missed or saved (`value_assist` 13/14/15). *Assist* = the shot
  was a goal (16). Key passes **exclude** assists.
- *Defensive*: won = `Tackle` success, `Interception`, `Ball recovery`, `Blocked Pass`, `Clearance`;
  lost = `Tackle` failure, `Attempted Tackle`, `Challenge` (= dribbled past).

> The selector is the one place a reader chooses what they see. Keep all modes on pills (no
> dropdown); at eight they wrap to two rows on narrow cards, which is fine. Keep `<Pitch>` mounted
> across mode changes: only the marks, title, note and legend swap.

### 4 · Percentile vs La Liga {position group}
**Same construction as the page-01 Team profile block.** Reuse `TeamProfile` (same card, row and
colour thresholds) with a player metric source.

- **Section head**: `flex; baseline; space-between; margin-bottom:12px`. Title Outfit 700 19px
  `-.015em`: `Percentil vs LaLiga · {grupo}`. The group is the player's **primary position group**,
  one of `Porteros, Centrales, Laterales derechos, Laterales izquierdos (or Laterales when pooled),
  Centrocampistas, Extremos, Delanteros`. Caption DM Mono 9px `.12em` uppercase `--faint`:
  `Por 90 · vs {peer_n} {grupo} con ≥ {min_minutes}' · barra = percentil, más es mejor`.
  If `is_utility` (primary position < 60% of minutes), add a mono chip
  `POLIVALENTE · {pct}% {posición}`. A percentile against centre backs means less when half the
  minutes came in midfield (GOLD_LAYER §4.6.2).
- **Grid**: `repeat(4,1fr); gap:14px; margin-bottom:22px`. Card: `--card` surface,
  `1px solid var(--line)`, radius 14px, padding 16px.
  - Card head: title Outfit 700 15px + unit caption DM Mono 8.5px `.1em` uppercase `--faint`
    (`percentil`), `border-bottom:1px solid var(--ink); padding-bottom:7px; margin-bottom:11px`.
  - 6 metric rows, `gap:10`. Line 1: label Figtree 12px 500 `--mid` / value DM Mono 12px 600 `--ink`.
    Line 2: 5px track (`--paper` bed, radius 3) filled to the percentile + 2-digit percentile
    DM Mono 9px in the fill colour, `width:16px; text-align:right`.
  - Fill colour: ≥70 `--pos`, ≥45 `--blue`, else `--neg`.
  - A row whose metric **isn't ranked for that group** shows its value with an empty track and `—`
    (e.g. conversion % for a centre back). No confident-looking bar describing nothing.

**Eligibility and peer set** (`md/WEB_DATA.md` §3.6; not the mock's "900' floor"):
- A player is ranked once `minutes ≥ 25% × club matches × 90`. That's `min_minutes` in `squad.json`
  (855' after 38 matchdays, ~68' after 3).
- The peer set is every La Liga player of the same primary group who clears that gate. RB and LB are
  pooled into `Laterales` only when either side has fewer than 20.
- `peer_n` always ships. Below the gate, the block renders the "todavía pocos minutos" state rather
  than bars.

**The four cards** (outfield; proposed mapping of the mock's 24 metrics onto real columns). Values per
90 unless marked %. **Bold** = an existing gold column; the rest are per-90s the export computes from
gold season totals.

- **Ataque**: **Goles** (`goals_per_90`), **Tiros** (`shots_per_90`), **npxG** (`npxg_per_90`; the
  mock said xG, and penalties would flatter takers), Ocasiones claras (`big_chances`, Q214), Toques en
  el área (`touches_in_box`), **Conversión %** (`shot_conversion_rate`).
- **Creación**: Asistencias (`assists`), **xA** (`xa_per_90`), **Pases clave** (`key_passes_per_90`),
  Pases al área (`passes_into_penalty_area`), **Pases progresivos** (`progressive_passes_per_90`),
  **Pases %** (`pass_completion_rate`).
- **Progresión**: **xT** (`xt_per_90`), Conducciones al último tercio (`carries_into_final_third`),
  **Conducciones progresivas** (`progressive_carries_per_90`), Regates ganados (`take_ons_won`),
  **Regates %** (`take_on_success_rate`), Distancia conducida (`carry_distance_total`, m).
- **Defensa**: **Recuperaciones** (`padj_ball_recoveries_per_90`), **Entradas**
  (`padj_tackles_per_90`), **Intercepciones** (`padj_interceptions_per_90`), Acciones def. en último
  tercio (`def_actions_final_third`; replaces *Pressures*, which Opta doesn't record), **Entradas
  ganadas %** (`tackle_success_rate`; replaces *Duels won %*, since there's no player duel column),
  **Duelos aéreos %** (`aerial_win_rate`).

Defensive volumes use the **possession-adjusted** (`padj_*`) form. Without it, the best defenders in
the league are whoever plays for the side that never has the ball.

**Goalkeepers** get one card, **Portería**: `save_pct`, `goals_prevented`, `gk_pass_completion_rate`,
`gk_long_ball_pct`, `claims_successful` / 90, `keeper_sweeper_actions` / 90. Then two cards,
Distribución and Valor, as the data allows (proposed).

The spine (key, card, per-90 or rate, direction, the groups it's ranked for) lives in
`src/export/config.py` as `PLAYER_SPINE`. For metrics that `gold.percentile_metrics` already ranks,
it uses the same groups and direction; the new ones get their groups there (proposed: add them to
`gold.percentile_metrics` as well, so Postgres and the site agree). Rates render with one decimal and
`%`; per-90 volumes with two decimals (`lib/format.ts`).

### 5 · Sequences involving {surname}
Same model as the page-02 sequence block and the screen-04 lab, scoped to chains the player touched
(`gold.sequence_players`).

- **Section head**: title Outfit 700 19px `Jugadas con {apellido}`; right side a
  `ORDENAR POR` caption + the existing `MetricToggle` (**xT / VAEP**). Changing the sort **re-sorts
  the list and resets the selection to the first row**.
- **Grid**: `minmax(0,1fr) minmax(0,320px); gap:16px`.

**Chain traces card (left)**
- Head: `Trazas` Outfit 700 16px + note `Punto = inicio · dorado = su toque · anillo = final`.
- Horizontal pitch `viewBox="-2 -2 109 72"`, `SequenceTraces`. Per chain: `polyline`
  (`stroke-linejoin:round`), start dot r 0.9, end ring r **1.8 when the chain ends in a shot** else
  0.9 (`fill:none`, stroke 0.4), and, **on the selected chain only**, a `--gold` r 1.5 dot on the
  player's own touch. The export forces that touch into the simplified polyline as a waypoint, so the
  dot always sits on the line.
- Selection styling: selected `--blue`, `stroke-width = 1.1 + value*3.4`, opacity 1; all others
  `--faint`, `stroke-width = 0.35 + value*1.2`, opacity 0.3, where `value` is the chain's xT or VAEP
  per the toggle. `SequenceTraces` needs a width-by-value prop for this.
- **Aggregate strip** below: `repeat(4,1fr); gap:12px; margin-top:14px;
  border-top:1px solid var(--line); padding-top:12px`. Label DM Mono 8.5px `.14em` uppercase `--mid`;
  figure Outfit 700 26px `line-height:1.05` (xT figure in `--blue`, rest `--ink`).
  Figures: **Jugadas · xT en jugadas · Acaban en tiro · Pases por jugada (media)**. They're computed
  over **every** chain involving the player with ≥ 3 events (GOLD_LAYER §4.1.9), not the top 10.

**Sequence list (right, 320px)**
- Head: `Top 10 por {xT|VAEP}` Outfit 700 16px + `clic para trazar` DM Mono 8.5px `--faint`.
- 10 buttons, `flex-direction:column; gap:4px`. Each: `1px solid var(--line)`, radius 11px,
  background `--paper`, `padding:8px 10px`, `text-align:left`, three lines:
  1. `J{matchday} · {minute}'` (DM Mono 9.5px `--mid`) + match code `GIR (L)` / `(V)` right-aligned.
     **Not `SEQ-####`**: `sequence_id`s renumber on every gold rebuild, so they're not citable.
  2. description, Figtree 12px. This is the **same `kind` row title the match page uses**
     (WEB_DATA §7.6), built by the same export function, e.g. "Recuperación alta → tiro".
  3. `{n} pases · {n} toques suyos · +0,000` (xT or VAEP per the toggle), DM Mono 9px `--mid`,
     `gap:10px`.
- **Selected row**: `--blue` background and border, white text, sub-text `rgba(255,255,255,.72)`.
  This page inverts the selected row rather than tinting it like page 02: the list is shorter and
  sits beside a quiet pitch. Hover on unselected rows → `border-color:var(--blue)`.
- Clicking a row selects that chain; the trace highlight follows immediately.

---

## Removed from the earlier first-pass
The match-by-match xT bar chart and the minutes timeline are **out**. There's **one pitch** on this
page, mode-switched by the selector; the shot map lives on as the `Tiros` mode, not as a second
pitch. Don't reintroduce a second pitch above the percentile block. Accordingly, `trend` and
`minutes_timeline` are dropped from the WEB_DATA §9 contract.

## State
A static site: **player and season are the route**, not client state. What remains client-side
lives in the two interactive blocks (each its own small client component; the page stays a server
component).

```ts
// Event map card (client)
vista: 'pases' | 'progresivos' | 'conducciones' | 'toques'
     | 'recepciones' | 'tiros' | 'defensivas' | 'pases-clave';   // default 'pases'
// Sequence block (client)
orden: 'xt' | 'vaep';            // default 'xt'
seleccion: number;               // index into the current top 10, default 0
```

- `vista` is mirrored to the URL as `?vista=…` (read on the client), and the tab-strip links carry
  it. So the mode is **sticky across players** (a scout comparing two players wants the same view)
  and a view is shareable.
- `orden` change → `seleccion → 0`.

| Trigger | Result |
|---|---|
| Player pill click | Navigate to that player's page, same `?vista` |
| Mode pill click | `vista → mode`; marks, title, note and legend swap; URL updated with `replaceState` |
| Rank-by toggle | `orden → metric`; list re-sorts, `seleccion → 0` |
| Sequence row click | `seleccion → index`; trace highlight + row inversion |
| Shot hover (`Tiros`) / diamond hover (`Defensivas`) | Same tooltips as page 02 |
| Row / pill hover | `border-color: var(--blue)` |

No transitions beyond hover colour. Mode switching is instant.

---

## Data contract: `teams/{team}/{season}/players/{player_id}.json`

One file per player in `squad.json`, written by `src/export/player.py` (to be implemented; specified
in `md/WEB_DATA.md` §9, which this replaces for screen 03). **Nothing is aggregated on the client and
no new gold tables are needed.** Every field below reads existing gold or silver.

| Block | Field | Source |
|---|---|---|
| Header, bio | `player` | `silver.players` (names, `known_name`, nationality, `preferred_foot`, `height_cm`, `date_of_birth`) ⋈ `gold.player_season_stats` (shirt, primary position and group, `is_utility`, `primary_position_pct`, matches, starts, minutes) |
| Tab strip | from `squad.json` (WEB_DATA §8) | already specified |
| KPIs, secondary row | `kpis[]`, `stats[]`: `{key, value, per_90?, sub?}` in display order | `gold.player_season_stats`; sets per group from `PLAYER_KPIS` |
| `Pases`, `Progresivos`, `Pases clave` | `maps.passes`: compact rows `[x, y, end_x, end_y, flags]`, flags bits = completed, progressive, key_pass, assist | `silver.events` `Pass` by the player; `gold.is_progressive`; `value_assist` |
| `Conducciones` | `maps.carries`: `[x, y, end_x, end_y, completed]`, progressive only | `silver.events` `Carry` + `gold.is_progressive` |
| `Toques` | `maps.touch_zones`: `[{zone_id, touches, share}]` | touch set grouped by `gold.zone_id` |
| `Recepciones` | `maps.reception_zones` (same shape) + `maps.progressive_receptions`: `[x, y, in_box]` | `gold.pass_receptions` (`receiver_id`, `rx_zone_id`, `rx_x/rx_y`, `is_progressive`, `rx_in_box`, `rx_gap_m ≤ 5`) |
| `Tiros` | `maps.shots`: same `Shot` shape as the match file | `silver.events` shot set, `xg`, `goal_mouth_y/z` |
| `Defensivas` | `maps.defence`: `[{type, x, y, outcome, minute}]` | `silver.events`, sets as in §3b |
| Percentiles | `percentiles`: `{eligible, peer_group, peer_n, pooled_fb, min_minutes, cards: [{key, metrics: [{key, value, percentile \| null, higher_is_better}]}]}` | `gold.player_season_stats` + `PLAYER_SPINE`, computed by the export (§3.6 gate) |
| Sequences | `sequences.aggregates` + `sequences.top_xt[10]`, `top_vaep[10]` (deduplicated by match + start): `{match_id, matchday, opponent_abbr, is_home, minute, kind, passes, player_touches, xt, vaep, ends_in_shot, points, player_touch_index}` | `gold.sequences` ⋈ `gold.sequence_players`, `event_count ≥ 3`; `points` built as in WEB_DATA §7.1, with the player's first touch forced in |

**Frame**: every coordinate is in metres, rounded to 0.1, in the player's team's attacking frame
(WEB_DATA §3.1). **Determinism**: every list is sorted on a unique key.

**Size and exposure** (`md/WEB_PLAN.md` §6, level B). A regular midfielder makes ~1,500–2,000
passes a season. Shipping all of them with coordinates is the per-player version of the
"every pass" file §6 rules out. Proposed caps, constants in `config.py`:
- `Pases`: the **150** most recent passes (`PLAYER_PASS_MAP_MAX`).
- `Progresivos` / `Conducciones`: top **60** by xT.
- `Pases clave`, `Tiros`, `Defensivas`: all (dozens, not thousands).
- Heat modes are zone counts, which is level A.

The note states the cap (`150 DE 1.284`). Estimated file: 25–45 KB raw per player, ~10 KB gzipped.

---

## Reuse and new components
Built from the existing set (`espanyol-viz-design` §2). New pieces:
- **`ActionArrows`**: the generalisation of `ProgressionArrows`, with per-arrow
  `style: 'primary' | 'secondary' | 'muted' | 'gold'`, `completed`, and `head: boolean`. Pass map, key
  passes, progressive passes and carries all use it, and `ProgressionArrows` becomes a thin wrapper.
  This is the third caller, so extract (WEB_PLAN §3.2).
- **A dot layer** for progressive receptions (filled dot, optional ring).
- **`SequenceTraces`**: add width-by-value and the gold touch dot.
- **`TeamProfile`**: accept the player metric source, the "not ranked" row and the not-eligible
  state.

Every new layer gets a fixture in `lib/viz/fixtures.ts` and a `/demo` card before it reaches the
page. Workflow: the `new-viz` skill.

## Responsive
Desktop-first, same rules as the rest of the app. Below ~1100px:
- the profile/map pair stacks (card first, pitch second)
- the percentile grid collapses `4 → 2 → 1`
- the sequence list moves below the traces pitch at full width
- the tab strip and mode pills wrap

## What changed from the mock

| Mock said | Now | Why |
|---|---|---|
| Progressive = "≥10 m upfield, ends in opp. half" | 25% closer to goal (`gold.is_progressive`) | The repo's rule for every progressive count on the site; the x-only rule credits sideways balls near the touchline |
| Carries "≥5 m under control" | Progressive carries (carries ≥ 1 m exist) | No 5 m rule in the data; all carries are too many short arrows |
| Touch heat on a 12×8 grid | 30 zones | 12×8 is the xT grid; the 30 zones are the site's one spatial grid (screen 02 made the same change) |
| Six modes | Eight: + Recepciones, + Tiros | The reception heatmap and shot map requested for this page, kept on the one pitch |
| Defensive dots coloured by type | Diamonds coloured by outcome, chips by type | Same component and grammar as page 02 (proposed, Open decisions #3) |
| Percentiles vs Forwards / Midfielders / Defenders, 900' floor | Seven formation-slot groups, 25%-of-club-minutes gate, `peer_n` shown | The project's peer groups and the site's gate (WEB_DATA §3.6) |
| *Pressures*, *Duels won %* | Def. actions in final third, Tackles won % | Not in Opta / no player duel column |
| Defensive volumes raw | Possession-adjusted | GOLD_LAYER §4.6.14 |
| Bio: Weight, Contract, photo URL | Partidos, Titularidades; hand-supplied photo or numeral fallback | Not in the data |
| KPIs Goals, Assists, xG, xT/90 for everyone | Set per position group | Four zeros for a centre back otherwise |
| `SEQ-####` ids | `J{md} · {min}'` | Ids renumber on every rebuild |
| Player state, re-query on tab click | Route per player; `?vista` sticky | Static export |
| Hypothetical gold tables per block | One export file, `players/{id}.json` | The site's architecture (WEB_PLAN §2) |

## Open decisions (defaults above, to confirm)
1. **Route** `/{team}/jugador/{season}/{player_id}`: adds the season to `routes.player`.
2. **KPI sets per position group** (table in §3a).
3. **Defensive mode**: outcome colour + type chips (page-02 grammar), versus the mock's colour-by-type
   dots.
4. **Percentile spine**: the 24-metric mapping in §4, "—" on metrics not ranked for the group, and
   whether the new spine metrics also go into `gold.percentile_metrics`.
5. **Exposure caps**: 150 passes, 60 progressive passes/carries.
6. **Photos**: hand-supplied, numeral fallback, or no photo block at all.

## Files
- `Espanyol Analytics.dc.html`: screen 03 lives under `data-screen-label="03 Player profile"`
  (mock data; layout reference only).
- There are no `screenshots/10-player-*.png` in this bundle (only `01–09`, screens 01–02). Use the
  `.dc.html` for proportions.
- The prototype runtime (`support.js`, `image-slot.js`) is design-time only; don't port it.
