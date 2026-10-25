# Handoff: Screen 03 · Player profile

> **Look superseded (24 Oct 2026).** Screen 03 is built in the "Pizarra" world
> (`DESIGN.md` at the repo root): the tokens, type, chips, gold and hover cards
> below no longer apply. What still holds is the **data contract**, the
> percentile spine, the modes and the decisions and reviews recorded here; the
> site follows them with Pizarra's interface (`md/CHANGELOG.md`, 24 Oct 2026).

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
   border-bottom:1px solid var(--line)`. 12 items (two columns of six), each a baseline-aligned
   `space-between` pair, `padding:3px 0`: key DM Mono 8.5px `.12em` uppercase `--faint`; value
   Figtree 12px 600 `--ink`. Keys, in order: **Edad, Nacimiento, Altura, Pie, Nacionalidad, Posición,
   Partidos, Titularidades, Minutos, Sustituido, Tarjetas, Penaltis** (decided 16 Oct 2026: everything
   about how much and how he has played sits here, as plain counts, never as a percentile bar).
   *Tarjetas* is `yellow_cards · red_cards` (`5 · 1`); *Penaltis* is `penalties_scored/penalties_taken`
   (`3/4`), `—` when he has taken none; *Sustituido* is `matches_subbed_off`. *Weight* and *Contract*
   from the mock are out because neither exists in the data.
   `height_cm` and `date_of_birth` are hand-filled in `silver.players` and may be NULL; render `—`.
   *Pie* is `preferred_foot`, derived from event foot qualifiers (`izquierdo / derecho / ambos`).
   *Posición* is the **primary position** from the formation slot (`CB`, `RB`, `DM`… mapped to Spanish
   in `labels.ts`), not `position_raw`.
3. **Four KPIs**: `grid-template-columns:1fr 1fr`, each cell `padding:14px 16px`,
   `border:1px solid var(--line); margin:-0.5px` (collapsed hairline grid). Label DM Mono 9px `.14em`
   uppercase `--mid`; value Outfit 700 **42px** `line-height:.86` (`--blue` for the first, headline
   KPI, `--ink` otherwise); sub DM Mono 9px `--faint`, `margin-top:3px` (per-90 or rank-in-squad line).
   **The set depends on the role** (decided 16 Oct 2026), so a centre back's card isn't four zeros.
   VAEP rather than xT is the model value here, as on the percentile cards (it covers every on-ball
   action and charges losing the ball):

   | Role | KPI 1 (headline) | KPI 2 | KPI 3 | KPI 4 |
   |---|---|---|---|---|
   | ST, W | Goles `goals` | Asistencias `assists` | npxG `npxg` | VAEP /90 `vaep_per_90` |
   | AM | Goles + asistencias `goal_contributions` | xA `xa` | Pases clave `key_passes` | VAEP /90 `vaep_per_90` |
   | MID | VAEP /90 `vaep_per_90` | Pases progresivos `progressive_passes` | Recuperaciones `ball_recoveries` | Pases % `pass_completion_rate` |
   | CB, RB, LB | VAEP /90 `vaep_per_90` | Duelos en el suelo % `ground_duel_win_rate` | Duelos aéreos % `aerial_win_rate` | Pases progresivos `progressive_passes` |
   | GK | Goles evitados `goals_prevented` | Paradas % `save_pct` | Porterías a cero `clean_sheets` | Pases % `gk_pass_completion_rate` |

   Season totals unless marked /90 or %; the sub line under a total is its per-90. The sets live in
   `src/export/config.py` (`PLAYER_KPIS` by role), not in the component.
4. ~~**Secondary stat row**~~ — **dropped (16 Oct 2026)**. Its six items (shots, key passes, take-ons
   won, recoveries, pass %, minutes) are all on the percentile cards now, with a league rank, or in
   the bio grid (minutes). The card ends at the KPI grid.

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
  `-.015em`: `Percentil vs LaLiga · {grupo}`. The group is the player's **role**
  (`role_group`: the formation-slot group, with midfielders who take ≥ 30% of their touches in the
  final third split out as mediapuntas, GOLD_LAYER §4.6.2a), one of `Porteros, Centrales, Laterales
  derechos, Laterales izquierdos (or Laterales when pooled), Centrocampistas, Mediapuntas (or
  Centrocampistas when pooled), Extremos, Delanteros`. The heading names the **peer group**, so a
  pooled mediapunta reads "vs centrocampistas". Caption DM Mono 9px `.12em` uppercase `--faint`:
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

**The cards, per role** (decided 15 Oct 2026, from the user's per-position sheet, reviewed metric by
metric). Volumes are per 90; defensive volumes are **possession-adjusted** (`padj_*`: without it, the
best defenders are whoever plays for the side that never has the ball). ↓ = lower is better (the
percentile is already flipped). ○ = value shown, **no bar** (`percentile: null`). Every column is on
`gold.player_season_stats`; the Spanish label goes in `labels.ts`.

*Duelos en el suelo* are the defender's ground contests: won = Entrada (`Tackle`, either outcome), lost
= Entrada fallida (`Attempted Tackle`) + Regateado (`Challenge`). Opta records no 50/50. The won count
is `padj_tackles`, so "Entradas" is not shown separately.

**Porteros (GK)**

| Portería | Área y salidas | Distribución | Valor |
|---|---|---|---|
| Paradas % `save_pct` | Blocajes /90 `claims_per_90` | Pases /90 ○ `passes_per_90` | VAEP /90 `vaep_per_90` |
| Goles evitados /90 `goals_prevented_per_90` | Blocajes % `claim_rate` | Pases % `gk_pass_completion_rate` | VAEP defensivo /90 `vaep_defensive_per_90` |
| Paradas /90 `saves_per_90` | Despejes de puños /90 `punches_per_90` | Pases largos (≥30 m) % `long_pass_completion_rate` | VAEP ofensivo /90 `vaep_offensive_per_90` |
| Goles encajados /90 ↓ `goals_conceded_per_90` | Salidas fuera del área /90 `keeper_sweeper_actions_per_90` | Pases en largo, cuota ○ `gk_long_ball_pct` | Errores /90 ↓ `errors_per_90` |
| Porterías a cero % `clean_sheet_rate` | Altura media de salida (m) `sweeper_avg_x` | Pases progresivos /90 `progressive_passes_per_90` | Penaltis parados ○ `penalties_saved` / `penalties_faced` |
| xG en contra /90 ○ `xg_faced_per_90` | Despejes /90 `padj_clearances_per_90` | Pases recibidos /90 ○ `passes_received_per_90` | |

**Centrales (CB)**

| Defensa | Duelos | Pase | Impacto |
|---|---|---|---|
| Intercepciones `padj_interceptions_per_90` | Duelos en el suelo ganados `padj_tackles_per_90` | Pases /90 `passes_per_90` | VAEP /90 `vaep_per_90` |
| Despejes `padj_clearances_per_90` | Duelos en el suelo % `ground_duel_win_rate` | Pases % `pass_completion_rate` | VAEP defensivo /90 `vaep_defensive_per_90` |
| Tiros bloqueados /90 `shots_blocked_per_90` | Regateado /90 ↓ `dribbled_past_per_90` | Balones largos /90 `long_balls_per_90` | VAEP ofensivo /90 `vaep_offensive_per_90` |
| Pases bloqueados `padj_blocked_passes_per_90` | Duelos aéreos ganados /90 `aerials_won_per_90` | Balones largos % `long_ball_accuracy` | Conducciones progresivas /90 `progressive_carries_per_90` |
| Recuperaciones `padj_ball_recoveries_per_90` | Duelos aéreos % `aerial_win_rate` | Pases progresivos /90 `progressive_passes_per_90` | Remates de cabeza /90 `headed_shots_per_90` |
| Errores /90 ↓ `errors_per_90` | Faltas cometidas /90 ↓ `fouls_committed_per_90` | Pases al último tercio /90 `passes_into_final_third_per_90` | Aéreos ganados en campo rival /90 `aerials_won_att_third_per_90` |

**Laterales (RB, LB)**

| Defensa | Duelos | Progresión | Creación |
|---|---|---|---|
| Intercepciones `padj_interceptions_per_90` | Duelos en el suelo ganados `padj_tackles_per_90` | Pases % `pass_completion_rate` | Centros /90 `crosses_per_90` |
| Recuperaciones `padj_ball_recoveries_per_90` | Duelos en el suelo % `ground_duel_win_rate` | Pases progresivos /90 `progressive_passes_per_90` | Centros % `cross_completion_rate` |
| Despejes `padj_clearances_per_90` | Regateado /90 ↓ `dribbled_past_per_90` | Conducciones progresivas /90 `progressive_carries_per_90` | Pases al área /90 `passes_into_penalty_area_per_90` |
| Tiros bloqueados /90 `shots_blocked_per_90` | Duelos aéreos ganados /90 `aerials_won_per_90` | Cond. prog. al último tercio /90 `progressive_carries_into_final_third_per_90` | xA /90 `xa_per_90` |
| Acciones def. último tercio /90 `def_actions_final_third_per_90` | Duelos aéreos % `aerial_win_rate` | Recepciones último tercio /90 `receptions_final_third_per_90` | Acciones de creación de tiro /90 `shot_creating_actions_per_90` |
| VAEP defensivo /90 `vaep_defensive_per_90` | Faltas cometidas /90 ↓ `fouls_committed_per_90` | Regates % `take_on_success_rate` | VAEP /90 `vaep_per_90` |

**Centrocampistas (MID)**

| Defensa | Duelos | Pase y progresión | Creación y llegada |
|---|---|---|---|
| Intercepciones `padj_interceptions_per_90` | Duelos en el suelo ganados `padj_tackles_per_90` | Pases /90 `passes_per_90` | Pases clave /90 `key_passes_per_90` |
| Recuperaciones `padj_ball_recoveries_per_90` | Duelos en el suelo % `ground_duel_win_rate` | Pases % `pass_completion_rate` | xA /90 `xa_per_90` |
| Pases bloqueados `padj_blocked_passes_per_90` | Regateado /90 ↓ `dribbled_past_per_90` | Pases progresivos /90 `progressive_passes_per_90` | Pases filtrados /90 `through_balls_per_90` |
| Acciones def. último tercio /90 `def_actions_final_third_per_90` | Duelos aéreos ganados /90 `aerials_won_per_90` | Pases al último tercio /90 `passes_into_final_third_per_90` | Acciones de creación de tiro /90 `shot_creating_actions_per_90` |
| VAEP defensivo /90 `vaep_defensive_per_90` | Duelos aéreos % `aerial_win_rate` | Conducciones progresivas /90 `progressive_carries_per_90` | npxG /90 `npxg_per_90` |
| Errores /90 ↓ `errors_per_90` | Faltas cometidas /90 ↓ `fouls_committed_per_90` | xT /90 `xt_per_90` | VAEP /90 `vaep_per_90` |

**Mediapuntas (AM role)**

| Presión | Pase | Conducción y regate | Creación y gol |
|---|---|---|---|
| Recuperaciones `padj_ball_recoveries_per_90` | Pases /90 `passes_per_90` | Cond. prog. al último tercio /90 `progressive_carries_into_final_third_per_90` | Pases clave /90 `key_passes_per_90` |
| Acciones def. último tercio /90 `def_actions_final_third_per_90` | Pases % `pass_completion_rate` | Regates /90 `take_ons_per_90` | xA /90 `xa_per_90` |
| Duelos en el suelo ganados `padj_tackles_per_90` | Pases progresivos /90 `progressive_passes_per_90` | Regates % `take_on_success_rate` | Acciones de creación de tiro /90 `shot_creating_actions_per_90` |
| Duelos en el suelo % `ground_duel_win_rate` | Pases al último tercio /90 `passes_into_final_third_per_90` | Recepciones último tercio /90 `receptions_final_third_per_90` | npxG /90 `npxg_per_90` |
| Intercepciones `padj_interceptions_per_90` | Pases al área /90 `passes_into_penalty_area_per_90` | xT /90 `xt_per_90` | Goles sin penalti /90 `goals_non_penalty_per_90` |
| VAEP defensivo /90 `vaep_defensive_per_90` | Pases filtrados /90 `through_balls_per_90` | Pérdidas /90 ↓ `dispossessed_per_90` | VAEP /90 `vaep_per_90` |

**Extremos (W)**

| Presión | Regate y conducción | Creación | Finalización |
|---|---|---|---|
| Recuperaciones `padj_ball_recoveries_per_90` | Regates /90 `take_ons_per_90` | Centros /90 `crosses_per_90` | npxG /90 `npxg_per_90` |
| Acciones def. último tercio /90 `def_actions_final_third_per_90` | Regates % `take_on_success_rate` | Centros % `cross_completion_rate` | Goles sin penalti /90 `goals_non_penalty_per_90` |
| Duelos en el suelo ganados `padj_tackles_per_90` | Cond. prog. al último tercio /90 `progressive_carries_into_final_third_per_90` | Pases al área /90 `passes_into_penalty_area_per_90` | Tiros /90 `shots_per_90` |
| Duelos en el suelo % `ground_duel_win_rate` | Conducciones al área /90 `carries_into_penalty_area_per_90` | Pases clave /90 `key_passes_per_90` | Toques en el área /90 `touches_in_box_per_90` |
| Intercepciones `padj_interceptions_per_90` | Recepciones progresivas /90 `progressive_receptions_per_90` | xA /90 `xa_per_90` | xG por tiro (sin penaltis) `npxg_per_shot` |
| VAEP defensivo /90 `vaep_defensive_per_90` | Pérdidas /90 ↓ `dispossessed_per_90` | Acciones de creación de tiro /90 `shot_creating_actions_per_90` | VAEP /90 `vaep_per_90` |

**Delanteros (ST)**

| Presión y duelos | Juego asociativo | Desmarque y regate | Finalización |
|---|---|---|---|
| Recuperaciones `padj_ball_recoveries_per_90` | Pases % `pass_completion_rate` | Recepciones en el área /90 `receptions_in_box_per_90` | npxG /90 `npxg_per_90` |
| Acciones def. último tercio /90 `def_actions_final_third_per_90` | Pases recibidos /90 `passes_received_per_90` | Recepciones progresivas /90 `progressive_receptions_per_90` | Goles sin penalti /90 `goals_non_penalty_per_90` |
| Duelos aéreos ganados /90 `aerials_won_per_90` | Pases clave /90 `key_passes_per_90` | Toques en el área /90 `touches_in_box_per_90` | Tiros /90 `shots_per_90` |
| Duelos aéreos % `aerial_win_rate` | xA /90 `xa_per_90` | Regates % `take_on_success_rate` | xG por tiro (sin penaltis) `npxg_per_shot` |
| Aéreos ganados en campo rival /90 `aerials_won_att_third_per_90` | Acciones de creación de tiro /90 `shot_creating_actions_per_90` | Conducciones progresivas /90 `progressive_carries_per_90` | Goles − xG (sin penaltis) /90 `xg_overperformance_per_90` |
| Faltas recibidas /90 `fouls_won_per_90` | VAEP /90 `vaep_per_90` | Pérdidas /90 ↓ `dispossessed_per_90` | Remates de cabeza /90 `headed_shots_per_90` |

**Not on any card, on purpose**: 50/50 duels (no such event), red cards and penalties taken (mostly
zeros, so the bar is noise: they go in the profile card as counts), `chances_created` (= assists + key
passes, both shown), shots on target and first-time shots, and xG (replaced by npxG). Keepers' short
and medium pass % were dropped because every keeper completes ~98% of passes under 30 m.

**Attempt floors.** A rate gets a bar only once its denominator reaches 10 (20 for pass %, 5 full
matches for clean-sheet %); below that it shows its value with "—". The floors live on
`gold.percentile_metrics` (`attempts_column`, `min_attempts`).

The layout lives in `src/export/config.py` as `PLAYER_SPINE` (role → cards → metrics, with display
scale and decimals). **What is ranked** comes from `gold.percentile_metrics`, whose seed is exactly
the ranked rows above, so Postgres and the site agree; the ranking itself is
`src/export/player_percentiles.py`. Rates render with one decimal and `%`; per-90 volumes with two
decimals, VAEP and xT with three (`lib/format.ts`).

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
| Header, bio | `player` | `silver.players` (names, `known_name`, nationality, `preferred_foot`, `height_cm`, `date_of_birth`) ⋈ `gold.player_season_stats` (shirt, primary position and group, `role_group`, `is_utility`, `primary_position_pct`, matches, starts, minutes, `matches_subbed_off`, `yellow_cards`, `red_cards`, `penalties_taken`, `penalties_scored`) |
| Tab strip | from `squad.json` (WEB_DATA §8) | already specified |
| KPIs | `kpis[]`: `{key, value, per_90?, sub?}` in display order | `gold.player_season_stats`; sets per role from `PLAYER_KPIS` |
| `Pases`, `Progresivos`, `Pases clave` | `maps.passes`: compact rows `[x, y, end_x, end_y, flags, m, xt, minute, s]`, flags bits = completed, progressive, key_pass, assist. Every map row ends on `minute, s` (see below) | `silver.events` `Pass` by the player; `gold.is_progressive`; `value_assist` |
| `Conducciones` | `maps.carries`: `[x, y, end_x, end_y, completed, m, xt, minute, s]`, progressive only | `silver.events` `Carry` + `gold.is_progressive` |
| `Toques` | `maps.touch_zones`: `[m, zone_id, count]` (heat) + `maps.touches`: `[x, y, t, m, minute, s]` (events), `t` indexing `maps.touch_types` | touch set grouped by `gold.zone_id`; the same set one by one |
| `Recepciones` | `maps.reception_zones` (same shape) + `maps.receptions`: every reception `[x, y, flags, m, minute, s]`, flags 1 = progressive (pass progressive and `rx_gap_m ≤ 5`), 2 = in the box. Replaced `progressive_receptions` on 17 Oct 2026 (`schema_version` 2) | `gold.pass_receptions` (`receiver_id`, `rx_zone_id`, `rx_x/rx_y`, `is_progressive`, `rx_in_box`, `rx_gap_m ≤ 5`) |
| `Tiros` | `maps.shots`: same `Shot` shape as the match file, plus `s` | `silver.events` shot set, `xg`, `goal_mouth_y/z` |
| `Defensivas` | `maps.defence`: `[type, x, y, won, minute, m, s]` | `silver.events`, sets as in §3b |
| Percentiles | `percentiles`: `{eligible, peer_group, peer_n, pooled_fb, min_minutes, cards: [{key, metrics: [{key, value, percentile \| null, higher_is_better}]}]}` | `gold.player_season_stats` + `PLAYER_SPINE`, computed by the export (§3.6 gate) |
| Sequences | `sequences.aggregates` + `sequences.top_xt[10]`, `top_vaep[10]` (deduplicated by match + start): `{match_id, matchday, opponent_abbr, is_home, minute, kind, passes, player_touches, xt, vaep, ends_in_shot, points, player_touch_index}` | `gold.sequences` ⋈ `gold.sequence_players`, `event_count ≥ 3`; `points` built as in WEB_DATA §7.1, with the player's first touch forced in |

**Chain link `s`** (17 Oct 2026): the row's index into `sequences.index`, the chain the event belongs to; `null` when it is in no chain the page can draw (an opponent's possession, or one under three events). Share of 2025/26 rows linked: carries 100 %, receptions 97 %, passes 88 %, touches 87 %, shots 67 % (penalties and direct free kicks are one- and two-event chains), defensive actions 45 %.

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
| Percentiles vs Forwards / Midfielders / Defenders, 900' floor | Eight roles (seven formation-slot groups + mediapuntas by final-third share), 25%-of-club-minutes gate, `peer_n` shown | The project's peer groups and the site's gate (WEB_DATA §3.6, GOLD_LAYER §4.6.2a) |
| *Pressures*, *Duels won %* | Def. actions in final third, Duelos en el suelo % (Tackle vs Attempted Tackle + Challenge) | Not in Opta; no 50/50 event |
| One 24-metric spine for every outfielder | Four cards × six per role (§4) | Decided with the user 15 Oct 2026 |
| Defensive volumes raw | Possession-adjusted | GOLD_LAYER §4.6.14 |
| Bio: Weight, Contract, photo URL | Partidos, Titularidades; hand-supplied photo or numeral fallback | Not in the data |
| KPIs Goals, Assists, xG, xT/90 for everyone | Set per role, VAEP as the model value | Four zeros for a centre back otherwise |
| Secondary stat row | Dropped; minutes, subbed, cards, penalties in the bio grid | Everything else in it is on the percentile cards |
| `SEQ-####` ids | `J{md} · {min}'` | Ids renumber on every rebuild |
| Player state, re-query on tab click | Route per player; `?vista` sticky | Static export |
| Hypothetical gold tables per block | One export file, `players/{id}.json` | The site's architecture (WEB_PLAN §2) |

## Open decisions (defaults above, to confirm)
1. **Route** `/{team}/jugador/{season}/{player_id}`: adds the season to `routes.player`.
2. ~~**KPI sets per position group**~~ — **decided 16 Oct 2026**: per role with VAEP (§3a); the
   secondary stat row dropped and the bio grid extended to 12 items.
3. **Defensive mode**: outcome colour + type chips (page-02 grammar), versus the mock's colour-by-type
   dots.
4. ~~**Percentile spine**~~ — **decided 15 Oct 2026**: per-role cards in §4, implemented in gold
   (`gold.percentile_metrics` is the ranked spine) and in the export (`PLAYER_SPINE`).
5. **Exposure caps**: 150 passes, 60 progressive passes/carries.
6. **Photos**: hand-supplied, numeral fallback, or no photo block at all.

## Files
- `Espanyol Analytics.dc.html`: screen 03 lives under `data-screen-label="03 Player profile"`
  (mock data; layout reference only).
- There are no `screenshots/10-player-*.png` in this bundle (only `01–09`, screens 01–02). Use the
  `.dc.html` for proportions.
- The prototype runtime (`support.js`, `image-slot.js`) is design-time only; don't port it.

## Built (16 Oct 2026)
Screen 03 is implemented: `src/export/squad.py` + `player.py`, route
`web/app/(handoff)/[equipo]/jugador/[season]/[player_id]`, components in `web/components/player/`,
fixture page `/demo/jugador`. The open decisions above were built with their **defaults**
(route with season; outcome-coloured diamonds + type chips; caps 150 / 60; numeral fallback
when no photo is in `web/public/players/{player_id}.jpg`), so each is still a one-line change.
Departures from this brief, made while building it:
- ~~`maps.passes` ships as three lists~~ — superseded 16 Oct 2026 (below): one uncapped list,
  caps applied on the site after the match filter.
- `Tiros` and `Defensivas` mount `ShotMap` / `DefensiveActions` (their own SVG, for the hover
  card and chips); the other six modes share one `<Pitch>`. Shot radii follow `ShotMap`'s law,
  so a shot is the same size here as on its match page.
- Aerials are not on the `Defensivas` map (they are on the duel cards); `Attempted Tackle` is.
- Percentile fill thresholds are the page-01 ones (≥70 `--pos`, ≥45 `--gold`, else `--neg`),
  the colour `TeamProfile` already ships, rather than `--blue` for the middle band.
- Until the export has run its `squad` step, the route builds one placeholder page
  (a static export refuses a dynamic route with no params).

## Review of 16 Oct 2026
Four changes after the first look at real data:
- **Plantilla tab** is live: `/{team}/plantilla/{season}` renders screen 03 for the first squad
  player (GK first). Player pages mark Plantilla as the active tab. The sortable squad table
  (WEB_PLAN 5b) is still not built.
- **Match filter** (`MatchFilter`, `?partidos=`): season, last 5, home, away, or any ticked set.
  It narrows the profile card, the event map and the sequences; **the percentile cards stay
  season-wide**. The player file is per match underneath (`matches[]`, every map row tagged
  `m`, uncapped; `sequences.index`). KPIs are rebuilt from `kpi_parts` by `KPI_FORMULAS`; the
  export fails if the per-match rebuild does not equal gold's season value. A filtered rate
  shows "en estos N partidos" instead of the squad rank (a season fact).
- **Player picker** replaces the pill strip: a dropdown grouped by role, with a shirt icon
  carrying the number. Its links carry `?vista=` and `?partidos=`.
- **Visuals follow the match page**: arrow weights from `ProgressionArrows` (pass 0.28/.55,
  carry 0.42/.9), mode chips from `DefensiveActions`, `SectionHead` + noun legends. The
  sequence block is the match page's `SequenceBrowser` (one play, action by action) with the
  player's nodes in gold, replacing the trace overlay. Chains outside the season top lists
  are drawn from the match files, fetched on demand: a deliberate exception to "no fetching",
  since shipping ~500 chains with actions per player would add ~1 MB per file. Player and
  match files must come from the same export run (they share `sequence_id`).

## Review of 17 Oct 2026
Three requests on the event map, built on the 16 Oct export:
- **Map → sequence link.** A click on a mark with a chain (`s` non-null) pins that chain in the
  sequence block and scrolls to it. It opens in place when already in the top ten, otherwise
  first; a gold-bordered bar names its match and offers "Volver a las 10 mejores". The pin
  belongs to the match selection it was made under and is dropped when the selection changes.
  Its match file is fetched like any filtered chain's.
- **Heat or events.** Toques, Recepciones and Defensivas get a "VER · Mapa de calor / Eventos"
  switch (not in the URL). Toques and Recepciones open as heat, Defensivas as marks; the
  defensive heat map is binned on the site from the chip-filtered actions with `zoneId`
  (`lib/viz/pitch.ts`, `gold.zone_id` word for word). Events display is uncapped.
- **Hover on every mark.** A card names the match, matchday, venue and minute, plus the mark's
  own facts (completed / failed and xT, xG, in the box), and "Clic: ver la jugada ↓" when it
  links. Heat zones answer with their count and share. Arrows and dots use one delegated
  listener (`data-i` / `data-zone`); the card is `viz/HoverCard`. `ShotMap` and
  `DefensiveActions` take optional `context` / `linked` / `onSelect`, so the match page is
  unchanged — except that the defensive tooltip now sits in a wrapper around the pitch alone,
  which fixes its offset by the chip row's height.
- Size: 2025/26 player files 2.6 MB → 3.5 MB (47 files).
