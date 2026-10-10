# Handoff: Screen 03 · Player profile

Companion to `README.md` (RCD Espanyol Analytics Platform). All design tokens, pitch geometry, the
Opta→pitch conversion, the HTML-over-SVG axis rule and the general implementation notes in that
document apply here unchanged — this file only specifies **screen 03**, which is now at the same
fidelity as screens 01 and 02 and supersedes the "first-pass" note for page 03.

## Purpose
Read one player's season: who he is, where he touches the ball, how he ranks inside his position
group, and which possession chains he is part of. Entered from the page-01 player leader rows
(`page → 'player'`, `player → name`) or from the nav.

---

## Page structure (top to bottom)

```
┌─ Page header ─────────────────────────────────────────────────────────────┐
├─ Player tab strip (6 players) ────────────────────────────────────────────┤
├─ Profile card (340px)        │ Event map card (selector + pitch)          │
├─ Percentile vs La Liga <group> — 4 cards ─────────────────────────────────┤
├─ Sequences involving <player>  ·  rank-by toggle ─────────────────────────┤
└─ Chain traces + aggregates    │ Top-10 sequence list (320px)              ┘
```

### 1 · Page header
`display:flex; align-items:flex-end; justify-content:space-between; gap:24px;`
`border-bottom:1px solid var(--ink); padding-bottom:12px; margin-bottom:22px`.

- **Left**: eyebrow `03 · PLAYER PROFILE` (DM Mono 9.5px, `.22em`, `--blue`, `margin-bottom:6px`),
  then a `flex, align-items:flex-end, gap:16px` row:
  - **Shirt block** — `border:2px solid var(--ink)`, radius 11px, `padding:6px 12px 4px`, background
    `--card`, centred. Number Outfit 700 34px `line-height:.86`; caption `SHIRT` DM Mono 7px `.16em`
    `--faint`.
  - **Name block** — first name DM Mono 10px `.22em` uppercase `--blue`; surname Outfit 700 **52px**
    `line-height:.95; letter-spacing:-.04em`.
- **Right**: mono 10px `.12em` uppercase `--mid`, `line-height:1.7`, two lines — position · age ·
  minutes meta, then `La Liga {season} · through {MD}`.

### 2 · Player tab strip
`flex, gap:6px, wrap, margin-bottom:16px`. One pill per squad player (6 in the mock): DM Mono 10px
`.1em` uppercase, `padding:6px 12px`, radius 999px, `1px solid var(--line)`. Selected = `--blue`
fill + white text; others `--paper` on `--mid`. Hover → `border-color:var(--blue)`.
Clicking sets `player`; **every block on the page re-queries for that player**.

### 3 · Profile card + event map
`grid-template-columns: minmax(0,340px) minmax(0,1fr); gap:16px; align-items:stretch;
margin-bottom:16px`. The left card uses `height:100%` and its photo block `flex:1`, so **the photo
grows to match the pitch card's height** — the two columns always bottom-align. Do not fix the photo
height; let it flex.

#### 3a · Profile card (left, 340px)
`background:var(--card); border:1px solid var(--line); border-radius:14px; padding:2px;
display:flex; flex-direction:column`. Four stacked blocks:

1. **Photo** — `position:relative; flex:1; min-height:206px; margin:-2px -2px 0;
   border-radius:14px 14px 0 0; overflow:hidden; background:#EFE3D4`. Image `object-fit:cover`,
   fills the block. Overlays (both `pointer-events:none`):
   - name plate bottom-left (`left:14px; bottom:11px`): inline-block, `background:rgba(23,24,28,.82)`,
     radius 9px, `padding:5px 10px 6px`; first name DM Mono 9px `.18em` `rgba(255,255,255,.8)`,
     surname Outfit 700 24px `line-height:1.05; letter-spacing:-.03em`, white.
   - shirt chip top-right (`right:12px; top:12px`): `--ink` surface, radius 9px, `padding:4px 9px 3px`,
     number Outfit 700 20px white.
   The plate/chip surfaces exist so the type passes contrast over **any** supplied photo — keep them.
2. **Bio grid** — `grid-template-columns:1fr 1fr; gap:0 14px; padding:12px 16px;
   border-bottom:1px solid var(--line)`. 8 rows, each a baseline-aligned `space-between` pair,
   `padding:3px 0`: key DM Mono 8.5px `.12em` uppercase `--faint`; value Figtree 12px 600 `--ink`.
   Keys, in order: **Age, Born, Height, Weight, Foot, Nationality, Position, Contract**.
3. **Four KPIs** — `grid-template-columns:1fr 1fr`, each cell `padding:14px 16px`,
   `border:1px solid var(--line); margin:-0.5px` (collapsed hairline grid). Label DM Mono 9px `.14em`
   uppercase `--mid`; value Outfit 700 **42px** `line-height:.86` (colour `--blue` for the headline
   metric, `--ink` otherwise); sub DM Mono 9px `--faint`, `margin-top:3px`.
   Mock set: **Goals, Assists, xG, xT per 90** — position-dependent; drive from the gold table, not
   hardcoded.
4. **Secondary stat row** — `flex; wrap; gap:14px; padding:14px 16px`. Each item is value
   (Outfit 700 19px) + key (DM Mono 8.5px `.1em` uppercase `--mid`), baseline-aligned, `gap:5px`.
   6 items (shots, key passes, take-ons, recoveries, pass %, minutes).

#### 3b · Event map card (right)
`background:var(--card); border:1px solid var(--line); radius 14px; padding:16px`.

- **Header** — `flex; align-items:baseline; space-between; margin-bottom:8px`. Title Outfit 700 17px
  `-.015em` (**the title changes with the selected mode**); right-hand legend/note DM Mono 8.5px
  `.1em` uppercase `--faint`.
- **Mode selector** — `flex; wrap; gap:5px; margin-bottom:10px`. Pills: DM Mono 9.5px `.08em`
  uppercase, `padding:5px 11px`, radius 999px, `1px solid var(--line)`; active `--blue` + white,
  inactive `--paper` + `--mid`; hover `border-color:var(--blue)`.
- **Pitch** — horizontal full pitch, `viewBox="-2 -2 109 72"`, attack right, `ATTACK →` static SVG
  text at `x=104 y=4`, DM Mono 3.2px, `--faint`. Draw order matters: **heat cells → pitch lines →
  arrows → dots**, so pitch markings read through the heat.

**Six modes** (`pViz` state). Each defines the title, the note, and the marks:

| Mode | Title | Marks | Note / legend |
|---|---|---|---|
| `Passes` | Pass map | ~104 arrows. Progressive: `--blue`, width 0.6, opacity 0.9. Other: `--faint`, width 0.3, opacity 0.38 | Blue = progressive · grey = other |
| `Progressive` | Progressive passes | ~38 arrows, `--blue`, width 0.7, opacity 0.88 | ≥10 m upfield, ends in opp. half |
| `Carries` | Carry map | ~32 arrows, `--gold`, width 0.75, opacity 0.9 | Ball carried ≥5 m under control |
| `Touch heat` | Touch heatmap | 12×8 zone grid (cell 8.75×8.5), `rgba(11,76,158,α)`, α 0→0.72 | Touch density · darker = more |
| `Defensive` | Defensive actions | ~46 dots: `--blue` r1.1 tackles, `--gold` r0.9 interceptions, `--faint` r0.8 recoveries, opacity 0.75 | Blue tackles · gold interceptions · grey recoveries |
| `Key passes` | Key passes & assists | ~20 arrows converging on the box: assists `--gold` width 0.85, other key passes `--blue` width 0.55, opacity 0.85 | Gold = assists · blue = other key passes |

All arrows are `<line>` with `marker-end` arrowheads (shared `<defs>`: `arB` blue, `arG` gold,
`arI` faint). Arrow origins in the mock are biased by position group (forwards start high, centre
backs deep); in production they are the real event coordinates — no biasing needed.

> The selector is the one place a reader chooses what they see. Keep all six modes on a single row of
> pills (no dropdown) and keep the pitch mounted across mode changes — only the marks swap.

### 4 · Percentile vs La Liga {position group}
**Identical construction to the page-01 Team profile block** — same card, same row, same colour
thresholds. Reuse that component with a different metric source.

- **Section head** — `flex; baseline; space-between; margin-bottom:12px`. Title Outfit 700 19px
  `-.015em`: `Percentile vs La Liga {Forwards|Midfielders|Defenders}`. Caption DM Mono 9px `.12em`
  uppercase `--faint`: `Per 90 · min 900' · bar = percentile within position group, higher is better`.
- **Grid** — `repeat(4,1fr); gap:14px; margin-bottom:22px`. Card: `--card` surface,
  `1px solid var(--line)`, radius 14px, padding 16px.
  - Card head — title Outfit 700 15px + unit caption DM Mono 8.5px `.1em` uppercase `--faint`
    (`percentile`), `border-bottom:1px solid var(--ink); padding-bottom:7px; margin-bottom:11px`.
  - 6 metric rows, `gap:10`. Line 1: label Figtree 12px 500 `--mid` / value DM Mono 12px 600 `--ink`.
    Line 2: 5px track (`--paper` bed, radius 3) filled to the percentile + 2-digit percentile
    DM Mono 9px in the fill colour, `width:16px; text-align:right`.
  - Fill colour: ≥70 `--pos`, ≥45 `--blue`, else `--neg`.

**The four categories and their metric sets** (general by design — they work for any outfield
position; swap only if the user specifies a position-specific spine):

- **Attacking** — Goals, Shots, xG, Big chances, Box touches, Conversion %
- **Playmaking** — Assists, xA, Key passes, Passes into box, Prog. passes, Pass cmp %
- **Progression** — xT added, Carries → F3, Prog. carries, Take-ons won, Take-on %, Dist. carried
- **Defending** — Recoveries, Tackles won, Interceptions, Pressures, Duels won %, Aerials won %

Percentiles are computed **within the position group**, not the whole league, with a 900-minute
qualification floor. Rate metrics render with one decimal and a `%`; volume metrics per 90 with two
decimals.

### 5 · Sequences involving {surname}
Same model as the page-02 sequence block and the screen-04 lab, scoped to chains the player touched.

- **Section head** — title Outfit 700 19px `Sequences involving {surname}`; right side a
  `Rank by` caption (DM Mono 9px `.12em` uppercase `--faint`) + two pills **xT / VAEP**
  (DM Mono 9.5px, radius 999px; active `--ink` + white, inactive `--paper` + `--mid`).
  Changing the sort **re-sorts the list and resets the selection to index 0**.
- **Grid** — `minmax(0,1fr) minmax(0,320px); gap:16px`.

**Chain traces card (left)**
- Head: `Chain traces` Outfit 700 16px + note `Dot = chain start · gold dot = his touch · ring = end`.
- Horizontal pitch `viewBox="-2 -2 109 72"`. Per chain: `polyline` (`stroke-linejoin:round`), start
  dot r 0.9, end ring r **1.8 when the chain ends in a shot** else 0.9 (`fill:none`, stroke 0.4), and
  — **selected chain only** — a `--gold` r1.5 dot on the player's own touch in that chain.
- Selection styling: selected `--blue`, `stroke-width = 1.1 + xT*3.4`, opacity 1; all others
  `--faint`, `stroke-width = 0.35 + xT*1.2`, opacity 0.3.
- **Aggregate strip** below: `repeat(4,1fr); gap:12px; margin-top:14px;
  border-top:1px solid var(--line); padding-top:12px`. Label DM Mono 8.5px `.14em` uppercase `--mid`;
  figure Outfit 700 26px `line-height:1.05` (xT figure in `--blue`, rest `--ink`).
  Figures: **Sequences involved · xT added in chains · Chains → shot · Avg passes in chain**.

**Sequence list (right, 320px)**
- Head: `Top 10 by {xT|VAEP}` Outfit 700 16px + `click to trace` DM Mono 8.5px `--faint`.
- 10 buttons, `flex-direction:column; gap:4px`. Each: `1px solid var(--line)`, radius 11px,
  background `--paper`, `padding:8px 10px`, `text-align:left`, three lines —
  1. `SEQ-####` (DM Mono 9.5px `--mid`) + match code `GIR (H)` right-aligned (DM Mono 9px `--mid`)
  2. description, Figtree 12px (`Received in half-space → shot`, `Carry from midfield → cross`,
     `One-two on the edge → shot`, `Wide overlap → cut-back`, `High regain → through ball`,
     `Drop deep → switch → box entry`)
  3. `{n} passes · {n} touches · +0.000` — DM Mono 9px `--mid`, `gap:10px`
- **Selected row**: `--blue` background and border, white text, sub-text `rgba(255,255,255,.72)`.
  (This page inverts the selected row rather than tinting it like page 02 — the list is shorter and
  sits beside a quiet pitch.) Hover on unselected rows → `border-color:var(--blue)`.
- Clicking a row sets `pSeq` to that index; the trace highlight follows immediately.

---

## Removed from the earlier first-pass
The player shot map and the match-by-match xT bar chart are **out**. One pitch on this page, mode-switched
by the selector. Do not reintroduce a second pitch above the percentile block.

## State additions
```ts
interface PlayerPageState {
  player: string;                    // selected player, e.g. 'Javi Puado'
  pViz: 'Passes' | 'Progressive' | 'Carries'
      | 'Touch heat' | 'Defensive' | 'Key passes';   // default 'Passes'
  pSort: 'xT' | 'VAEP';              // default 'xT'
  pSeq: number;                      // selected sequence index, default 0
}
```
`pSort` change → reset `pSeq` to 0. `player` change → reset `pViz`? **No** — keep the mode sticky
across players; a scout comparing two players wants the same view.

| Trigger | Result |
|---|---|
| Player tab click | `player → name`; all five blocks requery |
| Mode pill click | `pViz → mode`; pitch marks, title and legend swap |
| Rank-by pill click | `pSort → metric`; list re-sorts, `pSeq → 0` |
| Sequence row click | `pSeq → index`; trace highlight + row inversion |
| Row / pill hover | `border-color: var(--blue)` |

No transitions beyond hover colour. Mode switching is instant.

---

## Data contracts (Opta pipeline)

One gold table per block; nothing aggregated client-side.

**Identity + bio**
- `silver.players` / `gold.player_profile`: `player_id, first_name, last_name, shirt_number,
  birth_date, height_cm, weight_kg, preferred_foot, nationality, position, position_group,
  contract_until, photo_url`. `position_group ∈ {Forwards, Midfielders, Defenders}` drives the
  percentile cohort and the block heading. Age is derived from `birth_date` at render.

**KPI block**
- `gold.player_season_totals`: `season_id, player_id, minutes, appearances, goals, assists, xg, xa,
  npxg, shots, shots_on_target, key_passes, take_ons, take_ons_won, recoveries, pass_completion_pct,
  xt_per_90, progressive_passes, carries_into_final_third` → the four KPIs + the secondary row.

**Event map** — one query per mode, all filtered to `player_id` and `season_id`:
- `gold.player_events`: `event_id, match_id, type ('pass'|'carry'|'tackle'|'interception'|
  'recovery'|'touch'), x, y, end_x, end_y, is_progressive, is_key_pass, is_assist, outcome,
  xt_delta`. `Passes`/`Progressive`/`Carries`/`Key passes` filter this table by `type` + flags;
  `Defensive` filters by the three defensive types.
- `gold.player_touch_grid`: `zone_x (0–11), zone_y (0–7), touches` → the `Touch heat` mode.
  Normalise α as `touches / max(touches)` capped at 0.72.
- Opta coordinates are 0–100; convert with `x*1.05`, `y*0.68` as in the base README, and normalise
  direction so the player always attacks right.

**Percentile block**
- `gold.player_season_metrics`: long-form `(season_id, player_id, metric_key, value, percentile,
  position_group)` — 24 rows per player, one per metric in the four categories above. Percentile
  computed across all La Liga players in the same `position_group` with `minutes >= 900`. Store the
  percentile; do not recompute in the client.

**Sequences**
- `gold.player_sequences`: `sequence_id, match_id, match_code, start_minute, start_type, end_type,
  pass_count, player_touches, duration_s, xt_added, vaep_added, points (ordered [x,y] array or a
  child touch table), player_touch_index (index into points of this player's first touch)`.
  Top 10 by `xt_added` or `vaep_added`; `player_touch_index` is what the gold dot renders.
- Aggregates come from the unfiltered player-sequence set, not the top 10:
  `count(*)`, `sum(xt_added)`, `count(*) filter (where end_type='shot')`, `avg(pass_count)`.

---

## Responsive
Desktop-first, same rules as the rest of the app. Below ~1100px: the profile/map pair stacks (card
first, pitch second), the percentile grid collapses `4 → 2 → 1`, and the sequence list moves below
the traces pitch at full width.

## Files
- `Espanyol Analytics.dc.html` — screen 03 lives under `data-screen-label="03 Player profile"`.
- `screenshots/10-player-*.png` — reference renders of the four blocks, top to bottom.
- Prototype runtime (`support.js`, `image-slot.js`) is design-time only; do not port.
