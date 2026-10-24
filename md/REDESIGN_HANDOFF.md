# Redesign handoff — moving the remaining pages to the Pizarra world

Written 24 Oct 2026, when the landing `/` was rebuilt in the new world. This
file is the brief for the sessions that move every other page over. Delete it
when `web/app/(handoff)/` is empty.

## Where things stand

- **Branch** `redesign/visual-world`. `main` is untouched. Snapshot of the old
  design: tag `pre-redesign-2026-10-23`.
- **Done**: the landing `web/app/page.tsx` (components in
  `web/components/landing/`) and **screen 01** `web/app/[equipo]/temporada/`
  (24 Oct 2026). Shared Pizarra parts are in `web/components/shell/`:
  `PillNav` (`active`), `Arrow`, `Exits` (numbered exits), `BoardFooter`
  (`photoCredit`). The trend chart is `components/season/SeasonTrend`.
- **Still in the handoff world**, under `web/app/(handoff)/` (URLs unchanged):
  match list
  `/[equipo]/partidos/[season]`, screen 02 match `/[equipo]/partido/[match_id]`,
  squad and player `/[equipo]/plantilla/[season]`,
  `/[equipo]/jugador/[season]/[player_id]`, and `/demo`, `/demo/jugador`.

## The authorities

| What | Where |
|---|---|
| Visual system (tokens, type, components, rules) | `DESIGN.md` at the repo root, + `.impeccable/design.json` |
| The world's intent, the landing's contract | `.impeccable/surfaces/web-app-page-tsx.md` |
| Product truth, audience, brand commitments | `PRODUCT.md` |
| Chart marks and encodings (unchanged) | `espanyol-viz-design` skill; only colours change |
| Metric definitions (unchanged) | `football-metrics` skill |
| Old look, for the pages not yet moved | `web/design_handoff_espanyol_analytics/README.md` (marked "being superseded") |

## Rules of the world, in one screen

- Board `#F4F5F2` ground; panels one tone step up (`panel`, `panel-deep`).
  **No borders, no shadows**: depth only by one flat surface overlapping
  another. No eyebrows or kickers above headings.
- **Two inks**: `team` (Espanyol blue) and `rival` (red: opponent, negatives).
  The **one light-blue `spot` panel per screen** holds the signature
  interaction (the dark `night` panel was retired on 24 Oct 2026). Verdicts
  (good / middling / bad) are `good` / `fair` (`fair-fill` as a fill) / `bad`.
  The handoff's `gold`, `pos`, `blue2`, grey-brown opponent fills go away.
- Hanken Grotesk only (`.pizarra` root class). Figures weight 300, headings
  600, text 400–500. Minimum text 12 px. Mono only for real tabular data, if
  at all.
- Data is drawn as round-capped strokes; arrows are `components/shell/Arrow`,
  never "→".
- Navigation is the floating `PillNav` (`components/shell/`; pass `active`). The repeated 238 px navy masthead goes.
- Each page ends with the numbered exits (the landing's index order:
  Temporada → Partido → Secuencia → Jugador → Método) and `BoardFooter`
  (author, Opta, photo credit when the photo is shown).
- The stadium photo stays on the landing only, unless the author decides
  otherwise.

## Chart colour mapping (decide per chart, then record in DESIGN.md)

| Handoff | Pizarra |
|---|---|
| `--color-blue` team marks | `--color-team` (on panels and on spot) |
| grey-brown opponent marks | `--color-rival` (outline), red fills only for goals |
| `--color-gold` carries | team ink, **dashed** (amber now means "middling") |
| heat ramps (blue) | `panel` → `team` ramp; check the pitch-line casing still works |
| `--color-pos` / `--color-neg` deltas | `good` / `bad` |
| percentile tiers (green / grey / red) | `good` / `fair-fill` (number in `fair`) / `bad`, thresholds 70 / 45 |

Every chart still composes on `<Pitch>`; pass `lineColor="var(--color-chalk)"`
on panels and `var(--color-spot-line)` on spot.

## Order and method

One page per session, in this order (each depends on less than the next):

1. ~~**Screen 01, season**~~ — done 24 Oct 2026 (`md/CHANGELOG.md`).
2. **Match list.**
3. **Screen 02, match** — the spot panel goes to the map → sequence block.
4. **Squad + player (screen 03).**
5. **`/demo` pages**, then delete the `(handoff)` group, its layout, the
   handoff tokens in `globals.css`, the old `Masthead`/`Footer`, and mark the
   handoff README fully superseded.

For each page: `/impeccable shape <page>` (the world is settled, so this is
"a whole surface inside an established world": structure options only, no
new identity), then build, then the checks below. Move the page folder out of
`app/(handoff)/` in the same change, so the folder always lists what is left.

## Checks before calling a page done

- `npm run typecheck`, `npm run build`, browser look at 1440 × 900 and 390 px.
- `impeccable detect --json` on the changed files and on the rendered URL.
- Keyboard: every interactive map mark reachable and labelled (the critique of
  22 Oct found 523/523 match-map marks unreachable — fix it as each map moves).
- One `h1` per page; text ≥ 12 px; text contrast ≥ 4.5:1 on its own surface.
- Update `md/CHANGELOG.md`, the Current State in `CLAUDE.md`, and `DESIGN.md`
  if a new component or rule was added.

## Open decisions for the author

- Number locale: en-GB (`1.2`) kept; switch to es-ES (`1,2`) is one constant in
  `web/lib/format.ts`.
- The 2026/27 season reading in `web/content/narratives.ts` is stale (written
  at matchday 3); screen 01 shows it as written, and the author rewrites it.
- Merge `redesign/visual-world` into `main` after screen 01, or after all
  pages? Merging early publishes a site in two styles.

## Prompt to start the next session

> Continue the visual redesign of the Espanyol Analytics site on branch
> `redesign/visual-world`. Read `md/REDESIGN_HANDOFF.md`, `DESIGN.md` and
> `PRODUCT.md` first. Move the **match list (`/espanyol/partidos/2026-27`)**
> into the Pizarra world: run `/impeccable shape match list` to agree the
> structure with me, then build it, moving the page out of `web/app/(handoff)/`.
> Keep the data and chart encodings; change the interface. Show me 1440 and
> 390 px screenshots before committing.
