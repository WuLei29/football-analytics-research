---
paths:
  - "web/**"
---

# Web (Next.js) rules

Read `web/README.md` ("six things worth understanding") once. For any chart,
use the `espanyol-viz-design` catalog. For a new chart or block end to end,
use the `new-viz` skill.

- **This is Next.js 16.** APIs differ from training data. Check
  `web/node_modules/next/dist/docs/` before using a framework API
  (`web/AGENTS.md`).
- **Static export, data at build time.** Pages are server components that read
  `public/data/*.json` through `lib/data/load.ts`. No fetching, no client-side
  aggregation. Keep client components as small as possible. **One exception,
  screen 03's match filter** (16 Oct 2026): KPIs are summed from per-match
  parts (`lib/data/playerFilter.ts`, mirrored and checked in
  `src/export/player.py`), and filtered sequences fetch their match files.
- **One visual world: Pizarra** (since 24 Oct 2026; the design handoff is
  superseded). `DESIGN.md` at the repo root is the authority; tokens
  `board`/`panel`/`chalk`/`slate`/`team`/`rival`/`spot`/`good`/`fair`/`bad` in
  `globals.css`; every page's root element is `.pizarra`; shell parts in
  `components/shell/` (PillNav, Arrow, Exits, BoardFooter, Dropdown). Never
  hard-code a hex a token names. Blue says who (Espanyol), red the opponent;
  green / amber / red say how good. One light-blue spot panel per screen.
- **Every interactive map mark is reachable by keyboard and labelled**:
  a `role="button"` with an `aria-label`, one roving tab stop per map, arrow
  keys, Enter; a white `aria-live` readout instead of hover cards
  (`match/MatchMap`, `player/EventMap`).
- **No Spanish strings in components.** They go in `lib/labels.ts`; numbers go
  through `lib/format.ts`.
- **Every chart composes on `<Pitch>`.** Geometry and flips live in
  `lib/viz/pitch.ts`. Components never mirror coordinates. Low `y` = right
  flank = bottom of a horizontal pitch.
- **Every component opens with the header comment**: `Renders:` / `Props:` /
  `Data:` plus the reason for each deviation from DESIGN.md or the brief.
- **New layers get a fixture** in `lib/viz/fixtures.ts` and a card on `/demo`
  first.
- Verify with `npm run typecheck`, then `npm run build`, then a browser look at
  the page.
