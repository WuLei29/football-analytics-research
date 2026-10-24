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
- **Two visual worlds while the site moves over (24 Oct 2026).** The landing
  `app/page.tsx` (and every page moved out of `app/(handoff)/`) is the new
  **Pizarra** world: `DESIGN.md` at the repo root, tokens `board`/`panel`/
  `team`/`rival`/`spot`/`good`/`fair`/`bad` in `globals.css`, root element `.pizarra`, components in
  `components/landing/` and `components/shell/` (PillNav, Arrow, Exits,
  BoardFooter). Pages still under `app/(handoff)/` keep the handoff
  (`design_handoff_espanyol_analytics/README.md`, + `README-PLAYER.md` for screen
  03). Use the tokens of the page's own world; never hard-code a hex either names.
- **No Spanish strings in components.** They go in `lib/labels.ts`; numbers go
  through `lib/format.ts`.
- **Every chart composes on `<Pitch>`.** Geometry and flips live in
  `lib/viz/pitch.ts`. Components never mirror coordinates. Low `y` = right
  flank = bottom of a horizontal pitch.
- **Every component opens with the header comment**: `Renders:` / `Props:` /
  `Data:` plus the reason for each deviation from the handoff.
- **New layers get a fixture** in `lib/viz/fixtures.ts` and a card on `/demo`
  first.
- Verify with `npm run typecheck`, then `npm run build`, then a browser look at
  the page.
