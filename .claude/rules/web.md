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
  aggregation. Keep client components as small as possible.
- **Visual authority**: `design_handoff_espanyol_analytics/README.md` (+
  `README-PLAYER.md` for screen 03). Use the `:root` tokens; never hard-code a
  hex the handoff names.
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
