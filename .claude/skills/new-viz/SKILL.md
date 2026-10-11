---
name: new-viz
description: >
  End-to-end workflow for adding a chart, a visual mode or a data block to the
  Espanyol site: definitions check → export field (src/export, WEB_DATA.md,
  types.ts) → fixture and /demo card → component → page → verification
  (typecheck, build, browser screenshot against the handoff) → docs. Encodes
  the path screens 01 and 02 were built with, so each new chart follows it
  without being re-explained.

  Use when the user asks to add or build a chart, a pitch mode, a page block
  or a whole page in web/ ("add a reception heatmap to the player page",
  "build screen 03", "/new-viz shot map"). Not for pure styling tweaks to an
  existing component.
argument-hint: "<chart or block> [on <page>]"
---

# /new-viz: from gold to a chart on the page

Load **`espanyol-viz-design`** (catalog entry + component inventory) and
**`football-metrics`** (definitions) first. If the chart isn't in the catalog,
you'll write its entry in step 7.

The user's standing rule is **propose before changing** (memory:
`feedback-propose-before-changing`). Steps 1–2 end in a short proposal and
wait for approval. Steps 3–7 are execution.

## 1. Pin down what is drawn

From the catalog entry, write down:
- **Question** the chart answers, in one sentence.
- **Every metric or flag** it uses. Look each one up in `football-metrics`.
- **Conflicts**: wherever the handoff (`README.md` / `README-PLAYER.md`)
  defines a metric differently, uses a grid the repo doesn't, or names a table
  that doesn't exist (football-metrics §8 lists the known ones). These go to
  the user as questions with a recommended answer. Don't resolve them silently.
- **Scope**: which team(s), season(s), player(s); per match or per season.

## 2. Find or design the data

The site reads static JSON written by `src/export/`. Nothing is aggregated in
the browser, and pages never query a database.

1. Is the field already exported? Check `md/WEB_DATA.md` (§2.1 shows which page
   reads which file) and `web/lib/data/types.ts`.
2. If not, design it:
   - SQL goes in `src/export/db.py` (all export SQL lives there). Read
     **gold** where a gold column exists, and `silver.events` only for
     event-level marks. Order event streams by `(json_index, event_id)`.
   - Coordinates go in the analysed team's attacking frame, metres, rounded to
     0.1 (WEB_DATA §3.1–3.2). Mirror opponents in SQL, not in the component.
   - Every sort ends on a unique key (determinism; export diffs are the record).
   - Caps and thresholds become named constants in `src/export/config.py`,
     next to the exposure rules (WEB_PLAN §6).
   - **Estimate size**: rows × bytes per row, per file. Flag anything that
     takes a match file past ~400 KB or a player file past ~150 KB; propose a
     cap, sampling or aggregation (WEB_PLAN §9.3).
3. **Proposal to the user**: the JSON snippet, its source per field, the size
   estimate, and the open questions from step 1. Wait.

## 3. Export

- Implement in the exporter module for that file (`match.py`, `overview.py`,
  or a new `player.py` etc. registered in `src/export/__main__.py` `STEPS`).
- Specify the field in `md/WEB_DATA.md` under that file's section: JSON
  snippet plus a block→source table, same style as §7 and §9.
- Mirror it in `web/lib/data/types.ts`, which is **hand-written** (the
  `--emit-types` generator is still open). If you remove or retype a field,
  bump `SCHEMA_VERSION` in both `src/export/config.py` and `types.ts`.
- Run `python -m src.export --only <step>` (add `--season` / `--match-ids` while
  iterating). If the rename of `.tmp` fails with WinError 5, OneDrive is
  holding the file: wait and re-run (memory: `project-onedrive-rename-lock`).
- **Spot-check one number** against gold with a direct query (e.g. the
  player's `progressive_passes` from `gold.player_season_stats` equals the
  number of progressive arrows exported).

## 4. Fixture and /demo

- Add a seeded fixture to `web/lib/viz/fixtures.ts`, typed with the component's
  exported interface.
- Add a card to `web/app/demo/page.tsx`. The layer has to look right in
  isolation before it touches a page.

## 5. Component

- **Reuse first** (catalog §3, §5). A new style on an existing layer is a prop.
  On the third copy of a pattern, extract it.
- New file in `web/components/viz/`. Open it with the house header comment:
  `Renders:` / `Props:` / `Data:` (export key and WEB_DATA §), then the *why*
  of each deviation from the handoff.
- Compose on `<Pitch>`: heat in `underlay`, marks in `children`, element order
  = z-order. Use `project` / `zoneRect` / `xtCellRect` from `lib/viz/pitch.ts`.
  Never hand-roll coordinates or flips.
- Server component unless it answers a mouse. If it does, make the smallest
  client boundary possible (web/README.md §2).
- Strings come from `lib/labels.ts` (Spanish), numbers from `lib/format.ts`.
  Furniture (title, note, legend, tooltip) is HTML around the SVG.
- This is Next.js 16. Before using any framework API you aren't sure of, read
  the guide in `web/node_modules/next/dist/docs/` (web/AGENTS.md).

## 6. Page and verification

- Read data in the page (server) with `lib/data/load.ts` loaders; pass plain
  props down.
- Then run, from `web/`:
  1. `npm run typecheck`
  2. `npm run build` (static export; a missing or stale export file fails here,
     by design)
  3. `npm run dev`, then open the page and `/demo` in the browser
     (claude-in-chrome) and **screenshot** it. Compare against the handoff's
     `screenshots/` and its spec values (sizes, tokens, draw order).
- Sanity checks that catch real bugs:
  - **Orientation**: a right-sided player's marks sit in the lower half of a
    horizontal pitch, and attack goes right.
  - **Counts**: marks drawn = the exported count = the gold number from step 3.
  - **Empty state**: a player or match with zero of this action still renders
    the pitch and the message.
  - **Density**: the busiest real case (most passes, most shots) is still
    readable.
- Report honestly: what was verified, what was only typechecked.

## 7. Document

- `espanyol-viz-design` SKILL.md: add or update the catalog entry (set "planned"
  to the real export key) and the component inventory row.
- `md/WEB_PLAN.md`: record deviations from the handoff in the current phase's
  "what changed" section (§3.x style).
- New metric definitions go in `football-metrics` (and GOLD_LAYER if gold changed).
- Commit only when the user asks: code, `web/public/data` and docs together.
