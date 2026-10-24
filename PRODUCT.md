# Product

<!-- impeccable:product-schema 1 -->

## Platform

web

## Users

- **Primary: football-data peers.** Club analysts, data scouts, recruitment and
  analytics hiring managers, and the analytics community on LinkedIn. They arrive
  from a shared link, usually on a phone or a laptop between other things, and
  judge in a few seconds whether the work has depth and rigour. The Espanyol
  content is the proof of the author's skill.
- **Secondary: informed Espanyol fans.** They want to understand their team's
  season, a match or a player. The site must stay readable for them without
  dumbing down for the primary reader.

## Product Purpose

A public, Spanish-language portfolio site that analyses RCD Espanyol's La Liga
seasons (2025/26 onwards) from raw Opta event data. Success means a peer leaves
convinced the author can build the full chain (ingestion, models, analysis,
presentation), and a fan leaves understanding something about the team they did
not see in a box score.

## Positioning

One step further than FBref / Sofascore-style stat pages, for one club in depth:

- **Own models.** xG, xT, VAEP, possession sequences and phases of play are built
  by the author from raw events, not copied from a provider.
- **Extra layers of analysis.** Detailed shot maps, reception maps, progression,
  defensive actions, pass networks, sequence traces, with every map mark linked to
  the possession chain it belongs to.
- **Authored reading.** Written interpretation of each season and match, not
  numbers alone.
- **Method shown.** Every metric is defined and traceable to how it was computed.
- **Spanish, one club deep** rather than every league shallowly.

## Operating Context

- Static Next.js site; data is exported from the local gold layer to JSON at
  build time and committed. Refreshed manually after each matchday (pipeline ->
  gold -> export -> push). No server, login or runtime API.
- Routes: landing `/`; season `/{equipo}/temporada/{season}`; match list
  `/{equipo}/partidos/{season}`; match `/{equipo}/partido/{match_id}`; squad
  `/{equipo}/plantilla/{season}`; player `/{equipo}/jugador/{season}/{player_id}`;
  `/demo` fixtures.
- Most traffic arrives from a LinkedIn link: share metadata and the first
  viewport matter.
- The author learns by reading the frontend code: components carry explanatory
  header comments and `web/README.md` walks the structure.

## Capabilities and Constraints

- Spanish only (`lang="es"`, Spanish dates). Numbers are currently formatted
  `en-GB` (decimal point) by a deliberate switch in `web/lib/format.ts`
  (`NUMBER_LOCALE`); whether to return to es-ES is open. Labels centralised in
  `web/lib/labels.ts`.
- Espanyol first; export and routes are per team so other clubs can be added.
- Data licensing (`md/WEB_PLAN.md` §6): only aggregates and visualisation-shaped
  slices are published; no per-event tables, no bulk download. Source credited
  as Opta.
- Metric definitions come from the `football-metrics` skill; chart marks and
  encodings from `espanyol-viz-design`. A redesign changes the interface, not the
  data shown.
- Undecided: the sequence lab (screen 04), comparison (screen 05) and a
  methodology page are planned, not built.

## Brand Commitments

- Name: "Espanyol Analytics". Clearly **unofficial and independent**: not
  affiliated with RCD Espanyol or the provider.
- **The author is named**, with links (LinkedIn / GitHub) visible as the author
  of the work.
- Voice: Spanish, analytical, plain, confident; a personal analysis project.

## Evidence on Hand

- Real data: La Liga 2025/26 (380 matches) and 2026/27 in progress, from
  `web/public/data/` (exported from gold).
- Authored prose: `web/content/narratives.ts` (season and match readings).
- Design handoff (current visual authority until superseded):
  `web/design_handoff_espanyol_analytics/` (README.md, README-PLAYER.md,
  screenshots).
- No club crest, player photos, testimonials or press are on hand
  (`web/public/brand/README.md`); do not fabricate them.

## Product Principles

1. Depth is the pitch: show the extra layer a stat site does not have, then let
   the reader drill into it.
2. Every number is explainable: a definition or the path to it is one step away.
3. Interpretation over inventory: a written reading frames the charts.
4. Honest provenance: unofficial, source credited, author named.
5. Readable by an informed fan without diluting it for the analyst.

## Accessibility & Inclusion

No product-specific requirement established beyond WCAG 2.2 AA as the working
floor (keyboard access to the interactive maps and filters, contrast on the
warm palette).
