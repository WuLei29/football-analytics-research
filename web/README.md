# `web/` — Espanyol Analytics

The public site. It is a **Next.js static export**: `next build` turns the JSON
files in `public/data/` into a folder of plain HTML, CSS and JS. There is no
server, no database connection and no API at runtime — by the time anyone opens
a page, every number on it has already been rendered into the HTML.

That is not a shortcut, it is the architecture. The data only changes when you
run the pipeline on your machine, so the site does not need a live data source
(`md/WEB_PLAN.md` §2).

```
Postgres (local) ──build_gold──▶ gold.* ──python -m src.export──▶ web/public/data/*.json
                                                                          │
                                                            git push ─────┤
                                                                          ▼
                                                        Vercel runs `next build`
                                                        and serves the static output
```

---

## Run it locally

From this folder:

```bash
npm install          # once
npm run dev          # http://localhost:3000, hot reload
npm run build        # production build into out/
npm run typecheck    # tsc --noEmit, no build
```

`npm run dev` reads the same `public/data/` files the build does, so what you
see locally is what deploys.

---

## Where things are

```
web/
  app/                      routes; one folder per URL segment (App Router)
    layout.tsx                the <html> document and the one font (Hanken Grotesk)
    globals.css               THE DESIGN SYSTEM, as code — the Pizarra tokens
    page.tsx                  "/" — the landing page
    [equipo]/
      temporada/[season]/       screen 01 — the season overview
      partidos/[season]/        the match list
      partido/[match_id]/       screen 02 — one match
      plantilla/[season]/       the squad tab: screen 03 on the first player
      jugador/[season]/[player_id]/  screen 03 — one player's season
    demo/page.tsx             "/demo" — the drawing primitives, with fake data
    demo/jugador/             screen 03 drawn from the fixture player
  content/
    narratives.ts             the authored prose: season and match readings.
                              The ONLY content not produced by the export
  components/
    shell/                    PillNav, Arrow, Exits (numbered exits),
                              BoardFooter, Dropdown
    landing/                  SeasonHighlight, DepthIndex, ChainPitch, Thumbs, XgStroke
    season/                   screen 01: KpiStrip, LeagueTable, FormStrip,
                              SeasonTrend, TeamProfile (+ PercentileCard/Row),
                              PlayerLeaders
    matches/                  the match list: MatchIndex, MatchShots
    match/                    screen 02: MatchMap (the three event maps),
                              SequenceLink, MatchSequences, MatchTotals,
                              MatchPlayers, MatchPicker
    player/                   screen 03: PlayerRoute (the page shell), header,
                              picker, match filter, profile card, EventMap
                              (8 modes), percentiles, sequences
    sequences/                SequenceParts (ChainCard, SequenceList,
                              MetricSwitch), shared by 02, 03 and /demo
    viz/                      the shared primitives
      Pitch.tsx                 the pitch itself, both orientations
      ZoneHeatmap.tsx           the 30 zones of gold.pitch_zones
      PassNetwork.tsx           nodes and edges on a vertical pitch
      SequenceDetail.tsx        ONE sequence, action by action (the v1 port)
      Momentum.tsx              per-minute net xT over one match
      VizDefs.tsx               the arrowhead markers, declared once per page
  lib/
    data/
      types.ts                the data contract, as TypeScript
      load.ts                 how a page reads a JSON file
      sequences.ts            a match file's chain as the sequence block reads it
      playerFilter.ts         screen 03's match filter (sums per-match parts)
    viz/
      pitch.ts                metres → SVG: the only conversion in the codebase
      scale.ts                linear scales, rolling means, area paths
      fixtures.ts             invented data for /demo, and nowhere else
    format.ts                 every number (point decimal) and date (es-ES)
    labels.ts                 every Spanish string
    routes.ts                 every URL the site can produce
    brand.ts                  the stadium photo and its credit
  public/
    data/                     ← written by `python -m src.export`; committed
    brand/                    the image slots (see its README)
  design_handoff_espanyol_analytics/
                              the original design system and prototype (superseded)
```

Nothing in `design_handoff_espanyol_analytics/` is imported, and since 24 Oct
2026 it is not the visual authority either: `DESIGN.md` at the repo root is
(the "Pizarra" world). The handoff stays as history; `README-PLAYER.md` still
documents screen 03's data contract and decisions. The pre-redesign site is
the tag `pre-redesign-2026-10-23`.

---

## The six things worth understanding

### 1. Data flows one way, and it flows at build time

A page calls `getManifest()` (or `getOverview(...)`) from `lib/data/load.ts`.
That reads a file off disk with `fs`, parses it, and hands back a typed object.
Because the page is a **server component**, this happens once, during
`next build`, and the result is baked into the HTML.

`lib/data/load.ts` fails loudly: a missing file or a `schema_version` that does
not match stops the build with a message naming the command that fixes it. A
page rendered from a stale file looks plausible and is wrong, which is the
worst failure mode available.

Which route reads which file is the table in `md/WEB_DATA.md` §2.1.

### 2. `"use client"` is the boundary, and it is narrow

Everything is a server component by default: it runs at build time, ships as
HTML, and sends no JavaScript. A file that starts with `"use client"` ships as
JavaScript because it has to answer a click.

Keep client components small and at the leaves: a page that loads data stays a
server component and passes slices down to the interactive bits — the maps
(`match/MatchMap`, `player/EventMap`, `landing/ChainPitch`), the sequence
blocks, `season/SeasonTrend`, the dropdowns. `/demo` itself is still a
server component, which is the point: each of those declares its own boundary
rather than pulling the page across it.

`SequenceDetail` stays a pure drawing of whatever chain it is handed; the
selection lives in the block around it (`MatchSequences`, `PlayerSequences`,
`DemoSequences`). That split is what lets screen 04 reuse the same pitch with
the selection coming from filters instead.

Every interactive map mark is a labelled `role="button"` in one roving tab
stop per map: Tab enters, the arrow keys walk the marks, Enter opens the
chain, and a white `aria-live` readout says what is under the cursor or the
keyboard. Copy that model for any new interactive layer.

### 3. The design system is CSS, not a config file

`app/globals.css` is the whole design system, and `DESIGN.md` at the repo root
is its specification. Its `@theme` block declares the Pizarra tokens (board,
panel, panel-deep, chalk, slate, ink, team, team-deep, rival, spot and its
three companions, good, fair, fair-fill, bad), the one face, the two shadows
and the radii. Tailwind v4 turns each declaration into **both** a CSS custom
property and a utility class:

```
--color-team: #123E8C   ⇒   var(--color-team)   and   bg-team / text-team
```

So a component can use whichever reads better — utilities in markup, the raw
variable inside an SVG or a gradient. There is no `tailwind.config.ts`.

Every page's root element carries `.pizarra` (face, size, tabular numbers,
focus ring). The panel classes set `--viz-ground`, the colour the charts draw
their halos and node fills in, so a chart reads right on a panel, on spot or
on a white card without being told. There are no component classes: the
handoff's `.card`, `.eyebrow`, `.label`, `.dek` went with it.

### 4. Numbers and strings each have exactly one home

- **Numbers** go through `lib/format.ts`, which splits the locale in two:
  numbers use a decimal **point** (`41.7`, `1,464`), because that is how
  football numbers are written everywhere they are read, while dates stay
  Spanish (`3 de septiembre de 2026`), because those are words. One constant
  per side flips either. Nothing hand-formats a number — including digits
  inside a caption, which are built with `dec()` — because that is how a site
  ends up with a comma in one card and a dot in the next.
- **Strings** go in `lib/labels.ts`. A component never contains a Spanish
  sentence. A second language later costs one file, not a sweep.
- **URLs** go in `lib/routes.ts`. Rename a route there and TypeScript finds
  every caller.

A metric key with no Spanish label renders as `⟨key⟩`, on purpose — a visible
gap gets fixed, a fallback to the raw key does not.

### 5. Every component file says what it is

The header comment of each component says what it renders, what props it takes,
which JSON file feeds it, and which gold table that file came from. That chain
— gold table → export → JSON file → component — is the thing worth being able
to follow, and it is not recoverable from the code alone.

### 6. One pitch, one conversion, one flip

Every spatial chart draws into the same frame: real metres on a 105 × 68 pitch,
exactly as `silver.events` stores them and the export ships them. `lib/viz/
pitch.ts` is the only file that turns a metre into an SVG unit; a component
with a pitch number in it is a bug.

The part to read before touching anything spatial is the **flip**. The data
frame is mathematical — y grows upward — and SVG's y grows downward, so a
mark's SVG y is `68 - y`. That matters because **low `y` is the RIGHT flank**
(`GOLD_LAYER.md` §4.6.0): draw `y` straight and every wing, half-space and
cross on the site is mirrored, which looks entirely plausible. The pitch
markings are symmetric, so the pitch itself never gives it away.

`/demo` is where you check it. The zone-heatmap fixture is shaped like a
right-back's season: its heat belongs in the **lower** half of the horizontal
pitch.

Composition: `<Pitch>` takes two slots, `underlay` (drawn beneath the pitch
lines — heat fills, zone rectangles) and `children` (drawn above — marks,
arrows, nodes). That is the z-order ladder of the `espanyol-viz-design` skill
expressed as element order, and it is why no chart redraws pitch lines of its
own.

---

## What exists and what is next

Built (Phases 3-5, `WEB_PLAN.md` §5): the landing, screen 01 (season), the
match list, screen 02 (match), screen 03 (squad + player) and `/demo`, all in
the Pizarra world since 24 Oct 2026 (`md/CHANGELOG.md`).

The pill nav shows only built sections; an unbuilt one is left out. When a
page lands, flip its `ready` flag in `lib/routes.ts`; the numbered exits show
"Pronto" for a level with no page yet.

Next:

| Phase | What |
|---|---|
| 5a | **built** — a fidelity pass against the design screenshots is still to do |
| 5b | **player built** (16 Oct 2026); squad, sequence lab, comparison, methodology |
| 6 | Mobile collapse pass, share images, custom domain, Spanish copy review |

Screen 03 reads `squad.json`, the one OPTIONAL file (`getSquad` returns
null): until the export has run its `squad` step, the player route builds a
single "en preparación" placeholder and the screen-01 leader rows stay
unlinked; once it has, every squad row gets a page and its leader rows link.
`content/narratives.ts` holds an empty
`matchReadings` map — a match page shows no reading block until it has a key
there, on purpose (`WEB_PLAN.md` §3.7).

The only dependency still deliberately **not** installed is
`@tanstack/react-table` (the squad table, Phase 5b). `d3-scale` / `d3-shape`
were planned for Phase 4 and were **not** needed: every chart the design
specifies is straight-line geometry on a linear scale, and `lib/viz/scale.ts`
is that in about 120 readable lines (`WEB_PLAN.md` §3.6). Install `d3-shape` in
the screen that first needs a genuine curve, an arc or a stack.

---

## Refresh: new data on the site

From the repository root, after a matchday:

```bash
python src/silver/load_competition_seasons.py
python src/bronze/parse_teams_bronze.py && python src/silver/load_teams.py
python -m src.silver.squads --raw-root data/raw
python src/silver/load_matches.py && python src/silver/load_match_lineups.py
python -m src.silver.events --raw-root data/raw
python -m src.silver.events.sequences
python -m src.gold.build_gold
python -m src.export                       # rewrites web/public/data/

git add web/public/data && git commit -m "data: jornada N" && git push
```

The push is the deploy. Vercel rebuilds on every commit to the default branch,
and gives every other branch its own preview URL.

---

## Deploy

The site is hosted on Vercel with **Root Directory `web`**. Full first-time
setup, the settings that matter and the custom-domain step are in
`md/WEB_DEPLOY.md`.
