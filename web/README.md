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
    layout.tsx                the <html> document, the three fonts, page padding
    globals.css               THE DESIGN SYSTEM — tokens, base, the four idioms
    page.tsx                  "/" — the landing page
    demo/page.tsx             "/demo" — the drawing primitives, with fake data
  components/
    shell/
      Masthead.tsx            the dark hero + the section nav
      SeasonSwitcher.tsx      the season chips ("use client" — see below)
      Footer.tsx              provenance: source, derivations, build stamp
    viz/                      one component per graphical block
      Pitch.tsx                 the pitch itself, both orientations
      ShotMap.tsx               shots, both teams, area ∝ xG ("use client")
      HeatSurface.tsx           the 12 × 8 xT surface
      ZoneHeatmap.tsx           the 30 zones of gold.pitch_zones
      PassNetwork.tsx           nodes and edges on a vertical pitch
      ProgressionArrows.tsx     progressive passes and carries
      DefensiveActions.tsx      diamonds + the defensive line ("use client")
      SequenceTraces.tsx        12 possession chains as polylines, with selection
                                (built, not on /demo — for screen 02)
      SequenceDetail.tsx        ONE sequence, action by action (the v1 port)
      SequenceBrowser.tsx       that pitch + a selectable list of plays ("use client")
      RollingArea.tsx           the diverging 5-match rolling chart
      Momentum.tsx              per-minute net xT over one match
      VizDefs.tsx               the arrowhead markers, declared once per page
  lib/
    data/
      types.ts                the data contract, as TypeScript
      load.ts                 how a page reads a JSON file
    viz/
      pitch.ts                metres → SVG: the only conversion in the codebase
      scale.ts                linear scales, rolling means, area paths
      fixtures.ts             invented data for /demo, and nowhere else
    format.ts                 every number (point decimal) and date (es-ES)
    labels.ts                 every Spanish string
    routes.ts                 every URL the site can produce
    brand.ts                  the hero photo and crest slots
  public/
    data/                     ← written by `python -m src.export`; committed
    brand/                    the two image slots (see its README)
  design_handoff_espanyol_analytics/
                              the design system and the five-screen prototype
```

Nothing in `design_handoff_espanyol_analytics/` is imported. It is reference:
open `Espanyol Analytics.dc.html` in a browser to see the target, and read its
`README.md` for exact values. Its `support.js` and `image-slot.js` are
prototype runtime and are not ported.

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
server component and passes slices down to the interactive bits. There are four
so far — `shell/SeasonSwitcher.tsx`, and three in `viz/`: `ShotMap.tsx` and
`DefensiveActions.tsx` (both answer a hover) and `SequenceBrowser.tsx` (it owns
a selection). `/demo` itself is still a server component, which is the point:
each of those declares its own boundary rather than pulling the page across it.

There are two patterns for a selection and both are worth knowing:

- `SequenceTraces` takes `selectedId` as a prop and calls an optional
  `onSelect`, so the *caller* owns the state — and a page that renders it with
  nothing selected ships no JavaScript at all.
- `SequenceBrowser` owns its own selection, because the selection is the whole
  point of the component. It still delegates the drawing to `SequenceDetail`,
  which stays a pure server-renderable component that draws whatever it is
  handed. That split is what lets screen 04 reuse the same pitch with the
  selection coming from filters instead.

### 3. The design system is CSS, not a config file

`app/globals.css` is the whole design system. Its `@theme` block declares the
eleven colour tokens, the three font families, the type scale and the radii —
copied verbatim from `design_handoff_espanyol_analytics/README.md`. Tailwind v4
turns each declaration into **both** a CSS custom property and a utility class:

```
--color-blue: #0B4C9E   ⇒   var(--color-blue)   and   bg-blue / text-blue / border-blue
```

So a component can use whichever reads better — utilities in markup, the raw
variable inside an SVG or a gradient.

There is no `tailwind.config.ts`. Tailwind v4 replaced it with `@theme`.
(`WEB_PLAN.md` v0.4 §3 still names the config file; the tokens are the same,
the file they live in is not.)

Four idioms repeat on every screen and are component classes rather than
repeated utility strings: `.card`, `.eyebrow`, `.label`, `.dek`. Nothing else
gets one — duplicate twice, extract on the third.

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

Built (Phase 3, `WEB_PLAN.md` §5): the scaffold, the design tokens, the three
fonts, the masthead shell, the landing page, and the deploy. Built (Phase 4):
`lib/viz/` and `components/viz/` — the pitch in both orientations, seven pitch
layers and the two chart primitives, catalogued on `/demo`.

The nav lists the six sections, all marked *en construcción*. They are inert
text, not links — a static export has no useful 404. When a page lands, flip
its `ready` flag in `lib/routes.ts` and it becomes a link.

Next:

| Phase | What |
|---|---|
| 5a | Season overview (screen 01), match analysis (screen 02), the match list |
| 5b | Player, squad, sequence lab, comparison, methodology |
| 6 | Mobile collapse pass, share images, custom domain, Spanish copy review |

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
