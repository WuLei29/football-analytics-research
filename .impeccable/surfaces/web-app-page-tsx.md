---
version: 1
slug: "web-app-page-tsx"
primary_target: "web/app/page.tsx"
related_targets: []
---

# Landing `/` — surface brief

Mode: Persuade. Audience: football-data peers arriving from LinkedIn (phone or laptop, seconds to judge), informed Espanyol fans second. Job: show the season at a glance, prove the depth of the site, and send the visitor into a live analysis ("Ver la temporada"; "Último partido" second). Author named in the first viewport.

Proof: real 2026/27 figures from `overview.json`, the latest match's shots and chains from its match file, real counts (matches across seasons, squad size). The headline is facts only (table position vs xG-difference rank); authored judgement comes from `narratives.ts` and is not invented.

Unresolved: number locale (en-GB kept, open in PRODUCT.md); whether the stadium photo appears beyond the landing (proposed: landing only).

## Direction contract

THESIS: The site is the coach's tactics board the analysis is drawn on. It refuses the category default of a photo hero, headline, button and a row of equal stat cards: the board rises over the stadium and carries the season and a numbered path into the site.

OWN-WORLD: White enamel ground (#F4F5F2), flat panels separated by one tone step (#ECEEE9, #E3E6E0), no borders; depth by overlap, plus a raised frosted pill nav. Espanyol blue (#123E8C) for the team, marker red (#C4372B) for the opponent; verdicts in green / amber / red. The one highlight panel per screen is a light Espanyol blue (#DFE7F4) with the same inks (revised 24 Oct 2026: the dark #16191F panel read as too heavy). Hanken Grotesk throughout: large figures at weight 300, headings 600. Pill shapes for controls. Data drawn as round-capped strokes.

STORY: The visitor sees where Espanyol stands and the gap between table and xG, sees who made it (byline), understands the site goes season → match → possession → player, and opens the season or the latest match, where a shot draws its own possession.

FIRST VIEWPORT: Stadium photo across the top ~45%, a fixed floating pill nav over it. The board rises over the photo's lower edge, inset 40px each side. Left panel (~60%): two-line headline (position and points; xG rank in blue), one factual line, three light figures plus the per-match xG-difference stroke, "Ver la temporada" (team pill) and "Último partido" (tone pill), byline with LinkedIn and GitHub. Right: heading "Cada nivel abre el siguiente" and index rows 01–05 (Temporada, Partido, Secuencia, Jugador, Método), each a whole-row link with a real thumbnail; hover/focus steps one tone darker, title turns blue, arrow appears. "Datos hasta J#" locked top-right of the board.

FORM: Pizarra, assigned direction (position 7 of 7 on the grounded list), raised by depth-by-overlap, numbered exits, one shared 30-zone grid, two inks, one status corner. seed:3000dbe4ca1b9c9f

Signature interaction: on the light-blue latest-match panel below the fold, hovering, focusing or tapping an Espanyol shot draws its possession as a marker stroke (stroke-dashoffset, ease-out, off under reduced motion); the highest-xG chain is drawn by default.

FINISH: unreviewed and undocumented is unfinished; this build ends with the finish review, the verdict, DESIGN.md, and every shipping raster carrying its provenance
