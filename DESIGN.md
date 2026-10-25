---
name: Espanyol Analytics
description: The coach's tactics board that RCD Espanyol's seasons are drawn on. An independent analysis built from raw Opta events.
colors:
  board: "#f4f5f2"
  panel: "#eceee9"
  panel-deep: "#e3e6e0"
  chalk: "#d3d6d0"
  slate: "#5a5f66"
  ink: "#17181c"
  white: "#ffffff"
  team: "#123e8c"
  team-deep: "#0c2d68"
  rival: "#c4372b"
  spot: "#dfe7f4"
  spot-deep: "#cfdbef"
  spot-line: "#a3b6d8"
  spot-text: "#3f4f6e"
  good: "#17703f"
  fair: "#8a6414"
  fair-fill: "#e0b33a"
  bad: "#b3261e"
typography:
  display:
    fontFamily: "Hanken Grotesk, system-ui, sans-serif"
    fontSize: "clamp(34px, 3.9vw, 58px)"
    fontWeight: 600
    lineHeight: 1.02
    letterSpacing: "-0.035em"
  figure:
    fontFamily: "Hanken Grotesk, system-ui, sans-serif"
    fontSize: "clamp(34px, 3.6vw, 56px)"
    fontWeight: 300
    lineHeight: 0.9
    letterSpacing: "-0.04em"
    fontFeature: "\"tnum\""
  figure-medium:
    fontFamily: "Hanken Grotesk, system-ui, sans-serif"
    fontSize: "32px"
    fontWeight: 300
    lineHeight: 1
    letterSpacing: "-0.03em"
  headline:
    fontFamily: "Hanken Grotesk, system-ui, sans-serif"
    fontSize: "26px"
    fontWeight: 600
    lineHeight: 1.25
    letterSpacing: "-0.02em"
  title:
    fontFamily: "Hanken Grotesk, system-ui, sans-serif"
    fontSize: "22px"
    fontWeight: 600
    lineHeight: 1.3
    letterSpacing: "-0.02em"
  row-title:
    fontFamily: "Hanken Grotesk, system-ui, sans-serif"
    fontSize: "18px"
    fontWeight: 600
    lineHeight: 1.4
    letterSpacing: "-0.01em"
  lead:
    fontFamily: "Hanken Grotesk, system-ui, sans-serif"
    fontSize: "17px"
    fontWeight: 400
    lineHeight: 1.5
  body:
    fontFamily: "Hanken Grotesk, system-ui, sans-serif"
    fontSize: "15px"
    fontWeight: 400
    lineHeight: 1.5
    fontFeature: "\"tnum\""
  button:
    fontFamily: "Hanken Grotesk, system-ui, sans-serif"
    fontSize: "15px"
    fontWeight: 600
    lineHeight: 1.5
  label:
    fontFamily: "Hanken Grotesk, system-ui, sans-serif"
    fontSize: "14px"
    fontWeight: 400
    lineHeight: 1.45
  caption:
    fontFamily: "Hanken Grotesk, system-ui, sans-serif"
    fontSize: "12px"
    fontWeight: 400
    lineHeight: 1.25
rounded:
  row: "18px"
  panel: "22px"
  board: "28px"
  pill: "999px"
spacing:
  xs: "8px"
  sm: "12px"
  md: "16px"
  lg: "24px"
  xl: "32px"
  2xl: "40px"
  footer: "64px"
components:
  button-primary:
    backgroundColor: "{colors.team}"
    textColor: "{colors.white}"
    typography: "{typography.button}"
    rounded: "{rounded.pill}"
    padding: "14px 24px"
  button-primary-hover:
    backgroundColor: "{colors.team-deep}"
  button-tone:
    backgroundColor: "{colors.panel-deep}"
    textColor: "{colors.ink}"
    typography: "{typography.button}"
    rounded: "{rounded.pill}"
    padding: "14px 24px"
  button-tone-hover:
    backgroundColor: "{colors.chalk}"
  button-team:
    backgroundColor: "{colors.team}"
    textColor: "{colors.white}"
    typography: "{typography.button}"
    rounded: "{rounded.pill}"
    padding: "12px 20px"
  button-team-hover:
    backgroundColor: "{colors.team-deep}"
  nav-pill:
    backgroundColor: "rgb(255 255 255 / 0.78)"
    textColor: "{colors.ink}"
    rounded: "{rounded.pill}"
    padding: "6px"
  nav-link:
    textColor: "{colors.slate}"
    rounded: "{rounded.pill}"
    padding: "8px 12px"
  nav-link-hover:
    backgroundColor: "{colors.panel}"
    textColor: "{colors.ink}"
  nav-link-active:
    backgroundColor: "{colors.spot}"
    textColor: "{colors.team}"
  board:
    backgroundColor: "{colors.board}"
    rounded: "{rounded.board}"
    padding: "28px 40px 0"
  panel:
    backgroundColor: "{colors.panel}"
    textColor: "{colors.ink}"
    rounded: "{rounded.panel}"
    padding: "40px"
  panel-spot:
    backgroundColor: "{colors.spot}"
    textColor: "{colors.ink}"
    rounded: "{rounded.panel}"
    padding: "24px"
  figure-card:
    backgroundColor: "{colors.white}"
    textColor: "{colors.ink}"
    rounded: "{rounded.panel}"
    padding: "24px"
  index-row:
    backgroundColor: "{colors.panel}"
    textColor: "{colors.ink}"
    rounded: "{rounded.row}"
    padding: "16px 20px"
  index-row-hover:
    backgroundColor: "{colors.panel-deep}"
    textColor: "{colors.team}"
---

# Design System: Espanyol Analytics

## Overview

**Creative North Star: "Pizarra"**

The site is the coach's tactics board the analysis is drawn on. The ground is white enamel; flat panels are pinned on it one tone step apart; data is drawn on them in Espanyol blue for the team and marker red for the opponent, and every verdict (good, middling, bad) is green, amber or red. Surfaces have no borders. Depth comes from one surface overlapping another (the board rises over the stadium photo, a panel sits on the board) and, for exactly two things, from being raised: the frosted pill nav and the headline figure cards.

The board is calm and dense in the way a real tactics board is: large light figures, short factual lines, and strokes that look drawn by hand (round caps, round joins, straight segments). Each screen has at most one light Espanyol-blue panel, the "spot", where the page's signature interaction lives. Exits are numbered only where the order carries information (the depth ladder season, match, possession, player, method).

Pizarra is the new world, built first on the landing (`/`, 24 Oct 2026) and replacing the older design handoff page by page. Light theme only: there is no dark mode. (Until the author's review of 24 Oct 2026 the highlight panel was a dark "night" board with lightened inks; it read as too heavy and became the light-blue spot the same day.)

**Key Characteristics:**
- White enamel board, flat tonal panels, no borders; depth by overlap, plus two raised things (nav, figure cards).
- Blue is identity (Espanyol), red the opponent's marks; green / amber / red are verdicts.
- One light-blue spot panel per screen.
- One face, Hanken Grotesk: figures at 300, text at 400 to 500, headings at 600.
- Pills for every control; generous radii for board, panel and row.
- Data drawn as round-capped marker strokes; a possession draws itself.

## Colors

A near-neutral enamel ground with a faint green-grey cast, Espanyol blue for identity, a marker red for the opponent, one light-blue highlight surface, and a traffic-light set for verdicts.

### Primary
- **Espanyol Marker Blue** (team): ink 1. The team's marks, the emphasised half of a headline, index numbers, links in the byline, the season pill, focus outlines and selection. On a light panel it is the only colour that means "us".

### Secondary
- **Marker Red** (rival): ink 2. The opponent, and negatives (4.6:1 on panel). Never decorative, never a second accent for emphasis.

### Spot (the highlight panel)
- **Spot** (spot): the one light-blue panel per screen (1.14:1 against the board: a highlight, not a contrast jump). Also the Espanyol row band in the league table, the active nav link and the trend readout.
- **Spot Deep** (spot-deep): hover and focus step on spot.
- **Spot Line** (spot-line): pitch markings on spot.
- **Spot Text** (spot-text): secondary text on spot (6.6:1). Primary text on spot is ink; team blue reads 8.1:1 on it.
- **Team Deep** (team-deep): hover of every team-filled control (it replaced the old hover to ink).

### Verdicts
- **Good** (good): positive deltas, top percentile tier, wins, top-six rank chips (4.9:1 on spot, 6.1:1 on white).
- **Fair** (fair, fair-fill): the middle tier and draws. `fair-fill` (amber) for bars and discs, `fair` (dark amber) whenever it is text (4.6:1 on panel).
- **Bad** (bad): negative deltas, bottom tier, losses, bottom-six chips (5.3:1 on spot). Distinct from `rival`, which marks the opponent.

### Neutral
- **White Enamel** (board): the page ground and the nav pill.
- **Pinned Panel** (panel): a panel on the board; index rows; the author block.
- **Deep Panel** (panel-deep): the one tone step for hover and focus, and the fill of secondary (tone) pills.
- **Chalk** (chalk): zero lines, empty tracks, pitch lines on light panels, link underlines at rest, hover of tone pills, the scrollbar thumb.
- **Slate** (slate): secondary text, captions, meta lines (5.2:1 on panel-deep).
- **Ink** (ink): primary text and the primary action pill. Shared with the legacy handoff value, and the only token the two worlds share.
- **White** (white): text on team, good and bad fills; the figure cards and the sub-cards on spot.

### Named Rules
**The Identity / Verdict Rule.** Blue means "Espanyol" and red-orange (rival) means "the opponent"; green, amber and red mean "good, middling, bad". A mark says either who or how good, never both with one colour. No other data hues; distinguish within a colour by fill, opacity, dash or size. (Revised 24 Oct 2026: the author found the verdict colours clearer than a two-ink scheme with slate in the middle.)

**The One Spot Rule.** At most one light-blue spot panel per screen. It is where the signature interaction lives, and its rarity is what makes it read as the place to stop.

**The Tone Step Rule.** Surfaces separate by one lightness step (board, panel, panel-deep, chalk), never by a border. Hover and focus move a surface exactly one step darker.

### Chart colour mapping (for pages that move into Pizarra)

The shared viz components were drawn for the handoff palette. When a page moves out of `app/(handoff)/`, remap its marks like this:

- **Team marks** (shots, passes, touches, actions): team, on light panels and on spot alike.
- **Opponent marks**: rival as an outline only; a solid rival fill only for the opponent's goals.
- **Carries**: team ink, dashed. Not gold: the handoff's gold is retired (amber now means "middling").
- **Heat ramps** (touch, reception, xT surfaces): panel to team, one hue.
- **Deltas and diverging values**: good for positive / in Espanyol's favour, bad for negative.
- **Percentile tiers**: good (≥ 70), fair-fill (middle; the number in `fair`), bad (< 45).
- **Results**: good (win), fair-fill (draw), bad (loss).
- **Pitch markings**: chalk on light panels, spot-line on spot.
- **Zero lines and empty tracks**: chalk.

## Typography

**Display Font:** Hanken Grotesk (with system-ui, sans-serif)
**Body Font:** Hanken Grotesk
**Label/Mono Font:** none; labels are Hanken Grotesk at 13 to 14px in slate, sentence case.

**Character:** One grotesk, used at the extremes: heavy, tightly tracked headings against very light, oversized figures. Numbers are tabular everywhere on a Pizarra page.

Loaded with `next/font/google` (variable font, self-hosted at build time) and exposed as `--font-board`; `.pizarra` sets it as the page face at 15px / 1.5.

### Hierarchy
- **Display** (600, clamp(34px, 3.9vw, 58px), 1.02, -0.035em, balanced wrap): the page's single h1. Two lines of fact; the second may carry the team ink.
- **Figure** (300, clamp(34px, 3.6vw, 56px), 0.9, -0.04em): headline numbers. Units at 0.5em with normal tracking. Smaller figures follow the same light hand: score line 32px, author name clamp(28px, 3vw, 40px), index number and thumb figure 26 to 30px.
- **Headline** (600, 26px, -0.02em): the heading of the spot panel.
- **Title** (600, 22px, -0.02em): section headings on the board.
- **Row title** (600, 18px, -0.01em): index row titles; turns team blue on hover and focus.
- **Lead** (400, 17px, 1.5 to 1.55): the factual line under the h1, author body. Max 56 to 62ch.
- **Body** (400, 15px, 1.5): default text, buttons (at 600). Max 38 to 70ch depending on column.
- **Label** (400 to 500, 14px): meta, status line, captions, nav (500), figure labels (13px on phone).
- **Caption** (400, 12px): thumb captions only.

### Named Rules
**The Light Figure Rule.** Numbers that matter are large and light (300), never bold. Weight goes to the words that name them.

**The No Eyebrow Rule.** No kicker or uppercase label above a heading. Context (season, match count) goes in the line under the heading.

**The en-GB Numbers Rule.** The site is in Spanish, but numbers use the decimal point (`en-GB`, `NUMBER_LOCALE` in `web/lib/format.ts`). This is deliberate; do not hand-format numbers.

## Layout

- **Photo then board.** On the landing, a stadium photo runs edge to edge at clamp(240px, 42vh, 480px), with a top wash (rgb(10 20 40 / 0.38) fading to 0 by 42%). The board rises over its lower edge by 56px (phone) / 112px (lg) and is inset 12px / 24px / 40px each side, with only its top corners rounded.
- **Content width.** Inside the board, content is centred at max 1360px. Board padding 16px / 32px / 40px horizontally, 20px / 28px on top.
- **Grid.** The lead row is two columns at lg: 1.55fr and 1fr (season panel, depth index), gap 24px / 32px. Full-width panels below use 1fr and 2.2fr (heading column, content column). Everything stacks below lg.
- **Rhythm.** Panels stack 24px apart (32px at lg). Panel padding 24px / 32px / 36 to 40px. Index rows sit 8px apart. The footer starts 64px below the last panel.
- **Status corner.** The data's freshness ("Datos hasta J#") is one line locked to the top-right of the board at lg, left-aligned on a phone.
- **Breakpoints.** Tailwind defaults (640, 768, 1024px). Under 640px the latest-match pitch turns vertical (team attacking up); on a phone the nav keeps only the site name and the season pill.

## Elevation & Depth

Flat, with two raised exceptions. Depth is conveyed by one opaque surface overlapping another (photo, board, panel) and by the tone steps between surfaces. Two things are raised, because they must stand out on any ground:

- **The pill nav** (`shadow-float`: 0 1px 2px rgb(18 28 45 / 0.08), 0 14px 36px -10px rgb(18 28 45 / 0.28)), white at 78% with a strong backdrop blur and a hairline `ink/7%` ring. Over the photo, the board and the panels it always reads as a separate object. (Before 24 Oct 2026 it was an opaque board pill and vanished over the board.)
- **The headline figure cards** (`shadow-raised`: 0 1px 2px rgb(18 28 45 / 0.06), 0 8px 24px -6px rgb(18 28 45 / 0.12)), white with an `ink/6%` ring.

### Named Rules
**The Two Raised Things Rule.** Only the nav and the headline figures cast a shadow. Anything else that must read as "above" overlaps, or sits as a white card on the spot panel.

## Shapes

Large, soft rectangles and full pills. The board has 28px top corners; panels 22px; index rows 18px; every control is a full pill (999px). There are no borders on surfaces (the raised things carry a hairline ring, not a border); the only lines are data and pitch markings. Data strokes are round-capped and round-joined, drawn as straight segments between real points (no smoothing). The one arrow is a drawn 16px stroke (width 2, round caps), the same hand as the data, never a glyph.

## Components

### Buttons
Tactile pills, solid fills, colour-only hover. Nothing is filled black: the author found ink fills too heavy (24 Oct 2026).
- **Shape:** full pill (999px).
- **Primary:** team fill, white 15px/600 text, 14px by 24px padding; hover to team-deep. The main action ("Ver la temporada", "Ver el partido completo", "Ver los N partidos"), with a trailing arrow.
- **Tone (secondary):** panel-deep fill, ink text, same size; hover to chalk. Second actions and outbound links.
- **Team:** team fill, white text, 12px by 20px; hover to team-deep. The season pill in the nav, LinkedIn in the author block.
- **Switch:** two pills in a spot track; the pressed one team-filled (`aria-pressed`), the other team text, hover spot-deep. The trend chart's xG / xT.
- **Focus:** a 2px team outline, offset 3px.
- **Motion:** colour transition only (Tailwind default 150ms).

### Navigation
- **Pill nav:** one frosted, raised pill (see Elevation) fixed 12px (20px from sm) from the top, centred, max width viewport minus 24px. On the left the mark (a 30px team disc with a dotted chalk run ending in a dot) and the site name at 600; built sections as 14px/500 slate links (8px by 12px, hover panel with ink text), shown from md; the season as a team pill on the right. Unbuilt sections are omitted, not shown inert. The current section (`active`) is marked `aria-current` and sits on spot in team ink.
- **Season pills:** under a page's h1 when a club has more than one season: the current one a team pill (`aria-current`), the others tone pills. 14px/600, 8px by 16px.

### Cards / Containers
- **Board:** the page sheet; board colour, 28px top radius.
- **Panel:** panel colour, 22px radius, 24 / 32 / 40px padding, no border, no shadow.
- **Spot panel:** spot colour, 22px radius, ink and spot-text type. One per screen. On the season page it holds white sub-cards (18px radius) to separate its parts.
- **Figure card:** white, 22px radius, raised (see Elevation). Only for headline figures.

### Depth index (signature)
A numbered list of whole-row links, 01 to 05. Each row: a light 26px team-ink number, an 18px/600 title with a one-line slate dek, and a small real thumbnail on the right (76px, 112px from sm). Panel fill, 18px radius, 16px by 20px padding. Hover and keyboard focus step the row to panel-deep, turn the title team blue and slide in the arrow (200ms opacity and translate). An unbuilt row renders inert with "Pronto" in place of the thumbnail.

### Numbered exits
The foot of every page but the landing (`components/shell/Exits`): the depth index's five levels as tiles, stacked on a phone and five across at lg, without thumbnails (they would repeat the page's own charts). Same number, title, dek, hover and focus as the depth index. The current level sits on panel-deep with "Estás aquí" and `aria-current`; an unbuilt one is inert with "Pronto".

### Season page blocks (screen 01, 24 Oct 2026)
- **Figures:** five raised figure cards (2 / 3 / 5 across; the fifth spans both columns on a phone). Each: a rank chip (12px/600 pill: good on good/12% in the top six, bad on bad/10% in the bottom six, slate on panel between), the name (500, ink), the light figure, the secondary value (slate). Only "%" is printed as a unit.
- **Reading card:** a full-width panel; on the left a drawn team-blue quotation mark, the heading and the provenance line; on the right the paragraphs at 17px, in two columns from xl so short and long readings both fill the card.
- **League table:** no rules; 14px rows, tabular. A 3px zone bar is good (1 to 4), good at 45% (5), bad (relegation). Signed differences good / bad, also on the club's own band (spot, full pill radius). Phones drop the PJ G E P columns.
- **Form strip (white card on spot):** one link per match: opponent and venue (12px slate), a 28px result disc with the table's letter (G good, E fair-fill with ink letter, P bad), the score (16px), a pair of 4px round-capped bars (xG for in team, against in rival, one scale for the strip, never under 2.0), the xG difference (12px, good or bad). Five to a row on a phone. Hover and focus: spot.
- **Trend chart (`SeasonTrend`, white card on spot):** an xG / xT switch; one bar per match (the net value; good above zero, bad below, 38% opacity, 90% when selected, on a spot column); the rolling mean as a 3px team line ending in an 11px dot; chalk ticks and a slate zero line; labels in HTML at 12px; a fixed CSS height (200 / 230px) with non-scaling strokes. A spot readout card names the selected match (opponent, venue, score, result, for–against with the net, the rolling mean). Hover or tap a column, or Tab to the chart and use the arrow keys; the latest match is selected by default; the readout is `aria-live`.
- **Percentile track:** a 6px round-capped chalk stroke with the fill in the tier colour (good ≥ 70, fair-fill, bad < 45); the percentile number in good / fair / bad, "—" with an empty track when null.
- **Leader rows:** row radius, 15px; the leader is a team row in white (hover team-deep), the rest step to panel-deep.

### Match list blocks (24 Oct 2026)
- **Latest match (spot panel):** heading, meta line (matchday, date, venue), a 32px result disc with the scoreline at figure-medium (home–away order), the xG for–against at 32px light (for in team, against in rival) with the net in good / bad, the shot legend, the primary pill. Beside it `MatchShots`: every shot of the match on a spot-line pitch, `ChainPitch`'s encoding, static (one labelled image; nothing to focus).
- **Match rows (`MatchIndex`):** whole-row links on panel, 18px radius, 8px apart, newest first. Matchday · date · venue in 14px slate over the opponent as the 18px/600 row title; a 28px result disc and the score at 22px light; a flat pair of 4px round-capped xG strokes (for over against, 120px, one scale for the list, floor 2.0) with the values in slate and the net in good / bad; a 112px Espanyol shot thumb (lg only). Hover and focus as the depth index (panel-deep, title team, arrow slides in). On a phone the xG line drops under the title.

### Match page blocks (screen 02, 24 Oct 2026)
- **Header:** h1 = scoreline home first, then the result read against the xG in team blue; one slate line (competition, matchday, venue, date, half time); the match picker (a tone pill around a native select) and "Todos los partidos" top-right.
- **Figures:** five raised figure cards; the club's value in team ink, the rival's under it in slate ("ELC 0.75"). No rank chip: one match has no rank.
- **Explorer (the spot panel, `MatchExplorer`):** a map switch (pressed pill team-filled in a spot-deep track); the defence map adds type filter pills (pressed = white). The map on a spot-line pitch; a white readout card under it (`aria-live`), the legend in spot-text beside it; the opened chain on a white card (heading, meta line, `SequenceDetail`, five light numbers, "Acción por acción" as a disclosure); every chain below on a white card, rows step to panel on hover, the opened one on spot in team. Marks: one tab stop per map, arrows walk them in match order, Enter opens; keyboard focus is a solid ink ring, the opened mark a dotted ink ring.
- **Mark encodings:** shots as the chain pitch; progressive pass a solid team arrow, carry a dotted team arrow, failed at 0.32 opacity; defensive actions team diamonds, filled when won, outline when lost; the average line dashed slate.
- **Totals:** 20px light values either side of a 14px slate label, over a 6px chalk track holding two round strokes (team / rival) that meet at the split; 1 / 2 / 3 columns; which side is which said in the caption.
- **Player boxes:** panels of six rows; abbreviation in team or rival, name 15px/500, light figure; the leader row white.
- **Chart adapter `.pizarra-viz`:** wraps the shared charts (Momentum, PassNetwork, ZoneHeatmap, SequenceDetail, VizDefs) and remaps the legacy tokens they name to Pizarra values (blue and gold to team, opp to rival, mid and faint to slate, pitch, grid, line and track to chalk, card to the ground: white, or panel with `.on-panel`; all faces to Hanken). Transitional: it goes with the legacy tokens.

### Marker strokes and thumbnails
- **xG stroke:** a chalk zero line (1.5) and one team stroke through each match's value, round caps, ending in a dot on the latest match. Lead size 220 by 64 (stroke 3.5, dot 5.5); thumb 112 by 32 (2.5, 3.5). Symmetric around zero, never flatter than plus or minus 1.
- **Pitch thumbs:** chalk markings; shots as team circles (filled for goals); a chain as a 2px team stroke ending in a dot.

### Chain pitch (signature interaction)
On the spot panel: every shot of the latest match on a spot-line pitch, sized by xG (area proportional), solid for goals, half fill (0.45) on target, hollow off target. Team shots in team are focusable buttons; rival shots in rival are outline-only context (solid only for goals). Hover, focus or tap on a team shot draws its possession as one team marker stroke that draws itself (stroke-dashoffset 1 to 0, 0.9s, cubic-bezier(0.16, 1, 0.3, 1); no animation under reduced motion). The highest-xG chain is drawn by default. The selected shot gets an ink outline; keyboard focus an ink ring drawn inside the figure. Shots with no drawable chain are dimmed to 0.4 and are not controls.

### Footer
Provenance at 14px slate in two columns (1.55fr, 1fr): source, metrics note, unaffiliated line, the CC BY photo credit (only on a page that shows the photo: `photoCredit`); the author with links and the build stamp on the right. Links are ink with a chalk underline that turns team on hover.

## Do's and Don'ts

### Do:
- **Do** put `.pizarra` on the root element of every page that moves into this world; it sets the face, size, tabular numbers, selection and focus.
- **Do** use only the Pizarra tokens (board, panel, panel-deep, chalk, slate, ink, team, team-deep, rival, spot, spot-deep, spot-line, spot-text, good, fair, fair-fill, bad) and the board / panel / row / pill radii.
- **Do** separate surfaces by one tone step and show hover and focus as one step darker.
- **Do** draw data as round-capped strokes, blue / red for who and green / amber / red for how good, and remap legacy charts with the chart colour mapping above.
- **Do** name the author on every Pizarra page and credit the stadium photo (Elemaki, CC BY 3.0) wherever it appears.
- **Do** keep numbers formatted through `web/lib/format.ts` (en-GB).
- **Do** respect `prefers-reduced-motion`: drawn strokes appear without animating.

### Don't:
- **Don't** use borders on surfaces, or shadows on anything but the nav and the figure cards.
- **Don't** use a black (ink) fill for a control or a highlight; use team.
- **Don't** add data hues beyond identity and verdicts; carries are dashed team ink, not gold.
- **Don't** use more than one spot panel on a screen, or add a dark mode.
- **Don't** put kickers or uppercase eyebrows above headings, or decorative section numbers; numbers are allowed only where order carries meaning.
- **Don't** use Unicode glyphs as icons; use the drawn arrow.
- **Don't** use the legacy handoff tokens on a Pizarra page (see below).

### Legacy handoff tokens (do not use on Pizarra pages)
`web/app/globals.css` still declares the design handoff's tokens (paper, card, line, mid, faint, blue, blue2, pos, neg, gold, track, opp, opp-strong, pitch, grid, on-dark, navy; the Outfit / Figtree / DM Mono faces; the h1 / kpi / h2card / h3 / narrative sizes; the card, chip and hero radii; and the `.card`, `.eyebrow`, `.label`, `.dek` classes). They exist only for the pages still under `web/app/(handoff)/`, for which `web/design_handoff_espanyol_analytics/README.md` remains the authority until each page moves. They are legacy: never use them on a Pizarra page (the `.pizarra-viz` adapter, which redefines them for the shared charts, is the one sanctioned exception), and delete them when the `(handoff)` folder is empty.
