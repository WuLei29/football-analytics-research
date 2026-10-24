# `public/brand/`

The site's images. Nothing ships here whose rights are not clear.

| File / slot | Status | Why |
|---|---|---|
| `estadio-rcde-2400.jpg`, `estadio-rcde-1200.jpg` | **shipped** (24 Oct 2026) | The stadium photo of the Pizarra world (landing). Wikimedia Commons, [EstadioRCDE Pano.jpg](https://commons.wikimedia.org/wiki/File:EstadioRCDE_Pano.jpg) by Elemaki, **CC BY 3.0**, cropped (4 % left, 6.5 % right, removing two white video boards) and resized from 3978 px. The licence requires the credit, which the landing footer shows (`lib/brand.ts` → `stadiumPhoto.credit`). The origin is also embedded in each JPEG's comment field. |
| Handoff hero photo (`brand.heroPhoto`) | **empty** | The pages still in the handoff world render their masthead on the navy ground. Left alone on purpose: those pages are redesigned in the new world rather than patched. |
| Club crest | **not shipped, and not coming** | The crest is a trademark of RCD Espanyol. The site states that it is unaffiliated. |

`web/resources/banner.jpg` has unconfirmed provenance and is deliberately not
used anywhere.

To add another photo: only one you took, or Creative Commons with
attribution. Resize it (2400 and 1200 px wide is enough), drop it here, wire it
through `lib/brand.ts` with its credit, and show the credit wherever it appears.
