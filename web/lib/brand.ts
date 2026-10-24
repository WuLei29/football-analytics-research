/**
 * brand.ts — the two image slots of the design, and what stands in for them.
 *
 * The handoff's hero has two user-supplied images (`esp-hero`, `esp-badge`).
 * Neither ships (WEB_PLAN.md §9.4 decision C):
 *
 *   - The club crest is a trademark of RCD Espanyol. It is NOT used. A
 *     monogram tile stands in its place, and the methodology page states that
 *     the site is unaffiliated.
 *   - The hero photo may only be one whose rights you hold — taken by you, or
 *     Creative Commons with attribution. `web/resources/banner.jpg` has
 *     unconfirmed provenance and is deliberately not wired up.
 *
 * The layout works with either slot empty: with no photo the hero renders on
 * the navy ground the scrim was designed over, which is the intended fallback,
 * not a degraded state.
 *
 * To add a photo you own: drop it in `web/public/brand/`, set `heroPhoto` to
 * `"/brand/<file>"`, and fill `heroPhotoCredit`.
 */

export const brand = {
  /** `"/brand/hero.jpg"` once you have one you own. `null` = navy ground. */
  heroPhoto: null as string | null,

  /** Attribution line, required if the photo is Creative Commons. */
  heroPhotoCredit: null as string | null,

  /** Deliberately null. See the note above — do not point this at the crest. */
  crest: null as string | null,

  /** What the crest slot shows instead: four letters on the accent blue. */
  monogram: "RCDE",

  /**
   * The stadium photo of the Pizarra world (landing, 24 Oct 2026). Kept apart
   * from `heroPhoto` so the handoff masthead does not change underneath the
   * pages that still use it. Two widths of the same file, for `srcSet`.
   *
   * Source: Wikimedia Commons, "EstadioRCDE Pano.jpg" by Elemaki, CC BY 3.0
   * (https://commons.wikimedia.org/wiki/File:EstadioRCDE_Pano.jpg), resized
   * from 3978 px after cropping 4% off the left and 6.5% off the right (two
   * white video boards in the corners read as broken UI tiles). CC BY
   * requires the credit line below wherever it is shown.
   */
  stadiumPhoto: {
    src: "/brand/estadio-rcde-2400.jpg",
    srcSet: "/brand/estadio-rcde-1200.jpg 1200w, /brand/estadio-rcde-2400.jpg 2400w",
    width: 2400,
    height: 1409,
    credit: "Foto: Elemaki · CC BY 3.0 · Wikimedia Commons",
    creditHref: "https://commons.wikimedia.org/wiki/File:EstadioRCDE_Pano.jpg",
  },

  /**
   * The author, named on every page of the new world (PRODUCT.md, "Brand
   * Commitments"; critique of 22 Oct 2026, P0). Proper names and URLs, not
   * copy, so they live here rather than in `labels.ts`.
   */
  author: {
    name: "Jaume Blanco Aumatell",
    linkedin: "https://www.linkedin.com/in/jaume-blanco-aumatell-317403224/",
    github: "https://github.com/WuLei29",
    repo: "https://github.com/WuLei29/football-analytics-research",
  },
} as const;
