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
} as const;
