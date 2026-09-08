# `public/brand/`

The two image slots of the hero. Both are empty on purpose
(`md/WEB_PLAN.md` §9.4 decision C), and the layout is designed to work with
either one missing.

| Slot | Status | Why |
|---|---|---|
| Hero photo | **empty** | Only a photo whose rights you hold may ship — taken by you, or Creative Commons with attribution. `web/resources/banner.jpg` has unconfirmed provenance and is deliberately not wired up. |
| Club crest | **not shipped, and not coming** | The crest is a trademark of RCD Espanyol. A monogram tile stands in its place, and the site states that it is unaffiliated. |

To add a photo you own:

1. Drop the file here, e.g. `public/brand/hero.jpg` (~1600×600, it sits behind
   a dark scrim so detail in the shadows is wasted).
2. In `web/lib/brand.ts`, set `heroPhoto: "/brand/hero.jpg"`.
3. If it is Creative Commons, fill `heroPhotoCredit` with the attribution the
   licence requires.

With `heroPhoto` left `null` the hero renders on the navy ground the scrim was
designed over. That is the intended fallback, not a degraded state.
