/**
 * Crest — a club's crest in a square box, or its colour chip when no crest
 * file has been supplied.
 *
 * Renders: the crest image centred and contained in a `size` px square
 * (crests are taller than wide, or wider than tall; the box keeps a column
 * of them aligned), or a round chip in the club's colour with the
 * abbreviation in white. Decorative either way (`alt=""`, `aria-hidden`):
 * the club's name always sits beside it, or the link around it names it.
 * Props: `crest` (path or null), `color`, `abbr`, `size` in px (default 20)
 *        or a CSS length such as `"0.8em"` to scale with a heading,
 *        `className` for position.
 * Data:  `crest` / `color` (or `opponent_crest` / `opponent_color`) on the
 *        table, overview and match files (md/WEB_DATA.md §3.8), from
 *        `src/export/config.py` CLUB_COLOURS and `public/brand/crests/`.
 *
 * Crests belong to their clubs; the footer's no-affiliation note says so
 * (WEB_PLAN.md §10.1, reversing §9.4 C on 1 Nov 2026).
 */

export function Crest({
  crest,
  color,
  abbr,
  size = 20,
  className = "",
}: {
  crest: string | null;
  color: string;
  abbr: string;
  size?: number | string;
  className?: string;
}) {
  const box = { width: size, height: size };

  if (crest) {
    return (
      <span aria-hidden="true" className={`inline-flex shrink-0 items-center justify-center ${className}`} style={box}>
        {/* eslint-disable-next-line @next/next/no-img-element -- static export, no optimiser */}
        <img src={crest} alt="" loading="lazy" decoding="async" className="max-h-full max-w-full object-contain" />
      </span>
    );
  }

  return (
    <span
      aria-hidden="true"
      className={`inline-flex shrink-0 items-center justify-center rounded-full font-semibold tracking-[-0.02em] text-white ${className}`}
      style={{ ...box, background: color }}
    >
      {/* The type size sits on an inner span: on the box itself it would
          change what `em` means for an `em` size, shrinking the chip. */}
      <span style={{ fontSize: typeof size === "number" ? Math.max(7, Math.round(size * 0.32)) : `calc(${size} * 0.32)` }}>
        {abbr}
      </span>
    </span>
  );
}
