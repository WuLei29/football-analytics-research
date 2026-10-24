/**
 * XgStroke — a season's xG difference, match by match, as one marker stroke.
 *
 * Renders: an SVG with a chalk zero line and one round-capped stroke through
 * each match's xG for minus xG against, ending in a dot on the latest match.
 * Straight segments, not a curve: there is nothing between two matches to
 * interpolate.
 * Props: `values` (oldest first), `title` for screen readers, `size`
 * ("lead" in the season panel, "thumb" in the index), `ink` (the stroke
 * colour token).
 * Data:  `overview.json` -> `matches[].xg_for - xg_against` (md/WEB_DATA.md
 *        §6). Not the `rolling` series: that starts at matchday 5, and early
 *        in a season it would be a stroke of two points.
 *
 * Server component: nothing here moves.
 */

const SIZES = {
  lead: { w: 220, h: 64, stroke: 3.5, dot: 5.5 },
  thumb: { w: 112, h: 32, stroke: 2.5, dot: 3.5 },
} as const;

export function XgStroke({
  values,
  title,
  size = "lead",
  ink = "var(--color-team)",
  className = "",
}: {
  values: number[];
  title: string;
  size?: keyof typeof SIZES;
  ink?: string;
  className?: string;
}) {
  const { w, h, stroke, dot } = SIZES[size];
  if (values.length === 0) return null;

  // Symmetric around zero, never flatter than ±1 xG, so a quiet start does not
  // look like a wild one.
  const extent = Math.max(1, ...values.map((v) => Math.abs(v)));
  const pad = dot + 1;
  const x = (i: number) =>
    values.length === 1 ? w / 2 : pad + (i * (w - 2 * pad)) / (values.length - 1);
  const y = (v: number) => h / 2 - (v / extent) * (h / 2 - pad);

  const d = values.map((v, i) => `${i === 0 ? "M" : "L"}${x(i).toFixed(1)} ${y(v).toFixed(1)}`).join(" ");
  const last = values.length - 1;

  return (
    <svg viewBox={`0 0 ${w} ${h}`} width={w} height={h} role="img" className={`block h-auto max-w-full ${className}`}>
      <title>{title}</title>
      <line x1={0} y1={h / 2} x2={w} y2={h / 2} stroke="var(--color-chalk)" strokeWidth={1.5} />
      <path d={d} fill="none" stroke={ink} strokeWidth={stroke} strokeLinecap="round" strokeLinejoin="round" />
      <circle cx={x(last)} cy={y(values[last])} r={dot} fill={ink} />
    </svg>
  );
}
