/**
 * scale.ts — the small amount of maths the charts need.
 *
 * WEB_PLAN.md §3 names `d3-scale` + `d3-shape` as the chart toolkit. Neither
 * is installed, and this file is why: every chart the design specifies is
 * straight-line geometry on a linear scale — the rolling charts are polylines
 * between matchdays, the momentum is a polyline between minutes, the pitch
 * layers are metres mapped 1:1 into SVG units. `d3-shape` earns its place when
 * you need curve interpolation, stacking or arcs; none of the eleven blocks of
 * screens 01 and 02 does. Two dependencies for `(v - d0) / (d1 - d0)` is not a
 * trade worth making, and a reader can follow this file end to end.
 *
 * If a later screen needs a real curve, an arc or a stack, install `d3-shape`
 * then and use it there. This file is not an argument against that.
 */

/**
 * A linear map from a data range onto an output range, the `d3.scaleLinear`
 * shape without the library. Not clamped: a value outside the domain maps
 * outside the range, which is what a chart wants — a clipped point silently
 * lies about the data.
 */
export function linear(
  domain: readonly [number, number],
  range: readonly [number, number],
): (value: number) => number {
  const [d0, d1] = domain;
  const [r0, r1] = range;
  const span = d1 - d0;
  // A zero-width domain is a single repeated value; put it in the middle
  // rather than dividing by zero and painting NaN into the DOM.
  if (span === 0) return () => (r0 + r1) / 2;
  return (value) => r0 + ((value - d0) / span) * (r1 - r0);
}

/**
 * Trailing rolling mean over `window` samples. The first `window - 1` outputs
 * are `null`, not zero: before the fifth match there is no five-match mean,
 * and a zero there would draw a line the data does not support.
 *
 * Used for the momentum chart's 5-minute window, which the export deliberately
 * leaves to the client (WEB_DATA §7.1).
 */
export function rollingMean(values: readonly number[], window: number): (number | null)[] {
  const out: (number | null)[] = [];
  let sum = 0;
  for (let i = 0; i < values.length; i++) {
    sum += values[i];
    if (i >= window) sum -= values[i - window];
    out.push(i >= window - 1 ? sum / window : null);
  }
  return out;
}

/** `[{x, y}, ...]` -> `"M… L… L…"`. Empty input gives an empty path. */
export function linePath(points: readonly { x: number; y: number }[]): string {
  return points
    .map((p, i) => `${i ? "L" : "M"}${p.x.toFixed(2)},${p.y.toFixed(2)}`)
    .join(" ");
}

/**
 * One half of a diverging area: the region between the series and its zero
 * line, on one side only. `side` is +1 for the part above zero, -1 for below.
 *
 * The design's own prototype clamps each sample to its side and joins the
 * clamped points, which makes a crossing look like a step: the positive area
 * runs flat along zero all the way to the next sample before the negative one
 * starts. We interpolate the crossing instead, so the two areas meet at the
 * point where the series actually crosses zero. Same shape everywhere else,
 * and honest at the crossings — which on a rolling xG-difference chart are the
 * most-read part of the figure.
 *
 * Coordinates are already in SVG units; `zeroY` is the y of the zero line.
 */
export function divergingAreaPath(
  points: readonly { x: number; y: number }[],
  zeroY: number,
  side: 1 | -1,
): string {
  if (points.length < 2) return "";

  // In SVG y grows downward, so "above zero" is y < zeroY.
  const inside = (p: { y: number }) => (side > 0 ? p.y <= zeroY : p.y >= zeroY);
  /** Where the segment a->b meets the zero line. */
  const crossing = (a: { x: number; y: number }, b: { x: number; y: number }) => ({
    x: a.x + ((zeroY - a.y) / (b.y - a.y)) * (b.x - a.x),
    y: zeroY,
  });

  const segments: { x: number; y: number }[][] = [];
  let current: { x: number; y: number }[] = [];

  for (let i = 0; i < points.length; i++) {
    const p = points[i];
    const prev = points[i - 1];
    if (inside(p)) {
      if (prev && !inside(prev)) current.push(crossing(prev, p));
      current.push(p);
    } else {
      if (prev && inside(prev)) current.push(crossing(prev, p));
      if (current.length > 1) segments.push(current);
      current = [];
    }
  }
  if (current.length > 1) segments.push(current);

  // One closed subpath per excursion, each dropped back down to the zero line.
  return segments
    .map((seg) => {
      const first = seg[0];
      const last = seg[seg.length - 1];
      return `M${first.x.toFixed(2)},${zeroY} ${seg
        .map((p) => `L${p.x.toFixed(2)},${p.y.toFixed(2)}`)
        .join(" ")} L${last.x.toFixed(2)},${zeroY} Z`;
    })
    .join(" ");
}

/* --------------------------------------------------------------------------
 * Heat ramps
 * ------------------------------------------------------------------------ */

/**
 * The opacity band every heat layer uses: `fill-opacity` 0.06 to 0.92 of the
 * accent colour (handoff, screen 02 block 4). The floor keeps an empty-but-
 * present cell visible; the ceiling keeps the pitch lines readable through the
 * hottest one. v1 compressed its ramp for exactly the same two reasons
 * (espanyol-viz-design §2).
 */
export const HEAT_OPACITY: readonly [number, number] = [0.06, 0.92];

/**
 * Value -> fill-opacity, scaled against the maximum cell of the same layer.
 * Always relative: an absolute scale would make a quiet match look like an
 * empty one. `max <= 0` means nothing to draw.
 */
export function heatOpacity(value: number, max: number): number {
  if (max <= 0) return 0;
  const [lo, hi] = HEAT_OPACITY;
  return lo + (hi - lo) * Math.max(0, Math.min(1, value / max));
}

/**
 * A symmetric tick ladder around zero that just contains `peak`, the largest
 * absolute value of a diverging series.
 *
 * The step is the smallest of a short list of round numbers that needs at
 * most `maxTicks` ticks per side, so a ±0.9 xGD series gets `0.5` steps and a
 * ±2.9 xT series gets `1`. `decimals` is how many places that step needs, so
 * the labels can print "0.5" and "0.25" but never "1.0".
 *
 * The chart draws the top and bottom tick at its edges, which is what makes
 * the series fill the plot: the scale is the data's, not a fixed ladder that
 * clips a good month (WEB_PLAN.md §9.7).
 */
export function symmetricTicks(
  peak: number,
  maxTicks = 3,
): { ticks: number[]; max: number; decimals: 0 | 1 | 2 } {
  const STEPS = [0.05, 0.1, 0.2, 0.25, 0.5, 1, 2, 5, 10];
  const safePeak = peak > 0 ? peak : 1;
  const step = STEPS.find((s) => safePeak / s <= maxTicks) ?? STEPS[STEPS.length - 1];
  const count = Math.max(1, Math.ceil(safePeak / step - 1e-9));
  const max = count * step;
  const decimals = step % 1 === 0 ? 0 : step * 10 === Math.round(step * 10) ? 1 : 2;
  const ticks: number[] = [];
  // Rounded, so 3 * 0.1 is 0.3 and not 0.30000000000000004 as a React key.
  for (let i = -count; i <= count; i += 1) ticks.push(Number((i * step).toFixed(decimals)));
  return { ticks, max, decimals };
}
