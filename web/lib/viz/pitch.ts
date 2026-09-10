/**
 * pitch.ts — the pitch, as geometry. No JSX, no React, no colours.
 *
 * Every spatial component on this site draws into the same coordinate frame:
 * real metres on a 105 x 68 pitch, exactly as `silver.events` stores them and
 * exactly as `src/export/` ships them (md/WEB_DATA.md §3.2). This file is the
 * single place that knows how a metre becomes an SVG unit; a component that
 * hard-codes a pitch number is a bug.
 *
 * Sources, and which one wins where:
 *   - Marking dimensions and the 30-zone grid: the `pitch-guide` skill, which
 *     is the project's geometry authority and matches `gold.pitch_zones`.
 *   - `viewBox`, stroke width, which markings are drawn at all: the design
 *     handoff (`design_handoff_espanyol_analytics/README.md`, "Pitch geometry").
 *   - The 12 x 8 xT surface edges: `src/silver/events/xt.py`, via WEB_DATA §3.3.
 *
 * ---------------------------------------------------------------------------
 * THE ONE THING TO UNDERSTAND HERE: the vertical flip.
 *
 * The data frame is mathematical — y grows *upward*, (0,0) is the bottom-left
 * corner, and every team attacks towards x = 105. SVG's y grows *downward*.
 * So a mark's SVG y is `68 - y`, never `y`.
 *
 * This is not cosmetic. `GOLD_LAYER.md` §4.6.0 settled that **low y is the
 * RIGHT flank** (839:13 on foot preference). Attacking rightwards, your right
 * hand points at low y, which must therefore be drawn at the BOTTOM of the
 * image. Skip the flip and every wing, half-space and cross in the site is
 * mirrored — a mistake that looks perfectly plausible on screen.
 *
 * The pitch markings are symmetric about both axes, so the flip changes
 * nothing about the pitch itself; it only ever shows up in the marks.
 *
 * The handoff writes the vertical transpose as `(x, y) -> (y, 105 - x)`. We
 * use `(x, y) -> (68 - y, 105 - x)` for the same reason: its prototype was
 * filled with random numbers that have no flank, ours are real. The drawn
 * pitch is identical either way.
 * ------------------------------------------------------------------------ */

/* --------------------------------------------------------------------------
 * 1. The pitch, in metres (pitch-guide §2 and §12)
 * ------------------------------------------------------------------------ */

export const PITCH_LENGTH = 105;
export const PITCH_WIDTH = 68;

/** Penalty area: 16.5 m deep, y 13.84 -> 54.16. */
const PENALTY_DEPTH = 16.5;
const PENALTY_Y = [13.84, 54.16] as const;

/** Six-yard box: 5.5 m deep, y 24.84 -> 43.16. */
const SIX_YARD_DEPTH = 5.5;
const SIX_YARD_Y = [24.84, 43.16] as const;

const CENTRE = [PITCH_LENGTH / 2, PITCH_WIDTH / 2] as const;
const CENTRE_CIRCLE_R = 9.15;

/* --------------------------------------------------------------------------
 * 2. Orientation and projection
 * ------------------------------------------------------------------------ */

export type Orientation = "horizontal" | "vertical";

/** A point in SVG user units, ready to go straight into an attribute. */
export interface Point {
  x: number;
  y: number;
}

/**
 * The two frames the design uses, verbatim from the handoff. The padding in
 * the `viewBox` is breathing room for marks that sit on the touchline, not a
 * furniture margin: titles, legends and axis labels are HTML around the SVG,
 * never inside it (espanyol-viz-design §11).
 */
export const PITCH_VIEWBOX: Record<Orientation, string> = {
  horizontal: "-2 -2 109 72",
  vertical: "-2.5 -2.5 73 110",
};

/** Handoff: 0.35 on the horizontal pitch, 0.4 on the vertical. */
export const PITCH_LINE_WIDTH: Record<Orientation, number> = {
  horizontal: 0.35,
  vertical: 0.4,
};

/**
 * Metres -> SVG user units. The only conversion in the codebase.
 *
 * horizontal: attack runs left to right, low y (the right flank) at the bottom.
 * vertical:   attack runs bottom to top, low y (the right flank) at the right.
 */
export function project(o: Orientation, x: number, y: number): Point {
  return o === "horizontal"
    ? { x, y: PITCH_WIDTH - y }
    : { x: PITCH_WIDTH - y, y: PITCH_LENGTH - x };
}

/**
 * The same projection for an axis-aligned box given by two opposite corners in
 * metres. Projecting corners can swap which one is "top-left", so the result
 * is normalised back into the positive width/height SVG needs.
 */
export function projectRect(
  o: Orientation,
  x0: number,
  y0: number,
  x1: number,
  y1: number,
): { x: number; y: number; width: number; height: number } {
  const a = project(o, x0, y0);
  const b = project(o, x1, y1);
  return {
    x: Math.min(a.x, b.x),
    y: Math.min(a.y, b.y),
    width: Math.abs(b.x - a.x),
    height: Math.abs(b.y - a.y),
  };
}

/**
 * Where a point sits inside the `viewBox`, as a 0-1 fraction of each side.
 *
 * For HTML that has to line up with the SVG — the shot tooltip is the only
 * case today — because the SVG scales to its container and CSS pixels do not.
 * Fractions of the *viewBox*, padding included, so the wrapper element the
 * percentages are applied to must be exactly the SVG's box.
 */
export function projectFraction(o: Orientation, x: number, y: number): Point {
  const [vx, vy, vw, vh] = PITCH_VIEWBOX[o].split(" ").map(Number);
  const p = project(o, x, y);
  return { x: (p.x - vx) / vw, y: (p.y - vy) / vh };
}

/* --------------------------------------------------------------------------
 * 3. The markings, as data
 *
 * Only the markings the handoff draws: outline, both penalty areas, both
 * six-yard boxes, the halfway line and the centre circle. The penalty arcs,
 * the spots and the corner arcs exist in `pitch-guide` and are deliberately
 * left out — the design does not draw them, and a chart is not the place to
 * add furniture nobody asked for.
 * ------------------------------------------------------------------------ */

export interface PitchMarkings {
  rects: { x: number; y: number; width: number; height: number }[];
  lines: { x1: number; y1: number; x2: number; y2: number }[];
  circles: { cx: number; cy: number; r: number }[];
}

export function pitchMarkings(o: Orientation): PitchMarkings {
  const halfway = projectRect(o, CENTRE[0], 0, CENTRE[0], PITCH_WIDTH);
  const centre = project(o, CENTRE[0], CENTRE[1]);

  return {
    rects: [
      // Outline.
      projectRect(o, 0, 0, PITCH_LENGTH, PITCH_WIDTH),
      // Penalty areas, left then right.
      projectRect(o, 0, PENALTY_Y[0], PENALTY_DEPTH, PENALTY_Y[1]),
      projectRect(o, PITCH_LENGTH - PENALTY_DEPTH, PENALTY_Y[0], PITCH_LENGTH, PENALTY_Y[1]),
      // Six-yard boxes, left then right.
      projectRect(o, 0, SIX_YARD_Y[0], SIX_YARD_DEPTH, SIX_YARD_Y[1]),
      projectRect(o, PITCH_LENGTH - SIX_YARD_DEPTH, SIX_YARD_Y[0], PITCH_LENGTH, SIX_YARD_Y[1]),
    ],
    lines: [
      { x1: halfway.x, y1: halfway.y, x2: halfway.x + halfway.width, y2: halfway.y + halfway.height },
    ],
    circles: [{ cx: centre.x, cy: centre.y, r: CENTRE_CIRCLE_R }],
  };
}

/* --------------------------------------------------------------------------
 * 4. The two grids
 *
 * They are different grids for different blocks and must not be conflated
 * (WEB_DATA §3.3). The xT surface is the 12 x 8 grid `xt.py` scores on; the
 * zone heatmap is the 6 x 5 grid of `gold.pitch_zones`.
 * ------------------------------------------------------------------------ */

/** xT surface: 12 columns along x, 8 rows along y. Cells are 8.75 x 8.5 m. */
export const XT_COLS = 12;
export const XT_ROWS = 8;

/** The SVG box of one xT cell, addressed the way the export ships it. */
export function xtCellRect(o: Orientation, cx: number, cy: number) {
  const w = PITCH_LENGTH / XT_COLS;
  const h = PITCH_WIDTH / XT_ROWS;
  return projectRect(o, cx * w, cy * h, (cx + 1) * w, (cy + 1) * h);
}

/**
 * The 30-zone grid: 6 longitudinal strips x 5 lateral channels, boundaries on
 * the real markings. Identical to `gold.pitch_zones.x_min/x_max/y_min/y_max`;
 * that table is authoritative and these numbers must never drift from it.
 */
export const ZONE_X_BOUNDARIES = [0, 16.5, 35, 52.5, 70, 88.5, 105];
export const ZONE_Y_BOUNDARIES = [0, 13.84, 24.84, 43.16, 54.16, 68];

/** Channel names, low y first — low y is the RIGHT flank. */
export const ZONE_CHANNELS = [
  "wide_right",
  "half_space_right",
  "center",
  "half_space_left",
  "wide_left",
] as const;

/**
 * The SVG box of one of the 30 zones, from its `zone_id`.
 * `zone_id = x_strip * 10 + y_channel`, strips 1-6, channels 1-5 (GOLD_LAYER
 * §4.1.7), so 43 is strip 4, channel 3 — the centre of the attacking half.
 */
export function zoneRect(o: Orientation, zoneId: number) {
  const strip = Math.floor(zoneId / 10);
  const channel = zoneId % 10;
  if (strip < 1 || strip > 6 || channel < 1 || channel > 5) {
    throw new Error(`zoneRect: ${zoneId} is not a zone_id (expected 11..65)`);
  }
  return projectRect(
    o,
    ZONE_X_BOUNDARIES[strip - 1],
    ZONE_Y_BOUNDARIES[channel - 1],
    ZONE_X_BOUNDARIES[strip],
    ZONE_Y_BOUNDARIES[channel],
  );
}

/**
 * Re-bin a 12 x 8 xT surface onto the 30 zones, by area overlap.
 *
 * WHY THIS EXISTS. The two grids are different grids for different reasons:
 * `xt.py` scores on a uniform 12 x 8 because that is the grid the xT model was
 * fitted on, and `gold.pitch_zones` cuts the pitch on the real markings because
 * that is how a coach talks about it. Showing both on one page invites the
 * reader to compare a half-space in one picture with a rectangle that is not
 * quite a half-space in the other, and they cannot.
 *
 * WHAT IT DOES. Each 8.75 x 8.5 m source cell is split between the zones it
 * covers in proportion to how much of it each one covers, and the pieces are
 * summed. Because xT here is a *total* (`sum(xt)` per cell, WEB_DATA §7), that
 * is the right operator: the grand total is preserved exactly, which a
 * centroid assignment would not do.
 *
 * WHAT IT DOES NOT DO. It cannot recover where inside a source cell the threat
 * actually happened; it assumes it was spread evenly. The cells are 8.75 m
 * long and the narrowest zone is 11 m wide, so the error is real but bounded,
 * and it only ever moves threat between neighbouring zones. The exact version
 * of this picture is an `xt_zones` aggregate computed in SQL straight from
 * `silver.events` with `gold.pitch_zones` — worth adding to the exporter when
 * the match file lands, at which point this function is only needed for old
 * files.
 */
export function xtCellsToZones(
  cells: readonly { cx: number; cy: number; xt: number }[],
): { zone_id: number; value: number }[] {
  const w = PITCH_LENGTH / XT_COLS;
  const h = PITCH_WIDTH / XT_ROWS;
  const totals = new Map<number, number>();

  for (const cell of cells) {
    const [cx0, cx1] = [cell.cx * w, (cell.cx + 1) * w];
    const [cy0, cy1] = [cell.cy * h, (cell.cy + 1) * h];
    const cellArea = w * h;

    for (const zoneId of ZONE_IDS) {
      const strip = Math.floor(zoneId / 10);
      const channel = zoneId % 10;
      const overlapX =
        Math.min(cx1, ZONE_X_BOUNDARIES[strip]) - Math.max(cx0, ZONE_X_BOUNDARIES[strip - 1]);
      const overlapY =
        Math.min(cy1, ZONE_Y_BOUNDARIES[channel]) - Math.max(cy0, ZONE_Y_BOUNDARIES[channel - 1]);
      if (overlapX <= 0 || overlapY <= 0) continue;

      const share = (overlapX * overlapY) / cellArea;
      totals.set(zoneId, (totals.get(zoneId) ?? 0) + cell.xt * share);
    }
  }

  return [...totals].map(([zone_id, value]) => ({ zone_id, value }));
}

/** Every zone_id, in reading order. Useful for drawing an empty grid. */
export const ZONE_IDS: number[] = Array.from({ length: 30 }, (_, i) =>
  (Math.floor(i / 5) + 1) * 10 + (i % 5) + 1,
);
