/**
 * Pitch — the football pitch, drawn once, in either orientation.
 *
 * Renders: an SVG in metric pitch coordinates (105 x 68) with the design's
 * markings, and two slots for whatever is drawn on it.
 * Props: `orientation` ("horizontal" attacking right, "vertical" attacking
 * up), `underlay` (drawn *beneath* the lines), `children` (drawn *above*),
 * `lineColor` / `lineOpacity` for the heat surfaces that redraw the markings
 * in the card colour, `title` for screen readers.
 * Data:  none. Every layer component supplies its own.
 *
 * The two slots are the z-order ladder of the espanyol-viz-design skill (§5)
 * expressed as element order: heat fills and zone rectangles (z 1-3) go in
 * `underlay`, pitch lines are z 4, and marks, arrows and player nodes (z 5+)
 * go in `children`. Nothing else needs to think about stacking.
 *
 * Every chart on the site composes on top of this component; no chart redraws
 * pitch lines of its own (espanyol-viz-design §11). The geometry — including
 * the vertical flip that keeps the right flank on the right — lives in
 * `lib/viz/pitch.ts`, which is worth reading before any spatial work.
 *
 * There is no title, legend or axis label inside the SVG. Furniture is HTML
 * around it, so type stays at real CSS pixel sizes whatever the viewBox scales
 * to (handoff, "Implementation notes"; espanyol-viz-design §11).
 */

import type { ReactNode } from "react";

import {
  PITCH_LINE_WIDTH,
  PITCH_VIEWBOX,
  pitchMarkings,
  type Orientation,
} from "@/lib/viz/pitch";

interface PitchProps {
  orientation?: Orientation;
  /** Beneath the pitch lines: heat fills, zone rectangles. */
  underlay?: ReactNode;
  /** Above the pitch lines: marks, arrows, nodes, traces. */
  children?: ReactNode;
  /** Handoff default `#D9C9B6`. A heat layer passes ink + `cased`. */
  lineColor?: string;
  lineOpacity?: number;
  /** Draw a light halo under every marking — see `PitchLines`. */
  cased?: boolean;
  className?: string;
  /** Announced to screen readers in place of the drawing. */
  title: string;
}

export function Pitch({
  orientation = "horizontal",
  underlay,
  children,
  lineColor = "var(--color-pitch)",
  lineOpacity = 1,
  cased = false,
  className,
  title,
}: PitchProps) {
  return (
    <svg
      viewBox={PITCH_VIEWBOX[orientation]}
      width="100%"
      className={className}
      style={{ display: "block" }}
      role="img"
    >
      <title>{title}</title>

      {underlay}

      <PitchLines
        orientation={orientation}
        color={lineColor}
        opacity={lineOpacity}
        cased={cased}
      />

      {children}
    </svg>
  );
}

/**
 * Just the markings, as a `<g>`.
 *
 * Exported for the one component that has to own its own `<svg>` element:
 * `ShotMap`, whose tooltip is an HTML sibling inside a positioned wrapper.
 * Everything else composes on `<Pitch>` and never touches this.
 *
 * `cased` — WHY THIS EXISTS. On a bare pitch the markings sit on one known
 * background, so one colour works. On a heat layer they cross every value of
 * the ramp, from an untouched cell (the card colour) to the hottest one (deep
 * blue), and NO single flat colour is legible against both ends: a light line
 * disappears on the empty half of the pitch, a dark one disappears under the
 * hot cells. The first pass here drew them in the card colour, which is why
 * the penalty areas vanished wherever there was no heat.
 *
 * The fix is the mapmaker's one: a *casing*. Each marking is stroked twice —
 * a wide, light halo, then the real line, dark, on top of it. Over an empty
 * cell the halo is invisible against the card and you simply see a dark line;
 * over a hot cell the halo is what carries the contrast and you see a bright
 * band with a dark core. Legible at every point of the ramp, with one
 * mechanism and no extra hue spent.
 */
export function PitchLines({
  orientation,
  color = "var(--color-pitch)",
  opacity = 1,
  cased = false,
}: {
  orientation: Orientation;
  color?: string;
  opacity?: number;
  /** Halo the markings so they survive a heat fill of any strength. */
  cased?: boolean;
}) {
  const markings = pitchMarkings(orientation);
  const width = PITCH_LINE_WIDTH[orientation];

  const shapes = (
    <>
      {markings.rects.map((r, i) => (
        <rect key={i} x={r.x} y={r.y} width={r.width} height={r.height} />
      ))}
      {markings.lines.map((l, i) => (
        <line key={i} x1={l.x1} y1={l.y1} x2={l.x2} y2={l.y2} />
      ))}
      {markings.circles.map((c, i) => (
        <circle key={i} cx={c.cx} cy={c.cy} r={c.r} />
      ))}
    </>
  );

  return (
    <>
      {cased && (
        <g
          fill="none"
          stroke="var(--color-card)"
          strokeOpacity={0.85}
          // Three times the line: enough halo to read against a full-strength
          // cell, still thin enough that the pitch does not look inflated.
          strokeWidth={width * 3}
          strokeLinejoin="round"
        >
          {shapes}
        </g>
      )}
      <g fill="none" stroke={color} strokeOpacity={opacity} strokeWidth={width}>
        {shapes}
      </g>
    </>
  );
}
