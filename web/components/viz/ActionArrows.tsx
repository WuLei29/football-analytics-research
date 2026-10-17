/**
 * ActionArrows — on-ball actions as arrows, each with its own weight.
 *
 * Renders: one line per action from its start to its end, in one of three
 * tones (`blue`, `gold`, `faint`), solid when the action came off and dashed
 * when it did not, with or without an arrowhead.
 * Props: `actions` (`ArrowMark[]`), `orientation`, `title`. `ArrowLayer` is
 * the same marks as a bare `<g>`, for a caller that owns its `<Pitch>`.
 * Data:  `players/{player_id}.json` -> `maps.passes`, `maps.progressive_passes`,
 *        `maps.key_passes`, `maps.carries` (README-PLAYER §3b) and
 *        `matches/{match_id}.json` -> `progression` (WEB_DATA §7), all from
 *        `silver.events` in the team's attacking frame.
 *
 * The generalisation of `ProgressionArrows`, extracted on its third caller
 * (WEB_PLAN §3.2): the match page's progression block, and the player page's
 * pass map, progressive passes, carries and key passes. The tone and weight
 * are per arrow because the player's pass map draws two populations at once —
 * progressive passes loud, the rest quiet and headless (README-PLAYER §3b) —
 * and a head on two hundred grey lines is the loudest thing in the figure.
 *
 * Requires `<VizDefs />` on the page: the heads are its shared markers, one
 * per tone, never an inline `<marker>`.
 *
 * `hoverable` (16 Oct 2026, the player page's hover cards): each arrow also
 * gets a wide transparent hit line carrying `data-i`, its index in
 * `actions`, so the caller listens once on a parent `<g>` instead of
 * binding a handler per arrow. `highlight` redraws one arrow on top.
 */

import { project, type Orientation } from "@/lib/viz/pitch";

import { Pitch } from "./Pitch";

export interface ArrowMark {
  x: number;
  y: number;
  end_x: number;
  end_y: number;
  tone: "blue" | "gold" | "faint";
  /** Stroke width in metres. */
  width: number;
  opacity: number;
  /** False draws the line dashed. */
  completed: boolean;
  /** Default true. */
  head?: boolean;
}

const STROKE = {
  blue: "var(--color-blue)",
  gold: "var(--color-gold)",
  faint: "var(--color-faint)",
} as const;

const MARKER = {
  blue: "url(#viz-arrow-blue)",
  gold: "url(#viz-arrow-gold)",
  faint: "url(#viz-arrow-faint)",
} as const;

/** Width of the invisible hit line, in metres: a 0.3 m arrow is no target. */
const HIT_WIDTH = 1.6;

function Arrow({
  a,
  orientation,
  emphasis = false,
}: {
  a: ArrowMark;
  orientation: Orientation;
  emphasis?: boolean;
}) {
  const from = project(orientation, a.x, a.y);
  const to = project(orientation, a.end_x, a.end_y);
  // A hovered grey pass is redrawn blue: the quiet population has to be
  // findable once it is the one being read.
  const tone = emphasis && a.tone === "faint" ? "blue" : a.tone;
  return (
    <line
      x1={from.x}
      y1={from.y}
      x2={to.x}
      y2={to.y}
      stroke={STROKE[tone]}
      strokeWidth={emphasis ? Math.max(a.width * 1.8, 0.5) : a.width}
      strokeOpacity={emphasis ? 1 : a.opacity}
      strokeDasharray={a.completed ? undefined : "1.2 1"}
      markerEnd={a.head === false && !emphasis ? undefined : MARKER[tone]}
    />
  );
}

export function ArrowLayer({
  actions,
  orientation = "horizontal",
  hoverable = false,
  highlight = null,
}: {
  actions: ArrowMark[];
  orientation?: Orientation;
  hoverable?: boolean;
  /** Index into `actions` of the arrow to redraw loud, on top. */
  highlight?: number | null;
}) {
  const hot = highlight === null ? null : (actions[highlight] ?? null);
  return (
    <g>
      {actions.map((a, i) => (
        <Arrow key={i} a={a} orientation={orientation} />
      ))}
      {hot && <Arrow a={hot} orientation={orientation} emphasis />}
      {hoverable &&
        actions.map((a, i) => {
          const from = project(orientation, a.x, a.y);
          const to = project(orientation, a.end_x, a.end_y);
          return (
            <line
              key={i}
              data-i={i}
              x1={from.x}
              y1={from.y}
              x2={to.x}
              y2={to.y}
              stroke="transparent"
              strokeWidth={HIT_WIDTH}
              strokeLinecap="round"
              style={{ pointerEvents: "stroke", cursor: "pointer" }}
            />
          );
        })}
    </g>
  );
}

export function ActionArrows({
  actions,
  orientation = "horizontal",
  title,
  className,
}: {
  actions: ArrowMark[];
  orientation?: Orientation;
  title: string;
  className?: string;
}) {
  return (
    <Pitch orientation={orientation} title={title} className={className}>
      <ArrowLayer actions={actions} orientation={orientation} />
    </Pitch>
  );
}
