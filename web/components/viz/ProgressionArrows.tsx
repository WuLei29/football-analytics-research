/**
 * ProgressionArrows — how the ball was moved forward, as arrows.
 *
 * Renders: one arrow per progressive action, blue for a pass and gold for a
 * carry, on a horizontal pitch attacking right.
 * Props: `actions`, `orientation`, `title`.
 * Data:  `matches/{match_id}.json` -> `progression` (md/WEB_DATA.md §7): the
 *        top 30 progressive passes and carries by xT, from `silver.events`
 *        filtered with `gold.is_progressive`. A carry is the synthesised
 *        `type_id = -1` event.
 *
 * Requires `<VizDefs />` somewhere on the page — that is where the arrowheads
 * are declared.
 *
 * Encoding (handoff, screen 02 block 5): pass 0.28 wide at 0.55 opacity, carry
 * 0.42 at 0.9. The handoff specifies 0.45 and 0.7; both were thinned, and the
 * shared arrowhead shrunk with them, because thirty arrows at the handoff
 * width merge into each other on a busy match. Carries are still the louder
 * mark, because there are fewer of them and they are the more interesting
 * event; gold is the token the design reserves for carries everywhere.
 *
 * A failed action is drawn dashed rather than dropped: the design's list is
 * "the 30 most valuable progressions", and one that did not come off is part
 * of that story. Everything else about it is identical.
 */

import { project, type Orientation } from "@/lib/viz/pitch";

import { Pitch } from "./Pitch";

/** One entry of `progression[]` (WEB_DATA §7). */
export interface ProgressionAction {
  kind: "pass" | "carry";
  x: number;
  y: number;
  end_x: number;
  end_y: number;
  completed: boolean;
}

interface ProgressionArrowsProps {
  actions: ProgressionAction[];
  orientation?: Orientation;
  title: string;
  className?: string;
}

const STYLE = {
  pass: { stroke: "var(--color-blue)", width: 0.28, opacity: 0.55, marker: "url(#viz-arrow-blue)" },
  carry: { stroke: "var(--color-gold)", width: 0.42, opacity: 0.9, marker: "url(#viz-arrow-gold)" },
} as const;

export function ProgressionArrows({
  actions,
  orientation = "horizontal",
  title,
  className,
}: ProgressionArrowsProps) {
  return (
    <Pitch orientation={orientation} title={title} className={className}>
      {actions.map((action, i) => {
        const from = project(orientation, action.x, action.y);
        const to = project(orientation, action.end_x, action.end_y);
        const style = STYLE[action.kind];
        return (
          <line
            key={i}
            x1={from.x}
            y1={from.y}
            x2={to.x}
            y2={to.y}
            stroke={style.stroke}
            strokeWidth={style.width}
            strokeOpacity={style.opacity}
            strokeDasharray={action.completed ? undefined : "1.2 1"}
            markerEnd={style.marker}
          />
        );
      })}
    </Pitch>
  );
}
