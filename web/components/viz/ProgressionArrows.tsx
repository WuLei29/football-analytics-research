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

import type { Orientation } from "@/lib/viz/pitch";

import { ActionArrows, type ArrowMark } from "./ActionArrows";

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

/** Exported so the player page's arrow modes draw with the same weights. */
export const PROGRESSION_STYLE = {
  pass: { tone: "blue", width: 0.28, opacity: 0.55 },
  carry: { tone: "gold", width: 0.42, opacity: 0.9 },
} as const;

/**
 * A thin wrapper since 16 Oct 2026: the marks are `ActionArrows`, which the
 * player page also draws with. This keeps the match page's two-style encoding
 * in one place.
 */
export function ProgressionArrows({
  actions,
  orientation = "horizontal",
  title,
  className,
}: ProgressionArrowsProps) {
  const marks: ArrowMark[] = actions.map((a) => ({
    x: a.x,
    y: a.y,
    end_x: a.end_x,
    end_y: a.end_y,
    completed: a.completed,
    ...PROGRESSION_STYLE[a.kind],
  }));
  return (
    <ActionArrows actions={marks} orientation={orientation} title={title} className={className} />
  );
}
