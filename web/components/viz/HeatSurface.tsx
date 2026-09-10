/**
 * HeatSurface — the 12 x 8 xT surface, on either orientation of the pitch.
 *
 * Renders: one translucent blue rectangle per grid cell, with the pitch lines
 * redrawn on top in the card colour so the markings stay readable through the
 * heat.
 * Props: `cells` (only the non-zero ones; the rest are simply not drawn),
 * `orientation`, `title`.
 * Data:  `matches/{match_id}.json` -> `xt_grid` (md/WEB_DATA.md §7), which is
 *        `sum(xt)` per cell of the published team.
 *
 * The grid is NOT a design choice and must not be re-binned here: it is the
 * grid `src/silver/events/xt.py` scores on — `col = int(x / 105 * 12)`,
 * `row = int(y / 68 * 8)`, cells 8.75 x 8.5 m (WEB_DATA §3.3). The export
 * groups by those exact edges, so this component only has to place them.
 *
 * The other grid on the site, the 30 zones of `gold.pitch_zones`, is a
 * different grid for different blocks (`ZoneHeatmap`). Do not conflate them.
 */

import { heatOpacity } from "@/lib/viz/scale";
import { xtCellRect, type Orientation } from "@/lib/viz/pitch";

import { Pitch } from "./Pitch";

/** One entry of `xt_grid.cells` (WEB_DATA §7). */
export interface XtCell {
  /** Column along the pitch, 0-11. */
  cx: number;
  /** Row across the pitch, 0-7. */
  cy: number;
  xt: number;
}

interface HeatSurfaceProps {
  cells: XtCell[];
  orientation?: Orientation;
  title: string;
  className?: string;
}

export function HeatSurface({
  cells,
  orientation = "vertical",
  title,
  className,
}: HeatSurfaceProps) {
  const max = cells.reduce((m, c) => Math.max(m, c.xt), 0);

  return (
    <Pitch
      orientation={orientation}
      title={title}
      className={className}
      // Lines over the heat, cased. The handoff drew them in the card colour,
      // which is invisible on every cell the heat did not reach — see the
      // `cased` note in `Pitch.tsx`.
      lineColor="var(--color-ink)"
      lineOpacity={0.5}
      cased
      underlay={
        <g fill="var(--color-blue)">
          {cells.map((cell) => {
            const r = xtCellRect(orientation, cell.cx, cell.cy);
            return (
              <rect
                key={`${cell.cx}-${cell.cy}`}
                x={r.x}
                y={r.y}
                width={r.width}
                height={r.height}
                fillOpacity={heatOpacity(cell.xt, max)}
              />
            );
          })}
        </g>
      }
    />
  );
}
