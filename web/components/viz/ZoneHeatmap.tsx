/**
 * ZoneHeatmap — the 30-zone grid, filled by value.
 *
 * Renders: one translucent rectangle per occupied zone, the zone boundaries as
 * hairlines, and the pitch lines redrawn on top in the card colour.
 * Props: `cells` (zone_id + value), `orientation`, `title`, `showGrid`.
 * Data:  `players/{player_id}.json` -> `heatmap.cells` (md/WEB_DATA.md §9),
 *        bucketed with `gold.pitch_zones`. The sequence lab's start-zone
 *        filter (screen 04) reads the same grid.
 *
 * Six longitudinal strips x five lateral channels, boundaries on the real
 * markings (`pitch-guide` §5, `gold.pitch_zones`). `zone_id` is
 * `x_strip * 10 + y_channel`, so 43 is strip 4, channel 3.
 *
 * Low `y` is the RIGHT flank, and `lib/viz/pitch.ts` puts it at the bottom of
 * a horizontal pitch. A half-space heatmap drawn without that flip is mirrored
 * and looks entirely plausible, so it is worth checking against a player you
 * know: a right-back's heat belongs in the lower half.
 */

import { heatOpacity } from "@/lib/viz/scale";
import {
  ZONE_X_BOUNDARIES,
  ZONE_Y_BOUNDARIES,
  projectRect,
  zoneRect,
  type Orientation,
} from "@/lib/viz/pitch";

import { Pitch } from "./Pitch";

/** One entry of `heatmap.cells` (WEB_DATA §9), or anything shaped like it. */
export interface ZoneCell {
  /** `x_strip * 10 + y_channel`, 11..65. */
  zone_id: number;
  value: number;
}

interface ZoneHeatmapProps {
  cells: ZoneCell[];
  orientation?: Orientation;
  title: string;
  /** Draw the 30-zone boundaries. Off by default: the fills already show them. */
  showGrid?: boolean;
  className?: string;
}

export function ZoneHeatmap({
  cells,
  orientation = "horizontal",
  title,
  showGrid = false,
  className,
}: ZoneHeatmapProps) {
  const max = cells.reduce((m, c) => Math.max(m, c.value), 0);

  return (
    <Pitch
      orientation={orientation}
      title={title}
      className={className}
      lineColor="var(--color-ink)"
      lineOpacity={0.5}
      cased
      underlay={
        <>
          <g fill="var(--color-blue)">
            {cells.map((cell) => {
              const r = zoneRect(orientation, cell.zone_id);
              return (
                <rect
                  key={cell.zone_id}
                  x={r.x}
                  y={r.y}
                  width={r.width}
                  height={r.height}
                  fillOpacity={heatOpacity(cell.value, max)}
                />
              );
            })}
          </g>
          {showGrid && <ZoneGrid orientation={orientation} />}
        </>
      }
    />
  );
}

/**
 * The interior boundaries of the grid — the outer ones are the touchlines and
 * goal lines, which the pitch already draws. A binning device, not decoration,
 * so it stays a hairline (espanyol-viz-design §4).
 *
 * It has to be told apart from the pitch markings at a glance, and it used to
 * fail at that twice over: it was drawn in the *same* family as the markings
 * (`--color-pitch`) and at an opacity that disappeared over a filled cell. It
 * is now separated on three axes at once, none of which costs a new hue:
 *
 *   colour   `--color-mid`, a warm grey, against the markings' near-black
 *   weight   about half the marking's stroke
 *   dash     dashed, which is the honest signal — this is a measuring grid,
 *            not something painted on the grass
 *
 * The casing is the same trick `PitchLines` uses, with the dash pattern
 * repeated on the halo so it does not fill the gaps back in.
 */
function ZoneGrid({ orientation }: { orientation: Orientation }) {
  const strokeWidth = orientation === "horizontal" ? 0.18 : 0.22;
  const dash = "1.3 1.1";
  const interiorX = ZONE_X_BOUNDARIES.slice(1, -1);
  const interiorY = ZONE_Y_BOUNDARIES.slice(1, -1);

  const lines = (
    <>
      {interiorX.map((x) => {
        const r = projectRect(orientation, x, 0, x, 68);
        return <line key={`x${x}`} x1={r.x} y1={r.y} x2={r.x + r.width} y2={r.y + r.height} />;
      })}
      {interiorY.map((y) => {
        const r = projectRect(orientation, 0, y, 105, y);
        return <line key={`y${y}`} x1={r.x} y1={r.y} x2={r.x + r.width} y2={r.y + r.height} />;
      })}
    </>
  );

  return (
    <>
      <g
        stroke="var(--color-card)"
        strokeWidth={strokeWidth * 3}
        strokeOpacity={0.8}
        strokeDasharray={dash}
      >
        {lines}
      </g>
      <g
        stroke="var(--color-mid)"
        strokeWidth={strokeWidth}
        strokeOpacity={0.85}
        strokeDasharray={dash}
      >
        {lines}
      </g>
    </>
  );
}
