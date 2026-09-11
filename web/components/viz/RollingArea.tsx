/**
 * RollingArea — a five-match rolling difference, diverging around zero.
 *
 * Renders: the green area above zero, the red area below, the series line, a
 * tick ladder fitted to the data, and a strip of matchday labels in HTML under
 * the chart.
 * Props: `points` (one per matchday, already averaged by the export),
 * `axisLabel`, `xCaption`, `height` (viewBox height) and `labelEvery`.
 * Data:  `teams/{team}/{season}/overview.json` -> `rolling.points`
 *        (md/WEB_DATA.md §6): the trailing five-match mean of
 *        `xg_for - xg_against` and of `xt_for - xt_against`, from
 *        `gold.team_match_stats`, starting at the fifth match.
 *
 * THE SCALE IS THE DATA'S. The handoff fixed it — xGD at 60 px per 1.0 and
 * xT at 150 — so that the two charts never rescaled between matchdays. On the
 * real 2025/26 file that ladder clipped the series: the xT difference reached
 * +2.9 against a ±0.4 ladder and the good run of October left the plot. So
 * the ladder is now `symmetricTicks(peak)`: the largest swing of the season
 * sits at the top or bottom edge and the ticks are round numbers that contain
 * it. What is lost — a fixed reference across seasons — the tick labels give
 * back, since they are printed.
 *
 * The plot is symmetric around zero on purpose: the eye reads "above the line
 * is good" and the same value must be the same height on either side.
 *
 * Geometry in viewBox units: plot area x 56 -> 590, zero line at half the
 * height, the ladder's extremes `PAD` units inside the top and bottom edges.
 * `height` is a prop because screen 01 stacks two of these beside a twenty-row
 * table and wants them taller than the demo's 182.
 *
 * The left gutter is split three ways and every part of it is load-bearing:
 * the rotated axis caption sits at x = 12, the tick labels END at x = 48, and
 * the plot starts at x = 56. An earlier pass closed the gutter to 42 and put
 * the caption at x = 11, which left about a pixel between the caption and a
 * four-character tick like `-0.5` — they read as one string. Keep ~8 units of
 * air on each side of the tick column when changing the tick font size.
 *
 * The y tick labels are SVG text because they sit at positions in the viewBox
 * and scale with it. The matchday labels underneath are HTML, because there
 * are as many of them as matchdays and they must stay at a real 9.5 px however
 * wide the chart is rendered (handoff, "Implementation notes").
 */

import { signed } from "@/lib/format";
import { divergingAreaPath, linePath, symmetricTicks } from "@/lib/viz/scale";

/** One entry of `rolling.points`, reduced to the one series being drawn. */
export interface RollingPoint {
  matchday: number;
  value: number;
}

interface RollingAreaProps {
  points: RollingPoint[];
  /** Rotated caption on the y axis, e.g. "XGD MEDIA 5 PARTIDOS". */
  axisLabel: string;
  /** Caption under the matchday strip, e.g. "JORNADA". */
  xCaption: string;
  /** ViewBox height. The width is always 600. */
  height?: number;
  /** Label every nth matchday, so a 38-match season does not crowd. */
  labelEvery?: number;
  className?: string;
}

const PLOT = { left: 56, right: 590 };
/** Air between the ladder's extremes and the top / bottom of the viewBox. */
const PAD = 10;
/** Right edge of the tick-label column, and the axis caption's centre line. */
const TICK_LABEL_X = 48;
const AXIS_LABEL_X = 12;
const VIEWBOX_WIDTH = 600;

export function RollingArea({
  points,
  axisLabel,
  xCaption,
  height = 182,
  labelEvery = 5,
  className,
}: RollingAreaProps) {
  const zeroY = height / 2;
  const axisY = height - 1;

  const peak = points.reduce((m, p) => Math.max(m, Math.abs(p.value)), 0);
  const { ticks, max, decimals } = symmetricTicks(peak);
  const pxPerUnit = (zeroY - PAD) / max;

  // One x per point, evenly spaced: the series is one value per matchday, and
  // matchdays are ordinal. A single point would divide by zero, so it sits at
  // the left edge.
  const step = points.length > 1 ? (PLOT.right - PLOT.left) / (points.length - 1) : 0;
  const xAt = (i: number) => PLOT.left + i * step;
  const yAt = (value: number) => zeroY - value * pxPerUnit;

  const series = points.map((p, i) => ({ x: xAt(i), y: yAt(p.value) }));

  return (
    <div className={`flex flex-col ${className ?? ""}`}>
      <svg
        viewBox={`0 0 ${VIEWBOX_WIDTH} ${height}`}
        width="100%"
        style={{ display: "block" }}
        role="img"
      >
        <title>{axisLabel}</title>

        {/* Tick ladder. Zero is drawn separately below, as a hard rule. */}
        {ticks
          .filter((t) => t !== 0)
          .map((t) => (
            <line
              key={t}
              x1={PLOT.left}
              y1={yAt(t)}
              x2={PLOT.right}
              y2={yAt(t)}
              stroke="var(--color-line)"
              strokeWidth={1}
            />
          ))}

        {/* Green above, red below: the sign is the reading, and the page's
            blue is kept for the club. */}
        <path d={divergingAreaPath(series, zeroY, 1)} fill="rgba(30,122,77,.22)" />
        <path d={divergingAreaPath(series, zeroY, -1)} fill="rgba(168,58,44,.20)" />
        <path d={linePath(series)} fill="none" stroke="var(--color-ink)" strokeWidth={1.6} />

        <line
          x1={PLOT.left}
          y1={zeroY}
          x2={PLOT.right}
          y2={zeroY}
          stroke="var(--color-ink)"
          strokeWidth={1}
        />
        <line
          x1={PLOT.left}
          y1={axisY}
          x2={PLOT.right}
          y2={axisY}
          stroke="var(--color-line)"
          strokeWidth={1}
        />

        {ticks.map((t) => (
          <text
            key={t}
            x={TICK_LABEL_X}
            y={yAt(t) + 3.5}
            textAnchor="end"
            fontFamily="var(--font-mono)"
            fontSize={10}
            fill="var(--color-faint)"
          >
            {signed(t, decimals)}
          </text>
        ))}

        <text
          x={AXIS_LABEL_X}
          y={zeroY}
          transform={`rotate(-90 ${AXIS_LABEL_X} ${zeroY})`}
          textAnchor="middle"
          fontFamily="var(--font-mono)"
          fontSize={9}
          letterSpacing={1.6}
          fill="var(--color-faint)"
        >
          {axisLabel}
        </text>
      </svg>

      {/* The matchday strip: HTML positioned as a percentage of the viewBox
          width, so the labels line up with the plot at any rendered size. */}
      <div style={{ position: "relative", height: 14 }}>
        {points.map((p, i) =>
          i % labelEvery === 0 ? (
            <span
              key={p.matchday}
              style={{
                position: "absolute",
                left: `${(xAt(i) / VIEWBOX_WIDTH) * 100}%`,
                transform: "translateX(-50%)",
                fontFamily: "var(--font-mono)",
                fontSize: 9.5,
                color: "var(--color-faint)",
              }}
            >
              {p.matchday}
            </span>
          ) : null,
        )}
      </div>
      <div
        className="label"
        style={{ textAlign: "center", color: "var(--color-faint)", marginTop: 2 }}
      >
        {xCaption}
      </div>
    </div>
  );
}
