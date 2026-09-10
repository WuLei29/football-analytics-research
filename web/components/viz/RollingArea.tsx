/**
 * RollingArea — a five-match rolling difference, diverging around zero.
 *
 * Renders: the blue area above zero, the red area below, the series line, a
 * fixed tick ladder, and a strip of matchday labels in HTML under the chart.
 * Props: `points` (one per matchday, already averaged by the export),
 * `pxPerUnit` (the value scale), `ticks`, `axisLabel`, `xCaption`, `decimals`.
 * Data:  `teams/{team}/{season}/overview.json` -> `rolling.points`
 *        (md/WEB_DATA.md §6): the trailing five-match mean of
 *        `xg_for - xg_against` and of `xt_for - xt_against`, from
 *        `gold.team_match_stats`, starting at the fifth match.
 *
 * Screen 01 draws this twice with different scales — xGD at 60 px per 1.0 and
 * xT at 150 px per 1.0 — so the two charts are the same height and neither
 * rescales as the season goes on. That is the design's choice and it is the
 * right one: a chart whose axis moves cannot be read across matchdays.
 *
 * The geometry is the handoff's, in viewBox units: plot area x 56 -> 590, zero
 * line y = 100, x-axis rule y = 176. The handoff starts the plot at x = 58 to
 * clear 15 px tick labels; at that size the ladder shouted louder than the
 * series, so the labels are 10 px here.
 *
 * The left gutter is split three ways and every part of it is load-bearing:
 * the rotated axis caption sits at x = 12, the tick labels END at x = 48, and
 * the plot starts at x = 56. An earlier pass closed the gutter to 42 and put
 * the caption at x = 11, which left about a pixel between the caption and a
 * four-character tick like `-0.5` — they read as one string. Keep ~8 units of
 * air on each side of the tick column when changing the tick font size.
 *
 * The y tick labels are SVG text because they sit at fixed positions in the
 * viewBox and never change. The matchday labels underneath are HTML, because
 * there are as many of them as matchdays and they must stay at a real 9.5 px
 * however wide the chart is rendered (handoff, "Implementation notes").
 */

import { signed } from "@/lib/format";
import { divergingAreaPath, linePath } from "@/lib/viz/scale";

/** One entry of `rolling.points`, reduced to the one series being drawn. */
export interface RollingPoint {
  matchday: number;
  value: number;
}

interface RollingAreaProps {
  points: RollingPoint[];
  /** Vertical scale: SVG units per 1.0 of value. 60 for xGD, 150 for xT. */
  pxPerUnit: number;
  /** Values to rule and label, e.g. [1, 0.5, 0, -0.5, -1]. */
  ticks: number[];
  /** Rotated caption on the y axis, e.g. "XGD MEDIA 5 PARTIDOS". */
  axisLabel: string;
  /** Caption under the matchday strip, e.g. "JORNADA". */
  xCaption: string;
  decimals?: 0 | 1 | 2;
  /** Label every nth matchday, so a 38-match season does not crowd. */
  labelEvery?: number;
}

const PLOT = { left: 56, right: 590, zeroY: 100, axisY: 176 };
/** Right edge of the tick-label column, and the axis caption's centre line. */
const TICK_LABEL_X = 48;
const AXIS_LABEL_X = 12;
const VIEWBOX_WIDTH = 600;

export function RollingArea({
  points,
  pxPerUnit,
  ticks,
  axisLabel,
  xCaption,
  decimals = 1,
  labelEvery = 5,
}: RollingAreaProps) {
  // One x per point, evenly spaced: the series is one value per matchday, and
  // matchdays are ordinal. A single point would divide by zero, so it sits at
  // the left edge.
  const step = points.length > 1 ? (PLOT.right - PLOT.left) / (points.length - 1) : 0;
  const xAt = (i: number) => PLOT.left + i * step;
  const yAt = (value: number) => PLOT.zeroY - value * pxPerUnit;

  const series = points.map((p, i) => ({ x: xAt(i), y: yAt(p.value) }));

  return (
    <div>
      <svg viewBox={`0 0 ${VIEWBOX_WIDTH} 182`} width="100%" style={{ display: "block" }} role="img">
        <title>{axisLabel}</title>

        {/* Tick ladder. Zero is drawn separately below, as a hard rule. */}
        {ticks
          .filter((t) => t !== 0)
          .map((t) => (
            <g key={t}>
              <line
                x1={PLOT.left}
                y1={yAt(t)}
                x2={PLOT.right}
                y2={yAt(t)}
                stroke="var(--color-line)"
                strokeWidth={1}
              />
            </g>
          ))}

        <path d={divergingAreaPath(series, PLOT.zeroY, 1)} fill="rgba(11,76,158,.20)" />
        <path d={divergingAreaPath(series, PLOT.zeroY, -1)} fill="rgba(168,58,44,.18)" />
        <path d={linePath(series)} fill="none" stroke="var(--color-blue)" strokeWidth={2} />

        <line
          x1={PLOT.left}
          y1={PLOT.zeroY}
          x2={PLOT.right}
          y2={PLOT.zeroY}
          stroke="var(--color-ink)"
          strokeWidth={1}
        />
        <line
          x1={PLOT.left}
          y1={PLOT.axisY}
          x2={PLOT.right}
          y2={PLOT.axisY}
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
          y={PLOT.zeroY}
          transform={`rotate(-90 ${AXIS_LABEL_X} ${PLOT.zeroY})`}
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
