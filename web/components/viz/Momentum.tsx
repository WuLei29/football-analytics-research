/**
 * Momentum — who was threatening, minute by minute, over one match.
 *
 * Renders: the published team's xT above the axis in blue, the opponent's
 * below in grey, a labelled vertical rule per goal, card and period end, a
 * substitution icon per minute with a change in it, minute ticks, and both
 * team names.
 * Props: `bins` (one per minute), `markers`, `teamSide`, the two team names,
 * and the rolling `windowMinutes`.
 * Data:  `matches/{match_id}.json` -> `momentum` (md/WEB_DATA.md §7): per-minute
 *        `sum(xt)` per team from `silver.events`, zero-filled, both sides
 *        positive.
 *
 * The export ships raw per-minute sums and leaves the smoothing to the site
 * (WEB_DATA §7.1), which is why the window is a prop: the file is the data,
 * the window is a reading choice.
 *
 * What is drawn is one diverging series — the published team's rolling xT
 * minus the opponent's — not two independent areas. That is what makes the
 * chart readable: the height at any minute is who was on top and by how much,
 * rather than two curves the eye has to subtract.
 *
 * The line starts at minute `windowMinutes`, because a trailing five-minute mean does
 * not exist before the fifth minute. Drawing zeros there would invent a calm
 * opening the data does not describe.
 *
 * Substitutions are NOT drawn as labelled rules. A real match has seven to
 * ten of them, in pairs and triples at the same minute, and "CAMBIO 67'" three
 * times over on one x is unreadable and hides the goal label beside it. Each
 * minute with a change gets one faint dashed rule and one icon at the end of
 * it — the published team's at the top, the opponent's at the bottom — with a
 * "×2" / "×3" when several came on together, and the whole thing at 55 %
 * opacity so a goal marker on the same minute still reads through it.
 *
 * Geometry from the handoff, in viewBox units: 1180 x 268 (eight units taller,
 * so the minute ticks clear the opponent marker labels), zero line y = 120.
 * The vertical scale is NOT the handoff's fixed 200 units per 1.0 of xT — see
 * `scale` below.
 */

import { divergingAreaPath, rollingMean } from "@/lib/viz/scale";

/** One entry of `momentum.bins` (WEB_DATA §7). Both sides are positive. */
export interface MomentumBin {
  minute: number;
  home: number;
  away: number;
}

/** One entry of `momentum.markers`, with its label already translated. */
export interface MomentumMarker {
  minute: number;
  type: "goal" | "sub" | "card" | "period";
  side: "home" | "away" | null;
  label: string;
}

interface MomentumProps {
  bins: MomentumBin[];
  markers: MomentumMarker[];
  /** Which side is the published team — it is drawn above the axis. */
  teamSide: "home" | "away";
  teamName: string;
  opponentName: string;
  /** Minutes in the trailing mean. The design reads 5. */
  windowMinutes?: number;
  /**
   * SVG units per 1.0 of net xT. Omit — the default — and the chart scales
   * itself so the biggest swing of the match fills the plot.
   */
  pxPerUnit?: number;
  className?: string;
}

const VIEW = { width: 1180, height: 268, zeroY: 120 };
/** How far the area may reach from the zero line before it hits the labels. */
const AMPLITUDE = 92;
/** Handoff: gridlines at these four y values. */
const GRIDLINES = [30, 60, 180, 210];
const MINUTE_TICKS = [15, 30, 45, 60, 75, 90];

/** Per marker type: colour, dash pattern (handoff, screen 02 block 2). */
const MARKER_STYLE = {
  goal: { dash: undefined },
  sub: { dash: "3 3" },
  card: { dash: "3 3" },
  period: { dash: "2 3" },
} as const;

/** Top and bottom of every marker rule; the sub icons sit just past them. */
const RULE = { top: 8, bottom: 232 };
const SUB_OPACITY = 0.55;

/**
 * Substitutions folded to one entry per (minute, side), so the chart draws one
 * icon with a count rather than three rules on top of each other.
 */
function groupSubs(markers: MomentumMarker[]) {
  const groups = new Map<string, { minute: number; side: MomentumMarker["side"]; count: number }>();
  for (const m of markers) {
    if (m.type !== "sub") continue;
    const key = `${m.minute}-${m.side}`;
    const g = groups.get(key);
    if (g) g.count += 1;
    else groups.set(key, { minute: m.minute, side: m.side, count: 1 });
  }
  return [...groups.values()];
}

/**
 * The substitution glyph: two opposed arrows, 12 units wide, centred on
 * (0, 0). Drawn as strokes with the marker colour, so it inherits the side.
 */
function SubIcon({ x, y, color }: { x: number; y: number; color: string }) {
  return (
    <g transform={`translate(${x} ${y})`} stroke={color} strokeWidth={1.4} strokeLinecap="round" strokeLinejoin="round" fill="none">
      {/* → on top */}
      <path d="M-5.5,-2.2 H5.5 M3,-4.7 L5.5,-2.2 L3,0.3" />
      {/* ← underneath */}
      <path d="M5.5,2.2 H-5.5 M-3,-0.3 L-5.5,2.2 L-3,4.7" />
    </g>
  );
}

export function Momentum({
  bins,
  markers,
  teamSide,
  teamName,
  opponentName,
  windowMinutes = 5,
  pxPerUnit,
  className,
}: MomentumProps) {
  const lastMinute = bins.length ? bins[bins.length - 1].minute : 90;
  const xAt = (minute: number) => ((minute - 1) / Math.max(1, lastMinute - 1)) * VIEW.width;

  // Net threat, from the published team's point of view.
  const net = bins.map((b) => (teamSide === "home" ? b.home - b.away : b.away - b.home));
  const smoothed = rollingMean(net, windowMinutes);

  const values = bins
    .map((bin, i) => ({ minute: bin.minute, value: smoothed[i] }))
    .filter((p): p is { minute: number; value: number } => p.value !== null);

  // The handoff fixes 200 units per 1.0 of xT. A five-minute mean of net xT is
  // a few hundredths, so that scale drew every match as a flat line hugging the
  // axis. Unless a scale is passed in, the biggest swing of the match fills the
  // plot — this chart is read as shape and turning points, never as a value, so
  // there is nothing to compare across matches and nothing lost by rescaling.
  const peak = values.reduce((max, p) => Math.max(max, Math.abs(p.value)), 0);
  const scale = pxPerUnit ?? (peak > 0 ? AMPLITUDE / peak : 200);

  const series = values.map((p) => ({
    x: xAt(p.minute),
    y: VIEW.zeroY - p.value * scale,
  }));

  const subs = groupSubs(markers);
  const labelled = markers.filter((m) => m.type !== "sub");

  // A marker belongs to whoever it happened to; a period end to nobody.
  const colorOf = (side: MomentumMarker["side"]) =>
    side === null ? "var(--color-faint)" : side === teamSide ? "var(--color-blue)" : "var(--color-mid)";

  return (
    <svg
      viewBox={`0 0 ${VIEW.width} ${VIEW.height}`}
      width="100%"
      className={className}
      style={{ display: "block" }}
      role="img"
    >
      <title>{`${teamName} · ${opponentName}`}</title>

      {GRIDLINES.map((y) => (
        <line key={y} x1={0} y1={y} x2={VIEW.width} y2={y} stroke="var(--color-grid)" strokeWidth={1} />
      ))}

      <path d={divergingAreaPath(series, VIEW.zeroY, 1)} fill="rgba(11,76,158,.28)" />
      <path d={divergingAreaPath(series, VIEW.zeroY, -1)} fill="rgba(154,141,126,.35)" />

      <line
        x1={0}
        y1={VIEW.zeroY}
        x2={VIEW.width}
        y2={VIEW.zeroY}
        stroke="var(--color-ink)"
        strokeWidth={1}
      />

      {/* Substitutions first, so a goal on the same minute draws over them. */}
      {subs.map((sub) => {
        const x = xAt(sub.minute);
        const color = colorOf(sub.side);
        const atTop = sub.side === teamSide;
        const iconY = atTop ? RULE.top + 6 : RULE.bottom - 6;
        return (
          <g key={`sub-${sub.minute}-${sub.side}`} opacity={SUB_OPACITY}>
            <line
              x1={x}
              y1={atTop ? RULE.top + 13 : RULE.top}
              x2={x}
              y2={atTop ? RULE.bottom : RULE.bottom - 13}
              stroke={color}
              strokeWidth={1}
              strokeDasharray={MARKER_STYLE.sub.dash}
            />
            <SubIcon x={x} y={iconY} color={color} />
            {sub.count > 1 && (
              <text
                x={x + 8}
                y={iconY + 3}
                fontFamily="var(--font-mono)"
                fontSize={8.5}
                fontWeight={600}
                fill={color}
              >
                ×{sub.count}
              </text>
            )}
          </g>
        );
      })}

      {labelled.map((marker, i) => {
        const x = xAt(marker.minute);
        const color = colorOf(marker.side);
        // Past x = 900 the label would run off the right edge, so it flips.
        const flip = x > 900;
        return (
          <g key={`${marker.minute}-${marker.type}-${i}`}>
            <line
              x1={x}
              y1={RULE.top}
              x2={x}
              y2={RULE.bottom}
              stroke={color}
              strokeWidth={1}
              strokeDasharray={MARKER_STYLE[marker.type].dash}
            />
            <text
              x={flip ? x - 4 : x + 4}
              y={marker.side === teamSide ? 20 : 244}
              textAnchor={flip ? "end" : "start"}
              fontFamily="var(--font-mono)"
              fontSize={9.5}
              fontWeight={600}
              fill={color}
            >
              {marker.label}
            </text>
          </g>
        );
      })}

      {MINUTE_TICKS.filter((m) => m <= lastMinute).map((m) => (
        <text
          key={m}
          x={xAt(m)}
          y={262}
          textAnchor="middle"
          fontFamily="var(--font-mono)"
          fontSize={9}
          fill="var(--color-faint)"
        >
          {m}&#39;
        </text>
      ))}

      {/* The two team names are static labels at fixed viewBox positions, so
          they stay in the SVG — nothing about them depends on the data. The
          top one sits a few units above the y = 30 gridline rather than on
          it, so its baseline does not merge with the rule. */}
      <text x={0} y={25} fontFamily="var(--font-mono)" fontSize={9} letterSpacing={1.4} fill="var(--color-blue)">
        {teamName.toUpperCase()}
      </text>
      <text x={0} y={222} fontFamily="var(--font-mono)" fontSize={9} letterSpacing={1.4} fill="var(--color-mid)">
        {opponentName.toUpperCase()}
      </text>
    </svg>
  );
}
