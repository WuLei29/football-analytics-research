/**
 * Momentum — who was threatening, minute by minute, over one match.
 *
 * Renders: the published team's xT above the axis in blue, the opponent's
 * below in grey, a vertical rule per goal ending in a ball glyph, a labelled
 * rule at half time, a substitution icon per minute with a change in it,
 * minute ticks, and both team names.
 * Props: `bins` (one per period-minute, in playing order), `markers`,
 * `teamSide`, the two team names, and the rolling `windowMinutes`.
 * Data:  `matches/{match_id}.json` -> `momentum` (md/WEB_DATA.md §7): per-minute
 *        `sum(xt)` per team from `silver.events`, zero-filled, both sides
 *        positive.
 *
 * The x axis is the POSITION of a bin, not its minute. The match clock
 * restarts at 45 for the second half, so a first half that ran to 47' and a
 * second half that starts at 45' both contain minutes 45, 46 and 47. Keyed by
 * minute, the end of one half lands on top of the start of the other; keyed
 * by `(period, minute)` and laid out in the order the file gives them, 47' of
 * the first half is drawn before 45' of the second, and half time sits exactly
 * between the two. Every marker and tick is resolved to a bin the same way.
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
 * Goals are a rule ending in a ball glyph — the published team's at the top,
 * the opponent's at the bottom — and no text. "GOL 46'" next to "DESCANSO 47'"
 * next to a substitution's "×2" is three labels on one square centimetre; the
 * ball reads at a glance and the minute is on the axis below. Only half time
 * keeps a label, because it is the one marker that is not an icon.
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
  period: 1 | 2;
  minute: number;
  home: number;
  away: number;
}

/** One entry of `momentum.markers`, with its label already translated. */
export interface MomentumMarker {
  period: 1 | 2;
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
/** Axis ticks, each pinned to the half it belongs to: 45' is the first half's. */
const MINUTE_TICKS: { period: 1 | 2; minute: number }[] = [
  { period: 1, minute: 15 },
  { period: 1, minute: 30 },
  { period: 1, minute: 45 },
  { period: 2, minute: 60 },
  { period: 2, minute: 75 },
  { period: 2, minute: 90 },
];

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
  const groups = new Map<string, { period: 1 | 2; minute: number; side: MomentumMarker["side"]; count: number }>();
  for (const m of markers) {
    if (m.type !== "sub") continue;
    const key = `${m.period}-${m.minute}-${m.side}`;
    const g = groups.get(key);
    if (g) g.count += 1;
    else groups.set(key, { period: m.period, minute: m.minute, side: m.side, count: 1 });
  }
  return [...groups.values()];
}

/**
 * The goal glyph: a ball 12 units across, centred on (0, 0) — a circle with
 * the central pentagon filled and one seam from each of its corners to the
 * edge. Drawn with the marker colour so it inherits the side, like the sub
 * icon.
 */
const BALL = { r: 5.6, pentagon: 2.3 };
const BALL_CORNERS = Array.from({ length: 5 }, (_, i) => {
  const a = -Math.PI / 2 + (i * 2 * Math.PI) / 5;
  return { cos: Math.cos(a), sin: Math.sin(a) };
});

function BallIcon({ x, y, color }: { x: number; y: number; color: string }) {
  const at = (c: { cos: number; sin: number }, r: number) =>
    [(c.cos * r).toFixed(2), (c.sin * r).toFixed(2)] as const;
  const pentagon = BALL_CORNERS.map((c) => at(c, BALL.pentagon).join(",")).join(" ");
  return (
    <g transform={`translate(${x} ${y})`} stroke={color} strokeWidth={1.2} strokeLinejoin="round">
      <circle r={BALL.r} fill="var(--color-card)" />
      <polygon points={pentagon} fill={color} />
      {BALL_CORNERS.map((c, i) => {
        const [x1, y1] = at(c, BALL.pentagon);
        const [x2, y2] = at(c, BALL.r);
        return <line key={i} x1={x1} y1={y1} x2={x2} y2={y2} />;
      })}
    </g>
  );
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
  // x is the bin's position in the file, not its minute (see the header).
  const xAtIndex = (index: number) => (index / Math.max(1, bins.length - 1)) * VIEW.width;
  const indexOf = (period: 1 | 2, minute: number) =>
    bins.findIndex((b) => b.period === period && b.minute === minute);
  // A marker on a minute with no bin (a goal logged at 48' when the half's
  // last bin is 47', say) snaps to the nearest bin of its half.
  const xAt = (period: 1 | 2, minute: number) => {
    const exact = indexOf(period, minute);
    if (exact >= 0) return xAtIndex(exact);
    let best = -1;
    bins.forEach((b, i) => {
      if (b.period !== period) return;
      if (best < 0 || Math.abs(b.minute - minute) < Math.abs(bins[best].minute - minute)) best = i;
    });
    return best >= 0 ? xAtIndex(best) : 0;
  };
  // Half time is the gap between the last bin of one half and the first of
  // the next, whatever minute either of them carries.
  const firstSecondHalf = bins.findIndex((b) => b.period === 2);
  const halfTimeX =
    firstSecondHalf > 0 ? (xAtIndex(firstSecondHalf - 1) + xAtIndex(firstSecondHalf)) / 2 : null;

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

  const series = values.map((p, i) => ({
    // `values` is `bins` minus the first `windowMinutes - 1` entries (the ones
    // the trailing mean cannot fill), so the bin index is offset by that much.
    x: xAtIndex(i + (windowMinutes - 1)),
    y: VIEW.zeroY - p.value * scale,
  }));

  const subs = groupSubs(markers);
  const goals = markers.filter((m) => m.type === "goal");
  const halfTime = markers.find((m) => m.type === "period");
  // A goal and a substitution on the same minute and side would put the two
  // glyphs on top of each other; the sub icon moves inward to make room.
  const goalSlots = new Set(goals.map((g) => `${g.period}-${g.minute}-${g.side}`));

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
        const x = xAt(sub.period, sub.minute);
        const color = colorOf(sub.side);
        const atTop = sub.side === teamSide;
        const inset = goalSlots.has(`${sub.period}-${sub.minute}-${sub.side}`) ? 14 : 0;
        const iconY = atTop ? RULE.top + 6 + inset : RULE.bottom - 6 - inset;
        return (
          <g key={`sub-${sub.period}-${sub.minute}-${sub.side}`} opacity={SUB_OPACITY}>
            <line
              x1={x}
              y1={atTop ? iconY + 7 : RULE.top}
              x2={x}
              y2={atTop ? RULE.bottom : iconY - 7}
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

      {/* Half time: one labelled rule in the gap between the halves. The label
          prints the marker's own minute (47' on a half with two added), the
          position is the gap. */}
      {halfTime && halfTimeX !== null && (
        <g>
          <line
            x1={halfTimeX}
            y1={RULE.top}
            x2={halfTimeX}
            y2={RULE.bottom}
            stroke={colorOf(null)}
            strokeWidth={1}
            strokeDasharray={MARKER_STYLE.period.dash}
          />
          <text
            x={halfTimeX + 4}
            y={244}
            fontFamily="var(--font-mono)"
            fontSize={9.5}
            fontWeight={600}
            fill={colorOf(null)}
          >
            {halfTime.label}
          </text>
        </g>
      )}

      {/* Goals: a solid rule from the ball to the far edge of the plot. */}
      {goals.map((goal, i) => {
        const x = xAt(goal.period, goal.minute);
        const color = colorOf(goal.side);
        const atTop = goal.side === teamSide;
        const iconY = atTop ? RULE.top + 6 : RULE.bottom - 6;
        return (
          <g key={`goal-${goal.period}-${goal.minute}-${i}`}>
            <title>{goal.label}</title>
            <line
              x1={x}
              y1={atTop ? iconY + BALL.r : RULE.top}
              x2={x}
              y2={atTop ? RULE.bottom : iconY - BALL.r}
              stroke={color}
              strokeWidth={1}
              strokeDasharray={MARKER_STYLE.goal.dash}
            />
            <BallIcon x={x} y={iconY} color={color} />
          </g>
        );
      })}

      {MINUTE_TICKS.filter((t) => indexOf(t.period, t.minute) >= 0).map((t) => (
        <text
          key={`${t.period}-${t.minute}`}
          x={xAt(t.period, t.minute)}
          y={262}
          textAnchor="middle"
          fontFamily="var(--font-mono)"
          fontSize={9}
          fill="var(--color-faint)"
        >
          {t.minute}&#39;
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
