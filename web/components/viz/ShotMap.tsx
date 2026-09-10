"use client";

/**
 * ShotMap — every shot of a match, both teams, area proportional to xG.
 *
 * Renders: a horizontal pitch with one circle per shot, plus an HTML tooltip
 * on hover. The published team shoots to the right; the opponent to the left.
 * Props: `shots`, `teamSide` (which of home/away is the published team) and
 * the two teams' abbreviations.
 * Data:  `matches/{match_id}.json` -> `shots[]` (md/WEB_DATA.md §7), whose
 *        source is `silver.events` shots. The export has already mirrored the
 *        away side's coordinates (§3.1), so this component never flips
 *        anything itself — it draws what it is given.
 *
 * Client component, because it answers a mouse. It is the only interactive
 * primitive on screen 02 besides the sequence list.
 *
 * Encoding (handoff, screen 02 block 4, and espanyol-viz-design §7):
 *   radius   R0 + sqrt(xG) * K      -> AREA reads as xG, which is the point
 *   fill     three steps, not two: solid = goal, half = on target, hollow =
 *            off target or blocked
 *   colour   published team blue, opponent grey-brown
 *   hover    the hovered shot stays at full opacity, the rest drop to 0.3,
 *            and the tooltip adds a goal frame with the ball where the shot
 *            crossed the line — on-target shots only
 *
 * Two deliberate changes from the handoff's first pass:
 *
 *   - **The circles are smaller.** The handoff's `1.4 + sqrt(xG) * 5.2` puts a
 *     0.4 xG chance at nearly 10 m across, so a busy six-yard box became one
 *     blue blob. `SIZE` below is the same square-root law at a smaller
 *     constant, which keeps area ∝ xG — the encoding — while letting
 *     neighbouring shots be told apart. Overlap is still expected and is why
 *     they are drawn largest-first (see `byDescendingXg`).
 *   - **On target is now visible without hovering.** Filled/hollow alone threw
 *     away the single most-asked question of a shot map. A save and a shot
 *     into row Z now differ on the page, not only in the tooltip.
 */

import { useRef, useState } from "react";

import { shotBodyPart, shotOutcome, viz } from "@/lib/labels";
import { dec } from "@/lib/format";
import { PITCH_VIEWBOX, project, projectFraction } from "@/lib/viz/pitch";

import { PitchLines } from "./Pitch";

/** One row of `shots[]` (WEB_DATA §7). */
export interface Shot {
  shot_id: number;
  side: "home" | "away";
  name: string;
  minute: number;
  x: number;
  y: number;
  xg: number;
  body_part: string;
  outcome: string;
  /**
   * Where the shot crossed the goal line, in Opta's goalmouth frame (Q102/Q103,
   * stored raw in `silver.events.goal_mouth_y` / `goal_mouth_z`): y is the
   * pitch-width 0-100 scale with the posts at 45.2 and 54.8, z is 0 at the
   * ground and 38 at the crossbar. Only present on shots on target.
   */
  goal_mouth_y?: number | null;
  goal_mouth_z?: number | null;
}

interface ShotMapProps {
  shots: Shot[];
  /** Which side is the published team — it gets the accent colour. */
  teamSide: "home" | "away";
  abbr: { home: string; away: string };
  className?: string;
}

/**
 * Three fills per side rather than two: `goal`, `onTarget`, `offTarget`. Same
 * hue throughout — the extra information is carried by ink density, so the
 * chart still spends colour only on team identity (espanyol-viz-design §1).
 */
const PALETTE = {
  team: {
    stroke: "var(--color-blue)",
    goalFill: "var(--color-blue)",
    onTargetFill: "rgba(11,76,158,.45)",
    offTargetFill: "rgba(11,76,158,.07)",
    chip: "#7FB2ED",
  },
  opponent: {
    stroke: "var(--color-faint)",
    goalFill: "var(--color-opp-strong)",
    onTargetFill: "rgba(201,185,166,.60)",
    offTargetFill: "rgba(201,185,166,.14)",
    chip: "var(--color-pitch)",
  },
};

/**
 * Radius in metres: `base + sqrt(xG) * k`. The square root is what makes the
 * *area* proportional to xG; `base` only guarantees that a 0.01 chance is
 * still a visible dot. Tuned so the biggest realistic chance (a penalty, ~0.79)
 * lands near 3.7 m radius — a little over two of the six-yard box's 5.5 m.
 */
const SIZE = { base: 0.75, k: 3.3 };

/** Reached the frame. `blocked` did not — a body stopped it before the line. */
const ON_TARGET = new Set(["goal", "saved", "woodwork"]);

function shotRadius(xg: number): number {
  return SIZE.base + Math.sqrt(Math.max(0, xg)) * SIZE.k;
}

/**
 * One width for every tooltip, both sides. Between the two the free-growing
 * box used to produce: wide enough that "Roberto Fernández" sets on two lines
 * at most, narrow enough that the goal inset under it stays short.
 */
const TOOLTIP_WIDTH = 132;
/** The goal inset, capped so it cannot drive the height of the card. */
const GOALMOUTH_WIDTH = 96;
/**
 * The tallest the card gets — a two-line name plus the goal inset — plus the
 * gap it leaves above the mark. Used to decide whether there is room above.
 * A constant rather than a measurement: measuring would need a second render
 * pass to place the card, which is a visible flicker under the cursor. It only
 * has to be an upper bound, and over-estimating costs one early flip.
 */
const TOOLTIP_MAX_HEIGHT = 124;

/**
 * Big shots underneath, small ones on top. Without this the drawing order is
 * the order of the export (by minute), so a 0.6 xG circle drawn late buries
 * every tap-in around it and swallows their hover targets too.
 */
function byDescendingXg(a: Shot, b: Shot): number {
  return b.xg - a.xg;
}

export function ShotMap({ shots, teamSide, abbr, className }: ShotMapProps) {
  const [hovered, setHovered] = useState<number | null>(null);
  const active = shots.find((s) => s.shot_id === hovered) ?? null;
  // Measured, not assumed: the tooltip needs to know how many real pixels tall
  // the figure is to decide whether it has room above the mark. See `Tooltip`.
  const figureRef = useRef<HTMLDivElement>(null);

  return (
    <div ref={figureRef} className={className} style={{ position: "relative" }}>
      <svg viewBox={PITCH_VIEWBOX.horizontal} width="100%" style={{ display: "block" }} role="img">
        <title>{viz.shotMapTitle}</title>
        {/* This component owns its <svg> because the tooltip is an HTML
            sibling, so it draws the markings directly instead of wrapping
            <Pitch>. It is the only place on the site that does. */}
        <PitchLines orientation="horizontal" />

        {[...shots].sort(byDescendingXg).map((shot) => {
          const p = project("horizontal", shot.x, shot.y);
          const palette = shot.side === teamSide ? PALETTE.team : PALETTE.opponent;
          const fill =
            shot.outcome === "goal"
              ? palette.goalFill
              : ON_TARGET.has(shot.outcome)
                ? palette.onTargetFill
                : palette.offTargetFill;
          return (
            <circle
              key={shot.shot_id}
              cx={p.x}
              cy={p.y}
              r={shotRadius(shot.xg)}
              fill={fill}
              stroke={palette.stroke}
              // An off-target shot gets a lighter outline too, so a cluster of
              // them does not read as a solid ring of ink.
              strokeWidth={ON_TARGET.has(shot.outcome) ? 0.3 : 0.22}
              strokeOpacity={ON_TARGET.has(shot.outcome) ? 1 : 0.55}
              opacity={hovered === null || hovered === shot.shot_id ? 1 : 0.3}
              style={{ cursor: "pointer" }}
              onMouseEnter={() => setHovered(shot.shot_id)}
              onMouseLeave={() => setHovered(null)}
            />
          );
        })}
      </svg>

      {active && (
        <Tooltip
          shot={active}
          teamSide={teamSide}
          abbr={abbr}
          figureHeight={figureRef.current?.clientHeight ?? 0}
        />
      )}
    </div>
  );
}

/**
 * The tooltip is HTML, not SVG, so its type renders at real pixel sizes
 * whatever the pitch scales to. It is positioned as a percentage of the SVG's
 * own box — hence `projectFraction`, which accounts for the viewBox padding.
 *
 * IT HAS ONE FIXED WIDTH, and that is load-bearing. It used to be `minWidth`
 * with no maximum, so the box shrink-wrapped to the player's name — and since
 * the goalmouth inset below is `width="100%"` of a fixed-aspect viewBox, a
 * longer name made the box wider AND the inset proportionally taller. The two
 * dimensions compounded, so "Roberto Fernández" produced a visibly larger
 * card than "Rioja" and the tall ones ran off the top of the figure.
 *
 * Pinning the width fixes both symptoms at once: every tooltip is now the same
 * size whichever side took the shot, and the height cannot run away, because
 * the only variable-height element is capped by `GOALMOUTH_WIDTH`.
 *
 * IT FLIPS. The card sits above the mark, which is right almost everywhere and
 * wrong for a shot near the top touchline, where it runs off the figure. When
 * there is not room above, it is drawn below the mark instead.
 *
 * The test is in real pixels, not in a fraction of the pitch, and that
 * distinction matters: the SVG scales with its container, so a shot 8% down a
 * 380 px figure has 30 px of headroom while the same shot on a 190 px phone
 * has 15 px. A fixed fraction would be tuned for one viewport and wrong on the
 * other. `figureHeight` is the measured height of the wrapper, so the same
 * rule holds at every size. It is read from a ref, which is safe here because
 * a tooltip only ever renders in response to a mouse — long after mount — and
 * the fraction fallback covers the impossible case of a null ref.
 */
function Tooltip({
  shot,
  teamSide,
  abbr,
  figureHeight,
}: {
  shot: Shot;
  teamSide: "home" | "away";
  abbr: { home: string; away: string };
  /** Rendered height of the figure, in CSS px. 0 if not measured yet. */
  figureHeight: number;
}) {
  const at = projectFraction("horizontal", shot.x, shot.y);
  const palette = shot.side === teamSide ? PALETTE.team : PALETTE.opponent;

  const headroom = at.y * figureHeight;
  const above = figureHeight > 0 ? headroom > TOOLTIP_MAX_HEIGHT : at.y > 0.34;

  return (
    <div
      style={{
        position: "absolute",
        left: `${at.x * 100}%`,
        top: `${at.y * 100}%`,
        // Both offsets clear the mark by 24% of the card's own height, so the
        // gap between dot and card looks the same whichever way it points.
        transform: above ? "translate(-50%,-124%)" : "translate(-50%,24%)",
        background: "var(--color-ink)",
        borderRadius: 9,
        padding: "6px 8px",
        pointerEvents: "none",
        width: TOOLTIP_WIDTH,
        zIndex: 4,
      }}
    >
      <div
        style={{
          display: "flex",
          alignItems: "baseline",
          justifyContent: "space-between",
          gap: 6,
          marginBottom: 2,
        }}
      >
        <span
          style={{
            fontFamily: "var(--font-sans)",
            fontSize: 12,
            fontWeight: 600,
            lineHeight: 1.25,
            color: "var(--color-on-dark)",
            // `minWidth: 0` lets a long name wrap inside the fixed card
            // instead of forcing the flex row wider than its parent.
            minWidth: 0,
          }}
        >
          {shot.name}
        </span>
        <span
          style={{
            fontFamily: "var(--font-mono)",
            fontSize: 9,
            color: palette.chip,
            flexShrink: 0,
          }}
        >
          {abbr[shot.side]}
        </span>
      </div>
      <div
        style={{
          fontFamily: "var(--font-mono)",
          fontSize: 9,
          color: "rgba(246,239,230,.72)",
          lineHeight: 1.5,
        }}
      >
        {shot.minute}&#39; · {shotBodyPart(shot.body_part)}
        <br />
        {dec(shot.xg, 2)} xG · {shotOutcome(shot.outcome)}
      </div>

      <GoalMouth shot={shot} stroke={palette.chip} />
    </div>
  );
}

/* --------------------------------------------------------------------------
 * The goalmouth inset
 * ------------------------------------------------------------------------ */

/** Opta's goalmouth frame (Q102/Q103, Appendix 12), measured over 458 matches. */
const OPTA_GOAL = { postLow: 45.2, postHigh: 54.8, crossbar: 38 };
/** The real goal, in metres. */
const GOAL_M = { width: 7.32, height: 2.44 };

/**
 * Where the ball crossed the line, drawn as a goal seen from behind the
 * shooter. Only for a shot on target: the goalmouth qualifiers exist on every
 * shot, but off target they run to 0-100 and mean "somewhere out there", which
 * a 7.32 m frame cannot show.
 *
 * Low `goal_mouth_y` is the RIGHT flank (GOLD_LAYER §4.6.0), and from behind
 * the shooter the right flank is on the right of the image — hence `50 - y`.
 */
function GoalMouth({ shot, stroke }: { shot: Shot; stroke: string }) {
  if (
    !ON_TARGET.has(shot.outcome) ||
    shot.goal_mouth_y == null ||
    shot.goal_mouth_z == null
  ) {
    return null;
  }

  const span = OPTA_GOAL.postHigh - OPTA_GOAL.postLow;
  const at = {
    x: ((50 - shot.goal_mouth_y) / span) * GOAL_M.width,
    y: (shot.goal_mouth_z / OPTA_GOAL.crossbar) * GOAL_M.height,
  };
  // A shot can cross the line just outside the frame (a post, a save reaching
  // over the bar); keep it visible rather than clipping it away.
  const half = GOAL_M.width / 2;
  const cx = Math.max(-half - 0.9, Math.min(half + 0.9, at.x));
  const cy = Math.max(0, Math.min(GOAL_M.height + 0.9, at.y));
  const isGoal = shot.outcome === "goal";

  return (
    <svg
      viewBox={`${-half - 1.3} ${-1.1} ${GOAL_M.width + 2.6} ${GOAL_M.height + 1.9}`}
      width={GOALMOUTH_WIDTH}
      // Fixed width, centred, rather than 100% of the card: this is the one
      // element whose height follows its width, so leaving it fluid is what
      // let the tooltip grow in two dimensions at once.
      style={{ display: "block", margin: "4px auto 0" }}
      aria-hidden="true"
    >
      {/* Ground line, then the frame: posts and crossbar as one path. */}
      <line
        x1={-half - 0.9}
        y1={GOAL_M.height}
        x2={half + 0.9}
        y2={GOAL_M.height}
        stroke="rgba(246,239,230,.28)"
        strokeWidth={0.05}
      />
      <path
        d={`M${-half},${GOAL_M.height} L${-half},0 L${half},0 L${half},${GOAL_M.height}`}
        fill="none"
        stroke="rgba(246,239,230,.55)"
        strokeWidth={0.11}
        strokeLinecap="round"
      />
      <circle
        cx={cx}
        cy={GOAL_M.height - cy}
        r={0.2}
        fill={isGoal ? stroke : "none"}
        stroke={stroke}
        strokeWidth={0.09}
      />
    </svg>
  );
}
