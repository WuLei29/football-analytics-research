"use client";

/**
 * DefensiveActions — where the team defended, and how high its line sat.
 *
 * Renders: one diamond per defensive action, blue when the ball was regained
 * and grey-brown when the duel was lost, a dashed average-defensive-line
 * marker across the pitch, and a tooltip on hover.
 * Props: `actions`, `lineX` (metres up the pitch), `lineLabel`, `orientation`.
 * Data:  `matches/{match_id}.json` -> `defence` (md/WEB_DATA.md §7): the 34
 *        highest tackles, interceptions, recoveries, challenges, blocks and
 *        clearances, and `defensive_line_height` from `gold.team_match_stats`.
 *
 * `line_x` is in the same attacking frame as everything else, so a low value
 * is a deep line and the marker needs no interpretation (WEB_DATA §3.1).
 *
 * A diamond is a rotated square, and the design's prototype rotates it about
 * its own top-left corner, which leaves every mark offset by half a diagonal
 * from the event it describes. Here the square is centred on the action first,
 * so the mark sits where the tackle happened. Same shape, right place.
 *
 * Client component, because it answers a mouse. Thirty-four identical diamonds
 * say *where* the team defended but not *what happened*; hovering names the
 * action and the minute, which is the difference between a shape and a match.
 * The tooltip is HTML, not SVG, for the reason `ShotMap` gives: its type then
 * renders at real pixel sizes whatever the pitch scales to.
 */

import { useState } from "react";

import { defensiveActionType, viz } from "@/lib/labels";
import {
  project,
  projectFraction,
  projectRect,
  type Orientation,
} from "@/lib/viz/pitch";

import { Pitch } from "./Pitch";

/** One entry of `defence.actions` (WEB_DATA §7). */
export interface DefensiveAction {
  type: string;
  x: number;
  y: number;
  outcome: "won" | "lost";
  /** Match minute. Optional: without it the tooltip just omits the line. */
  minute?: number | null;
  /** Who made it. Optional for the same reason. */
  player?: string | null;
}

interface DefensiveActionsProps {
  actions: DefensiveAction[];
  /** `defence.line_x`, in metres. Omit to leave the line off. */
  lineX?: number;
  /** Mono caption drawn beside the line — build the number with `dec()`. */
  lineLabel?: string;
  orientation?: Orientation;
  title: string;
  className?: string;
}

/** Side of the square, in metres, before rotation (handoff: 1.9). */
const SIDE = 1.9;
/**
 * The invisible disc that catches the mouse. A 1.9 m diamond is a small target
 * at the sizes this pitch renders at, so the hit area is bigger than the mark —
 * standard practice, and it costs nothing because the disc has no fill.
 */
const HIT_RADIUS = 2.4;

export function DefensiveActions({
  actions,
  lineX,
  lineLabel,
  orientation = "horizontal",
  title,
  className,
}: DefensiveActionsProps) {
  const [hovered, setHovered] = useState<number | null>(null);
  const active = hovered === null ? null : (actions[hovered] ?? null);

  return (
    <div className={className} style={{ position: "relative" }}>
      <Pitch orientation={orientation} title={title}>
        {lineX !== undefined && (
          <DefensiveLine orientation={orientation} lineX={lineX} label={lineLabel} />
        )}

        {actions.map((action, i) => {
          const p = project(orientation, action.x, action.y);
          return (
            <g
              key={i}
              opacity={hovered === null || hovered === i ? 1 : 0.3}
              style={{ cursor: "pointer" }}
              onMouseEnter={() => setHovered(i)}
              onMouseLeave={() => setHovered(null)}
            >
              <rect
                x={p.x - SIDE / 2}
                y={p.y - SIDE / 2}
                width={SIDE}
                height={SIDE}
                fill={action.outcome === "won" ? "var(--color-blue)" : "var(--color-opp)"}
                transform={`rotate(45 ${p.x.toFixed(2)} ${p.y.toFixed(2)})`}
              />
              <circle cx={p.x} cy={p.y} r={HIT_RADIUS} fill="transparent" />
            </g>
          );
        })}
      </Pitch>

      {active && <Tooltip action={active} orientation={orientation} />}
    </div>
  );
}

/**
 * Positioned as a percentage of the SVG's own box — hence `projectFraction`,
 * which accounts for the viewBox padding. Same mechanism as the shot tooltip.
 */
function Tooltip({
  action,
  orientation,
}: {
  action: DefensiveAction;
  orientation: Orientation;
}) {
  const at = projectFraction(orientation, action.x, action.y);
  const won = action.outcome === "won";

  return (
    <div
      style={{
        position: "absolute",
        left: `${at.x * 100}%`,
        top: `${at.y * 100}%`,
        transform: "translate(-50%,-128%)",
        background: "var(--color-ink)",
        borderRadius: 10,
        padding: "6px 9px",
        pointerEvents: "none",
        whiteSpace: "nowrap",
        zIndex: 4,
      }}
    >
      <div
        style={{
          fontFamily: "var(--font-sans)",
          fontSize: 12,
          fontWeight: 600,
          color: "var(--color-on-dark)",
        }}
      >
        {defensiveActionType(action.type)}
      </div>
      <div
        style={{
          fontFamily: "var(--font-mono)",
          fontSize: 9.5,
          color: won ? "#7FB2ED" : "rgba(246,239,230,.72)",
          lineHeight: 1.6,
        }}
      >
        {action.minute != null && `${action.minute}' · `}
        {won ? viz.legend.regain : viz.legend.duelLost}
        {action.player && (
          <>
            <br />
            {action.player}
          </>
        )}
      </div>
    </div>
  );
}

function DefensiveLine({
  orientation,
  lineX,
  label,
}: {
  orientation: Orientation;
  lineX: number;
  label?: string;
}) {
  const line = projectRect(orientation, lineX, 0, lineX, 68);
  // Put the caption just past the line, at the top of whichever frame we are in.
  const anchor = project(orientation, lineX, 68);

  return (
    <g>
      <line
        x1={line.x}
        y1={line.y}
        x2={line.x + line.width}
        y2={line.y + line.height}
        stroke="var(--color-neg)"
        strokeWidth={0.4}
        strokeDasharray="1.4 1.4"
      />
      {label && (
        <text
          x={anchor.x + (orientation === "horizontal" ? 0.8 : 0)}
          y={anchor.y + (orientation === "horizontal" ? 2.6 : -1.1)}
          fontFamily="var(--font-mono)"
          fontSize={2.1}
          fill="var(--color-neg)"
        >
          {label}
        </text>
      )}
    </g>
  );
}
