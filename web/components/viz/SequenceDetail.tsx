"use client";

/**
 * SequenceDetail — one possession sequence, action by action.
 *
 * Renders: a horizontal pitch with every action of a single sequence drawn in
 * its own mark — passes, crosses, carries, take-ons, regains and the shot —
 * plus a numbered circle for each player who touched the ball, with the final
 * actor inverted; a ring around where the chain began and a ring where the
 * ball ended; and an HTML tooltip on hover naming the hovered action.
 * Props: `actions` (ordered), `title`, `orientation`, node sizing overrides,
 *        `describe` (the caller's words for the tooltip).
 * Data:  `matches/{match_id}.json` -> `sequences[].actions` (md/WEB_DATA.md
 *        §7), from `silver.events` ordered by `(json_index, event_id)`.
 *
 * This is the v1 Streamlit "sequence pitch" ported: the mark vocabulary is
 * `espanyol-viz-design` §7 and the marker radii are its
 * `references/web-translation.md` §4. It is the detailed counterpart to
 * `SequenceTraces`, which draws twelve chains as bare polylines so they can be
 * compared. Use traces to choose a sequence, this to read it.
 *
 * The encoding, and why each mark is what it is:
 *
 *   pass      straight line + arrowhead past the midpoint — direction matters
 *   cross     the same line, curved                       — the curve IS the
 *                                                            cross; no colour
 *                                                            needed to say so
 *   carry     dotted line, a dot at each end              — the ball travels
 *                                                            with the player
 *   take-on   a single team-colour dot                    — a point event
 *   shot      arrow to where it crossed the goal line     — `end_y` is the
 *                                                            goalmouth y in
 *                                                            metres; the goal
 *                                                            centre only when
 *                                                            the export had none
 *   clearance dashed line + arrowhead                     — the ball travels,
 *                                                            nobody meant it to
 *                                                            arrive anywhere
 *   regain    a small team-colour diamond                 — a tackle, an
 *                                                            interception, a
 *                                                            recovery: the ball
 *                                                            was WON here. The
 *                                                            diamond is the
 *                                                            defensive-action
 *                                                            mark of the site
 *                                                            (DefensiveActions)
 *   other     a hollow ring                               — a touch, a claim,
 *                                                            named on hover
 *   start     a team-colour ring around the first mark    — where the chain
 *                                                            began; the tooltip
 *                                                            says how
 *   end       a ring where the ball finished              — the last action's
 *                                                            destination; on a
 *                                                            shot it sits on
 *                                                            the goal line
 *
 * The start and end rings were added on 20 Sep 2026 after the review pass:
 * the moment possession is regained is the key tactical event of a chain, and
 * it was drawn as the same grey ring as any other point event. A regain that
 * happens mid-chain (a failed pass won straight back) keeps the diamond but
 * not the ring, so the two questions "where did we win it" and "where did it
 * start" get different answers when they should.
 *
 * Hover is an HTML tooltip, not an SVG `<title>` (20 Sep 2026): the native
 * one waits a second and cannot carry two lines. The pattern is `ShotMap`'s —
 * a positioned sibling placed by `projectFraction`, flipped below the mark
 * when there is no room above. The hovered action stays at full opacity and
 * the rest fade, so a busy chain can be read one action at a time. This file
 * holds no Spanish: `describe` returns the words, this file places them.
 *
 * Line STYLE carries the action type and colour is kept for team identity, so
 * the chart still obeys the one rule that survives from v1: inside a pitch
 * figure, only the analysed team gets the accent (espanyol-viz-design §1).
 *
 * Two deliberate departures from v1, both recorded in `WEB_PLAN.md` §3.6:
 *
 *   - v1 marked a cross origin in **orange**, "the only orange in the system".
 *     The v2 palette has no orange; `--gold` is the nearest token and is free
 *     inside this chart (its usual meaning, carries, is carried here by the
 *     dotted line instead).
 *   - v1's player node is `s=200`, about r 0.86 in pitch units. That was sized
 *     for a three-column small multiple. On a full-width pitch a 0.86 circle
 *     cannot hold a shirt number; the default here was 1.9 until 20 Sep 2026
 *     and is now 1.5 — the review found 1.9 crowded a dense chain, and a
 *     two-digit number still sets at 1.5 with the 1.8 mono size.
 */

import { useRef, useState } from "react";

import { project, projectFraction, type Orientation } from "@/lib/viz/pitch";

import { Pitch } from "./Pitch";

/** One entry of `sequences[].actions` (WEB_DATA §7). */
export interface SequenceAction {
  kind: "pass" | "cross" | "carry" | "take_on" | "shot" | "clearance" | "other";
  /** `silver.events.event_type` as a snake_case key, e.g. `ball_recovery`. */
  type: string;
  /** Match clock of the action. Absent on the demo fixtures. */
  minute?: number | null;
  second?: number | null;
  x: number;
  y: number;
  /**
   * Absent on point events (a take-on, a failed touch). On a shot it is the
   * goal line at the goalmouth y, in metres (WEB_DATA §7.3).
   */
  end_x?: number | null;
  end_y?: number | null;
  /**
   * Null on a team-level event inside the chain — a corner awarded, a ball
   * out. Those are part of the possession and are drawn as marks, but they
   * get no player node (see `playerNodes`).
   */
  player_id: number | null;
  surname: string | null;
  shirt_number: number | null;
  outcome: "success" | "fail";
  /** Only on a shot. */
  xg?: number | null;
}

/** Which of the chain's two ends a hovered action is, if either. */
export type ActionRole = "start" | "end" | null;

/** The words of one tooltip: a bold title line and up to two mono lines. */
export interface ActionTooltip {
  title: string;
  lines: string[];
}

interface SequenceDetailProps {
  actions: SequenceAction[];
  orientation?: Orientation;
  title: string;
  /** Radius of the numbered player circle, in pitch units. */
  nodeRadius?: number;
  className?: string;
  /**
   * The tooltip of one action — the caller's translation of `type`, the
   * player, the outcome, and for the first/last action how the chain began
   * or ended. Without it the marks have no hover.
   */
  describe?: (action: SequenceAction, role: ActionRole) => ActionTooltip;
}

/** Pitch-unit radii, from espanyol-viz-design references/web-translation.md §4. */
const EVENT_DOT = 0.36;
const TAKE_ON_DOT = 0.55;
/** Half-diagonal of the regain diamond. */
const REGAIN_DIAMOND = 0.75;
/** The ring where the ball ended. */
const END_RING = 1.05;
/** Goal centre — only where a shot shipped without an `end_y`. */
const GOAL = { x: 105, y: 34 };

/**
 * Event types that mean the ball was WON with this action. Drawn as the
 * diamond so they are not mistaken for a touch or a claim. `keeper_pick_up`
 * and `claim` are here too: a keeper gathering a cross is how a chain often
 * begins, and it is a regain in the same sense.
 */
const REGAIN_TYPES = new Set([
  "tackle",
  "interception",
  "ball_recovery",
  "blocked_pass",
  "aerial",
  "keeper_pick_up",
  "claim",
  "keeper_sweeper",
  "punch",
]);

/** Line marks, whose hover target is the line itself. */
const LINE_KINDS = new Set(["pass", "cross", "carry", "clearance", "shot"]);

/** Faded when another action is hovered. */
const DIMMED = 0.3;

/** The tallest tooltip (title + two lines) plus its gap, in CSS px. */
const TOOLTIP_MAX_HEIGHT = 74;
const TOOLTIP_WIDTH = 150;

export function SequenceDetail({
  actions,
  orientation = "horizontal",
  title,
  nodeRadius = 1.5,
  className,
  describe,
}: SequenceDetailProps) {
  const [hovered, setHovered] = useState<number | null>(null);
  const figureRef = useRef<HTMLDivElement>(null);

  const at = (x: number, y: number) => project(orientation, x, y);
  /** An action's destination, falling back to its origin for point events. */
  const endOf = (a: SequenceAction) => ({
    x: a.end_x ?? (a.kind === "shot" ? GOAL.x : a.x),
    y: a.end_y ?? (a.kind === "shot" ? GOAL.y : a.y),
  });
  const opacityOf = (i: number) => (hovered === null || hovered === i ? 1 : DIMMED);
  const roleOf = (i: number): ActionRole =>
    i === 0 ? "start" : i === actions.length - 1 ? "end" : null;

  const first = actions[0];
  const last = actions[actions.length - 1];
  const hoveredAction = hovered !== null ? actions[hovered] : null;

  return (
    <div ref={figureRef} className={className} style={{ position: "relative" }}>
      <Pitch orientation={orientation} title={title}>
        {/* z 2 — the travel of the ball. */}
        <g fill="none">
          {actions.map((action, i) => {
            const from = at(action.x, action.y);
            const end = endOf(action);
            const to = at(end.x, end.y);
            const faded = (action.outcome === "fail" ? 0.45 : 0.8) * opacityOf(i);

            switch (action.kind) {
              case "pass": {
                // Three points so `marker-mid` puts the arrowhead at the middle
                // vertex, which is what v1 draws just past the midpoint.
                const mid = { x: (from.x + to.x) / 2, y: (from.y + to.y) / 2 };
                return (
                  <polyline
                    key={i}
                    points={`${from.x},${from.y} ${mid.x},${mid.y} ${to.x},${to.y}`}
                    stroke="var(--color-mid)"
                    strokeWidth={0.35}
                    strokeOpacity={faded}
                    markerMid="url(#viz-arrow-faint)"
                  />
                );
              }
              case "cross":
                return (
                  <path
                    key={i}
                    d={arcPath(from, to, 0.3)}
                    stroke="var(--color-mid)"
                    strokeWidth={0.35}
                    strokeOpacity={faded}
                    markerEnd="url(#viz-arrow-faint)"
                  />
                );
              case "clearance":
                return (
                  <line
                    key={i}
                    x1={from.x}
                    y1={from.y}
                    x2={to.x}
                    y2={to.y}
                    stroke="var(--color-mid)"
                    strokeWidth={0.35}
                    strokeOpacity={faded}
                    strokeDasharray="2 1"
                    markerEnd="url(#viz-arrow-faint)"
                  />
                );
              case "carry":
                return (
                  <line
                    key={i}
                    x1={from.x}
                    y1={from.y}
                    x2={to.x}
                    y2={to.y}
                    stroke="var(--color-mid)"
                    strokeWidth={0.4}
                    strokeOpacity={faded}
                    strokeDasharray="0.9 0.9"
                    strokeLinecap="round"
                  />
                );
              case "shot":
                return (
                  <line
                    key={i}
                    x1={from.x}
                    y1={from.y}
                    x2={to.x}
                    y2={to.y}
                    stroke="var(--color-blue)"
                    strokeWidth={0.5}
                    strokeOpacity={0.9 * opacityOf(i)}
                    markerEnd="url(#viz-arrow-blue)"
                  />
                );
              default:
                return null;
            }
          })}
        </g>

        {/* z 3 — origin and end dots. */}
        <g>
          {actions.map((action, i) => {
            const from = at(action.x, action.y);
            const end = endOf(action);
            const to = at(end.x, end.y);
            const opacity = opacityOf(i);

            if (action.kind === "take_on") {
              return (
                <circle
                  key={i}
                  cx={from.x}
                  cy={from.y}
                  r={TAKE_ON_DOT}
                  fill="var(--color-blue)"
                  fillOpacity={0.7 * opacity}
                />
              );
            }
            if (action.kind === "other") {
              if (REGAIN_TYPES.has(action.type)) {
                // The ball was won here: the site's defensive-action diamond,
                // in team colour, so a regain is never read as a mere touch.
                return (
                  <path
                    key={i}
                    d={diamondPath(from, REGAIN_DIAMOND)}
                    fill="var(--color-blue)"
                    fillOpacity={0.85 * opacity}
                    stroke="var(--color-card)"
                    strokeWidth={0.2}
                  />
                );
              }
              // A point event with no travel: a ring rather than a dot, so it
              // is not mistaken for the origin of a line that was never drawn.
              return (
                <circle
                  key={i}
                  cx={from.x}
                  cy={from.y}
                  r={EVENT_DOT * 1.6}
                  fill="var(--color-card)"
                  stroke="var(--color-mid)"
                  strokeWidth={0.3}
                  opacity={opacity}
                />
              );
            }
            return (
              <g key={i} opacity={opacity}>
                <circle
                  cx={from.x}
                  cy={from.y}
                  r={EVENT_DOT}
                  // The cross is the one action whose origin is called out: it is
                  // where the delivery came from, which is the thing being read.
                  fill={action.kind === "cross" ? "var(--color-gold)" : "var(--color-mid)"}
                />
                {action.kind === "carry" && (
                  <circle cx={to.x} cy={to.y} r={EVENT_DOT} fill="var(--color-mid)" />
                )}
              </g>
            );
          })}
        </g>

        {/* z 4 — where the ball ended. On a shot this is the goal line at the
            goalmouth y, so a ring on the line IS the shot's placement. Drawn
            before the player nodes so the finisher's circle sits over it when
            the chain ends where it was last touched. */}
        {last && (
          <circle
            cx={at(endOf(last).x, endOf(last).y).x}
            cy={at(endOf(last).x, endOf(last).y).y}
            r={END_RING}
            fill="none"
            stroke="var(--color-ink)"
            strokeWidth={0.4}
            opacity={opacityOf(actions.length - 1)}
          />
        )}

        {/* z 6 — the players, one node per consecutive block of touches. */}
        <g>
          {playerNodes(actions).map((node) => {
            const p = at(node.x, node.y);
            return (
              <g key={`${node.player_id}-${node.index}`}>
                <circle
                  cx={p.x}
                  cy={p.y}
                  r={nodeRadius}
                  fill={node.isFinal ? "var(--color-blue)" : "var(--color-card)"}
                  stroke="var(--color-blue)"
                  strokeWidth={0.3}
                />
                <text
                  x={p.x}
                  y={p.y + 0.65}
                  textAnchor="middle"
                  fontFamily="var(--font-mono)"
                  fontSize={1.8}
                  fontWeight={600}
                  fill={node.isFinal ? "var(--color-card)" : "var(--color-blue)"}
                >
                  {node.shirt_number ?? node.surname?.slice(0, 2).toUpperCase() ?? ""}
                </text>
              </g>
            );
          })}
        </g>

        {/* z 7 — where the chain began: a ring around the first action's
            origin, outside the player node so the number stays legible. */}
        {first && (
          <circle
            cx={at(first.x, first.y).x}
            cy={at(first.x, first.y).y}
            r={nodeRadius + 0.7}
            fill="none"
            stroke="var(--color-blue)"
            strokeWidth={0.35}
            opacity={opacityOf(0)}
          />
        )}

        {/* z 8 — invisible hit targets, one per action, so a 0.35-wide line
            can be hovered without aiming. Point events get a generous disc. */}
        {describe && (
          <g fill="transparent" stroke="transparent" style={{ cursor: "default" }}>
            {actions.map((action, i) => {
              const from = at(action.x, action.y);
              const end = endOf(action);
              const to = at(end.x, end.y);
              const handlers = {
                onMouseEnter: () => setHovered(i),
                onMouseLeave: () => setHovered(null),
              };
              if (LINE_KINDS.has(action.kind) && (from.x !== to.x || from.y !== to.y)) {
                return action.kind === "cross" ? (
                  <path key={i} d={arcPath(from, to, 0.3)} strokeWidth={2.2} {...handlers} />
                ) : (
                  <line
                    key={i}
                    x1={from.x}
                    y1={from.y}
                    x2={to.x}
                    y2={to.y}
                    strokeWidth={2.2}
                    {...handlers}
                  />
                );
              }
              return <circle key={i} cx={from.x} cy={from.y} r={1.4} {...handlers} />;
            })}
          </g>
        )}
      </Pitch>

      {describe && hoveredAction && hovered !== null && (
        <Tooltip
          content={describe(hoveredAction, roleOf(hovered))}
          anchor={tooltipAnchor(hoveredAction, endOf(hoveredAction))}
          orientation={orientation}
          figureHeight={figureRef.current?.clientHeight ?? 0}
        />
      )}
    </div>
  );
}

/* --------------------------------------------------------------------------
 * The tooltip
 * ------------------------------------------------------------------------ */

/** A line is labelled at its midpoint; a point event where it happened. */
function tooltipAnchor(action: SequenceAction, end: { x: number; y: number }) {
  if (LINE_KINDS.has(action.kind)) {
    return { x: (action.x + end.x) / 2, y: (action.y + end.y) / 2 };
  }
  return { x: action.x, y: action.y };
}

/**
 * `ShotMap`'s tooltip, reduced: HTML so the type sets at real pixel sizes,
 * positioned as a fraction of the SVG's box, fixed width so every card is the
 * same size, flipped below the mark when the measured headroom is short.
 */
function Tooltip({
  content,
  anchor,
  orientation,
  figureHeight,
}: {
  content: ActionTooltip;
  anchor: { x: number; y: number };
  orientation: Orientation;
  /** Rendered height of the figure, in CSS px. 0 if not measured yet. */
  figureHeight: number;
}) {
  const at = projectFraction(orientation, anchor.x, anchor.y);
  const headroom = at.y * figureHeight;
  const above = figureHeight > 0 ? headroom > TOOLTIP_MAX_HEIGHT : at.y > 0.25;

  return (
    <div
      style={{
        position: "absolute",
        left: `${at.x * 100}%`,
        top: `${at.y * 100}%`,
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
          fontFamily: "var(--font-sans)",
          fontSize: 12,
          fontWeight: 600,
          lineHeight: 1.25,
          color: "var(--color-on-dark)",
          marginBottom: 2,
        }}
      >
        {content.title}
      </div>
      <div
        style={{
          fontFamily: "var(--font-mono)",
          fontSize: 9,
          color: "rgba(246,239,230,.72)",
          lineHeight: 1.5,
        }}
      >
        {content.lines.map((line, i) => (
          <div key={i}>{line}</div>
        ))}
      </div>
    </div>
  );
}

/* --------------------------------------------------------------------------
 * Geometry
 * ------------------------------------------------------------------------ */

/**
 * A quadratic Bézier bulging to one side of the chord — matplotlib's
 * `connectionstyle="arc3,rad=r"`, which is how every v1 cross was drawn. The
 * control point sits at the midpoint displaced by `rad` times the chord,
 * rotated 90 degrees, so the bulge is always on the same side of travel.
 */
function arcPath(
  from: { x: number; y: number },
  to: { x: number; y: number },
  rad: number,
): string {
  const dx = to.x - from.x;
  const dy = to.y - from.y;
  const cx = (from.x + to.x) / 2 - rad * dy;
  const cy = (from.y + to.y) / 2 + rad * dx;
  return `M${from.x.toFixed(2)},${from.y.toFixed(2)} Q${cx.toFixed(2)},${cy.toFixed(2)} ${to.x.toFixed(2)},${to.y.toFixed(2)}`;
}

/** A square on its corner, `r` from centre to each corner. */
function diamondPath(c: { x: number; y: number }, r: number): string {
  return `M${c.x},${c.y - r} L${c.x + r},${c.y} L${c.x},${c.y + r} L${c.x - r},${c.y} Z`;
}

/**
 * One node per *consecutive block* of actions by the same player, placed at
 * that block's last action — v1's rule, and the reason a player who receives,
 * carries and passes gets one circle rather than three. A player who touches
 * the ball twice in a sequence with someone else in between gets two nodes,
 * which is correct: they were in two different places.
 */
function playerNodes(actions: SequenceAction[]) {
  const nodes: {
    player_id: number;
    surname: string | null;
    shirt_number: number | null;
    x: number;
    y: number;
    index: number;
    isFinal: boolean;
  }[] = [];

  actions.forEach((action, i) => {
    // A team-level event has nobody to label, and two of them in a row are not
    // "the same player still on the ball".
    if (action.player_id === null) return;

    const previous = nodes[nodes.length - 1];
    if (previous && previous.player_id === action.player_id && previous.index === i - 1) {
      // Same player still on the ball: move the node to this, later, action.
      previous.x = action.x;
      previous.y = action.y;
      previous.index = i;
      return;
    }
    nodes.push({
      player_id: action.player_id,
      surname: action.surname,
      shirt_number: action.shirt_number,
      x: action.x,
      y: action.y,
      index: i,
      isFinal: false,
    });
  });

  if (nodes.length) nodes[nodes.length - 1].isFinal = true;
  return nodes;
}
