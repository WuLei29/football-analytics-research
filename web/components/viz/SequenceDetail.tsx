/**
 * SequenceDetail — one possession sequence, action by action.
 *
 * Renders: a horizontal pitch with every action of a single sequence drawn in
 * its own mark — passes, crosses, carries, take-ons and the shot — plus a
 * numbered circle for each player who touched the ball, with the final actor
 * inverted.
 * Props: `actions` (ordered), `title`, `orientation`, node sizing overrides.
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
 *   pass     straight line + arrowhead past the midpoint  — direction matters
 *   cross    the same line, curved                        — the curve IS the
 *                                                           cross; no colour
 *                                                           needed to say so
 *   carry    dotted line, a dot at each end               — the ball travels
 *                                                           with the player
 *   take-on  a single team-colour dot                     — a point event
 *   shot     arrow to the goal centre, team colour        — where it was aimed
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
 *     cannot hold a shirt number, so the default here is 1.9 — in line with the
 *     pass-network node the handoff specifies at 1.7-2.96.
 */

import { project, type Orientation } from "@/lib/viz/pitch";

import { Pitch } from "./Pitch";

/** One entry of `sequences[].actions` (WEB_DATA §7). */
export interface SequenceAction {
  kind: "pass" | "cross" | "carry" | "take_on" | "shot" | "other";
  x: number;
  y: number;
  /** Absent on point events (a take-on, a failed touch). */
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
}

interface SequenceDetailProps {
  actions: SequenceAction[];
  orientation?: Orientation;
  title: string;
  /** Radius of the numbered player circle, in pitch units. */
  nodeRadius?: number;
  className?: string;
}

/** Pitch-unit radii, from espanyol-viz-design references/web-translation.md §4. */
const EVENT_DOT = 0.43;
const TAKE_ON_DOT = 0.66;
/** Goal centre, where a shot is aimed (pitch-guide: the attacking goal). */
const GOAL = { x: 105, y: 34 };

export function SequenceDetail({
  actions,
  orientation = "horizontal",
  title,
  nodeRadius = 1.9,
  className,
}: SequenceDetailProps) {
  const at = (x: number, y: number) => project(orientation, x, y);
  /** An action's destination, falling back to its origin for point events. */
  const endOf = (a: SequenceAction) => ({
    x: a.end_x ?? a.x,
    y: a.end_y ?? a.y,
  });

  return (
    <Pitch orientation={orientation} title={title} className={className}>
      {/* z 2 — the travel of the ball. */}
      <g fill="none">
        {actions.map((action, i) => {
          const from = at(action.x, action.y);
          const end = endOf(action);
          const to = at(end.x, end.y);
          const faded = action.outcome === "fail" ? 0.45 : 0.8;

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
                  x2={at(GOAL.x, GOAL.y).x}
                  y2={at(GOAL.x, GOAL.y).y}
                  stroke="var(--color-blue)"
                  strokeWidth={0.5}
                  strokeOpacity={0.9}
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

          if (action.kind === "take_on") {
            return (
              <circle
                key={i}
                cx={from.x}
                cy={from.y}
                r={TAKE_ON_DOT}
                fill="var(--color-blue)"
                fillOpacity={0.7}
              />
            );
          }
          return (
            <g key={i}>
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
                y={p.y + 0.8}
                textAnchor="middle"
                fontFamily="var(--font-mono)"
                fontSize={2.2}
                fontWeight={600}
                fill={node.isFinal ? "var(--color-card)" : "var(--color-blue)"}
              >
                {node.shirt_number ?? node.surname?.slice(0, 2).toUpperCase() ?? ""}
              </text>
            </g>
          );
        })}
      </g>
    </Pitch>
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
