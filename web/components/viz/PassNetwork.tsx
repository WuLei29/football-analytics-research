/**
 * PassNetwork — the published team's passing structure, on a vertical pitch.
 *
 * Renders: one line per passing pair, one circle per player at their mean
 * position, and the surname above each node.
 * Props: `nodes`, `edges`, `orientation` (vertical in the design), `title`.
 * Data:  `matches/{match_id}.json` -> `network` (md/WEB_DATA.md §7). Nodes are
 *        the mean (x, y) of that player's on-ball events while on the pitch,
 *        with `touches` from `gold.player_match_stats`; edges are unordered
 *        pair counts with the >= 4 threshold applied by the export.
 *
 * Encoding (handoff, screen 02 block 3):
 *   node radius   1.7 -> 2.96, proportional to touches
 *   edge width    0.35 -> 1.85, proportional to combinations, at 0.28 opacity
 *   published team only — the design draws one network, not two
 *
 * Both scales are relative to this match's own maximum, so a low-volume match
 * still uses the full range. That makes node sizes comparable within a match
 * and not across matches, which is what the block is for.
 */

import { linear } from "@/lib/viz/scale";
import { project, type Orientation } from "@/lib/viz/pitch";

import { Pitch } from "./Pitch";

/** One entry of `network.nodes` (WEB_DATA §7). */
export interface NetworkNode {
  player_id: number;
  surname: string;
  x: number;
  y: number;
  touches: number;
}

/** One entry of `network.edges`. `from`/`to` are `player_id`s. */
export interface NetworkEdge {
  from: number;
  to: number;
  passes: number;
}

interface PassNetworkProps {
  nodes: NetworkNode[];
  edges: NetworkEdge[];
  orientation?: Orientation;
  title: string;
  className?: string;
}

const NODE_RADIUS: readonly [number, number] = [1.7, 2.96];
const EDGE_WIDTH: readonly [number, number] = [0.35, 1.85];

export function PassNetwork({
  nodes,
  edges,
  orientation = "vertical",
  title,
  className,
}: PassNetworkProps) {
  const positions = new Map(nodes.map((n) => [n.player_id, project(orientation, n.x, n.y)]));

  const maxTouches = nodes.reduce((m, n) => Math.max(m, n.touches), 0);
  const maxPasses = edges.reduce((m, e) => Math.max(m, e.passes), 0);
  // Domains start at 0 so the smallest node is small rather than minimum-sized.
  const radius = linear([0, maxTouches], NODE_RADIUS);
  const width = linear([0, maxPasses], EDGE_WIDTH);

  return (
    <Pitch orientation={orientation} title={title} className={className}>
      <g stroke="var(--color-blue)" strokeOpacity={0.28}>
        {edges.map((edge) => {
          const a = positions.get(edge.from);
          const b = positions.get(edge.to);
          // An edge to a player with no node would draw from the origin. The
          // export should never emit one; if it does, drop it silently rather
          // than painting a line out of the corner.
          if (!a || !b) return null;
          return (
            <line
              key={`${edge.from}-${edge.to}`}
              x1={a.x}
              y1={a.y}
              x2={b.x}
              y2={b.y}
              strokeWidth={width(edge.passes)}
            />
          );
        })}
      </g>

      {nodes.map((node) => {
        const p = positions.get(node.player_id)!;
        return (
          <g key={node.player_id}>
            <circle
              cx={p.x}
              cy={p.y}
              r={radius(node.touches)}
              fill="var(--color-blue)"
              stroke="var(--color-card)"
              strokeWidth={0.5}
            />
            {/* The one label that has to live inside the SVG: it is positioned
                relative to a pitch coordinate, so it scales with the pitch. */}
            <text
              x={p.x}
              y={p.y - 3.3}
              textAnchor="middle"
              fontFamily="var(--font-mono)"
              fontSize={3}
              fontWeight={500}
              fill="var(--color-ink)"
            >
              {node.surname}
            </text>
          </g>
        );
      })}
    </Pitch>
  );
}
