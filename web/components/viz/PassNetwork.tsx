"use client";

/**
 * PassNetwork — the published team's passing structure, on a vertical pitch.
 *
 * Renders: one line per passing pair, one circle per player at their mean
 * position, and the surname above each node. Every line and every node is a
 * button: hover, focus or tap shows a `HoverCard` — for a pair, the passes
 * between the two, each direction, and any key passes or assists; for a
 * player, their touches — and dims everything not involved.
 * Props: `nodes`, `edges`, `orientation` (vertical in the design), `title`.
 * Data:  `matches/{match_id}.json` -> `network` (md/WEB_DATA.md §7). Nodes are
 *        the mean (x, y) of that player's on-ball events while on the pitch,
 *        with `touches` from `gold.player_match_stats`; edges are unordered
 *        pair counts with the >= 4 threshold applied by the export, and since
 *        1 Nov 2026 per-direction passes, key passes and assists
 *        (`passes_ab`… where `ab` is from -> to). The fixture on /demo has
 *        only `passes`, so every per-direction line is optional.
 *
 * Pizarra (24 Oct 2026, the last handoff page moved): drawn in the Pizarra
 * tokens directly, by DESIGN.md's chart colour mapping (team for the club,
 * rival for the opponent, slate for secondary marks, chalk for markings).
 * `--viz-ground` is the surface the chart sits on, set by the panel classes
 * in globals.css, for the halos and node fills drawn "in the ground colour".
 *
 * Encoding (handoff, screen 02 block 3):
 *   node radius   1.7 -> 2.96, proportional to touches
 *   edge width    0.35 -> 1.85, proportional to combinations, at 0.28 opacity
 *   published team only — the design draws one network, not two
 *
 * Both scales are relative to this match's own maximum, so a low-volume match
 * still uses the full range. That makes node sizes comparable within a match
 * and not across matches, which is what the block is for.
 *
 * Interactive since 1 Nov 2026 (author's review). It owns its <svg> and draws
 * `PitchLines` itself, as `ChainPitch` and `MatchMap` do, because `<Pitch>`
 * sets `role="img"` and would hide the buttons. Keyboard: one tab stop, the
 * arrow keys walk the pairs (most passes first) and then the players; an
 * `aria-live` line repeats the card.
 */

import { useRef, useState, type KeyboardEvent } from "react";

import { matchHover as copy } from "@/lib/labels";
import { linear } from "@/lib/viz/scale";
import { PITCH_VIEWBOX, project, type Orientation } from "@/lib/viz/pitch";

import { HoverCard } from "./HoverCard";
import { PitchLines } from "./Pitch";

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
  passes_ab?: number;
  passes_ba?: number;
  key_passes_ab?: number;
  key_passes_ba?: number;
  assists_ab?: number;
  assists_ba?: number;
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
/** The invisible stroke a pointer catches a thin line by, in metres. */
const EDGE_HIT = 2.4;

/** One hoverable thing: a pair or a player. */
interface Spot {
  key: string;
  kind: "edge" | "node";
  /** The players it involves, for dimming everything else. */
  players: number[];
  at: { x: number; y: number };
  title: string;
  lines: string[];
}

function edgeLines(e: NetworkEdge, a: string, b: string): string[] {
  const lines = [copy.edgePasses(e.passes)];
  if (e.passes_ab != null && e.passes_ba != null) {
    lines.push(copy.edgeDirection(a, b, e.passes_ab), copy.edgeDirection(b, a, e.passes_ba));
  }
  if (e.key_passes_ab) lines.push(copy.edgeKeyPasses(a, b, e.key_passes_ab));
  if (e.key_passes_ba) lines.push(copy.edgeKeyPasses(b, a, e.key_passes_ba));
  if (e.assists_ab) lines.push(copy.edgeAssists(a, b, e.assists_ab));
  if (e.assists_ba) lines.push(copy.edgeAssists(b, a, e.assists_ba));
  return lines;
}

export function PassNetwork({
  nodes,
  edges,
  orientation = "vertical",
  title,
  className,
}: PassNetworkProps) {
  const [hot, setHot] = useState<number | null>(null);
  const [focusIndex, setFocusIndex] = useState(0);
  const refs = useRef<(SVGGElement | null)[]>([]);

  const positions = new Map(nodes.map((n) => [n.player_id, project(orientation, n.x, n.y)]));
  const names = new Map(nodes.map((n) => [n.player_id, n.surname]));
  const [vx, vy, vw, vh] = PITCH_VIEWBOX[orientation].split(" ").map(Number);

  const maxTouches = nodes.reduce((m, n) => Math.max(m, n.touches), 0);
  const maxPasses = edges.reduce((m, e) => Math.max(m, e.passes), 0);
  // Domains start at 0 so the smallest node is small rather than minimum-sized.
  const radius = linear([0, maxTouches], NODE_RADIUS);
  const width = linear([0, maxPasses], EDGE_WIDTH);

  // An edge to a player with no node would draw from the origin. The export
  // should never emit one; if it does, drop it rather than paint a line out
  // of the corner.
  const drawn = edges.filter((e) => positions.has(e.from) && positions.has(e.to));

  // Keyboard order: pairs by passes, then players by touches. Drawing order
  // is separate (edges under nodes), so each spot keeps its index here.
  const spots: Spot[] = [
    ...[...drawn]
      .sort((p, q) => q.passes - p.passes || p.from - q.from || p.to - q.to)
      .map((e): Spot => {
        const a = positions.get(e.from)!;
        const b = positions.get(e.to)!;
        const an = names.get(e.from)!;
        const bn = names.get(e.to)!;
        return {
          key: `e-${e.from}-${e.to}`,
          kind: "edge",
          players: [e.from, e.to],
          at: { x: (a.x + b.x) / 2, y: (a.y + b.y) / 2 },
          title: copy.edgeTitle(an, bn),
          lines: edgeLines(e, an, bn),
        };
      }),
    ...[...nodes]
      .sort((p, q) => q.touches - p.touches || p.player_id - q.player_id)
      .map((n): Spot => ({
        key: `n-${n.player_id}`,
        kind: "node",
        players: [n.player_id],
        at: positions.get(n.player_id)!,
        title: n.surname,
        lines: [copy.nodeTouches(n.touches)],
      })),
  ];
  const indexOf = new Map(spots.map((s, i) => [s.key, i]));
  const active = hot !== null ? spots[hot] : null;

  // What stays lit: a pair lights its two players; a player lights every pair
  // they are in.
  const litEdge = (e: NetworkEdge) =>
    !active ||
    (active.kind === "edge" ? active.key === `e-${e.from}-${e.to}` : active.players.some((p) => p === e.from || p === e.to));
  const litNode = (id: number) =>
    !active ||
    active.players.includes(id) ||
    (active.kind === "node" && drawn.some((e) => (e.from === id || e.to === id) && active.players.some((p) => p === e.from || p === e.to)));

  function onKey(e: KeyboardEvent, i: number) {
    const next =
      e.key === "ArrowRight" || e.key === "ArrowDown"
        ? Math.min(spots.length - 1, i + 1)
        : e.key === "ArrowLeft" || e.key === "ArrowUp"
          ? Math.max(0, i - 1)
          : e.key === "Home"
            ? 0
            : e.key === "End"
              ? spots.length - 1
              : null;
    if (next === null) return;
    e.preventDefault();
    setFocusIndex(next);
    refs.current[next]?.focus();
  }

  const handlers = (i: number) => ({
    ref: (el: SVGGElement | null) => {
      refs.current[i] = el;
    },
    role: "button",
    tabIndex: i === focusIndex ? 0 : -1,
    "aria-label": [spots[i].title, ...spots[i].lines].join(". "),
    className: "cursor-pointer outline-none",
    onMouseEnter: () => setHot(i),
    onFocus: () => {
      setHot(i);
      setFocusIndex(i);
    },
    onBlur: () => setHot(null),
    onClick: () => setHot(i),
    onKeyDown: (e: KeyboardEvent) => onKey(e, i),
  });

  return (
    <div className={`relative ${className ?? ""}`}>
      <svg
        viewBox={PITCH_VIEWBOX[orientation]}
        width="100%"
        role="group"
        aria-label={`${title}. ${copy.networkLabel(nodes.length, drawn.length)}`}
        className="block"
        onMouseLeave={() => setHot(null)}
      >
        <PitchLines orientation={orientation} />

        {/* Thick pairs first, so a thin one is never buried under a thick one. */}
        {[...drawn]
          .sort((p, q) => q.passes - p.passes)
          .map((e) => {
            const a = positions.get(e.from)!;
            const b = positions.get(e.to)!;
            const i = indexOf.get(`e-${e.from}-${e.to}`)!;
            const lit = litEdge(e);
            const isHot = hot === i;
            return (
              <g key={`${e.from}-${e.to}`} {...handlers(i)}>
                <line x1={a.x} y1={a.y} x2={b.x} y2={b.y} stroke="transparent" strokeWidth={EDGE_HIT} />
                <line
                  x1={a.x}
                  y1={a.y}
                  x2={b.x}
                  y2={b.y}
                  stroke="var(--color-team)"
                  strokeOpacity={isHot ? 0.85 : lit ? (active ? 0.55 : 0.28) : 0.08}
                  strokeWidth={width(e.passes)}
                  strokeLinecap="round"
                  pointerEvents="none"
                />
              </g>
            );
          })}

        {nodes.map((node) => {
          const p = positions.get(node.player_id)!;
          const i = indexOf.get(`n-${node.player_id}`)!;
          const lit = litNode(node.player_id);
          return (
            <g key={node.player_id} {...handlers(i)} opacity={lit ? 1 : 0.3}>
              <circle
                cx={p.x}
                cy={p.y}
                r={radius(node.touches)}
                fill="var(--color-team)"
                stroke={hot === i ? "var(--color-ink)" : "var(--viz-ground, white)"}
                strokeWidth={hot === i ? 0.6 : 0.5}
              />
              {/* The one label that has to live inside the SVG: it is positioned
                  relative to a pitch coordinate, so it scales with the pitch. */}
              <text
                x={p.x}
                y={p.y - 3.3}
                textAnchor="middle"
                fontFamily="var(--font-board)"
                fontSize={3}
                fontWeight={500}
                fill="var(--color-ink)"
                pointerEvents="none"
              >
                {node.surname}
              </text>
            </g>
          );
        })}
      </svg>
      {active && (
        <HoverCard
          x={(active.at.x - vx) / vw}
          y={(active.at.y - vy) / vh}
          title={active.title}
          lines={active.lines}
          accent="var(--color-team)"
        />
      )}
      <p aria-live="polite" className="sr-only">
        {active ? [active.title, ...active.lines].join(". ") : ""}
      </p>
    </div>
  );
}
