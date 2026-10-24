"use client";

/**
 * ChainPitch — the landing's signature: the latest match's shots on the
 * light-blue spot panel, where each Espanyol shot draws the possession that created it.
 *
 * Renders: a horizontal pitch on `--color-spot` with every shot of the match
 * (Espanyol in `--color-team`, shooting right; the rival in `--color-rival`,
 * shooting left), the selected shot's possession drawn
 * over it as one round-capped stroke, and a caption under the pitch saying
 * which shot and which possession it is.
 * Props: `shots` (straight from the match file), `chains` (the possessions
 * those shots point at, keyed by the shot's `s`), the two abbreviations.
 * Data:  `matches/{match_id}.json` -> `shots[]` and `sequences[]`, joined by
 *        `shots[].s` (md/WEB_DATA.md §7.1). The page passes only the chains a
 *        shot points at, so this island ships a few hundred bytes of points,
 *        not the 300 KB match file.
 *
 * Interaction: hover, keyboard focus or a tap on an Espanyol shot selects it,
 * and its chain draws itself from first touch to the shot (stroke-dashoffset,
 * an ease-out of 0.9 s; no animation under `prefers-reduced-motion`). With
 * nothing touched, the highest-xG shot that has a chain is selected, so the
 * figure is never empty and works on a phone without hover.
 *
 * On a phone (under 640 px) the pitch turns vertical, Espanyol attacking up:
 * a horizontal pitch 300 px wide makes every shot a speck and every target
 * smaller than a fingertip. The server renders horizontal; the switch happens
 * after hydration (`useNarrow`), before anyone can reach the figure.
 *
 * Deviations from the shared viz components, on purpose:
 *   - It owns its <svg> and draws `PitchLines` itself, like `ShotMap`, because
 *     the shots are focusable buttons: `<Pitch>` sets `role="img"`, which hides
 *     its children from assistive technology. Here the svg is a `group`.
 *   - Colours are the Pizarra inks on the spot panel (team, rival, ink for
 *     the selected outline and focus ring), not the handoff's blue and
 *     grey-brown. Until 24 Oct 2026 this was a dark "night" panel with lit
 *     inks; the author found it too heavy (DESIGN.md).
 *   - Shot size and the three fills are `ShotMap`'s encoding (area = xG;
 *     solid goal, half on target, hollow off), so the two maps read alike.
 */

import { useEffect, useState } from "react";

import { dec } from "@/lib/format";
import { landing, shotOutcome } from "@/lib/labels";
import { PITCH_VIEWBOX, project, type Orientation } from "@/lib/viz/pitch";

import { PitchLines } from "@/components/viz/Pitch";

export interface ChainShot {
  shot_id: number;
  side: "home" | "away";
  name: string;
  minute: number;
  x: number;
  y: number;
  xg: number;
  outcome: string;
  /** Index into the match file's `sequences[]`; null when not drawable. */
  s: number | null;
}

export interface Chain {
  points: [number, number][];
  events: number;
  passes: number;
  duration_s: number;
  xt: number | null;
}

/** `ShotMap`'s radius law: area proportional to xG. */
const SIZE = { base: 0.75, k: 3.3 };
const ON_TARGET = new Set(["goal", "saved", "woodwork"]);

/** True under 640 px. False on the server and on the first client render. */
function useNarrow(): boolean {
  const [narrow, setNarrow] = useState(false);
  useEffect(() => {
    const mq = window.matchMedia("(max-width: 639px)");
    const update = () => setNarrow(mq.matches);
    update();
    mq.addEventListener("change", update);
    return () => mq.removeEventListener("change", update);
  }, []);
  return narrow;
}

function radius(xg: number): number {
  return SIZE.base + Math.sqrt(Math.max(0, xg)) * SIZE.k;
}

/**
 * The export simplifies a chain to its start, its end and up to six waypoints,
 * and the end of a chain that finishes in a shot is where the BALL went (the
 * goal line, or wide), not where it was struck. So the shot's own spot can
 * fall off the drawn line, and the stroke would miss the mark it belongs to.
 * Put the shot back in as the second-to-last point when it is not already on
 * the line (within 1.5 m).
 */
function throughShot(points: [number, number][], shot: { x: number; y: number }): [number, number][] {
  const near = points.some(([x, y]) => Math.hypot(x - shot.x, y - shot.y) < 1.5);
  if (near || points.length < 2) return points;
  return [...points.slice(0, -1), [shot.x, shot.y], points[points.length - 1]];
}

export function ChainPitch({
  shots,
  chains,
  teamSide,
  abbr,
}: {
  shots: ChainShot[];
  chains: Record<number, Chain>;
  teamSide: "home" | "away";
  abbr: { team: string; rival: string };
}) {
  const o: Orientation = useNarrow() ? "vertical" : "horizontal";
  // A fingertip-sized target on a phone (r 5 m is ~46 px across at 340 px
  // wide); targets overlap in a busy box and the smaller chance, drawn last,
  // wins. On desktop a pointer is precise.
  const minHit = o === "vertical" ? 5 : 2.2;
  const ours = shots.filter((s) => s.side === teamSide);
  const theirs = shots.filter((s) => s.side !== teamSide);
  const hasChain = (s: ChainShot) => s.s != null && chains[s.s] !== undefined;
  const linked = ours.filter(hasChain);
  const unlinked = ours.filter((s) => !hasChain(s));

  // The default selection: the biggest chance that has a chain to draw.
  const fallback = [...linked].sort((a, b) => b.xg - a.xg)[0] ?? null;
  const [selectedId, setSelectedId] = useState<number | null>(fallback?.shot_id ?? null);
  const selected = linked.find((s) => s.shot_id === selectedId) ?? fallback;
  const chain = selected && selected.s != null ? chains[selected.s] : undefined;

  const path = chain
    ? throughShot(chain.points, selected!)
        .map(([x, y], i) => {
          const p = project(o, x, y);
          return `${i === 0 ? "M" : "L"}${p.x.toFixed(2)} ${p.y.toFixed(2)}`;
        })
        .join(" ")
    : null;
  const start = chain ? project(o, chain.points[0][0], chain.points[0][1]) : null;

  // Big chances underneath, small ones on top, so every mark stays reachable.
  const byXg = (a: ChainShot, b: ChainShot) => b.xg - a.xg;

  return (
    <figure className="m-0">
      <svg
        viewBox={PITCH_VIEWBOX[o]}
        width="100%"
        role="group"
        aria-label={landing.latestHeading}
        className={o === "vertical" ? "mx-auto block max-w-[340px]" : "block"}
      >
        <PitchLines orientation={o} color="var(--color-spot-line)" />

        {/* The rival's shots: context, not controls. */}
        <g aria-label={landing.rivalShotsLabel(abbr.rival, theirs.length)} role="img">
          {[...theirs].sort(byXg).map((s) => {
            const p = project(o, s.x, s.y);
            return (
              <circle
                key={s.shot_id}
                cx={p.x}
                cy={p.y}
                r={radius(s.xg)}
                fill={s.outcome === "goal" ? "var(--color-rival)" : "none"}
                fillOpacity={s.outcome === "goal" ? 0.9 : 0}
                stroke="var(--color-rival)"
                strokeWidth={0.3}
                strokeOpacity={0.75}
              />
            );
          })}
        </g>

        {/* The selected possession, drawn as one marker stroke. Keyed by the
            shot so a new selection restarts the drawing. */}
        {path && start && (
          <g key={selected!.shot_id} pointerEvents="none">
            <path
              d={path}
              pathLength={1}
              className="chain-stroke"
              fill="none"
              stroke="var(--color-team)"
              strokeWidth={0.75}
              strokeLinecap="round"
              strokeLinejoin="round"
            />
            <circle cx={start.x} cy={start.y} r={0.9} fill="var(--color-team)" />
          </g>
        )}

        {/* Espanyol shots with no drawable chain (under three actions):
            shown, dimmed, and not controls — hovering them would draw nothing. */}
        <g role="img" aria-label={landing.unlinkedShotsLabel(unlinked.length)} opacity={0.4}>
          {[...unlinked].sort(byXg).map((s) => {
            const p = project(o, s.x, s.y);
            return (
              <circle
                key={s.shot_id}
                cx={p.x}
                cy={p.y}
                r={radius(s.xg)}
                fill="var(--color-team)"
                fillOpacity={s.outcome === "goal" ? 1 : ON_TARGET.has(s.outcome) ? 0.45 : 0}
                stroke="var(--color-team)"
                strokeWidth={0.35}
              />
            );
          })}
        </g>

        {[...linked].sort(byXg).map((s) => {
          const p = project(o, s.x, s.y);
          const isSelected = s.shot_id === selected?.shot_id;
          const fillOpacity =
            s.outcome === "goal" ? 1 : ON_TARGET.has(s.outcome) ? 0.45 : 0;
          const select = () => setSelectedId(s.shot_id);
          return (
            <g
              key={s.shot_id}
              role="button"
              tabIndex={0}
              aria-pressed={isSelected}
              aria-label={landing.shotLabel(s.minute, s.name, dec(s.xg, 2), shotOutcome(s.outcome))}
              onMouseEnter={select}
              onFocus={select}
              onClick={select}
              onKeyDown={(e) => {
                if (e.key === "Enter" || e.key === " ") {
                  e.preventDefault();
                  select();
                }
              }}
              className="cursor-pointer outline-none"
            >
              {/* A generous invisible target: small chances are tiny marks. */}
              <circle cx={p.x} cy={p.y} r={Math.max(radius(s.xg), minHit)} fill="transparent" />
              <circle
                cx={p.x}
                cy={p.y}
                r={radius(s.xg)}
                fill="var(--color-team)"
                fillOpacity={fillOpacity}
                stroke={isSelected ? "var(--color-ink)" : "var(--color-team)"}
                strokeWidth={isSelected ? 0.55 : 0.35}
              />
              {/* Focus ring drawn in the figure: an outline on an svg <g>
                  is not painted consistently across browsers. */}
              <circle
                cx={p.x}
                cy={p.y}
                r={radius(s.xg) + 1.1}
                fill="none"
                stroke="var(--color-ink)"
                strokeWidth={0.35}
                className="shot-ring"
              />
            </g>
          );
        })}
      </svg>

      <figcaption className="mt-4 min-h-[3.25rem]" aria-live="polite">
        {selected ? (
          <>
            <p className="text-[17px] font-semibold text-ink">
              {landing.chainCaption(selected.minute, selected.name, dec(selected.xg, 2), shotOutcome(selected.outcome))}
            </p>
            <p className="text-[14px] text-spot-text">
              {chain
                ? landing.chainDetail(chain.events, chain.passes, Math.round(chain.duration_s)) +
                  (chain.xt != null ? landing.chainXt(dec(chain.xt, 2)) : "")
                : landing.noChain}
            </p>
          </>
        ) : null}
      </figcaption>
    </figure>
  );
}
