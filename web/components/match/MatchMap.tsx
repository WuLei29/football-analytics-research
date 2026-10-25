"use client";

/**
 * MatchMap — one of screen 02's three event maps (shots, progression,
 * defensive actions), linked to the sequence block.
 *
 * Renders: the map on a chalk pitch, a white readout card under it naming the
 * mark under the cursor or the keyboard, and the key in words and real marks.
 * The defence map adds its type filter as pills. A click, or Enter, on a
 * mark whose chain is drawable opens that chain in the sequence block
 * (`SequenceLink`).
 * Props: `mode`, the rows of that map straight from the match file,
 *        `teamSide` and the two abbreviations.
 * Data:  `matches/{match_id}.json` -> `shots`, `progression`,
 *        `defence` (md/WEB_DATA.md §7.1); each row's `s` indexes
 *        `sequences[]`.
 *
 * Replaces, on 24 Oct 2026 (Pizarra), the handoff's `ShotMap` /
 * `ProgressionArrows` / `DefensiveActions` wrappers on this page. The block
 * layout is the handoff's (shots beside the xT surface, progression beside
 * defence); what changed is the colours and the keyboard.
 *
 * Keyboard (md/REDESIGN_HANDOFF.md: the critique of 22 Oct found 523 of 523
 * map marks unreachable). Every mark is a labelled `role="button"`. Each map
 * is ONE tab stop (a roving tabindex): Tab enters on the last mark visited,
 * the arrow keys walk the marks in match order, Home / End jump to the ends,
 * Enter or Space opens the chain. Focus or hover updates the readout, which
 * is `aria-live`, so the map reads out as it is walked. The rival's shots
 * and the unlinked marks are reachable too: they say what they are and that
 * no chain can be drawn. The readout replaces the handoff's black hover
 * cards (Pizarra has no black fills).
 *
 * Encodings are the handoff's, recoloured by DESIGN.md's chart mapping:
 *   - shots: `ShotMap`'s (area = xG; solid goal, half on target, hollow
 *     off); Espanyol in team, the rival in rival outline, solid only for goals;
 *   - progression: pass a solid team arrow, carry a dotted team arrow (the
 *     handoff's gold is retired); a failed action at a third of the opacity,
 *     where the handoff dashed it, because dashes now say "carry";
 *   - defence: diamonds, team-filled when the ball was won and team outline
 *     when the duel was lost (one identity, outcome by fill), the average
 *     line dashed in slate.
 * Under 640 px the pitch turns vertical, attacking up, as `ChainPitch` does.
 */

import { useEffect, useMemo, useRef, useState, type KeyboardEvent, type ReactNode } from "react";

import { dec } from "@/lib/format";
import { defensiveActionType, matchExplorer as copy, shotBodyPart, shotOutcome } from "@/lib/labels";
import { PITCH_VIEWBOX, project, type Orientation } from "@/lib/viz/pitch";

import { PitchLines } from "@/components/viz/Pitch";

import { useSequenceLink } from "./SequenceLink";

export interface MapShot {
  shot_id: number;
  side: "home" | "away";
  name: string;
  minute: number;
  x: number;
  y: number;
  xg: number;
  body_part: string;
  outcome: string;
  s?: number | null;
}

export interface MapProgression {
  kind: "pass" | "carry";
  x: number;
  y: number;
  end_x: number;
  end_y: number;
  xt: number | null;
  completed: boolean;
  minute?: number;
  player?: string | null;
  s?: number | null;
}

export interface MapDefence {
  type: string;
  x: number;
  y: number;
  outcome: "won" | "lost";
  minute: number;
  player: string | null;
  s?: number | null;
}

type MapProps =
  | { mode: "shots"; shots: MapShot[]; teamSide: "home" | "away"; abbr: { team: string; rival: string } }
  | { mode: "progression"; actions: MapProgression[] }
  | { mode: "defence"; actions: MapDefence[]; lineX: number | null };

/** One mark of the map, whatever it draws. */
interface Mark {
  key: string;
  s: number | null;
  /** Bigger first in the drawing, so small marks stay on top and reachable. */
  size: number;
  title: string;
  line: string;
  draw: (o: Orientation, state: { active: boolean; opened: boolean; focus: boolean }) => ReactNode;
}

/** `ShotMap`'s radius law: area proportional to xG. */
const SHOT = { base: 0.75, k: 3.3 };
const ON_TARGET = new Set(["goal", "saved", "woodwork"]);
const shotRadius = (xg: number) => SHOT.base + Math.sqrt(Math.max(0, xg)) * SHOT.k;

const DIAMOND = 1.4;
const HIT = 1.8;

/** The defence filter's pill order: the handoff's chip order. */
const DEFENCE_ORDER = ["ball_recovery", "clearance", "interception", "tackle", "aerial", "challenge", "blocked_pass", "block"];

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

/** A ring around the mark with keyboard focus (solid) or whose chain is open (dotted). */
function Ring({ x, y, r, focus }: { x: number; y: number; r: number; focus: boolean }) {
  return (
    <circle
      cx={x}
      cy={y}
      r={r}
      fill="none"
      stroke="var(--color-ink)"
      strokeWidth={focus ? 0.5 : 0.35}
      strokeDasharray={focus ? undefined : "0.1 0.7"}
      strokeLinecap="round"
      pointerEvents="none"
    />
  );
}

function shotMarks(shots: MapShot[], teamSide: "home" | "away", abbr: { team: string; rival: string }): Mark[] {
  return [...shots]
    .sort((a, b) => a.minute - b.minute || a.shot_id - b.shot_id)
    .map((s) => {
      const ours = s.side === teamSide;
      const goal = s.outcome === "goal";
      return {
        key: `shot-${s.shot_id}`,
        s: ours ? (s.s ?? null) : null,
        size: s.xg,
        title: copy.shotTitle(s.name, ours ? abbr.team : abbr.rival),
        line: copy.shotLine(s.minute, dec(s.xg, 2), shotOutcome(s.outcome).toLowerCase(), shotBodyPart(s.body_part).toLowerCase()),
        draw: (o, st) => {
          const p = project(o, s.x, s.y);
          const r = shotRadius(s.xg);
          const ink = ours ? "var(--color-team)" : "var(--color-rival)";
          return (
            <>
              <circle cx={p.x} cy={p.y} r={Math.max(r, HIT)} fill="transparent" />
              <circle
                cx={p.x}
                cy={p.y}
                r={r}
                fill={ink}
                fillOpacity={ours ? (goal ? 1 : ON_TARGET.has(s.outcome) ? 0.45 : 0) : goal ? 0.9 : 0}
                stroke={ink}
                strokeWidth={st.active ? 0.6 : ours ? 0.35 : 0.3}
                strokeOpacity={ours ? 1 : 0.8}
              />
              {(st.focus || st.opened) && <Ring x={p.x} y={p.y} r={r + 0.9} focus={st.focus} />}
            </>
          );
        },
      };
    });
}

function progressionMarks(actions: MapProgression[]): Mark[] {
  return actions
    .map((a, i) => ({ a, i }))
    .sort((x, y) => (x.a.minute ?? 0) - (y.a.minute ?? 0) || x.i - y.i)
    .map(({ a, i }) => ({
      key: `prog-${i}`,
      s: a.s ?? null,
      size: 0,
      title: a.player ? `${copy.progressionTitle[a.kind]} · ${a.player}` : copy.progressionTitle[a.kind],
      line: copy.progressionLine(a.minute ?? null, a.xt == null ? null : dec(a.xt, 3), !a.completed),
      draw: (o, st) => {
        const from = project(o, a.x, a.y);
        const to = project(o, a.end_x, a.end_y);
        const carry = a.kind === "carry";
        const loud = st.active || st.opened || st.focus;
        const opacity = a.completed ? (carry ? 0.95 : 0.75) : 0.32;
        return (
          <>
            <line x1={from.x} y1={from.y} x2={to.x} y2={to.y} stroke="transparent" strokeWidth={HIT} strokeLinecap="round" />
            <line
              x1={from.x}
              y1={from.y}
              x2={to.x}
              y2={to.y}
              stroke="var(--color-team)"
              strokeOpacity={loud ? 1 : opacity}
              strokeWidth={(carry ? 0.5 : 0.32) * (loud ? 1.8 : 1)}
              strokeLinecap="round"
              strokeDasharray={carry ? "0.05 0.95" : undefined}
              markerEnd="url(#map-head)"
            />
            {(st.focus || st.opened) && <Ring x={from.x} y={from.y} r={1.4} focus={st.focus} />}
          </>
        );
      },
    }));
}

function defenceMarks(actions: MapDefence[], type: string | null): Mark[] {
  return actions
    .map((a, i) => ({ a, i }))
    .filter(({ a }) => type === null || a.type === type)
    .sort((x, y) => x.a.minute - y.a.minute || x.i - y.i)
    .map(({ a, i }) => ({
      key: `def-${i}`,
      s: a.s ?? null,
      size: 0,
      title: a.player ? `${defensiveActionType(a.type)} · ${a.player}` : defensiveActionType(a.type),
      line: copy.defenceLine(a.minute, a.outcome === "won"),
      draw: (o, st) => {
        const p = project(o, a.x, a.y);
        const won = a.outcome === "won";
        const half = (DIAMOND / 2) * (st.active ? 1.5 : 1);
        return (
          <>
            <circle cx={p.x} cy={p.y} r={HIT} fill="transparent" />
            <rect
              x={p.x - half}
              y={p.y - half}
              width={half * 2}
              height={half * 2}
              transform={`rotate(45 ${p.x} ${p.y})`}
              fill="var(--color-team)"
              fillOpacity={won ? 0.8 : 0}
              stroke="var(--color-team)"
              strokeWidth={0.28}
            />
            {(st.focus || st.opened) && <Ring x={p.x} y={p.y} r={1.6} focus={st.focus} />}
          </>
        );
      },
    }));
}

export function MatchMap(props: MapProps) {
  const o: Orientation = useNarrow() ? "vertical" : "horizontal";
  const { ids, opened, open } = useSequenceLink();
  const [defenceType, setDefenceType] = useState<string | null>(null);
  const [active, setActive] = useState<number | null>(null);
  const [focusIndex, setFocusIndex] = useState(0);
  const [keyboard, setKeyboard] = useState(false);
  const markRefs = useRef<(SVGGElement | null)[]>([]);

  const marks: Mark[] = useMemo(() => {
    if (props.mode === "shots") return shotMarks(props.shots, props.teamSide, props.abbr);
    if (props.mode === "progression") return progressionMarks(props.actions);
    return defenceMarks(props.actions, defenceType);
  }, [props, defenceType]);

  const defenceCounts = useMemo(() => {
    if (props.mode !== "defence") return [];
    const counts = new Map<string, number>();
    for (const a of props.actions) counts.set(a.type, (counts.get(a.type) ?? 0) + 1);
    const rank = (type: string) => (DEFENCE_ORDER.includes(type) ? DEFENCE_ORDER.indexOf(type) : 99);
    return [...counts.entries()].sort((a, b) => rank(a[0]) - rank(b[0]) || b[1] - a[1]);
  }, [props]);

  // A new filter starts from nothing under the cursor and the first mark.
  useEffect(() => {
    setActive(null);
    setFocusIndex(0);
  }, [defenceType]);

  // Big shots underneath, small ones on top, so every mark stays reachable.
  const drawOrder = useMemo(() => marks.map((_, i) => i).sort((a, b) => marks[b].size - marks[a].size), [marks]);

  const openedId = opened?.sequence_id ?? null;
  const isOpened = (m: Mark) => openedId !== null && m.s != null && ids[m.s] === openedId;
  const hot = active !== null ? marks[active] : null;

  function onMarkKey(e: KeyboardEvent, i: number) {
    let next: number | null = null;
    if (e.key === "ArrowRight" || e.key === "ArrowDown") next = Math.min(marks.length - 1, i + 1);
    else if (e.key === "ArrowLeft" || e.key === "ArrowUp") next = Math.max(0, i - 1);
    else if (e.key === "Home") next = 0;
    else if (e.key === "End") next = marks.length - 1;
    else if (e.key === "Enter" || e.key === " ") {
      e.preventDefault();
      open(marks[i]?.s);
      return;
    }
    if (next !== null) {
      e.preventDefault();
      setFocusIndex(next);
      markRefs.current[next]?.focus();
    }
  }

  const lineX = props.mode === "defence" ? props.lineX : null;
  const linePts = lineX != null ? [project(o, lineX, 0), project(o, lineX, 68)] : null;

  return (
    <div className="flex flex-col gap-3">
      {props.mode === "defence" && (
        <div role="group" aria-label={copy.filterLabel} className="flex flex-wrap gap-1.5">
          {[[null, props.actions.length] as const, ...defenceCounts].map(([type, n]) => {
            const on = type === defenceType;
            return (
              <button
                key={type ?? "all"}
                type="button"
                aria-pressed={on}
                onClick={() => setDefenceType(type)}
                className={`rounded-full px-3 py-1.5 text-[13px] font-medium transition-colors ${
                  on ? "bg-team text-white" : "bg-panel-deep text-ink hover:bg-chalk"
                }`}
              >
                {type === null ? copy.filterAll(n) : copy.filterType(defensiveActionType(type), n)}
              </button>
            );
          })}
        </div>
      )}

      <svg
        viewBox={PITCH_VIEWBOX[o]}
        width="100%"
        role="group"
        aria-label={copy.mapLabel[props.mode](marks.length)}
        className={o === "vertical" ? "mx-auto block max-w-[360px]" : "block"}
        onMouseLeave={() => setActive(null)}
      >
        {props.mode === "progression" && (
          <defs>
            <marker id="map-head" viewBox="0 0 10 10" refX={8} refY={5} markerWidth={4} markerHeight={4} orient="auto-start-reverse">
              <path d="M0,1.5 L8.5,5 L0,8.5 z" fill="var(--color-team)" />
            </marker>
          </defs>
        )}
        <PitchLines orientation={o} color="var(--color-chalk)" />
        {linePts && (
          <line
            x1={linePts[0].x}
            y1={linePts[0].y}
            x2={linePts[1].x}
            y2={linePts[1].y}
            stroke="var(--color-slate)"
            strokeWidth={0.35}
            strokeDasharray="1 1"
            strokeLinecap="round"
          />
        )}
        {drawOrder.map((i) => {
          const m = marks[i];
          return (
            <g
              key={m.key}
              ref={(el) => {
                markRefs.current[i] = el;
              }}
              role="button"
              tabIndex={i === focusIndex ? 0 : -1}
              aria-label={copy.markLabel(m.title, m.line, m.s != null)}
              className="cursor-pointer outline-none"
              onMouseEnter={() => setActive(i)}
              onFocus={(e) => {
                setActive(i);
                setFocusIndex(i);
                setKeyboard(e.currentTarget.matches(":focus-visible"));
              }}
              onBlur={() => setKeyboard(false)}
              onClick={() => open(m.s)}
              onKeyDown={(e) => onMarkKey(e, i)}
            >
              {m.draw(o, { active: active === i, opened: isOpened(m), focus: keyboard && focusIndex === i })}
            </g>
          );
        })}
      </svg>

      <div aria-live="polite" className="min-h-[76px] rounded-row bg-white px-4 py-3">
        {hot ? (
          <>
            <p className="text-[15px] font-semibold">{hot.title}</p>
            <p className="text-[14px] text-slate">{hot.line}</p>
            <p className={`mt-1 text-[13px] font-medium ${hot.s != null ? "text-team" : "text-slate"}`}>
              {hot.s == null ? copy.noChain : isOpened(hot) ? copy.opened : copy.open}
            </p>
          </>
        ) : (
          <p className="text-[14px] text-slate">{copy.idle}</p>
        )}
      </div>

      <Legend {...props} />
    </div>
  );
}

/** The key under the map, in words and real marks. */
function Legend(props: MapProps) {
  const L = copy.legend;
  const dot = (fill: string, fillOpacity: number, stroke: string) => (
    <svg viewBox="0 0 12 12" width="12" height="12" aria-hidden="true" className="mr-2 inline-block shrink-0">
      <circle cx="6" cy="6" r="4.5" fill={fill} fillOpacity={fillOpacity} stroke={stroke} strokeWidth="1.2" />
    </svg>
  );
  const stroke = (dash?: string, opacity = 1, color = "var(--color-team)") => (
    <svg viewBox="0 0 22 12" width="22" height="12" aria-hidden="true" className="mr-2 inline-block shrink-0">
      <line x1="2" y1="6" x2="20" y2="6" stroke={color} strokeOpacity={opacity} strokeWidth="2.4" strokeLinecap="round" strokeDasharray={dash} />
    </svg>
  );
  const diamond = (filled: boolean) => (
    <svg viewBox="0 0 12 12" width="12" height="12" aria-hidden="true" className="mr-2 inline-block shrink-0">
      <rect x="3" y="3" width="6" height="6" transform="rotate(45 6 6)" fill="var(--color-team)" fillOpacity={filled ? 0.8 : 0} stroke="var(--color-team)" strokeWidth="1.2" />
    </svg>
  );

  const items: ReactNode[] =
    props.mode === "shots"
      ? [
          <>{dot("var(--color-team)", 1, "var(--color-team)")}{L.shotTeam(props.abbr.team)}</>,
          <>{dot("var(--color-rival)", 0, "var(--color-rival)")}{L.shotRival(props.abbr.rival)}</>,
          <>{L.size}</>,
        ]
      : props.mode === "progression"
        ? [<>{stroke()}{L.pass}</>, <>{stroke("0.1 4")}{L.carry}</>, <>{stroke(undefined, 0.32)}{L.failed}</>]
        : [
            <>{diamond(true)}{L.won}</>,
            <>{diamond(false)}{L.lost}</>,
            ...(props.lineX != null ? [<>{stroke("3 3", 1, "var(--color-slate)")}{L.line(dec(props.lineX, 1))}</>] : []),
          ];

  return (
    <ul className="flex flex-wrap gap-x-5 gap-y-1 text-[13px] text-slate">
      {items.map((item, i) => (
        <li key={i} className="flex items-center">
          {item}
        </li>
      ))}
    </ul>
  );
}
