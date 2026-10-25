"use client";

/**
 * MatchExplorer — screen 02's light-blue spot panel: from a mark on a map to
 * the possession it belongs to.
 *
 * Renders: a switch between three maps (Tiros / Progresión / Defensa), the
 * chosen map on a spot-line pitch, a white readout card under it naming the
 * mark under the cursor or the keyboard, and beside it a white card with the
 * opened chain (`SequenceDetail`, its numbers and its actions in words).
 * Underneath, every chain of the match as a list sorted by xT or VAEP.
 * Props: rows straight from the match file (the page trims nothing but the
 *        fields it never reads), `teamSide` and the two abbreviations.
 * Data:  `matches/{match_id}.json` -> `shots`, `progression`,
 *        `defence`, and `sequences` in FILE order, because each map row's `s`
 *        indexes that array (md/WEB_DATA.md §7.1, §7.3).
 *
 * Replaces, on 24 Oct 2026, the handoff page's three map cards, its
 * `SequenceLink` context and `MatchSequences`: with all of them in one panel
 * there is no cross-page link to carry and no page scroll on a click.
 *
 * Keyboard (md/REDESIGN_HANDOFF.md: the critique of 22 Oct found 523 of 523
 * map marks unreachable). Every mark is a labelled `role="button"`. The map
 * is ONE tab stop (a roving tabindex): Tab enters on the last mark visited,
 * the arrow keys walk the marks in match order, Home / End jump to the ends,
 * Enter or Space opens the chain. Focus or hover updates the readout, which
 * is `aria-live`, so the map reads out as it is walked. The rival's shots
 * and the unlinked marks are reachable too: they say what they are and that
 * no chain can be drawn, rather than being silent.
 *
 * Encodings are the handoff's, recoloured by DESIGN.md's chart mapping:
 *   - shots: `ShotMap`'s (area = xG; solid goal, half on target, hollow
 *     off); Espanyol in team, the rival in rival outline, solid only for goals;
 *   - progression: pass a solid team arrow, carry a dotted team arrow (the
 *     handoff's gold is retired); a failed action at a third of the opacity,
 *     where the handoff dashed it, because dashes now say "carry";
 *   - defence: diamonds, team-filled when the ball was won and team outline
 *     when the duel was lost (one identity, outcome by fill), the average
 *     line dashed in slate, type filter pills as the handoff's chips.
 * Under 640 px both pitches turn vertical, attacking up, as `ChainPitch` does.
 */

import { useEffect, useMemo, useRef, useState, type KeyboardEvent, type ReactNode } from "react";

import { dec } from "@/lib/format";
import {
  defensiveActionType,
  matchExplorer as copy,
  metricDefinitions,
  sequenceActionType,
  sequenceEnding,
  sequenceKind,
  sequencePhase,
  sequenceTrigger,
  shotBodyPart,
  shotOutcome,
  viz,
  type SequenceMetric,
} from "@/lib/labels";
import { PITCH_VIEWBOX, project, type Orientation } from "@/lib/viz/pitch";

import { PitchLines } from "@/components/viz/Pitch";
import type { SequenceEntry } from "@/components/viz/SequenceBrowser";
import { SequenceDetail } from "@/components/viz/SequenceDetail";

export interface ExplorerShot {
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

export interface ExplorerProgression {
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

export interface ExplorerDefence {
  type: string;
  x: number;
  y: number;
  outcome: "won" | "lost";
  minute: number;
  player: string | null;
  s?: number | null;
}

type Mode = "shots" | "progression" | "defence";

/** One mark of the current map, whatever it draws. */
interface Mark {
  key: string;
  minute: number;
  s: number | null;
  title: string;
  line: string;
  draw: (o: Orientation, state: { active: boolean; opened: boolean; focus: boolean }) => ReactNode;
}

/* ---- encodings ---------------------------------------------------------- */

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

const clockOf = (minute: number | null | undefined, second: number | null | undefined) =>
  minute == null ? null : `${minute}'${String(second ?? 0).padStart(2, "0")}"`;

/** A ring drawn around the mark that has keyboard focus or is opened. */
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

export function MatchExplorer({
  shots,
  progression,
  defence,
  sequences,
  teamSide,
  abbr,
}: {
  shots: ExplorerShot[];
  progression: ExplorerProgression[];
  defence: { line_x: number | null; actions: ExplorerDefence[] };
  /** In FILE order: a map row's `s` is an index into this array. */
  sequences: SequenceEntry[];
  teamSide: "home" | "away";
  abbr: { team: string; rival: string };
}) {
  const o: Orientation = useNarrow() ? "vertical" : "horizontal";
  const [mode, setMode] = useState<Mode>("shots");
  const [defenceType, setDefenceType] = useState<string | null>(null);
  const [active, setActive] = useState<number | null>(null);
  const [focusIndex, setFocusIndex] = useState(0);
  const [keyboard, setKeyboard] = useState(false);
  const [openedMark, setOpenedMark] = useState<string | null>(null);
  const [openedSeq, setOpenedSeq] = useState<number | null>(sequences.length > 0 ? 0 : null);
  const [metric, setMetric] = useState<SequenceMetric>("xt");

  const markRefs = useRef<(SVGGElement | null)[]>([]);
  const chainRef = useRef<HTMLDivElement>(null);
  const listRef = useRef<HTMLUListElement>(null);

  /* ---- the marks of the current map, in match order -------------------- */
  const defenceCounts = useMemo(() => {
    const counts = new Map<string, number>();
    for (const a of defence.actions) counts.set(a.type, (counts.get(a.type) ?? 0) + 1);
    const rank = (type: string) => (DEFENCE_ORDER.includes(type) ? DEFENCE_ORDER.indexOf(type) : 99);
    return [...counts.entries()].sort((a, b) => rank(a[0]) - rank(b[0]) || b[1] - a[1]);
  }, [defence.actions]);

  const marks: Mark[] = useMemo(() => {
    if (mode === "shots") {
      return [...shots]
        .sort((a, b) => a.minute - b.minute || a.shot_id - b.shot_id)
        .map((s) => {
          const ours = s.side === teamSide;
          const goal = s.outcome === "goal";
          return {
            key: `shot-${s.shot_id}`,
            minute: s.minute,
            s: ours ? (s.s ?? null) : null,
            title: copy.shotTitle(s.name, ours ? abbr.team : abbr.rival),
            line: copy.shotLine(s.minute, dec(s.xg, 2), shotOutcome(s.outcome).toLowerCase(), shotBodyPart(s.body_part).toLowerCase()),
            draw: (or, st) => {
              const p = project(or, s.x, s.y);
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
    if (mode === "progression") {
      return progression
        .map((a, i) => ({ a, i }))
        .sort((x, y) => (x.a.minute ?? 0) - (y.a.minute ?? 0) || x.i - y.i)
        .map(({ a, i }) => ({
          key: `prog-${i}`,
          minute: a.minute ?? 0,
          s: a.s ?? null,
          title: a.player ? `${copy.progressionTitle[a.kind]} · ${a.player}` : copy.progressionTitle[a.kind],
          line: copy.progressionLine(a.minute ?? null, a.xt == null ? null : dec(a.xt, 3), !a.completed),
          draw: (or, st) => {
            const from = project(or, a.x, a.y);
            const to = project(or, a.end_x, a.end_y);
            const carry = a.kind === "carry";
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
                  strokeOpacity={st.active || st.opened ? 1 : opacity}
                  strokeWidth={(carry ? 0.5 : 0.32) * (st.active || st.opened || st.focus ? 1.8 : 1)}
                  strokeLinecap="round"
                  strokeDasharray={carry ? "0.05 0.95" : undefined}
                  markerEnd="url(#explorer-head)"
                />
                {(st.focus || st.opened) && <Ring x={from.x} y={from.y} r={1.4} focus={st.focus} />}
              </>
            );
          },
        }));
    }
    return defence.actions
      .map((a, i) => ({ a, i }))
      .filter(({ a }) => defenceType === null || a.type === defenceType)
      .sort((x, y) => x.a.minute - y.a.minute || x.i - y.i)
      .map(({ a, i }) => ({
        key: `def-${i}`,
        minute: a.minute,
        s: a.s ?? null,
        title: a.player ? `${defensiveActionType(a.type)} · ${a.player}` : defensiveActionType(a.type),
        line: copy.defenceLine(a.minute, a.outcome === "won"),
        draw: (or, st) => {
          const p = project(or, a.x, a.y);
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
  }, [mode, shots, progression, defence.actions, defenceType, teamSide, abbr]);

  // A new map starts from nothing under the cursor and the first mark as the
  // tab stop.
  useEffect(() => {
    setActive(null);
    setFocusIndex(0);
  }, [mode, defenceType]);

  const hot = active !== null ? marks[active] : null;

  /* ---- opening a chain -------------------------------------------------- */
  function openMark(i: number) {
    const m = marks[i];
    if (!m || m.s == null || !sequences[m.s]) return;
    setOpenedMark(m.key);
    setOpenedSeq(m.s);
    // On a phone the chain card is below the map: bring it up without
    // jumping when it is already in view.
    chainRef.current?.scrollIntoView({ block: "nearest", behavior: "smooth" });
  }

  function onMarkKey(e: KeyboardEvent, i: number) {
    let next: number | null = null;
    if (e.key === "ArrowRight" || e.key === "ArrowDown") next = Math.min(marks.length - 1, i + 1);
    else if (e.key === "ArrowLeft" || e.key === "ArrowUp") next = Math.max(0, i - 1);
    else if (e.key === "Home") next = 0;
    else if (e.key === "End") next = marks.length - 1;
    else if (e.key === "Enter" || e.key === " ") {
      e.preventDefault();
      openMark(i);
      return;
    }
    if (next !== null) {
      e.preventDefault();
      setFocusIndex(next);
      markRefs.current[next]?.focus();
    }
  }

  /* ---- the list -------------------------------------------------------- */
  const ordered = useMemo(() => {
    const rows = sequences.map((seq, index) => ({ seq, index }));
    return metric === "xt" ? rows : [...rows].sort((a, b) => b.seq[metric] - a.seq[metric]);
  }, [sequences, metric]);

  // Keep the opened row in sight inside the list (not the page).
  useEffect(() => {
    const list = listRef.current;
    const row = list?.querySelector<HTMLElement>("[aria-pressed='true']");
    if (list && row && (row.offsetTop < list.scrollTop || row.offsetTop > list.scrollTop + list.clientHeight - 40)) {
      list.scrollTop = row.offsetTop - 8;
    }
  }, [openedSeq, metric]);

  const chain = openedSeq !== null ? sequences[openedSeq] : null;
  const M = metricDefinitions[metric].abbr;

  /* ---- drawing order: big shots under small ones, so all stay reachable -- */
  const drawOrder = useMemo(() => {
    const idx = marks.map((_, i) => i);
    if (mode === "shots") {
      const xg = new Map(shots.map((s) => [`shot-${s.shot_id}`, s.xg]));
      idx.sort((a, b) => (xg.get(marks[b].key) ?? 0) - (xg.get(marks[a].key) ?? 0));
    }
    return idx;
  }, [marks, mode, shots]);

  const lineX = defence.line_x;
  const linePts =
    mode === "defence" && lineX != null ? [project(o, lineX, 0), project(o, lineX, 68)] : null;

  return (
    <div className="flex flex-col gap-4">
      {/* The map switch, and the type filter of the defence map. */}
      <div className="flex flex-wrap items-center gap-x-4 gap-y-3 px-2">
        <div role="group" aria-label={copy.modesLabel} className="flex gap-0.5 rounded-full bg-spot-deep/60 p-1 sm:gap-1">
          {(["shots", "progression", "defence"] as const).map((m) => {
            const n = m === "shots" ? shots.length : m === "progression" ? progression.length : defence.actions.length;
            const on = m === mode;
            return (
              <button
                key={m}
                type="button"
                aria-pressed={on}
                onClick={() => setMode(m)}
                className={`rounded-full px-3 py-2 text-[13px] font-semibold whitespace-nowrap transition-colors sm:px-4 sm:text-[14px] ${
                  on ? "bg-team text-white" : "text-team hover:bg-spot-deep"
                }`}
              >
                {copy.modes[m](n)}
              </button>
            );
          })}
        </div>
        {mode === "defence" && (
          <div role="group" aria-label={copy.filterLabel} className="flex flex-wrap gap-1.5">
            {[[null, defence.actions.length] as const, ...defenceCounts].map(([type, n]) => {
              const on = type === defenceType;
              return (
                <button
                  key={type ?? "all"}
                  type="button"
                  aria-pressed={on}
                  onClick={() => setDefenceType(type)}
                  className={`rounded-full px-3 py-1.5 text-[13px] font-medium transition-colors ${
                    on ? "bg-white text-team" : "text-spot-text hover:bg-spot-deep"
                  }`}
                >
                  {type === null ? copy.filterAll(n) : copy.filterType(defensiveActionType(type), n)}
                </button>
              );
            })}
          </div>
        )}
      </div>

      <div className="grid gap-4 lg:grid-cols-[minmax(0,1.45fr)_minmax(0,1fr)]">
        {/* --- The map, and its readout ------------------------------------ */}
        <div className="flex flex-col gap-3">
          <svg
            viewBox={PITCH_VIEWBOX[o]}
            width="100%"
            role="group"
            aria-label={copy.mapLabel[mode](marks.length)}
            className={o === "vertical" ? "mx-auto block max-w-[360px]" : "block"}
            onMouseLeave={() => setActive(null)}
          >
            <defs>
              <marker id="explorer-head" viewBox="0 0 10 10" refX={8} refY={5} markerWidth={4} markerHeight={4} orient="auto-start-reverse">
                <path d="M0,1.5 L8.5,5 L0,8.5 z" fill="var(--color-team)" />
              </marker>
            </defs>
            <PitchLines orientation={o} color="var(--color-spot-line)" />
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
                  onClick={() => openMark(i)}
                  onKeyDown={(e) => onMarkKey(e, i)}
                >
                  {m.draw(o, {
                    active: active === i,
                    opened: openedMark === m.key,
                    focus: keyboard && focusIndex === i,
                  })}
                </g>
              );
            })}
          </svg>

          <div className="grid gap-3 sm:grid-cols-[minmax(0,1fr)_auto]">
            <div aria-live="polite" className="min-h-[76px] rounded-row bg-white px-4 py-3">
              {hot ? (
                <>
                  <p className="text-[15px] font-semibold">{hot.title}</p>
                  <p className="text-[14px] text-slate">{hot.line}</p>
                  <p className={`mt-1 text-[13px] font-medium ${hot.s != null ? "text-team" : "text-slate"}`}>
                    {hot.s == null ? copy.noChain : openedMark === hot.key ? copy.opened : copy.open}
                  </p>
                </>
              ) : (
                <p className="text-[14px] text-slate">{copy.idle}</p>
              )}
            </div>
            <Legend mode={mode} abbr={abbr} lineX={lineX} />
          </div>
        </div>

        {/* --- The opened chain -------------------------------------------- */}
        <div ref={chainRef} className="scroll-mt-24 rounded-row bg-white p-4 sm:p-5">
          {chain ? (
            <ChainCard chain={chain} orientation={o} />
          ) : (
            <p className="text-[15px] text-slate">{copy.chainEmpty}</p>
          )}
        </div>
      </div>

      {/* --- Every chain ---------------------------------------------------- */}
      {sequences.length > 0 && (
        <div className="rounded-row bg-white p-4 sm:p-5">
          <div className="flex flex-wrap items-end justify-between gap-3">
            <div>
              <h3 className="text-[18px] font-semibold tracking-[-0.01em]">{copy.listHeading}</h3>
              <p className="text-[14px] text-slate">{copy.listCaption(sequences.length)}</p>
            </div>
            <div role="group" aria-label={copy.sortLabel} className="flex gap-1 rounded-full bg-spot p-1">
              {(["xt", "vaep"] as const).map((m) => (
                <button
                  key={m}
                  type="button"
                  aria-pressed={metric === m}
                  onClick={() => setMetric(m)}
                  className={`rounded-full px-4 py-1.5 text-[14px] font-semibold transition-colors ${
                    metric === m ? "bg-team text-white" : "text-team hover:bg-spot-deep"
                  }`}
                >
                  {metricDefinitions[m].abbr}
                </button>
              ))}
            </div>
          </div>
          <ul ref={listRef} className="relative mt-3 grid max-h-[400px] gap-1 overflow-y-auto pr-1 lg:grid-cols-2 lg:gap-x-3">
            {ordered.map(({ seq, index }) => {
              const on = index === openedSeq;
              return (
                <li key={seq.sequence_id}>
                  <button
                    type="button"
                    aria-pressed={on}
                    onClick={() => {
                      setOpenedSeq(index);
                      setOpenedMark(null);
                    }}
                    className={`grid w-full grid-cols-[minmax(0,1fr)_auto] items-baseline gap-x-3 rounded-[12px] px-3 py-2 text-left transition-colors ${
                      on ? "bg-spot" : "hover:bg-panel"
                    }`}
                  >
                    <span className={`truncate text-[15px] font-semibold ${on ? "text-team" : ""}`}>
                      {seq.kind ? sequenceKind(seq.kind) : sequencePhase(seq.primary_phase)}
                    </span>
                    <span className="text-[14px] text-slate tabular-nums">
                      {clockOf(seq.start_minute, seq.start_second)}
                    </span>
                    <span className="truncate text-[13px] text-slate">
                      {sequenceEnding(seq.outcome, seq.end_action)} ·{" "}
                      {viz.sequenceList.summary(String(seq.event_count), dec(seq.duration_s, 1))}
                    </span>
                    <span className="text-[13px] font-medium text-team tabular-nums">
                      {copy.rowValue(M, dec(seq[metric], 3))}
                    </span>
                  </button>
                </li>
              );
            })}
          </ul>
        </div>
      )}
    </div>
  );
}

/** The opened chain: its name, how it began and ended, the drawing, the numbers. */
function ChainCard({ chain, orientation }: { chain: SequenceEntry; orientation: Orientation }) {
  const panel = viz.sequencePanel;
  const clock = clockOf(chain.start_minute, chain.start_second) ?? "";
  return (
    <div>
      <h3 className="text-[18px] font-semibold tracking-[-0.01em]">
        {chain.kind ? sequenceKind(chain.kind) : sequencePhase(chain.primary_phase)}
      </h3>
      <p className="text-[14px] text-slate">{copy.chainMeta(clock, sequenceEnding(chain.outcome, chain.end_action))}</p>

      {/* The shared drawing, recoloured by the `.pizarra-viz` adapter
          (globals.css). No `describe`: its marks get no hover card, so none
          of them is a control the keyboard cannot reach; the actions are
          listed in words below instead. */}
      <div className={`pizarra-viz mt-3 ${orientation === "vertical" ? "mx-auto max-w-[300px]" : ""}`}>
        <SequenceDetail key={chain.sequence_id} actions={chain.actions} title={copy.chainPitch} orientation={orientation} />
      </div>

      <dl className="mt-3 grid grid-cols-2 gap-x-3 gap-y-2 min-[420px]:grid-cols-3 sm:grid-cols-5">
        {[
          [panel.trigger, chain.start_kind ? sequenceTrigger(chain.start_kind) : "—"],
          [panel.duration, `${dec(chain.duration_s, 1)} s`],
          [panel.events, String(chain.event_count)],
          [panel.xt, dec(chain.xt, 3)],
          [panel.vaep, dec(chain.vaep, 3)],
        ].map(([k, v]) => (
          <div key={k} className="min-w-0">
            <dt className="text-[13px] text-slate">{k}</dt>
            <dd className="text-[17px] leading-tight font-light break-words tabular-nums">{v}</dd>
          </div>
        ))}
      </dl>

      <details className="group mt-3">
        <summary className="cursor-pointer rounded-full text-[14px] font-semibold text-team">
          {copy.chainActions(chain.actions.length)}
        </summary>
        <ol className="mt-2 list-decimal space-y-0.5 pl-5 text-[14px] marker:text-slate">
          {chain.actions.map((a, i) => (
            <li key={i}>
              {copy.chainActionLine(
                clockOf(a.minute, a.second),
                sequenceActionType(a.type),
                a.surname ? (a.shirt_number !== null ? `${a.surname} (${a.shirt_number})` : a.surname) : null,
              )}
            </li>
          ))}
        </ol>
      </details>
    </div>
  );
}

/** The key under the map, in words and real marks. */
function Legend({ mode, abbr, lineX }: { mode: Mode; abbr: { team: string; rival: string }; lineX: number | null }) {
  const L = copy.legend;
  const dot = (fill: string, fillOpacity: number, stroke: string) => (
    <svg viewBox="0 0 12 12" width="12" height="12" aria-hidden="true" className="mr-2 inline-block shrink-0 align-[-1px]">
      <circle cx="6" cy="6" r="4.5" fill={fill} fillOpacity={fillOpacity} stroke={stroke} strokeWidth="1.2" />
    </svg>
  );
  const stroke = (dash?: string, opacity = 1, color = "var(--color-team)") => (
    <svg viewBox="0 0 22 12" width="22" height="12" aria-hidden="true" className="mr-2 inline-block shrink-0 align-[-1px]">
      <line x1="2" y1="6" x2="20" y2="6" stroke={color} strokeOpacity={opacity} strokeWidth="2.4" strokeLinecap="round" strokeDasharray={dash} />
    </svg>
  );
  const diamond = (filled: boolean) => (
    <svg viewBox="0 0 12 12" width="12" height="12" aria-hidden="true" className="mr-2 inline-block shrink-0 align-[-1px]">
      <rect x="3" y="3" width="6" height="6" transform="rotate(45 6 6)" fill="var(--color-team)" fillOpacity={filled ? 0.8 : 0} stroke="var(--color-team)" strokeWidth="1.2" />
    </svg>
  );

  const items: ReactNode[] =
    mode === "shots"
      ? [
          <>{dot("var(--color-team)", 1, "var(--color-team)")}{L.shotTeam(abbr.team)}</>,
          <>{dot("var(--color-rival)", 0, "var(--color-rival)")}{L.shotRival(abbr.rival)}</>,
          <>{L.size}</>,
        ]
      : mode === "progression"
        ? [<>{stroke()}{L.pass}</>, <>{stroke("0.1 4")}{L.carry}</>, <>{stroke(undefined, 0.32)}{L.failed}</>]
        : [
            <>{diamond(true)}{L.won}</>,
            <>{diamond(false)}{L.lost}</>,
            ...(lineX != null ? [<>{stroke("3 3", 1, "var(--color-slate)")}{L.line(dec(lineX, 1))}</>] : []),
          ];

  return (
    <ul className="flex flex-col gap-1 self-center text-[13px] text-spot-text sm:max-w-[34ch]">
      {items.map((item, i) => (
        <li key={i} className="flex items-center">
          {item}
        </li>
      ))}
    </ul>
  );
}
