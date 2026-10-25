"use client";

/**
 * EventMap — the one pitch of screen 03, switched between eight modes.
 *
 * Renders: a panel with the title (it follows the mode), the caption (how
 * many marks of how many, and which way the team attacks), the mode pills,
 * for Toques / Recepciones / Defensivas a heat-or-events switch (and for
 * Defensivas the type pills), the pitch, a white readout card naming the
 * mark or zone under the cursor or the keyboard, and the mode's key. A
 * click, or Enter, on a mark whose chain the page can draw opens it in the
 * sequence block (`onOpenSequence`).
 * Props: `view` (the selection's marks, `lib/data/playerFilter.ts:mapView`),
 *        `matches` (to name a mark's match), `touchTypes` (the vocabulary a
 *        touch row's `t` indexes), `openedS` (the chain open in the sequence
 *        block, ringed on the map), `onOpenSequence` (called with an index
 *        into `sequences.index`).
 * Data:  `players/{player_id}.json` -> `maps` (README-PLAYER "Data
 *        contract"): `silver.events` by the player, `gold.pass_receptions`,
 *        in his team's attacking frame, filtered to the selected matches and
 *        capped after the filter. Nothing is flipped here: low y is the right
 *        flank and sits at the bottom of a horizontal pitch.
 *
 * The mode is mirrored to `?vista=` (`lib/vista.ts`) so a view is shareable
 * and the player picker can carry it to the next player. The heat/events
 * switch is not: it is a second look at the same marks.
 *
 * Pizarra (24 Oct 2026). The block and its modes are the handoff's; the
 * interface changed:
 *   - one SVG for every mode, drawn here (the handoff swapped in `ShotMap`
 *     and `DefensiveActions`, whose black hover cards Pizarra has no room
 *     for); the readout card replaces every hover card;
 *   - keyboard: every mark (and every zone of a heat display) is a labelled
 *     `role="button"` in one roving tab stop, as on the match page
 *     (`components/match/MatchMap`): Tab enters, the arrow keys walk the
 *     marks in match order, Home / End, Enter or Space opens the chain. Hover,
 *     focus, keys and clicks are delegated to the SVG, so a 2,000-touch
 *     season still binds a handful of handlers;
 *   - colours by DESIGN.md's chart mapping: progressive passes solid team,
 *     the rest of the passes thin and headless in chalk, carries dotted team
 *     (the gold is retired), a failed action faint; key passes team and
 *     assists team, thicker, ending in a dot (they were gold); touches team
 *     dots; receptions team when progressive, slate otherwise, a ring in the
 *     box; shots `ShotMap`'s encoding in team; defensive actions team
 *     diamonds, filled when won, outline when lost; heat panel to team.
 * Under 640 px the pitch turns vertical, attacking up, as on the match page.
 */

import { memo, useEffect, useMemo, useRef, useState, type FocusEvent, type KeyboardEvent, type MouseEvent, type ReactNode } from "react";

import { dec, num, pct } from "@/lib/format";
import {
  PLAYER_MAP_MODES,
  defensiveActionType,
  matchExplorer,
  player as copy,
  playerMapModes,
  sequenceActionType,
  shotBodyPart,
  shotOutcome,
  viz,
  type PlayerMapMode,
} from "@/lib/labels";
import { PITCH_VIEWBOX, project, zoneId, zoneRect, type Orientation } from "@/lib/viz/pitch";
import { heatOpacity } from "@/lib/viz/scale";
import { setVista, useVista } from "@/lib/vista";
import type { MapView } from "@/lib/data/playerFilter";
import { PASS_FLAG, RX_FLAG, type CarryRow, type PassRow, type PlayerMatch, type ReceptionRow } from "@/lib/data/types";

import { PitchLines } from "../viz/Pitch";

/* -- What a mark is ---------------------------------------------------------- */

type Geometry =
  | {
      kind: "arrow";
      x: number;
      y: number;
      ex: number;
      ey: number;
      /** `rest` is the quiet population of the pass map. */
      tone: "team" | "rest";
      width: number;
      dotted?: boolean;
      failed?: boolean;
      /** An assist: thicker, ending in a dot. */
      assist?: boolean;
    }
  | { kind: "dot"; x: number; y: number; r: number; quiet?: boolean; ring?: boolean }
  | { kind: "shot"; x: number; y: number; xg: number; outcome: string }
  | { kind: "diamond"; x: number; y: number; won: boolean }
  | { kind: "zone"; zone: number; value: number };

interface Item {
  g: Geometry;
  title: string;
  line: string;
  s: number | null;
}

/** The modes with a heat-or-events switch, and where each one opens. */
type HeatMode = "toques" | "recepciones" | "defensivas";
type Display = "heat" | "events";
const DEFAULT_DISPLAY: Record<HeatMode, Display> = {
  toques: "heat",
  recepciones: "heat",
  // The match page draws defensive actions as marks; heat is the extra.
  defensivas: "events",
};
const isHeatMode = (m: PlayerMapMode): m is HeatMode => m in DEFAULT_DISPLAY;

const isProgressive = (r: PassRow) => (r[4] & PASS_FLAG.progressive) !== 0;
const isAssist = (r: PassRow) => (r[4] & PASS_FLAG.assist) !== 0;
const isRxProgressive = (r: ReceptionRow) => (r[2] & RX_FLAG.progressive) !== 0;

/** `ShotMap`'s radius law: area proportional to xG. */
const SHOT = { base: 0.75, k: 3.3 };
const ON_TARGET = new Set(["goal", "saved", "woodwork"]);
const shotRadius = (xg: number) => SHOT.base + Math.sqrt(Math.max(0, xg)) * SHOT.k;

/** The defence type pills' order: the handoff's chip order. */
const TYPE_ORDER = ["ball_recovery", "clearance", "aerial", "tackle", "challenge", "attempted_tackle", "interception", "blocked_pass"];

/** Invisible hit sizes, in metres: a 0.3 m line or a 0.6 m dot is no target. */
const HIT_LINE = 1.6;
const HIT_DOT = 1.1;

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

/** Where a mark sits, for its focus ring. */
function anchor(o: Orientation, g: Geometry): { x: number; y: number; r: number } | null {
  if (g.kind === "zone") return null;
  const p = project(o, g.x, g.y);
  const r = g.kind === "shot" ? shotRadius(g.xg) + 0.9 : g.kind === "arrow" ? 1.3 : 1.5;
  return { ...p, r };
}

export function EventMap({
  view,
  matches,
  touchTypes,
  openedS = null,
  onOpenSequence,
}: {
  view: MapView;
  matches: PlayerMatch[];
  touchTypes: string[];
  openedS?: number | null;
  onOpenSequence: (s: number) => void;
}) {
  const o: Orientation = useNarrow() ? "vertical" : "horizontal";
  const mode = useVista();
  const c = view.counts;
  const H = copy.hover;

  const [displays, setDisplays] = useState(DEFAULT_DISPLAY);
  const display: Display = isHeatMode(mode) ? displays[mode] : "events";
  const heat = display === "heat";
  const [defenceType, setDefenceType] = useState<string | null>(null);

  /** "J5 · LEV (L) · 57'". */
  const context = (m: number, minute: number | null) => {
    const match = matches[m];
    return H.context(match.matchday, match.opponent_abbr, match.is_home, minute);
  };

  const passItem = (r: PassRow, g: Geometry): Item => {
    const done = (r[4] & PASS_FLAG.completed) !== 0;
    const title = isAssist(r)
      ? H.assist
      : (r[4] & PASS_FLAG.keyPass) !== 0
        ? H.keyPass
        : isProgressive(r)
          ? H.progressivePass
          : H.pass;
    // A failed action earns 0 xT by construction; the line says "Fallado".
    const result = done ? [H.completed, r[6] === null ? null : H.xt(dec(r[6], 3))] : [H.failed];
    return { g, title, line: [context(r[5], r[7]), ...result.filter(Boolean)].join(" · "), s: r[8] };
  };
  const arrowOf = (r: PassRow | CarryRow, extra: Partial<Extract<Geometry, { kind: "arrow" }>>): Geometry => ({
    kind: "arrow",
    x: r[0],
    y: r[1],
    ex: r[2],
    ey: r[3],
    tone: "team",
    width: 0.3,
    ...extra,
  });

  const defenceCounts = useMemo(() => {
    const counts = new Map<string, number>();
    for (const a of view.defence) counts.set(a.type, (counts.get(a.type) ?? 0) + 1);
    const rank = (t: string) => (TYPE_ORDER.includes(t) ? TYPE_ORDER.indexOf(t) : 99);
    return [...counts.entries()].sort((a, b) => rank(a[0]) - rank(b[0]) || b[1] - a[1]);
  }, [view.defence]);

  // The marks of the current display, in match order (the keyboard's order).
  const items: Item[] = useMemo(() => {
    const zoneItems = (cells: { zone_id: number; value: number }[], say: (n: number) => string): Item[] => {
      const total = cells.reduce((a, z) => a + z.value, 0);
      return [...cells]
        .filter((z) => z.value > 0)
        .sort((a, b) => a.zone_id - b.zone_id)
        .map((z) => ({
          g: { kind: "zone", zone: z.zone_id, value: z.value } as Geometry,
          title: say(z.value),
          line: viz.zoneHover.share(pct((z.value / total) * 100, 0)),
          s: null,
        }));
    };
    switch (mode) {
      case "pases":
        return view.passes.map((r) => {
          const done = (r[4] & PASS_FLAG.completed) !== 0;
          return passItem(r, arrowOf(r, isProgressive(r) ? { failed: !done } : { tone: "rest", width: 0.18, failed: !done }));
        });
      case "progresivos":
        return view.progressive.map((r) => passItem(r, arrowOf(r, { failed: (r[4] & PASS_FLAG.completed) === 0 })));
      case "conducciones":
        return view.carries.map((r) => ({
          g: arrowOf(r, { width: 0.45, dotted: true, failed: r[4] !== 1 }),
          title: H.carry,
          line: [context(r[5], r[7]), r[4] === 1 ? (r[6] === null ? H.completed : H.xt(dec(r[6], 3))) : H.carryLost].join(" · "),
          s: r[8],
        }));
      case "pases-clave":
        return view.keyPasses.map((r) =>
          passItem(r, arrowOf(r, isAssist(r) ? { width: 0.5, assist: true } : { failed: (r[4] & PASS_FLAG.completed) === 0 })),
        );
      case "toques":
        if (heat) return zoneItems(view.touchZones, viz.zoneHover.touches);
        return view.touches.map(([x, y, t, m, minute, s]) => ({
          g: { kind: "dot", x, y, r: 0.6 } as Geometry,
          title: sequenceActionType(touchTypes[t] ?? ""),
          line: [context(m, minute), H.touch].join(" · "),
          s,
        }));
      case "recepciones": {
        // Heat: the zones, then the progressive dots over them (README-PLAYER
        // §3b). Events: every reception.
        const rows = heat ? view.receptions.filter(isRxProgressive) : view.receptions;
        const dots = rows.map((r) => ({
          g: (isRxProgressive(r)
            ? { kind: "dot", x: r[0], y: r[1], r: 0.9, ring: (r[2] & RX_FLAG.inBox) !== 0 }
            : { kind: "dot", x: r[0], y: r[1], r: 0.6, quiet: true }) as Geometry,
          title: isRxProgressive(r) ? H.progressiveReception : H.reception,
          line: [context(r[3], r[4]), ...((r[2] & RX_FLAG.inBox) !== 0 ? [H.inBox] : [])].join(" · "),
          s: r[5],
        }));
        return heat ? [...zoneItems(view.receptionZones, viz.zoneHover.receptions), ...dots] : dots;
      }
      case "tiros":
        return view.shots.map((s) => ({
          g: { kind: "shot", x: s.x, y: s.y, xg: s.xg, outcome: s.outcome } as Geometry,
          title: `${shotOutcome(s.outcome)} · ${dec(s.xg, 2)} xG`,
          line: [
            context(matches.findIndex((m) => m.match_id === s.match_id), s.minute),
            shotBodyPart(s.body_part).toLowerCase(),
          ].join(" · "),
          s: s.s,
        }));
      case "defensivas": {
        const rows = view.defence.filter((d) => defenceType === null || d.type === defenceType);
        if (heat) {
          const by = new Map<number, number>();
          for (const d of rows) {
            const z = zoneId(d.x, d.y);
            by.set(z, (by.get(z) ?? 0) + 1);
          }
          return zoneItems([...by].map(([zone_id, value]) => ({ zone_id, value })), viz.zoneHover.defence);
        }
        return rows.map((d) => ({
          g: { kind: "diamond", x: d.x, y: d.y, won: d.outcome === "won" } as Geometry,
          title: defensiveActionType(d.type),
          line: [context(d.m, d.minute), d.outcome === "won" ? copy.legendWon : copy.legendLost].join(" · "),
          s: d.s,
        }));
      }
    }
    // `context` and the builders read only `matches` and `touchTypes`.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [mode, heat, view, matches, touchTypes, defenceType]);

  // Hover, focus and the tab stop are keyed by what is drawn, so a mode
  // switch drops them without an effect.
  const drawn = `${mode}:${display}:${defenceType ?? ""}`;
  const [cursor, setCursor] = useState<{ at: string; active: number | null; focus: number; keyboard: boolean }>({
    at: drawn,
    active: null,
    focus: 0,
    keyboard: false,
  });
  const live = cursor.at === drawn ? cursor : { at: drawn, active: null, focus: 0, keyboard: false };
  const svgRef = useRef<SVGSVGElement>(null);

  const indexOf = (e: { target: EventTarget }) => {
    const el = (e.target as Element).closest?.("[data-i]");
    return el ? Number(el.getAttribute("data-i")) : null;
  };
  const focusMark = (i: number) => {
    setCursor({ at: drawn, active: i, focus: i, keyboard: true });
    svgRef.current?.querySelector<SVGGElement>(`[data-i="${i}"]`)?.focus();
  };
  const open = (i: number | null) => {
    const s = i === null ? null : (items[i]?.s ?? null);
    if (s !== null) onOpenSequence(s);
  };

  function onKeyDown(e: KeyboardEvent) {
    const i = indexOf(e);
    if (i === null) return;
    let next: number | null = null;
    if (e.key === "ArrowRight" || e.key === "ArrowDown") next = Math.min(items.length - 1, i + 1);
    else if (e.key === "ArrowLeft" || e.key === "ArrowUp") next = Math.max(0, i - 1);
    else if (e.key === "Home") next = 0;
    else if (e.key === "End") next = items.length - 1;
    else if (e.key === "Enter" || e.key === " ") {
      e.preventDefault();
      open(i);
      return;
    }
    if (next !== null) {
      e.preventDefault();
      focusMark(next);
    }
  }

  const hot = live.active !== null ? (items[live.active] ?? null) : null;
  const ring = live.active !== null && live.keyboard ? anchor(o, items[live.active]?.g ?? { kind: "zone", zone: 0, value: 0 }) : null;
  const zoneMax = items.reduce((m, it) => (it.g.kind === "zone" ? Math.max(m, it.g.value) : m), 0);
  const hasZones = zoneMax > 0;
  const title = playerMapModes[mode].title;

  const shown =
    mode === "pases" ? view.passes.length
    : mode === "progresivos" ? view.progressive.length
    : mode === "conducciones" ? view.carries.length
    : mode === "pases-clave" ? view.keyPasses.length
    : mode === "tiros" ? view.shots.length
    : mode === "defensivas" ? view.defence.length
    : mode === "toques" ? c.touches
    : c.receptions;
  const total =
    mode === "pases" ? c.passes
    : mode === "progresivos" ? c.progressive
    : mode === "conducciones" ? c.carries
    : mode === "pases-clave" ? c.keyPasses
    : shown;
  const empty = total === 0;

  return (
    <article className="flex h-full flex-col rounded-panel bg-panel p-5 sm:p-7">
      <h2 className="text-[22px] font-semibold tracking-[-0.02em]">{title}</h2>
      <p className="mt-1 text-[14px] text-slate">{copy.mapCaption(mode, num(shown), num(total), shown < total, display)}</p>

      <div role="group" aria-label={copy.mapModesLabel} className="mt-4 flex flex-wrap gap-1.5">
        {PLAYER_MAP_MODES.map((m) => (
          <Pill key={m} pressed={m === mode} onClick={() => setVista(m)}>
            {playerMapModes[m].pill}
          </Pill>
        ))}
      </div>

      {isHeatMode(mode) && (
        <div className="mt-2 flex flex-wrap items-center gap-x-4 gap-y-2">
          {isHeatMode(mode) && (
            <div role="group" aria-label={copy.mapDisplay.label} className="flex flex-wrap items-center gap-1.5">
              <span className="mr-1 text-[13px] text-slate">{copy.mapDisplay.label}</span>
              {(["heat", "events"] as const).map((d) => (
                <Pill key={d} small pressed={display === d} onClick={() => setDisplays((prev) => ({ ...prev, [mode]: d }))}>
                  {copy.mapDisplay[d]}
                </Pill>
              ))}
            </div>
          )}
          {mode === "defensivas" && (
            <div role="group" aria-label={matchExplorer.filterLabel} className="flex flex-wrap gap-1.5">
              {[[null, view.defence.length] as const, ...defenceCounts].map(([type, n]) => (
                <Pill key={type ?? "all"} small pressed={type === defenceType} onClick={() => setDefenceType(type)}>
                  {type === null ? matchExplorer.filterAll(n) : matchExplorer.filterType(defensiveActionType(type), n)}
                </Pill>
              ))}
            </div>
          )}
        </div>
      )}

      <div className="pizarra-viz on-panel mt-4 flex-1">
        <svg
          ref={svgRef}
          viewBox={PITCH_VIEWBOX[o]}
          width="100%"
          role="group"
          aria-label={copy.mapLabel(title, items.length)}
          className={o === "vertical" ? "mx-auto block max-w-[360px]" : "block"}
          onMouseOver={(e: MouseEvent) => {
            const i = indexOf(e);
            if (i !== null) setCursor((cur) => ({ ...(cur.at === drawn ? cur : live), at: drawn, active: i, keyboard: false }));
          }}
          onMouseLeave={() => setCursor((cur) => ({ ...(cur.at === drawn ? cur : live), at: drawn, active: null }))}
          onClick={(e: MouseEvent) => open(indexOf(e))}
          onFocus={(e: FocusEvent) => {
            const i = indexOf(e);
            if (i !== null)
              setCursor({ at: drawn, active: i, focus: i, keyboard: (e.target as Element).matches(":focus-visible") });
          }}
          onBlur={() => setCursor((cur) => ({ ...cur, keyboard: false }))}
          onKeyDown={onKeyDown}
        >
          <defs>
            <marker id="event-head" viewBox="0 0 10 10" refX={8} refY={5} markerWidth={4} markerHeight={4} orient="auto-start-reverse">
              <path d="M0,1.5 L8.5,5 L0,8.5 z" fill="var(--color-team)" />
            </marker>
          </defs>
          {hasZones &&
            items.map((it, i) =>
              it.g.kind === "zone" ? (
                <rect key={`h${i}`} {...zoneRect(o, it.g.zone)} fill="var(--color-team)" fillOpacity={heatOpacity(it.g.value, zoneMax)} />
              ) : null,
            )}
          <PitchLines orientation={o} color={hasZones ? "var(--color-ink)" : "var(--color-chalk)"} opacity={hasZones ? 0.45 : 1} cased={hasZones} />
          <MarkLayer items={items} o={o} focus={live.focus} openedS={openedS} />
          {hot && (
            <Highlight o={o} g={hot.g} />
          )}
          {ring && (
            <circle cx={ring.x} cy={ring.y} r={ring.r} fill="none" stroke="var(--color-ink)" strokeWidth={0.5} pointerEvents="none" />
          )}
        </svg>
      </div>

      <div aria-live="polite" className="mt-3 min-h-[76px] rounded-row bg-white px-4 py-3">
        {empty ? (
          <p className="text-[14px] text-slate">{copy.mapEmpty}</p>
        ) : hot ? (
          <>
            <p className="text-[15px] font-semibold">{hot.title}</p>
            <p className="text-[14px] text-slate">{hot.line}</p>
            {hot.g.kind !== "zone" && (
              <p className={`mt-1 text-[13px] font-medium ${hot.s != null ? "text-team" : "text-slate"}`}>
                {hot.s == null ? matchExplorer.noChain : hot.s === openedS ? matchExplorer.opened : matchExplorer.open}
              </p>
            )}
          </>
        ) : (
          <p className="text-[14px] text-slate">{copy.legend.hoverRule}</p>
        )}
      </div>

      {!empty && <Key mode={mode} heat={heat} />}
    </article>
  );
}

/** A Pizarra pill: pressed team-filled, the others a tone step. */
function Pill({ pressed, onClick, small = false, children }: { pressed: boolean; onClick: () => void; small?: boolean; children: ReactNode }) {
  return (
    <button
      type="button"
      aria-pressed={pressed}
      onClick={onClick}
      className={`rounded-full font-semibold transition-colors ${small ? "px-3 py-1.5 text-[13px]" : "px-3.5 py-2 text-[14px]"} ${
        pressed ? "bg-team text-white" : "bg-panel-deep text-ink hover:bg-chalk"
      }`}
    >
      {children}
    </button>
  );
}

/**
 * Every mark, each a labelled button carrying `data-i`. Memoised: a hover
 * re-renders the highlight, not two thousand dots; only the tab stop moves
 * the layer.
 */
const MarkLayer = memo(function MarkLayer({
  items,
  o,
  focus,
  openedS,
}: {
  items: Item[];
  o: Orientation;
  focus: number;
  openedS: number | null;
}) {
  // Big shots first, so the small ones stay on top and reachable; the rest
  // keep match order (the quiet passes are listed in time, drawn first).
  const order = items.map((_, i) => i);
  if (items.some((it) => it.g.kind === "shot")) {
    order.sort((a, b) => ((items[b].g as { xg?: number }).xg ?? 0) - ((items[a].g as { xg?: number }).xg ?? 0));
  } else if (items.some((it) => it.g.kind === "arrow" && it.g.tone === "rest")) {
    order.sort((a, b) => Number(items[a].g.kind === "arrow" && items[a].g.tone !== "rest") - Number(items[b].g.kind === "arrow" && items[b].g.tone !== "rest"));
  }
  return (
    <g>
      {order.map((i) => {
        const it = items[i];
        return (
          <g
            key={i}
            data-i={i}
            role="button"
            tabIndex={i === focus ? 0 : -1}
            aria-label={matchExplorer.markLabel(it.title, it.line, it.s != null)}
            className={`outline-none ${it.s != null ? "cursor-pointer" : ""}`}
          >
            <Mark o={o} g={it.g} opened={openedS !== null && it.s === openedS} />
          </g>
        );
      })}
    </g>
  );
});

function Mark({ o, g, opened }: { o: Orientation; g: Geometry; opened: boolean }) {
  switch (g.kind) {
    case "zone":
      // The fill is drawn under the pitch lines; this is the target.
      return <rect {...zoneRect(o, g.zone)} fill="transparent" />;
    case "arrow": {
      const from = project(o, g.x, g.y);
      const to = project(o, g.ex, g.ey);
      const rest = g.tone === "rest";
      return (
        <>
          <line x1={from.x} y1={from.y} x2={to.x} y2={to.y} stroke="transparent" strokeWidth={HIT_LINE} strokeLinecap="round" />
          <line
            x1={from.x}
            y1={from.y}
            x2={to.x}
            y2={to.y}
            stroke={rest ? "var(--color-slate)" : "var(--color-team)"}
            strokeOpacity={opened ? 1 : rest ? (g.failed ? 0.18 : 0.4) : g.failed ? 0.32 : g.dotted ? 0.95 : 0.75}
            strokeWidth={g.width * (opened ? 1.8 : 1)}
            strokeLinecap="round"
            strokeDasharray={g.dotted ? "0.05 0.95" : undefined}
            markerEnd={rest || g.assist ? undefined : "url(#event-head)"}
          />
          {g.assist && <circle cx={to.x} cy={to.y} r={0.8} fill="var(--color-team)" />}
          {opened && <circle cx={from.x} cy={from.y} r={1.3} fill="none" stroke="var(--color-ink)" strokeWidth={0.35} strokeDasharray="0.1 0.7" strokeLinecap="round" />}
        </>
      );
    }
    case "dot": {
      const p = project(o, g.x, g.y);
      return (
        <>
          <circle cx={p.x} cy={p.y} r={HIT_DOT} fill="transparent" />
          {g.ring && <circle cx={p.x} cy={p.y} r={1.4} fill="none" stroke="var(--color-team)" strokeWidth={0.3} />}
          <circle
            cx={p.x}
            cy={p.y}
            r={g.r}
            fill={g.quiet ? "var(--color-slate)" : "var(--color-team)"}
            fillOpacity={g.quiet ? 0.45 : g.r > 0.8 ? 1 : 0.5}
            stroke={g.r > 0.8 ? "white" : "none"}
            strokeWidth={0.15}
          />
          {opened && <circle cx={p.x} cy={p.y} r={1.5} fill="none" stroke="var(--color-ink)" strokeWidth={0.35} strokeDasharray="0.1 0.7" strokeLinecap="round" />}
        </>
      );
    }
    case "shot": {
      const p = project(o, g.x, g.y);
      const r = shotRadius(g.xg);
      return (
        <>
          <circle cx={p.x} cy={p.y} r={Math.max(r, 1.8)} fill="transparent" />
          <circle
            cx={p.x}
            cy={p.y}
            r={r}
            fill="var(--color-team)"
            fillOpacity={g.outcome === "goal" ? 1 : ON_TARGET.has(g.outcome) ? 0.45 : 0}
            stroke="var(--color-team)"
            strokeWidth={0.35}
          />
          {opened && <circle cx={p.x} cy={p.y} r={r + 0.9} fill="none" stroke="var(--color-ink)" strokeWidth={0.35} strokeDasharray="0.1 0.7" strokeLinecap="round" />}
        </>
      );
    }
    case "diamond": {
      const p = project(o, g.x, g.y);
      const h = 0.7;
      return (
        <>
          <circle cx={p.x} cy={p.y} r={1.8} fill="transparent" />
          <rect
            x={p.x - h}
            y={p.y - h}
            width={h * 2}
            height={h * 2}
            transform={`rotate(45 ${p.x} ${p.y})`}
            fill="var(--color-team)"
            fillOpacity={g.won ? 0.8 : 0}
            stroke="var(--color-team)"
            strokeWidth={0.28}
          />
          {opened && <circle cx={p.x} cy={p.y} r={1.6} fill="none" stroke="var(--color-ink)" strokeWidth={0.35} strokeDasharray="0.1 0.7" strokeLinecap="round" />}
        </>
      );
    }
  }
}

/** The mark under the cursor or the keyboard, redrawn loud on top. */
function Highlight({ o, g }: { o: Orientation; g: Geometry }) {
  if (g.kind === "zone") {
    return <rect {...zoneRect(o, g.zone)} fill="none" stroke="var(--color-ink)" strokeWidth={0.4} pointerEvents="none" />;
  }
  if (g.kind === "arrow") {
    const from = project(o, g.x, g.y);
    const to = project(o, g.ex, g.ey);
    return (
      <line
        x1={from.x}
        y1={from.y}
        x2={to.x}
        y2={to.y}
        stroke="var(--color-team)"
        strokeWidth={Math.max(g.width * 2, 0.55)}
        strokeLinecap="round"
        markerEnd="url(#event-head)"
        pointerEvents="none"
      />
    );
  }
  const a = anchor(o, g)!;
  return <circle cx={a.x} cy={a.y} r={a.r} fill="none" stroke="var(--color-ink)" strokeWidth={0.35} pointerEvents="none" />;
}

/** The key under the map, in words and real marks. */
function Key({ mode, heat }: { mode: PlayerMapMode; heat: boolean }) {
  const L = copy.legend;
  const V = viz.legend;
  const sw = (child: ReactNode, w = 14) => (
    <svg viewBox={`0 0 ${w} 12`} width={w} height="12" aria-hidden="true" className="mr-1.5 inline-block shrink-0">
      {child}
    </svg>
  );
  const line = (opts: { dash?: string; opacity?: number; color?: string; dot?: boolean } = {}) =>
    sw(
      <>
        <line x1="1" y1="6" x2="19" y2="6" stroke={opts.color ?? "var(--color-team)"} strokeOpacity={opts.opacity ?? 1} strokeWidth="2" strokeLinecap="round" strokeDasharray={opts.dash} />
        {opts.dot && <circle cx="19" cy="6" r="2.4" fill="var(--color-team)" />}
      </>,
      22,
    );
  const dot = (fill: string, opacity = 1, ring = false) =>
    sw(
      <>
        {ring && <circle cx="6" cy="6" r="5.2" fill="none" stroke="var(--color-team)" strokeWidth="1" />}
        <circle cx="6" cy="6" r={ring ? 3 : 4} fill={fill} fillOpacity={opacity} />
      </>,
    );
  const ramp = (
    <span aria-hidden="true" className="mr-1.5 inline-block h-1.5 w-12 rounded-full bg-[linear-gradient(90deg,var(--color-panel-deep),var(--color-team))]" />
  );
  const failed: [ReactNode, string] = [line({ opacity: 0.32 }), L.failed];

  const items: [ReactNode, string][] =
    mode === "pases" ? [[line(), V.progressivePass], [line({ color: "var(--color-slate)", opacity: 0.6 }), L.restPasses], failed]
    : mode === "progresivos" ? [[line(), V.progressivePass], failed, [null, L.progressiveRule]]
    : mode === "conducciones" ? [[line({ dash: "0.1 3.5" }), V.carry], failed, [null, L.progressiveRule]]
    : mode === "pases-clave" ? [[line(), L.keyPass], [line({ dot: true }), L.assist]]
    : mode === "toques" ? (heat ? [[ramp, `${V.low} → ${V.high}`]] : [[dot("var(--color-team)", 0.5), L.touchDot]])
    : mode === "recepciones"
      ? [
          heat ? [ramp, L.receptionZones] : [dot("var(--color-slate)", 0.45), L.receptionAll],
          [dot("var(--color-team)"), L.receptionDot],
          [dot("var(--color-team)", 1, true), L.receptionRing],
        ]
    : mode === "tiros"
      ? [
          [dot("var(--color-team)"), V.shotGoal],
          [sw(<circle cx="6" cy="6" r="4.5" fill="var(--color-team)" fillOpacity="0.45" stroke="var(--color-team)" strokeWidth="1.2" />), V.shotOn],
          [sw(<circle cx="6" cy="6" r="4.5" fill="none" stroke="var(--color-team)" strokeWidth="1.2" />), V.shotOff],
          [null, matchExplorer.legend.size],
        ]
    : heat ? [[ramp, `${V.low} → ${V.high}`]]
    : [
        [sw(<rect x="3" y="3" width="6" height="6" transform="rotate(45 6 6)" fill="var(--color-team)" fillOpacity="0.8" stroke="var(--color-team)" />), matchExplorer.legend.won],
        [sw(<rect x="3" y="3" width="6" height="6" transform="rotate(45 6 6)" fill="none" stroke="var(--color-team)" strokeWidth="1.2" />), matchExplorer.legend.lost],
      ];

  return (
    <ul className="mt-3 flex flex-wrap gap-x-5 gap-y-1 text-[13px] text-slate">
      {items.map(([mark, text]) => (
        <li key={text} className="flex items-center">
          {mark}
          {text}
        </li>
      ))}
    </ul>
  );
}
