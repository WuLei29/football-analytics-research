"use client";

/**
 * EventMap — the one pitch of screen 03, switched between eight modes.
 *
 * Renders: a card in the match page's block form — `SectionHead` (the title
 * follows the mode; the caption says how many marks of how many, and which
 * way the team attacks), a row of mode chips, for Toques / Recepciones /
 * Defensivas a heat-or-events switch, the pitch, and the mode's legend.
 * Every mark answers a hover with a card naming the match and minute, and a
 * click on a mark whose chain the page can draw opens it in the sequence
 * block (`onOpenSequence`).
 * Props: `view` (the selection's marks, `lib/data/playerFilter.ts:mapView`),
 *        `matches` (to name a mark's match), `touchTypes` (the vocabulary a
 *        touch row's `t` indexes), `playerName`, `teamAbbr`,
 *        `onOpenSequence` (called with an index into `sequences.index`).
 * Data:  `players/{player_id}.json` -> `maps` (README-PLAYER "Data
 *        contract"): `silver.events` by the player, `gold.pass_receptions`,
 *        in his team's attacking frame, filtered to the selected matches and
 *        capped after the filter. Nothing is flipped here — low y is the
 *        right flank and sits at the bottom, so a right back's marks are in
 *        the lower half.
 *
 * Client component: the mode is the reader's choice. It is mirrored to
 * `?vista=` (`lib/vista.ts`) so a view is shareable and the player picker can
 * carry it to the next player. The heat/events switch is not: it is a
 * second look at the same marks, not a different view.
 *
 * Restyled on 16 Oct 2026 to the match page's blocks, at the review's
 * request: the arrows use `ProgressionArrows`' weights (pass 0.28 at 0.55,
 * carry 0.42 at 0.9 — the brief's 0.6/0.7 merged on a 150-pass map), the
 * mode pills are `DefensiveActions`' chips, the legends name the mark as
 * the match page does, and shots and defensive actions are the match page's
 * components with its legends. Deliberate leftovers from the brief:
 *   - `<Pitch>` is swapped per mode (`ShotMap` and `DefensiveActions` own
 *     their SVG for the hover card and the type chips).
 *   - The shot circles use `ShotMap`'s own radius law, so a shot is the same
 *     size here as on its match page.
 *
 * Hover, links and the events display were added the same day, at the
 * second review's request. Hover is delegated: every mark and zone carries
 * `data-i` / `data-zone`, and one listener on the figure reads it, so a
 * 2,000-touch season binds two handlers, not 4,000. The touch and reception
 * events displays are not capped: a dot is small, and the filter is the
 * reader's tool for thinning them.
 */

import { memo, useMemo, useRef, useState, type MouseEvent } from "react";

import { dec, num, pct } from "@/lib/format";
import {
  PLAYER_MAP_MODES,
  player as copy,
  playerMapModes,
  sequenceActionType,
  viz,
  type PlayerMapMode,
} from "@/lib/labels";
import { project, zoneCentre } from "@/lib/viz/pitch";
import { setVista, useVista } from "@/lib/vista";
import type { MapView } from "@/lib/data/playerFilter";
import {
  PASS_FLAG,
  RX_FLAG,
  type CarryRow,
  type PassRow,
  type PlayerMatch,
  type ReceptionRow,
} from "@/lib/data/types";

import { SectionHead } from "../shell/SectionHead";
import { ArrowLayer, type ArrowMark } from "../viz/ActionArrows";
import { Chip, DefensiveActions } from "../viz/DefensiveActions";
import { HoverCard } from "../viz/HoverCard";
import { Legend, Ramp, type LegendItem } from "../viz/Legend";
import { Pitch } from "../viz/Pitch";
import { PROGRESSION_STYLE } from "../viz/ProgressionArrows";
import { ShotMap } from "../viz/ShotMap";
import { ZoneHeatmap } from "../viz/ZoneHeatmap";

/* -- Marks per mode ---------------------------------------------------------- */

type Style = Omit<ArrowMark, "x" | "y" | "end_x" | "end_y" | "completed">;

function arrow(row: PassRow | CarryRow, style: Style, completed = (row[4] & PASS_FLAG.completed) !== 0): ArrowMark {
  return { x: row[0], y: row[1], end_x: row[2], end_y: row[3], completed, ...style };
}

/** The quiet population of the pass map: thinner and lighter than a match
    page pass, and headless — a head on 120 grey lines is the loudest thing
    in the figure. */
const REST: Style = { tone: "faint", width: 0.18, opacity: 0.45, head: false };
const PASS: Style = PROGRESSION_STYLE.pass;
const CARRY: Style = PROGRESSION_STYLE.carry;

const isProgressive = (r: PassRow) => (r[4] & PASS_FLAG.progressive) !== 0;
const isAssist = (r: PassRow) => (r[4] & PASS_FLAG.assist) !== 0;
const isRxProgressive = (r: ReceptionRow) => (r[2] & RX_FLAG.progressive) !== 0;

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

/** A dot of the touch / reception layers. */
interface Dot {
  x: number;
  y: number;
  fill: string;
  r: number;
  opacity: number;
  ring?: boolean;
}

/** What a hover card says about a mark, and which chain a click opens. */
interface Meta {
  x: number;
  y: number;
  title: string;
  lines: string[];
  s: number | null;
}

interface ModeView {
  shown: number;
  total: number;
  legend: LegendItem[];
  /** A sentence under the legend: the definition behind the marks. */
  rule?: string;
}

export function EventMap({
  view,
  matches,
  touchTypes,
  playerName,
  teamAbbr,
  onOpenSequence,
}: {
  view: MapView;
  matches: PlayerMatch[];
  touchTypes: string[];
  playerName: string;
  teamAbbr: string;
  onOpenSequence: (s: number) => void;
}) {
  const mode = useVista();
  const c = view.counts;
  const L = copy.legend;
  const H = copy.hover;

  const [displays, setDisplays] = useState(DEFAULT_DISPLAY);
  const display: Display = isHeatMode(mode) ? displays[mode] : "events";

  // Hover state is keyed by what is drawn, so a mode switch drops it
  // without an effect: an index from the pass map means nothing on touches.
  const drawn = `${mode}:${display}`;
  const [hover, setHover] = useState<{ at: string; i: number | null; zone: number | null }>({
    at: drawn,
    i: null,
    zone: null,
  });
  const hovered = hover.at === drawn ? hover.i : null;
  const hotZone = hover.at === drawn ? hover.zone : null;
  const figureRef = useRef<HTMLDivElement>(null);

  /** "J5 · LEV (L) · 57'". */
  const context = (m: number, minute: number | null) => {
    const match = matches[m];
    return H.context(match.matchday, match.opponent_abbr, match.is_home, minute);
  };

  const passMeta = (r: PassRow): Meta => {
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
    return {
      x: r[0],
      y: r[1],
      title,
      lines: [context(r[5], r[7]), result.filter(Boolean).join(" · ")],
      s: r[8],
    };
  };

  const carryMeta = (r: CarryRow): Meta => ({
    x: r[0],
    y: r[1],
    title: H.carry,
    lines: [
      context(r[5], r[7]),
      r[4] === 1 ? (r[6] === null ? H.completed : H.xt(dec(r[6], 3))) : H.carryLost,
    ],
    s: r[8],
  });

  const receptionMeta = (r: ReceptionRow): Meta => ({
    x: r[0],
    y: r[1],
    title: isRxProgressive(r) ? H.progressiveReception : H.reception,
    lines: [context(r[3], r[4]), ...((r[2] & RX_FLAG.inBox) !== 0 ? [H.inBox] : [])],
    s: r[5],
  });

  // The marks of the arrow and dot modes, and a Meta per mark at the same
  // index. Shots and defensive actions describe themselves (their own
  // components own the hover card).
  const { marks, dots, meta } = useMemo((): { marks: ArrowMark[]; dots: Dot[]; meta: Meta[] } => {
    const none = { marks: [], dots: [], meta: [] };
    switch (mode) {
      case "pases": {
        // The quiet population first, so every blue arrow is drawn above it.
        const rows = [...view.passes.filter((r) => !isProgressive(r)), ...view.passes.filter(isProgressive)];
        return {
          marks: rows.map((r) => arrow(r, isProgressive(r) ? PASS : REST)),
          dots: [],
          meta: rows.map(passMeta),
        };
      }
      case "progresivos":
        return { marks: view.progressive.map((r) => arrow(r, PASS)), dots: [], meta: view.progressive.map(passMeta) };
      case "conducciones":
        return {
          marks: view.carries.map((r) => arrow(r, CARRY, r[4] === 1)),
          dots: [],
          meta: view.carries.map(carryMeta),
        };
      case "pases-clave": {
        const rows = [...view.keyPasses.filter((r) => !isAssist(r)), ...view.keyPasses.filter(isAssist)];
        return {
          marks: rows.map((r) => arrow(r, isAssist(r) ? CARRY : PASS)),
          dots: [],
          meta: rows.map(passMeta),
        };
      }
      case "toques":
        if (display === "heat") return none;
        return {
          marks: [],
          dots: view.touches.map(([x, y]) => ({ x, y, fill: "var(--color-blue)", r: 0.6, opacity: 0.5 })),
          meta: view.touches.map(([x, y, t, m, minute, s]) => ({
            x,
            y,
            title: sequenceActionType(touchTypes[t] ?? ""),
            lines: [context(m, minute), H.touch],
            s,
          })),
        };
      case "recepciones": {
        // Heat: the progressive dots over the zones (README-PLAYER §3b).
        // Events: every reception, the rest quiet underneath.
        const rows =
          display === "heat"
            ? view.receptions.filter(isRxProgressive)
            : [...view.receptions.filter((r) => !isRxProgressive(r)), ...view.receptions.filter(isRxProgressive)];
        return {
          marks: [],
          dots: rows.map((r) =>
            isRxProgressive(r)
              ? { x: r[0], y: r[1], fill: "var(--color-blue)", r: 0.9, opacity: 1, ring: (r[2] & RX_FLAG.inBox) !== 0 }
              : { x: r[0], y: r[1], fill: "var(--color-faint)", r: 0.6, opacity: 0.6 },
          ),
          meta: rows.map(receptionMeta),
        };
      }
      default:
        return none;
    }
    // `context` and the meta builders read only `matches`, `touchTypes`.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [mode, display, view, matches, touchTypes]);

  // Delegated hover and click for the modes this component draws itself.
  const ownsHover = mode !== "tiros" && mode !== "defensivas";
  function onOver(e: MouseEvent) {
    const el = e.target as Element;
    const i = el.getAttribute("data-i");
    const zone = el.getAttribute("data-zone");
    setHover({ at: drawn, i: i === null ? null : Number(i), zone: zone === null ? null : Number(zone) });
  }
  function onClick(e: MouseEvent) {
    const i = (e.target as Element).getAttribute("data-i");
    const s = i === null ? null : (meta[Number(i)]?.s ?? null);
    if (s !== null) onOpenSequence(s);
  }

  const failed: LegendItem = { dashed: "var(--color-mid)", text: L.failed };
  const heat = display === "heat";
  const modes: Record<PlayerMapMode, ModeView> = {
    pases: {
      shown: view.passes.length,
      total: c.passes,
      legend: [
        { bar: "var(--color-blue)", text: viz.legend.progressivePass },
        { bar: "var(--color-faint)", text: L.restPasses },
        failed,
      ],
    },
    progresivos: {
      shown: view.progressive.length,
      total: c.progressive,
      legend: [{ bar: "var(--color-blue)", text: viz.legend.progressivePass }, failed],
      rule: L.progressiveRule,
    },
    conducciones: {
      shown: view.carries.length,
      total: c.carries,
      legend: [{ bar: "var(--color-gold)", text: viz.legend.carry }, failed],
      rule: L.progressiveRule,
    },
    toques: {
      shown: c.touches,
      total: c.touches,
      legend: heat ? [] : [{ swatch: "var(--color-blue)", text: L.touchDot }],
    },
    recepciones: {
      shown: c.receptions,
      total: c.receptions,
      legend: heat
        ? [
            { swatch: "rgba(11,76,158,.35)", text: L.receptionZones },
            { swatch: "var(--color-blue)", text: L.receptionDot },
            { halo: "var(--color-blue)", text: L.receptionRing },
          ]
        : [
            { swatch: "var(--color-faint)", text: L.receptionAll },
            { swatch: "var(--color-blue)", text: L.receptionDot },
            { halo: "var(--color-blue)", text: L.receptionRing },
          ],
    },
    tiros: {
      shown: view.shots.length,
      total: view.shots.length,
      legend: [
        { swatch: "var(--color-blue)", text: viz.legend.shotGoal },
        { swatch: "rgba(11,76,158,.45)", outline: "var(--color-blue)", text: viz.legend.shotOn },
        { outline: "var(--color-blue)", text: viz.legend.shotOff },
      ],
      rule: viz.legend.shotArea,
    },
    defensivas: {
      shown: view.defence.length,
      total: view.defence.length,
      // The heat display draws its own ramp, under the chips' pitch.
      legend: heat
        ? []
        : [
            { swatch: "var(--color-blue)", text: viz.legend.regain, diamond: true },
            { swatch: "var(--color-opp)", text: viz.legend.duelLost, diamond: true },
          ],
    },
    "pases-clave": {
      shown: view.keyPasses.length,
      total: c.keyPasses,
      legend: [
        { bar: "var(--color-blue)", text: L.keyPass },
        { bar: "var(--color-gold)", text: L.assist },
      ],
    },
  };
  const current = modes[mode];
  const empty = current.total === 0;
  const title = playerMapModes[mode].title;
  // Every display but a bare heat map has marks to hover and click.
  const hoverRule = !(heat && mode === "toques") ? L.hoverRule : null;

  const zoneCells = mode === "toques" ? view.touchZones : view.receptionZones;
  const zoneTotal = zoneCells.reduce((a, z) => a + z.value, 0);
  const zoneCount = zoneCells.find((z) => z.zone_id === hotZone)?.value ?? 0;
  const hot = hovered === null ? null : (meta[hovered] ?? null);
  const figureHeight = figureRef.current?.clientHeight ?? 0;

  let figure;
  if (mode === "tiros") {
    figure = (
      <ShotMap
        shots={view.shots.map((s) => ({
          ...s,
          side: "home" as const,
          name: playerName,
          context: context(matches.findIndex((m) => m.match_id === s.match_id), null),
          linked: s.s !== null,
        }))}
        teamSide="home"
        abbr={{ home: teamAbbr, away: teamAbbr }}
        onSelect={(shot) => {
          const s = view.shots.find((x) => x.shot_id === shot.shot_id)?.s;
          if (s != null) onOpenSequence(s);
        }}
      />
    );
  } else if (mode === "defensivas") {
    figure = (
      <DefensiveActions
        actions={view.defence.map((d) => ({ ...d, context: context(d.m, null), linked: d.s !== null }))}
        title={title}
        display={heat ? "heat" : "marks"}
        onSelect={(i) => {
          const s = view.defence[i]?.s;
          if (s != null) onOpenSequence(s);
        }}
      />
    );
  } else if (heat && (mode === "toques" || mode === "recepciones")) {
    figure = (
      <ZoneHeatmap cells={zoneCells} title={title} highlight={hotZone}>
        <DotLayer dots={dots} />
        <DotHighlight dot={hovered === null ? null : (dots[hovered] ?? null)} />
      </ZoneHeatmap>
    );
  } else if (dots.length > 0 || mode === "toques" || mode === "recepciones") {
    figure = (
      <Pitch title={title}>
        <DotLayer dots={dots} />
        <DotHighlight dot={hovered === null ? null : (dots[hovered] ?? null)} />
      </Pitch>
    );
  } else {
    figure = (
      <Pitch title={title}>
        <ArrowLayer actions={marks} hoverable highlight={hovered} />
      </Pitch>
    );
  }

  return (
    <article className="card flex h-full flex-col">
      <SectionHead
        title={title}
        caption={copy.mapCaption(mode, num(current.shown), num(current.total), current.shown < current.total, display)}
        rule="none"
        as="h3"
      />

      <div role="group" aria-label={copy.mapModesLabel} className="mt-2 mb-[6px] flex flex-wrap gap-[4px]">
        {PLAYER_MAP_MODES.map((m) => (
          <Chip key={m} label={playerMapModes[m].pill} pressed={m === mode} onClick={() => setVista(m)} />
        ))}
      </div>

      {isHeatMode(mode) && (
        <div role="group" aria-label={copy.mapDisplay.label} className="mb-[6px] flex flex-wrap items-center gap-[4px]">
          <span className="label" style={{ marginRight: 2 }}>
            {copy.mapDisplay.label}
          </span>
          {(["heat", "events"] as const).map((d) => (
            <Chip
              key={d}
              label={copy.mapDisplay[d]}
              pressed={display === d}
              onClick={() => setDisplays((prev) => ({ ...prev, [mode]: d }))}
            />
          ))}
        </div>
      )}

      <div className="mt-1 flex-1">
        <div
          ref={figureRef}
          style={{ position: "relative" }}
          onMouseOver={ownsHover ? onOver : undefined}
          onMouseLeave={ownsHover ? () => setHover({ at: drawn, i: null, zone: null }) : undefined}
          onClick={ownsHover ? onClick : undefined}
        >
          {figure}
          {ownsHover && hot && (
            <HoverCard
              x={hot.x}
              y={hot.y}
              figureHeight={figureHeight}
              title={hot.title}
              lines={hot.lines}
              hint={hot.s !== null ? viz.openSequence : null}
            />
          )}
          {ownsHover && !hot && hotZone !== null && zoneCount > 0 && (
            <HoverCard
              {...zoneCentre(hotZone)}
              figureHeight={figureHeight}
              title={mode === "toques" ? viz.zoneHover.touches(zoneCount) : viz.zoneHover.receptions(zoneCount)}
              lines={[viz.zoneHover.share(pct((zoneCount / zoneTotal) * 100, 0))]}
            />
          )}
        </div>
      </div>

      {empty ? (
        <p className="label mt-2">{copy.mapEmpty}</p>
      ) : (
        <>
          {mode === "toques" && heat && <Ramp low={viz.legend.low} high={viz.legend.high} />}
          {current.legend.length > 0 && <Legend items={current.legend} />}
          {current.rule && <p className="label mt-1">{current.rule}</p>}
          {hoverRule && <p className="label mt-1">{hoverRule}</p>}
        </>
      )}
    </article>
  );
}

/** Radius of a dot's invisible hit disc, in metres. */
const DOT_HIT = 1.1;

/**
 * The dots of the touch and reception displays: one filled dot per event,
 * a ring when a progressive reception landed in the box (README-PLAYER
 * §3b), and an invisible hit disc carrying `data-i`. Memoised: a hover
 * re-renders `DotHighlight`, not two thousand dots.
 */
const DotLayer = memo(function DotLayer({ dots }: { dots: Dot[] }) {
  return (
    <g>
      {dots.map((d, i) => {
        const p = project("horizontal", d.x, d.y);
        return (
          <g key={i}>
            {d.ring && (
              <circle cx={p.x} cy={p.y} r={1.4} fill="none" stroke="var(--color-blue)" strokeWidth={0.3} />
            )}
            <circle
              cx={p.x}
              cy={p.y}
              r={d.r}
              fill={d.fill}
              fillOpacity={d.opacity}
              stroke={d.opacity === 1 ? "var(--color-card)" : "none"}
              strokeWidth={0.15}
            />
          </g>
        );
      })}
      {dots.map((d, i) => {
        const p = project("horizontal", d.x, d.y);
        return (
          <circle key={i} data-i={i} cx={p.x} cy={p.y} r={DOT_HIT} fill="transparent" style={{ cursor: "pointer" }} />
        );
      })}
    </g>
  );
});

function DotHighlight({ dot }: { dot: Dot | null }) {
  if (!dot) return null;
  const p = project("horizontal", dot.x, dot.y);
  return (
    <circle
      cx={p.x}
      cy={p.y}
      r={1.5}
      fill="none"
      stroke="var(--color-ink)"
      strokeWidth={0.35}
      pointerEvents="none"
    />
  );
}
