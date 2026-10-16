"use client";

/**
 * EventMap — the one pitch of screen 03, switched between eight modes.
 *
 * Renders: a card in the match page's block form — `SectionHead` (the title
 * follows the mode; the caption says how many marks of how many, and which
 * way the team attacks), a row of mode chips, the pitch, and the mode's
 * legend under it.
 * Props: `view` (the selection's marks, `lib/data/playerFilter.ts:mapView`),
 *        `caps`, `playerName`, `teamAbbr`.
 * Data:  `players/{player_id}.json` -> `maps` (README-PLAYER "Data
 *        contract"): `silver.events` by the player, `gold.pass_receptions`,
 *        in his team's attacking frame, filtered to the selected matches and
 *        capped after the filter. Nothing is flipped here — low y is the
 *        right flank and sits at the bottom, so a right back's marks are in
 *        the lower half.
 *
 * Client component: the mode is the reader's choice. It is mirrored to
 * `?vista=` (`lib/vista.ts`) so a view is shareable and the player picker can
 * carry it to the next player.
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
 */

import { useMemo } from "react";

import { num } from "@/lib/format";
import {
  PLAYER_MAP_MODES,
  player as copy,
  playerMapModes,
  viz,
  type PlayerMapMode,
} from "@/lib/labels";
import { project } from "@/lib/viz/pitch";
import { setVista, useVista } from "@/lib/vista";
import type { MapView } from "@/lib/data/playerFilter";
import { PASS_FLAG, type CarryRow, type PassRow, type ReceptionRow } from "@/lib/data/types";

import { SectionHead } from "../shell/SectionHead";
import { ArrowLayer, type ArrowMark } from "../viz/ActionArrows";
import { Chip, DefensiveActions } from "../viz/DefensiveActions";
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

interface ModeView {
  shown: number;
  total: number;
  legend: LegendItem[];
  /** A sentence under the legend: the definition behind the marks. */
  rule?: string;
}

export function EventMap({
  view,
  playerName,
  teamAbbr,
}: {
  view: MapView;
  playerName: string;
  teamAbbr: string;
}) {
  const mode = useVista();
  const c = view.counts;
  const L = copy.legend;

  const marks = useMemo((): ArrowMark[] => {
    switch (mode) {
      case "pases":
        // The quiet population first, so every blue arrow is drawn above it.
        return [
          ...view.passes.filter((r) => !isProgressive(r)).map((r) => arrow(r, REST)),
          ...view.passes.filter(isProgressive).map((r) => arrow(r, PASS)),
        ];
      case "progresivos":
        return view.progressive.map((r) => arrow(r, PASS));
      case "conducciones":
        return view.carries.map((r) => arrow(r, CARRY, r[4] === 1));
      case "pases-clave":
        return [
          ...view.keyPasses.filter((r) => !isAssist(r)).map((r) => arrow(r, PASS)),
          ...view.keyPasses.filter(isAssist).map((r) => arrow(r, CARRY)),
        ];
      default:
        return [];
    }
  }, [mode, view]);

  const failed: LegendItem = { dashed: "var(--color-mid)", text: L.failed };
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
    toques: { shown: c.touches, total: c.touches, legend: [] },
    recepciones: {
      shown: c.receptions,
      total: c.receptions,
      legend: [
        { swatch: "rgba(11,76,158,.35)", text: L.receptionZones },
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
      legend: [
        { swatch: "var(--color-blue)", text: viz.legend.regain, diamond: true },
        { swatch: "var(--color-opp)", text: viz.legend.duelLost, diamond: true },
      ],
      rule: viz.legend.defenceHover,
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

  return (
    <article className="card flex h-full flex-col">
      <SectionHead
        title={title}
        caption={copy.mapCaption(mode, num(current.shown), num(current.total), current.shown < current.total)}
        rule="none"
        as="h3"
      />

      <div role="group" aria-label={copy.mapModesLabel} className="mt-2 mb-[6px] flex flex-wrap gap-[4px]">
        {PLAYER_MAP_MODES.map((m) => (
          <Chip key={m} label={playerMapModes[m].pill} pressed={m === mode} onClick={() => setVista(m)} />
        ))}
      </div>

      <div className="mt-1 flex-1">
        {mode === "tiros" ? (
          <ShotMap
            shots={view.shots.map((s) => ({ ...s, side: "home" as const, name: playerName }))}
            teamSide="home"
            abbr={{ home: teamAbbr, away: teamAbbr }}
          />
        ) : mode === "defensivas" ? (
          <DefensiveActions actions={view.defence} title={title} />
        ) : mode === "toques" ? (
          <ZoneHeatmap cells={view.touchZones} title={title} />
        ) : mode === "recepciones" ? (
          <ZoneHeatmap cells={view.receptionZones} title={title}>
            <ReceptionDots rows={view.progressiveReceptions} />
          </ZoneHeatmap>
        ) : (
          <Pitch title={title}>
            <ArrowLayer actions={marks} />
          </Pitch>
        )}
      </div>

      {empty ? (
        <p className="label mt-2">{copy.mapEmpty}</p>
      ) : (
        <>
          {mode === "toques" && <Ramp low={viz.legend.low} high={viz.legend.high} />}
          {current.legend.length > 0 && <Legend items={current.legend} />}
          {current.rule && <p className="label mt-1">{current.rule}</p>}
        </>
      )}
    </article>
  );
}

/**
 * The dot layer of `Recepciones`: one filled `--blue` dot per progressive
 * reception, with a ring when it was received in the box. Drawn above the
 * zone heat, which counts EVERY reception; the dots are the progressive
 * subset within 5 m of the pass end (README-PLAYER §3b).
 */
function ReceptionDots({ rows }: { rows: ReceptionRow[] }) {
  return (
    <g>
      {rows.map(([x, y, inBox], i) => {
        const p = project("horizontal", x, y);
        return (
          <g key={i}>
            {inBox === 1 && (
              <circle cx={p.x} cy={p.y} r={1.4} fill="none" stroke="var(--color-blue)" strokeWidth={0.3} />
            )}
            <circle cx={p.x} cy={p.y} r={0.9} fill="var(--color-blue)" stroke="var(--color-card)" strokeWidth={0.15} />
          </g>
        );
      })}
    </g>
  );
}
