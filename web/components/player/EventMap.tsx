"use client";

/**
 * EventMap — the one pitch of screen 03, switched between eight modes.
 *
 * Renders: README-PLAYER §3b — the title (which follows the mode), the
 * `ATAQUE →` cue, the count note ("150 DE 1,284 PASES" when a cap applies),
 * eight mode pills, a horizontal pitch attacking right, and the mode's
 * legend in HTML under it.
 * Props: `maps` (the whole `maps` block), `playerName`, `teamAbbr`.
 * Data:  `players/{player_id}.json` -> `maps` (README-PLAYER "Data
 *        contract"): `silver.events` by the player, `gold.pass_receptions`,
 *        in his team's attacking frame. Nothing is flipped here — low y is the
 *        right flank and sits at the bottom, so a right back's marks are in
 *        the lower half.
 *
 * Client component: the mode is the reader's one choice on the page. It is
 * mirrored to `?vista=` (`lib/vista.ts`) so a view is shareable and the tab
 * strip can carry it to the next player.
 *
 * Deviations from the brief, both deliberate:
 *   - `<Pitch>` stays mounted across the five arrow and heat modes (they are
 *     all children of one `ZoneHeatmap`/`Pitch` slot); `Tiros` and
 *     `Defensivas` swap in `ShotMap` and `DefensiveActions`, which own their
 *     own SVG for the hover card and the type chips. Re-implementing either
 *     to share one SVG would fork two components the match page also uses.
 *   - The shot circles use `ShotMap`'s own radius law (area ∝ xG at a smaller
 *     constant than the brief's `1.4 + sqrt(xG) * 5.2`), so a shot is the same
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
import { PASS_FLAG, type PassRow, type PlayerMaps } from "@/lib/data/types";

import { ArrowLayer, type ArrowMark } from "../viz/ActionArrows";
import { DefensiveActions } from "../viz/DefensiveActions";
import { Legend, Ramp, type LegendItem } from "../viz/Legend";
import { Pitch } from "../viz/Pitch";
import { ShotMap } from "../viz/ShotMap";
import { ZoneHeatmap } from "../viz/ZoneHeatmap";

/* -- Marks per mode (README-PLAYER §3b table) ------------------------------ */

function arrow(row: PassRow, style: Omit<ArrowMark, "x" | "y" | "end_x" | "end_y" | "completed">): ArrowMark {
  return {
    x: row[0],
    y: row[1],
    end_x: row[2],
    end_y: row[3],
    completed: (row[4] & PASS_FLAG.completed) !== 0,
    ...style,
  };
}

const PROGRESSIVE = { tone: "blue", width: 0.6, opacity: 0.9 } as const;
const REST = { tone: "faint", width: 0.3, opacity: 0.38, head: false } as const;

function passMap(rows: PassRow[]): ArrowMark[] {
  const progressive = (r: PassRow) => (r[4] & PASS_FLAG.progressive) !== 0;
  // The quiet population first, so every blue arrow is drawn above the grey.
  return [
    ...rows.filter((r) => !progressive(r)).map((r) => arrow(r, REST)),
    ...rows.filter(progressive).map((r) => arrow(r, PROGRESSIVE)),
  ];
}

function keyPassMap(rows: PassRow[]): ArrowMark[] {
  const assist = (r: PassRow) => (r[4] & PASS_FLAG.assist) !== 0;
  return [
    ...rows.filter((r) => !assist(r)).map((r) => arrow(r, { tone: "blue", width: 0.55, opacity: 0.85 })),
    ...rows.filter(assist).map((r) => arrow(r, { tone: "gold", width: 0.85, opacity: 0.85 })),
  ];
}

interface ModeView {
  /** How many marks are drawn, and of how many the season has. */
  shown: number;
  total: number;
  legend: LegendItem[];
  /** A sentence under the legend: the definition behind the marks. */
  rule?: string;
}

export function EventMap({
  maps,
  playerName,
  teamAbbr,
}: {
  maps: PlayerMaps;
  playerName: string;
  teamAbbr: string;
}) {
  const mode = useVista();
  const c = maps.counts;
  const L = copy.legend;

  const marks = useMemo(() => {
    switch (mode) {
      case "pases":
        return passMap(maps.passes);
      case "progresivos":
        return maps.progressive_passes.map((r) =>
          arrow(r, { tone: "blue", width: 0.7, opacity: 0.88 }),
        );
      case "conducciones":
        return maps.carries.map((r) =>
          arrow(r, { tone: "gold", width: 0.75, opacity: 0.9 }),
        );
      case "pases-clave":
        return keyPassMap(maps.key_passes);
      default:
        return [];
    }
  }, [mode, maps]);

  const view: Record<PlayerMapMode, ModeView> = {
    pases: {
      shown: maps.passes.length,
      total: c.passes,
      legend: [
        { bar: "var(--color-blue)", text: L.progressive },
        { bar: "var(--color-faint)", text: L.rest },
        { dashed: "var(--color-mid)", text: L.failed },
      ],
    },
    progresivos: {
      shown: maps.progressive_passes.length,
      total: c.progressive_passes,
      legend: [
        { bar: "var(--color-blue)", text: viz.legend.progressivePass },
        { dashed: "var(--color-mid)", text: L.failed },
      ],
      rule: L.progressiveRule,
    },
    conducciones: {
      shown: maps.carries.length,
      total: c.progressive_carries,
      legend: [{ bar: "var(--color-gold)", text: viz.legend.carry }],
      rule: L.carryRule,
    },
    toques: { shown: c.touches, total: c.touches, legend: [], rule: L.touchDensity },
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
      shown: maps.shots.length,
      total: maps.shots.length,
      legend: [
        { swatch: "var(--color-blue)", text: viz.legend.shotGoal },
        { swatch: "rgba(11,76,158,.45)", outline: "var(--color-blue)", text: viz.legend.shotOn },
        { outline: "var(--color-blue)", text: viz.legend.shotOff },
      ],
      rule: L.shotSize,
    },
    defensivas: {
      shown: maps.defence.length,
      total: maps.defence.length,
      legend: [
        { swatch: "var(--color-blue)", diamond: true, text: L.regain },
        { swatch: "var(--color-opp)", diamond: true, text: L.lost },
      ],
    },
    "pases-clave": {
      shown: maps.key_passes.length,
      total: c.key_passes + c.assists,
      legend: [
        { bar: "var(--color-gold)", text: L.assist },
        { bar: "var(--color-blue)", text: L.keyPass },
      ],
    },
  };
  const current = view[mode];
  const empty = current.total === 0;

  return (
    <article className="card flex h-full flex-col">
      <div className="mb-2 flex flex-wrap items-baseline justify-between gap-x-4 gap-y-1">
        <div className="flex items-baseline gap-3">
          <h2 className="font-display text-[17px] font-bold" style={{ letterSpacing: "-0.015em" }}>
            {playerMapModes[mode].title}
          </h2>
          <span className="font-mono text-[8.5px] tracking-[0.1em] text-faint">{copy.mapAttack}</span>
        </div>
        <span className="font-mono text-[8.5px] tracking-[0.1em] uppercase text-faint">
          {copy.mapCount(
            num(current.shown),
            num(current.total),
            copy.mapNouns[mode],
            current.shown < current.total,
          )}
        </span>
      </div>

      <div role="tablist" className="mb-2.5 flex flex-wrap gap-[5px]">
        {PLAYER_MAP_MODES.map((m) => {
          const active = m === mode;
          return (
            <button
              key={m}
              type="button"
              role="tab"
              aria-selected={active}
              onClick={() => setVista(m)}
              className="cursor-pointer rounded-pill border font-mono uppercase hover:border-blue"
              style={{
                fontSize: 9.5,
                letterSpacing: "0.08em",
                padding: "5px 11px",
                background: active ? "var(--color-blue)" : "var(--color-paper)",
                borderColor: active ? "var(--color-blue)" : "var(--color-line)",
                color: active ? "#fff" : "var(--color-mid)",
              }}
            >
              {playerMapModes[m].pill}
            </button>
          );
        })}
      </div>

      <div className="flex-1">
        {mode === "tiros" ? (
          <ShotMap
            shots={maps.shots.map((s) => ({ ...s, side: "home" as const, name: playerName }))}
            teamSide="home"
            abbr={{ home: teamAbbr, away: teamAbbr }}
          />
        ) : mode === "defensivas" ? (
          <DefensiveActions actions={maps.defence} title={playerMapModes[mode].title} />
        ) : mode === "toques" ? (
          <ZoneHeatmap
            cells={maps.touch_zones.map((z) => ({ zone_id: z.zone_id, value: z.count }))}
            title={playerMapModes[mode].title}
          />
        ) : mode === "recepciones" ? (
          <ZoneHeatmap
            cells={maps.reception_zones.map((z) => ({ zone_id: z.zone_id, value: z.count }))}
            title={playerMapModes[mode].title}
          >
            <ReceptionDots rows={maps.progressive_receptions} />
          </ZoneHeatmap>
        ) : (
          <Pitch title={playerMapModes[mode].title}>
            <ArrowLayer actions={marks} />
          </Pitch>
        )}
      </div>

      {empty ? (
        <p className="dek mt-2">{copy.mapEmpty}</p>
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
function ReceptionDots({ rows }: { rows: PlayerMaps["progressive_receptions"] }) {
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
