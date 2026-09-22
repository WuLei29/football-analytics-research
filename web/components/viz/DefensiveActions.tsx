"use client";

/**
 * DefensiveActions — where the team defended, and how high its line sat.
 *
 * Renders: a row of type filter chips, one diamond per defensive action, blue
 * when the ball was regained and grey-brown when the duel was lost, a dashed
 * average-defensive-line marker across the pitch, and a tooltip on hover.
 * Props: `actions`, `lineX` (metres up the pitch), `lineLabel`, `orientation`.
 * Data:  `matches/{match_id}.json` -> `defence` (md/WEB_DATA.md §7): every
 *        tackle, interception, recovery, challenge, blocked pass, clearance
 *        and aerial the team contested — ~128 a match — and
 *        `defensive_line_height` from `gold.team_match_stats`.
 *
 * `line_x` is in the same attacking frame as everything else, so a low value
 * is a deep line and the marker needs no interpretation (WEB_DATA §3.1).
 *
 * A diamond is a rotated square, and the design's prototype rotates it about
 * its own top-left corner, which leaves every mark offset by half a diagonal
 * from the event it describes. Here the square is centred on the action first,
 * so the mark sits where the tackle happened. Same shape, right place.
 *
 * Client component, because it answers a mouse. A hundred identical diamonds
 * say *where* the team defended but not *what happened*; hovering names the
 * action and the minute, which is the difference between a shape and a match.
 * At this density the marks are drawn small and slightly transparent, so an
 * overlap reads as a darker patch — the busy parts of the block emerge from
 * the stacking instead of being hidden by it.
 *
 * The chips subset by type because the seven are not one question. Clearances
 * and recoveries are the bulk of the rows and sit deep by definition, so they
 * set the shape of the cloud and bury the aerial and ground duels a reader
 * actually wants to locate. Type is deliberately NOT encoded in the mark:
 * colour already carries the outcome (§1 of the viz skill: inside a pitch
 * figure one accent, one meaning), and seven shapes would be a key to learn
 * rather than a picture to read. Subsetting answers the same question without
 * spending an encoding channel.
 *
 * Counts on the chips are of the whole match, not of the current selection, so
 * a chip label never changes as you click and the row stays a summary of the
 * match as well as a control.
 *
 * The tooltip is HTML, not SVG, for the reason `ShotMap` gives: its type then
 * renders at real pixel sizes whatever the pitch scales to.
 */

import { useMemo, useState } from "react";

import { defensiveActionType, viz } from "@/lib/labels";
import {
  project,
  projectFraction,
  projectRect,
  type Orientation,
} from "@/lib/viz/pitch";

import { Pitch } from "./Pitch";

/** One entry of `defence.actions` (WEB_DATA §7). */
export interface DefensiveAction {
  type: string;
  x: number;
  y: number;
  outcome: "won" | "lost";
  /** Match minute. Optional: without it the tooltip just omits the line. */
  minute?: number | null;
  /** Who made it. Optional for the same reason. */
  player?: string | null;
}

interface DefensiveActionsProps {
  actions: DefensiveAction[];
  /** `defence.line_x`, in metres. Omit to leave the line off. */
  lineX?: number;
  /** Mono caption drawn beside the line — build the number with `dec()`. */
  lineLabel?: string;
  orientation?: Orientation;
  title: string;
  className?: string;
}

/**
 * Side of the square, in metres, before rotation. The handoff says 1.9, which
 * was sized for the 34 marks this chart used to draw; at ~105 it crowds, so
 * the mark is smaller here and the handoff value stays right everywhere else.
 */
const SIDE = 1.4;
/** Overlapping marks then darken instead of occluding one another. */
const FILL_OPACITY = 0.78;
/**
 * The invisible disc that catches the mouse. A 1.4 m diamond is a small target
 * at the sizes this pitch renders at, so the hit area is bigger than the mark —
 * standard practice, and it costs nothing because the disc has no fill. Kept
 * just under the old radius so neighbouring discs do not fight for the cursor.
 */
const HIT_RADIUS = 1.8;

/**
 * Chip order: the two highest-volume types first, because those are the ones a
 * reader reaches for the switch to turn OFF, then the duels they are trying to
 * uncover, then the rarer interventions. Any type the export starts emitting
 * that is missing here still renders — it is appended in first-seen order by
 * `useTypeCounts` — so a new Opta type cannot silently vanish from the pitch.
 */
const TYPE_ORDER = [
  "ball_recovery",
  "clearance",
  "aerial",
  "tackle",
  "challenge",
  "interception",
  "blocked_pass",
];

/**
 * The types present in this match, in TYPE_ORDER, with how many of each. A
 * type with no rows gets no chip: an always-disabled control is noise, and the
 * legend idiom of the design is to build items conditionally.
 */
function useTypeCounts(actions: DefensiveAction[]) {
  return useMemo(() => {
    const counts = new Map<string, number>();
    for (const a of actions) counts.set(a.type, (counts.get(a.type) ?? 0) + 1);

    const known = TYPE_ORDER.filter((t) => counts.has(t));
    const unknown = [...counts.keys()].filter((t) => !TYPE_ORDER.includes(t));
    return [...known, ...unknown].map((type) => ({
      type,
      count: counts.get(type) ?? 0,
    }));
  }, [actions]);
}

export function DefensiveActions({
  actions,
  lineX,
  lineLabel,
  orientation = "horizontal",
  title,
  className,
}: DefensiveActionsProps) {
  const [hovered, setHovered] = useState<number | null>(null);
  const types = useTypeCounts(actions);

  // `null` means "every type", which is not the same as a set holding all of
  // them: it survives a match change, where a hard-coded set of the previous
  // match's types would filter this one against the wrong vocabulary.
  const [only, setOnly] = useState<Set<string> | null>(null);
  const shows = (type: string) => only === null || only.has(type);

  // Hover is indexed against `actions`, not against the filtered list, so a
  // chip click cannot renumber what the tooltip is pointing at. It is still
  // cleared on every change, because the hovered mark may have just left.
  const active = hovered === null ? null : (actions[hovered] ?? null);
  const visible = active !== null && shows(active.type) ? active : null;

  function toggle(type: string) {
    setHovered(null);
    setOnly((current) => {
      const next = new Set(current ?? types.map((t) => t.type));
      if (next.has(type)) next.delete(type);
      else next.add(type);
      return next.size === types.length ? null : next;
    });
  }

  return (
    <div className={className} style={{ position: "relative" }}>
      <TypeChips
        types={types}
        shows={shows}
        onToggle={toggle}
        onAll={() => {
          setHovered(null);
          setOnly(null);
        }}
        allActive={only === null}
      />

      <Pitch orientation={orientation} title={title}>
        {lineX !== undefined && (
          <DefensiveLine orientation={orientation} lineX={lineX} label={lineLabel} />
        )}

        {actions.map((action, i) => {
          if (!shows(action.type)) return null;
          const p = project(orientation, action.x, action.y);
          return (
            <g
              key={i}
              opacity={hovered === null || hovered === i ? 1 : 0.22}
              style={{ cursor: "pointer" }}
              onMouseEnter={() => setHovered(i)}
              onMouseLeave={() => setHovered(null)}
            >
              <rect
                x={p.x - SIDE / 2}
                y={p.y - SIDE / 2}
                width={SIDE}
                height={SIDE}
                fill={action.outcome === "won" ? "var(--color-blue)" : "var(--color-opp)"}
                fillOpacity={FILL_OPACITY}
                transform={`rotate(45 ${p.x.toFixed(2)} ${p.y.toFixed(2)})`}
              />
              <circle cx={p.x} cy={p.y} r={HIT_RADIUS} fill="transparent" />
            </g>
          );
        })}
      </Pitch>

      {visible && <Tooltip action={visible} orientation={orientation} />}

      {/* Only when the pitch is genuinely blank -- otherwise the marks speak
          for themselves and a standing line of small print under every match
          is height the column beside this one has to match. */}
      {only !== null && only.size === 0 && (
        <p className="label mt-2">{viz.legend.defenceEmpty}</p>
      )}
    </div>
  );
}

/**
 * The filter row. A `group` of independent toggles rather than a `radiogroup`,
 * because the types are not alternatives — any combination is a legitimate
 * reading. `aria-pressed` is what carries that to a screen reader.
 *
 * Styling follows `MetricToggle`: DM Mono at 10px, tracked, pressed state is
 * `--blue` on `--paper`. The chips are separate rounded pills, not one
 * segmented bar, since a segmented bar reads as "pick one".
 */
function TypeChips({
  types,
  shows,
  onToggle,
  onAll,
  allActive,
}: {
  types: { type: string; count: number }[];
  shows: (type: string) => boolean;
  onToggle: (type: string) => void;
  onAll: () => void;
  allActive: boolean;
}) {
  return (
    <div role="group" aria-label={viz.legend.defenceFilter} className="mb-[6px]">
      <div className="flex flex-wrap items-center gap-[4px]">
        <span className="label" style={{ marginRight: 2 }}>
          {viz.legend.defenceFilter}
        </span>

        <Chip
          label={viz.legend.defenceFilterAll}
          pressed={allActive}
          onClick={onAll}
        />

        {/* A hairline gutter: "Todas" resets, the rest subset. */}
        <span
          aria-hidden="true"
          style={{ width: 1, height: 11, background: "var(--color-line)" }}
        />

        {types.map(({ type, count }) => (
          <Chip
            key={type}
            label={viz.legend.defenceChip(defensiveActionType(type), count)}
            pressed={shows(type)}
            onClick={() => onToggle(type)}
          />
        ))}
      </div>
    </div>
  );
}

function Chip({
  label,
  pressed,
  onClick,
}: {
  label: string;
  pressed: boolean;
  onClick: () => void;
}) {
  return (
    <button
      type="button"
      aria-pressed={pressed}
      onClick={onClick}
      className="font-mono rounded-[6px] border"
      style={{
        fontSize: 9,
        letterSpacing: "0.05em",
        padding: "2px 7px",
        lineHeight: 1.5,
        cursor: "pointer",
        borderColor: pressed ? "var(--color-blue)" : "var(--color-line)",
        color: pressed ? "var(--color-paper)" : "var(--color-mid)",
        background: pressed ? "var(--color-blue)" : "transparent",
      }}
    >
      {label}
    </button>
  );
}

/**
 * Positioned as a percentage of the SVG's own box — hence `projectFraction`,
 * which accounts for the viewBox padding. Same mechanism as the shot tooltip.
 */
function Tooltip({
  action,
  orientation,
}: {
  action: DefensiveAction;
  orientation: Orientation;
}) {
  const at = projectFraction(orientation, action.x, action.y);
  const won = action.outcome === "won";

  return (
    <div
      style={{
        position: "absolute",
        left: `${at.x * 100}%`,
        top: `${at.y * 100}%`,
        transform: "translate(-50%,-128%)",
        background: "var(--color-ink)",
        borderRadius: 10,
        padding: "6px 9px",
        pointerEvents: "none",
        whiteSpace: "nowrap",
        zIndex: 4,
      }}
    >
      <div
        style={{
          fontFamily: "var(--font-sans)",
          fontSize: 12,
          fontWeight: 600,
          color: "var(--color-on-dark)",
        }}
      >
        {defensiveActionType(action.type)}
      </div>
      <div
        style={{
          fontFamily: "var(--font-mono)",
          fontSize: 9.5,
          color: won ? "#7FB2ED" : "rgba(246,239,230,.72)",
          lineHeight: 1.6,
        }}
      >
        {action.minute != null && `${action.minute}' · `}
        {won ? viz.legend.regain : viz.legend.duelLost}
        {action.player && (
          <>
            <br />
            {action.player}
          </>
        )}
      </div>
    </div>
  );
}

function DefensiveLine({
  orientation,
  lineX,
  label,
}: {
  orientation: Orientation;
  lineX: number;
  label?: string;
}) {
  const line = projectRect(orientation, lineX, 0, lineX, 68);
  // Put the caption just past the line, at the top of whichever frame we are in.
  const anchor = project(orientation, lineX, 68);

  return (
    <g>
      <line
        x1={line.x}
        y1={line.y}
        x2={line.x + line.width}
        y2={line.y + line.height}
        stroke="var(--color-neg)"
        strokeWidth={0.4}
        strokeDasharray="1.4 1.4"
      />
      {label && (
        <text
          x={anchor.x + (orientation === "horizontal" ? 0.8 : 0)}
          y={anchor.y + (orientation === "horizontal" ? 2.6 : -1.1)}
          fontFamily="var(--font-mono)"
          fontSize={2.1}
          fill="var(--color-neg)"
        >
          {label}
        </text>
      )}
    </g>
  );
}
