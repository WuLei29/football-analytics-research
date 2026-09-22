"use client";

/**
 * SequenceBrowser — a pitch, and the list of plays you can put on it.
 *
 * Renders: `SequenceDetail` for the selected sequence, and beside it a
 * vertical list of every sequence in the match. A collapsed row is one line —
 * minute, phase, outcome — and the selected row opens into the full reading of
 * that play: start, duration, actions, xT, VAEP.
 * Props: `sequences` (ordered as they should be listed), `title`, an optional
 * `legend` node drawn under the pitch, and `initialIndex`.
 * Data:  `matches/{match_id}.json` -> `sequences[]` (md/WEB_DATA.md §7), each
 *        carrying its header from `gold.sequences` and its `actions[]` from
 *        `silver.events` ordered by `(json_index, event_id)`.
 *
 * WHY AN ACCORDION AND NOT A TABLE. A match has ~170 sequences per team. The
 * numbers that describe one of them (xT, VAEP, duration) mean nothing until
 * you have chosen it, and a column of 170 xT values invites reading down the
 * column, which is a different — and worse — question than "what happened in
 * this play". So the list carries only what you need to *choose* a play, and
 * the numbers appear on the one you chose, next to the drawing of it.
 *
 * Client component: it owns the selection. `SequenceDetail` stays a pure
 * drawing of whatever it is handed, which is what makes it reusable on the
 * sequence lab (screen 04) where the selection comes from filters instead.
 *
 * The list is a `<ul>` of real `<button>`s, not clickable `<div>`s: keyboard
 * focus and Enter/Space come free, and `aria-expanded` says what the click did.
 */

import { useState, type ReactNode } from "react";

import { dec } from "@/lib/format";
import {
  sequenceActionType,
  sequenceEnding,
  sequenceKind,
  sequencePhase,
  sequenceTrigger,
  viz,
} from "@/lib/labels";
import type { Orientation } from "@/lib/viz/pitch";

import {
  SequenceDetail,
  type ActionRole,
  type ActionTooltip,
  type SequenceAction,
} from "./SequenceDetail";

/**
 * One entry of `sequences[]`: the `gold.sequences` header plus its actions.
 *
 * `primary_phase` and `outcome` are the raw column VALUES, not Spanish: the
 * closed enums of GOLD_LAYER §4.1.6 and §4.1.3. They are translated here at
 * render time through `lib/labels.ts`, so the export never ships prose and an
 * enum value added in gold shows up as `⟨new_value⟩` rather than silently.
 */
export interface SequenceEntry {
  sequence_id: string;
  /** `gold.sequences.primary_phase`, e.g. `"fast_attacking"`. */
  primary_phase: string;
  /**
   * The row's title — the export's priority ladder over the phase flags
   * (WEB_DATA §7.6): `counter_attack`, `high_transition`, `corner`,
   * `throw_in`, `free_kick`, `direct_long`, `positional`, `buildup`, `fast`.
   * Optional so files exported before 21 Sep 2026 fall back to the phase.
   */
  kind?: string | null;
  /** `gold.sequences.outcome`, e.g. `"shot_saved"`. */
  outcome: string;
  /**
   * How the chain began and, on a `turnover`, how it lost the ball — the
   * export's `start_kind` and `end_action` (WEB_DATA §7.3). Optional so the
   * demo fixtures need neither.
   */
  start_kind?: string | null;
  end_action?: string | null;
  start_minute: number;
  start_second: number;
  duration_s: number;
  event_count: number;
  xt: number;
  vaep: number;
  actions: SequenceAction[];
}

interface SequenceBrowserProps {
  sequences: SequenceEntry[];
  title: string;
  /** The marks legend, drawn under the pitch. Built by the page. */
  legend?: ReactNode;
  orientation?: Orientation;
  /** Which sequence is open on first paint. */
  initialIndex?: number;
  /** Height of the scrolling list, in px. */
  listHeight?: number;
  className?: string;
}

export function SequenceBrowser({
  sequences,
  title,
  legend,
  orientation = "horizontal",
  initialIndex = 0,
  listHeight = 430,
  className,
}: SequenceBrowserProps) {
  const [selected, setSelected] = useState(initialIndex);
  const active = sequences[selected] ?? sequences[0] ?? null;

  /**
   * The hover card of one mark. Title "Recuperación · Calatrava (22)"; then
   * the clock (and xG on a shot); then, on the chain's first or last action, how
   * it began ("Inicio · Saque de banda") or ended ("Final · Pérdida · pase
   * largo fallado"). The words live here so `SequenceDetail` holds no Spanish.
   */
  const describe = (a: SequenceAction, role: ActionRole): ActionTooltip => {
    const t = viz.sequenceTooltip;
    const who = a.surname
      ? a.shirt_number !== null ? `${a.surname} (${a.shirt_number})` : a.surname
      : null;
    const what = sequenceActionType(a.type);

    const clock =
      a.minute != null ? `${a.minute}'${String(a.second ?? 0).padStart(2, "0")}"` : null;
    const xg = a.kind === "shot" && a.xg != null ? `${dec(a.xg, 2)} xG` : null;
    const lines = [[clock, xg].filter(Boolean).join(" · ")];

    if (role === "start" && active?.start_kind) {
      lines.push(t.startedBy(sequenceTrigger(active.start_kind)));
    } else if (role === "end" && active) {
      lines.push(t.endedBy(sequenceEnding(active.outcome, active.end_action)));
    }
    return { title: who ? `${what} · ${who}` : what, lines };
  };

  if (!active) {
    return <p className="label">{viz.sequenceList.empty}</p>;
  }

  return (
    <div className={`grid gap-4 lg:grid-cols-[2fr_1fr] ${className ?? ""}`}>
      <div>
        <SequenceDetail
          actions={active.actions}
          title={title}
          orientation={orientation}
          describe={describe}
        />
        {/* The legend gets a wrapper of its own rather than sitting beside the
            pitch as a sibling. It arrives from a server component through the
            Flight payload as a lazy node, which React cannot mark as
            key-validated at JSX time, so as one of two array children it
            trips "each child in a list should have a unique key" in dev — a
            false positive, but one the reader sees. As a sole child there is
            no list to validate. */}
        {legend && <div>{legend}</div>}
      </div>

      <aside className="flex min-w-0 flex-col gap-2">
        <div className="flex items-baseline justify-between gap-2">
          <p className="eyebrow">{viz.sequenceList.heading}</p>
          <span className="label" style={{ letterSpacing: 0, textTransform: "none" }}>
            {viz.sequenceList.count(sequences.length)}
          </span>
        </div>

        <ul
          className="flex flex-col"
          style={{
            maxHeight: listHeight,
            overflowY: "auto",
            borderTop: "1px solid var(--color-line)",
          }}
        >
          {sequences.map((sequence, i) => (
            <SequenceRow
              key={sequence.sequence_id}
              sequence={sequence}
              open={i === selected}
              onSelect={() => setSelected(i)}
            />
          ))}
        </ul>

        <p className="label" style={{ letterSpacing: 0, textTransform: "none" }}>
          {viz.sequenceList.hint}
        </p>
      </aside>
    </div>
  );
}

/**
 * One row. Collapsed it is a single line you can scan a column of; open it
 * grows the five numbers that describe the play.
 *
 * The open row is marked with a blue rule down its left edge rather than a
 * filled background: the pitch beside it is already spending blue on the ball,
 * and two competing blues in one figure is exactly the v1 mistake the design
 * skill lists (espanyol-viz-design §1).
 */
function SequenceRow({
  sequence,
  open,
  onSelect,
}: {
  sequence: SequenceEntry;
  open: boolean;
  onSelect: () => void;
}) {
  const panel = viz.sequencePanel;
  const clock = `${sequence.start_minute}'${String(sequence.start_second).padStart(2, "0")}"`;

  return (
    <li style={{ borderBottom: "1px solid var(--color-line)" }}>
      <button
        type="button"
        onClick={onSelect}
        aria-expanded={open}
        style={{
          width: "100%",
          textAlign: "left",
          padding: "7px 8px 7px 9px",
          cursor: "pointer",
          background: open ? "rgba(11,76,158,.06)" : "transparent",
          borderLeft: `2px solid ${open ? "var(--color-blue)" : "transparent"}`,
        }}
      >
        <span className="flex items-baseline justify-between gap-2">
          <span
            style={{
              fontFamily: "var(--font-sans)",
              fontSize: 12.5,
              fontWeight: open ? 600 : 500,
              color: "var(--color-ink)",
              overflow: "hidden",
              textOverflow: "ellipsis",
              whiteSpace: "nowrap",
            }}
          >
            {sequence.kind
              ? sequenceKind(sequence.kind)
              : sequencePhase(sequence.primary_phase)}
          </span>
          <span
            style={{
              fontFamily: "var(--font-mono)",
              fontSize: 10,
              color: "var(--color-faint)",
              flexShrink: 0,
            }}
          >
            {clock}
          </span>
        </span>

        <span
          style={{
            display: "block",
            fontFamily: "var(--font-mono)",
            fontSize: 9.5,
            color: "var(--color-faint)",
            marginTop: 1,
          }}
        >
          {sequenceEnding(sequence.outcome, sequence.end_action)} ·{" "}
          {viz.sequenceList.summary(String(sequence.event_count), dec(sequence.duration_s, 1))}
        </span>
      </button>

      {open && (
        <dl
          className="grid grid-cols-3 gap-x-3 gap-y-[6px]"
          style={{ padding: "2px 8px 9px 11px" }}
        >
          <Stat label={panel.start} value={clock} />
          <Stat label={panel.duration} value={`${dec(sequence.duration_s, 1)} s`} />
          <Stat label={panel.events} value={String(sequence.event_count)} />
          <Stat label={panel.xt} value={dec(sequence.xt, 3)} />
          <Stat label={panel.vaep} value={dec(sequence.vaep, 3)} />
          <Stat label="ID" value={sequence.sequence_id} />
          {/* How it began and how it ended — the two facts the 20 Sep 2026
              review asked for. The ending spans two columns: "Pérdida · pase
              en profundidad fallado" does not fit one. */}
          {sequence.start_kind && (
            <Stat label={panel.trigger} value={sequenceTrigger(sequence.start_kind)} />
          )}
          <div className="col-span-2 min-w-0">
            <dt className="label">{panel.outcome}</dt>
            <dd className="font-[family-name:var(--font-mono)]" style={{ fontSize: 12 }}>
              {sequenceEnding(sequence.outcome, sequence.end_action)}
            </dd>
          </div>
        </dl>
      )}
    </li>
  );
}

/** One label/value pair of the open row. */
function Stat({ label, value }: { label: string; value: string }) {
  return (
    <div className="min-w-0">
      <dt className="label">{label}</dt>
      <dd
        className="font-[family-name:var(--font-mono)]"
        style={{ fontSize: 12, overflow: "hidden", textOverflow: "ellipsis" }}
      >
        {value}
      </dd>
    </div>
  );
}
