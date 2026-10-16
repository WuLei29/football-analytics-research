"use client";

/**
 * PlayerSequences — screen 03 block 5: the possession chains the player was
 * part of, ranked by xT or VAEP.
 *
 * Renders: README-PLAYER §5 — a section head with the `MetricToggle`; on the
 * left the traces pitch (`SequenceTraces`, player variant: width by value,
 * gold dot on his touch) over a four-figure aggregate strip; on the right the
 * top-10 list, the selected row inverted in `--blue`.
 * Props: `sequences` (the file's block), `surname`.
 * Data:  `players/{player_id}.json` -> `sequences` (README-PLAYER "Data
 *        contract"): `gold.sequences` ⋈ `gold.sequence_players`, chains with
 *        3+ events while his team had the ball; `points` simplified by the
 *        export with his first touch forced in.
 *
 * Client component: it owns the toggle and the selection. Changing the
 * toggle swaps the list (the export ships both top tens) and resets the
 * selection to the first row; a row click selects that chain.
 *
 * Rows are named `J7 · 42'` and the match, never `SEQ-####`: sequence ids
 * renumber on every gold rebuild, so they are not citable.
 */

import { useState } from "react";

import { dec, num, signed } from "@/lib/format";
import {
  metricDefinitions,
  player as copy,
  sequenceKind,
  type SequenceMetric,
} from "@/lib/labels";
import type { PlayerSequence, PlayerSequences as Sequences } from "@/lib/data/types";

import { MetricToggle } from "../viz/MetricToggle";
import { SequenceTraces, type SequenceTrace } from "../viz/SequenceTraces";

function key(s: PlayerSequence) {
  return String(s.id);
}

export function PlayerSequences({ sequences, surname }: { sequences: Sequences; surname: string }) {
  const [metric, setMetric] = useState<SequenceMetric>("xt");
  const [selected, setSelected] = useState(0);

  const list = metric === "xt" ? sequences.top_xt : sequences.top_vaep;
  const current = list[selected] ?? list[0];
  const traces: SequenceTrace[] = list.map((s) => ({
    sequence_id: key(s),
    points: s.points,
    ends_in_shot: s.ends_in_shot,
    value: metric === "xt" ? s.xt : s.vaep,
    touch: s.player_touch_index === null ? null : s.points[s.player_touch_index] ?? null,
  }));
  const agg = sequences.aggregates;
  const abbr = metricDefinitions[metric].abbr;

  return (
    <section className="mt-2">
      <div className="mb-3 flex flex-wrap items-baseline justify-between gap-3">
        <h2 className="font-display text-h2card font-bold">{copy.sequencesHeading(surname)}</h2>
        <MetricToggle
          value={metric}
          onChange={(m) => {
            setMetric(m);
            setSelected(0);
          }}
        />
      </div>

      {list.length === 0 ? (
        <p className="dek">{copy.sequencesEmpty}</p>
      ) : (
        <div className="grid gap-4 lg:grid-cols-[minmax(0,1fr)_minmax(0,320px)]">
          <article className="card">
            <div className="mb-2 flex flex-wrap items-baseline justify-between gap-2">
              <h3 className="font-display text-h3 font-bold">{copy.tracesHeading}</h3>
              <span className="font-mono text-[8.5px] tracking-[0.1em] uppercase text-faint">
                {copy.tracesNote}
              </span>
            </div>
            <SequenceTraces
              sequences={traces}
              selectedId={current ? key(current) : null}
              onSelect={(id) => setSelected(Math.max(0, list.findIndex((s) => key(s) === id)))}
              variant="player"
              title={copy.tracesHeading}
            />
            <div
              className="grid grid-cols-2 gap-3 border-t border-line sm:grid-cols-4"
              style={{ marginTop: 14, paddingTop: 12 }}
            >
              <Figure label={copy.aggregates.sequences} value={num(agg.sequences)} />
              <Figure
                label={copy.aggregates.xt}
                value={agg.xt === null ? "—" : dec(agg.xt, 2)}
                accent
              />
              <Figure label={copy.aggregates.shots} value={num(agg.ends_in_shot)} />
              <Figure
                label={copy.aggregates.passes}
                value={agg.passes_per_sequence === null ? "—" : dec(agg.passes_per_sequence, 1)}
              />
            </div>
            <p className="label mt-2" style={{ letterSpacing: 0, textTransform: "none" }}>
              {copy.aggregatesNote(sequences.min_events)}
            </p>
          </article>

          <article className="card">
            <div className="mb-2.5 flex items-baseline justify-between gap-2">
              <h3 className="font-display text-h3 font-bold">{copy.topHeading(abbr)}</h3>
              <span className="font-mono text-[8.5px] text-faint">{copy.topHint}</span>
            </div>
            <ol className="flex flex-col gap-1">
              {list.map((s, i) => {
                const on = s === current;
                const value = metric === "xt" ? s.xt : s.vaep;
                return (
                  <li key={key(s)}>
                    <button
                      type="button"
                      onClick={() => setSelected(i)}
                      aria-pressed={on}
                      className="w-full cursor-pointer rounded-[11px] border text-left hover:border-blue"
                      style={{
                        padding: "8px 10px",
                        background: on ? "var(--color-blue)" : "var(--color-paper)",
                        borderColor: on ? "var(--color-blue)" : "var(--color-line)",
                        color: on ? "#fff" : "var(--color-ink)",
                      }}
                    >
                      <div
                        className="flex justify-between gap-2 font-mono"
                        style={{ fontSize: 9.5, color: on ? "rgba(255,255,255,.72)" : "var(--color-mid)" }}
                      >
                        <span>{copy.sequenceWhen(s.matchday, s.minute)}</span>
                        <span>{copy.sequenceOpponent(s.opponent_abbr, s.is_home)}</span>
                      </div>
                      <div className="mt-0.5 font-sans" style={{ fontSize: 12 }}>
                        {sequenceKind(s.kind)}
                      </div>
                      <div
                        className="mt-0.5 flex gap-2.5 font-mono"
                        style={{ fontSize: 9, color: on ? "rgba(255,255,255,.72)" : "var(--color-mid)" }}
                      >
                        <span>{copy.sequenceLine(num(s.passes), num(s.player_touches))}</span>
                        <span>{value === null ? "—" : `${signed(value, 3)} ${abbr}`}</span>
                      </div>
                    </button>
                  </li>
                );
              })}
            </ol>
          </article>
        </div>
      )}
    </section>
  );
}

function Figure({ label: text, value, accent = false }: { label: string; value: string; accent?: boolean }) {
  return (
    <div>
      <div className="font-mono text-[8.5px] tracking-[0.14em] uppercase text-mid">{text}</div>
      <div
        className="font-display font-bold"
        style={{ fontSize: 26, lineHeight: 1.05, color: accent ? "var(--color-blue)" : "var(--color-ink)" }}
      >
        {value}
      </div>
    </div>
  );
}
