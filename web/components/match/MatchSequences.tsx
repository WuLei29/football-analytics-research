"use client";

/**
 * MatchSequences — screen 02's sequence block, on the light-blue spot panel:
 * one chain drawn large, and every chain of the match listed beside it.
 *
 * Renders: the block heading with the xT / VAEP switch; on the left a white
 * card with the selected chain (`SequenceDetail` large, its name, how it
 * began and ended, five light numbers, and its actions in words); on the
 * right a white card with the list, a scrolling column of buttons sorted by
 * the chosen metric. Stacked on a phone, the chain first.
 * Props: `sequences` in FILE order (the export's, best xT first): a map
 *        row's `s` indexes this array.
 * Data:  `matches/{match_id}.json` -> `sequences[]` (md/WEB_DATA.md §7.3).
 *
 * The layout is the handoff's block 6 (the big pitch, the list on the right),
 * redrawn in Pizarra on 24 Oct 2026. It is the page's one spot panel because
 * it is where the maps above lead: a chain opened from a map mark
 * (`SequenceLink`) is selected here, scrolled into the list, and the page
 * scrolls to this block. Switching the metric keeps the selected chain.
 *
 * `SequenceDetail` is drawn without `describe`, so its marks have no hover
 * card and none of them is a control the keyboard cannot reach; the actions
 * are listed in words under the drawing instead. It is recoloured by the
 * `.pizarra-viz` adapter (globals.css).
 */

import { useEffect, useMemo, useRef, useState } from "react";

import { dec } from "@/lib/format";
import {
  matchExplorer as copy,
  metricDefinitions,
  sequenceActionType,
  sequenceEnding,
  sequenceKind,
  sequencePhase,
  sequenceTrigger,
  viz,
  type SequenceMetric,
} from "@/lib/labels";

import type { SequenceEntry } from "@/components/viz/SequenceBrowser";
import { SequenceDetail } from "@/components/viz/SequenceDetail";

import { useSequenceLink } from "./SequenceLink";

const clockOf = (minute: number | null | undefined, second: number | null | undefined) =>
  minute == null ? null : `${minute}'${String(second ?? 0).padStart(2, "0")}"`;

const nameOf = (seq: SequenceEntry) => (seq.kind ? sequenceKind(seq.kind) : sequencePhase(seq.primary_phase));

export function MatchSequences({ sequences }: { sequences: SequenceEntry[] }) {
  const { opened } = useSequenceLink();
  const [selected, setSelected] = useState<number | null>(sequences.length > 0 ? 0 : null);
  const [metric, setMetric] = useState<SequenceMetric>("xt");
  const sectionRef = useRef<HTMLDivElement>(null);
  const listRef = useRef<HTMLUListElement>(null);

  // A chain opened from a map: select it and bring the block up. Every new
  // open, even of the chain already showing, scrolls (the `nonce`).
  const nonce = opened?.nonce;
  useEffect(() => {
    if (!opened) return;
    const index = sequences.findIndex((s) => s.sequence_id === opened.sequence_id);
    if (index >= 0) setSelected(index);
    sectionRef.current?.scrollIntoView({ behavior: "smooth", block: "start" });
    // Only a new open should move the page, not a new `sequences` array.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [nonce]);

  const ordered = useMemo(() => {
    const rows = sequences.map((seq, index) => ({ seq, index }));
    return metric === "xt" ? rows : [...rows].sort((a, b) => b.seq[metric] - a.seq[metric]);
  }, [sequences, metric]);

  // Keep the selected row in sight inside the list (not the page).
  useEffect(() => {
    const list = listRef.current;
    const row = list?.querySelector<HTMLElement>("[aria-pressed='true']");
    if (list && row && (row.offsetTop < list.scrollTop || row.offsetTop > list.scrollTop + list.clientHeight - 48)) {
      list.scrollTop = row.offsetTop - 8;
    }
  }, [selected, metric]);

  const chain = selected !== null ? sequences[selected] : null;
  const M = metricDefinitions[metric].abbr;
  const panel = viz.sequencePanel;

  return (
    <div ref={sectionRef} className="flex scroll-mt-24 flex-col gap-4">
      <div className="flex flex-wrap items-end justify-between gap-x-6 gap-y-3 px-2 pt-2">
        <div>
          <h2 id="jugadas-titulo" className="text-[26px] leading-tight font-semibold tracking-[-0.02em]">
            {copy.sequencesHeading(M)}
          </h2>
          <p className="mt-2 max-w-[70ch] text-[15px] text-spot-text">{copy.sequencesDek(sequences.length)}</p>
        </div>
        <div role="group" aria-label={copy.sortLabel} className="flex gap-1 rounded-full bg-spot-deep/60 p-1">
          {(["xt", "vaep"] as const).map((m) => (
            <button
              key={m}
              type="button"
              aria-pressed={metric === m}
              onClick={() => setMetric(m)}
              className={`rounded-full px-4 py-2 text-[14px] font-semibold transition-colors ${
                metric === m ? "bg-team text-white" : "text-team hover:bg-spot-deep"
              }`}
            >
              {metricDefinitions[m].abbr}
            </button>
          ))}
        </div>
      </div>

      {chain ? (
        <div className="grid gap-4 lg:grid-cols-[minmax(0,2fr)_minmax(0,1fr)]">
          {/* --- The chain, large ------------------------------------------ */}
          <div className="rounded-row bg-white p-4 sm:p-6">
            <h3 className="text-[18px] font-semibold tracking-[-0.01em]">{nameOf(chain)}</h3>
            <p className="text-[14px] text-slate">
              {copy.chainMeta(clockOf(chain.start_minute, chain.start_second) ?? "", sequenceEnding(chain.outcome, chain.end_action))}
            </p>

            <div className="pizarra-viz mt-4">
              <SequenceDetail key={chain.sequence_id} actions={chain.actions} title={copy.chainPitch} />
            </div>

            <dl className="mt-4 grid grid-cols-2 gap-x-4 gap-y-2 min-[420px]:grid-cols-3 sm:grid-cols-5">
              {[
                [panel.trigger, chain.start_kind ? sequenceTrigger(chain.start_kind) : "—"],
                [panel.duration, `${dec(chain.duration_s, 1)} s`],
                [panel.events, String(chain.event_count)],
                [panel.xt, dec(chain.xt, 3)],
                [panel.vaep, dec(chain.vaep, 3)],
              ].map(([k, v]) => (
                <div key={k} className="min-w-0">
                  <dt className="text-[13px] text-slate">{k}</dt>
                  <dd className="text-[20px] leading-tight font-light break-words tabular-nums">{v}</dd>
                </div>
              ))}
            </dl>

            <details className="mt-4">
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

          {/* --- The list, on the right ------------------------------------ */}
          <div className="flex min-h-0 flex-col rounded-row bg-white p-3 sm:p-4">
            <div className="flex items-baseline justify-between gap-2 px-2">
              <h3 className="text-[18px] font-semibold tracking-[-0.01em]">{copy.listHeading}</h3>
              <span className="text-[14px] text-slate">{viz.sequenceList.count(sequences.length)}</span>
            </div>
            <ul ref={listRef} className="relative mt-2 flex max-h-[560px] flex-col gap-1 overflow-y-auto pr-1 lg:max-h-[640px]">
              {ordered.map(({ seq, index }) => {
                const on = index === selected;
                return (
                  <li key={seq.sequence_id}>
                    <button
                      type="button"
                      aria-pressed={on}
                      onClick={() => setSelected(index)}
                      className={`grid w-full grid-cols-[minmax(0,1fr)_auto] items-baseline gap-x-3 rounded-[12px] px-3 py-2 text-left transition-colors ${
                        on ? "bg-spot" : "hover:bg-panel"
                      }`}
                    >
                      <span className={`truncate text-[15px] font-semibold ${on ? "text-team" : ""}`}>{nameOf(seq)}</span>
                      <span className="text-[14px] text-slate tabular-nums">{clockOf(seq.start_minute, seq.start_second)}</span>
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
        </div>
      ) : (
        <p className="rounded-row bg-white p-5 text-[15px] text-slate">{copy.chainEmpty}</p>
      )}
    </div>
  );
}
