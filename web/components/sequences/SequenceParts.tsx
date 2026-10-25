"use client";

/**
 * SequenceParts — the two halves of a Pizarra sequence block, shared by the
 * match page (`components/match/MatchSequences`) and the player page
 * (`components/player/PlayerSequences`).
 *
 * Renders: `ChainCard`, one chain on a white card: its name, when it began
 * and how it ended, `SequenceDetail` large, the marks key, five light
 * numbers and its actions in words; `SequenceList`, the chains as a
 * scrolling column of buttons, the selected one on spot in team ink, each
 * row with its clock (and its match on the player page) and the value of
 * the metric it is sorted by. `MetricSwitch`, the xT / VAEP pills.
 * Props: a `SequenceEntry` (`lib/data/sequences.ts`) per chain; the player
 *        page adds `focusPlayerId` / `focusName` so his nodes are drawn in
 *        spot-line light blue and named in the key.
 * Data:  `matches/{match_id}.json` -> `sequences[]` (md/WEB_DATA.md §7.3).
 *
 * Pizarra (24 Oct 2026) redraw of the handoff's `SequenceBrowser` +
 * `SequenceLegend`: same layout (the chain large, the list on the right),
 * no mono labels, no ruled rows, no gold. `SequenceDetail` is drawn without
 * `describe`, so its marks have no hover card and none of them is a control
 * the keyboard cannot reach; the actions are listed in words instead.
 */

import type { ReactNode, Ref } from "react";

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

import type { SequenceEntry } from "@/lib/data/sequences";
import { SequenceDetail } from "@/components/viz/SequenceDetail";

export const clockOf = (minute: number | null | undefined, second: number | null | undefined) =>
  minute == null ? null : `${minute}'${String(second ?? 0).padStart(2, "0")}"`;

export const chainName = (seq: SequenceEntry) => (seq.kind ? sequenceKind(seq.kind) : sequencePhase(seq.primary_phase));

/** The xT / VAEP pills, pressed one team-filled, in a spot-deep track. */
export function MetricSwitch({ value, onChange }: { value: SequenceMetric; onChange: (m: SequenceMetric) => void }) {
  return (
    <div role="group" aria-label={copy.sortLabel} className="flex gap-1 rounded-full bg-spot-deep/60 p-1">
      {(["xt", "vaep"] as const).map((m) => (
        <button
          key={m}
          type="button"
          aria-pressed={value === m}
          onClick={() => onChange(m)}
          className={`rounded-full px-4 py-2 text-[14px] font-semibold transition-colors ${
            value === m ? "bg-team text-white" : "text-team hover:bg-spot-deep"
          }`}
        >
          {metricDefinitions[m].abbr}
        </button>
      ))}
    </div>
  );
}

export function ChainCard({
  chain,
  focusPlayerId,
  focusName,
  top,
}: {
  chain: SequenceEntry;
  focusPlayerId?: number;
  focusName?: string;
  /** Drawn above the heading: the player page's "opened from the map" bar. */
  top?: ReactNode;
}) {
  const panel = viz.sequencePanel;
  const meta = copy.chainMeta(clockOf(chain.start_minute, chain.start_second) ?? "", sequenceEnding(chain.outcome, chain.end_action));
  return (
    <div className="rounded-row bg-white p-4 sm:p-6">
      {top}
      <h3 className="text-[18px] font-semibold tracking-[-0.01em]">{chainName(chain)}</h3>
      <p className="text-[14px] text-slate">{chain.context ? `${chain.context} · ${meta}` : meta}</p>

      <div className="mt-4">
        <SequenceDetail
          key={chain.sequence_id}
          actions={chain.actions}
          title={copy.chainPitch}
          focusPlayerId={focusPlayerId}
        />
      </div>

      <ChainKey focusName={focusName} />

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
  );
}

/**
 * The marks of `SequenceDetail`, in its Pizarra colours:
 * the handoff `SequenceLegend` vocabulary, drawn as real marks.
 */
function ChainKey({ focusName }: { focusName?: string }) {
  const L = viz.legend;
  const sw = (child: ReactNode, w = 14) => (
    <svg viewBox={`0 0 ${w} 12`} width={w} height="12" aria-hidden="true" className="mr-1.5 inline-block shrink-0">
      {child}
    </svg>
  );
  const line = (dash?: string, color = "var(--color-slate)") =>
    sw(<line x1="1" y1="6" x2="21" y2="6" stroke={color} strokeWidth="1.6" strokeLinecap="round" strokeDasharray={dash} />, 22);
  const items: [ReactNode, string][] = [
    ...(focusName
      ? [[sw(<circle cx="6" cy="6" r="4.5" fill="var(--color-spot-line)" stroke="var(--color-spot-line)" />), focusName] as [ReactNode, string]]
      : []),
    [sw(<circle cx="6" cy="6" r="4.5" fill="none" stroke="var(--color-team)" strokeWidth="1.2" />), L.sequenceStart],
    [sw(<rect x="3" y="3" width="6" height="6" transform="rotate(45 6 6)" fill="var(--color-team)" />), L.sequenceRegain],
    [sw(<circle cx="6" cy="6" r="4.5" fill="var(--color-team)" />), L.finisher],
    [line(), L.pass],
    [line("0.1 3.5"), L.carry],
    [line("3 2.5"), L.clearance],
    [sw(<path d="M1 10 Q11 0 21 8" fill="none" stroke="var(--color-slate)" strokeWidth="1.6" strokeLinecap="round" />, 22), L.cross],
    [line(undefined, "var(--color-team)"), L.shot],
  ];
  return (
    <ul className="mt-3 flex flex-wrap gap-x-4 gap-y-1 text-[13px] text-slate">
      {items.map(([mark, text]) => (
        <li key={text} className="flex items-center">
          {mark}
          {text}
        </li>
      ))}
    </ul>
  );
}

export interface ListRow {
  key: string;
  seq: SequenceEntry;
}

export function SequenceList({
  rows,
  selectedKey,
  onSelect,
  metric,
  count,
  listRef,
}: {
  rows: ListRow[];
  selectedKey: string | null;
  onSelect: (key: string) => void;
  metric: SequenceMetric;
  /** The number beside the heading, when it is not `rows.length`. */
  count?: number;
  listRef?: Ref<HTMLUListElement>;
}) {
  const M = metricDefinitions[metric].abbr;
  return (
    <div className="flex min-h-0 flex-col rounded-row bg-white p-3 sm:p-4">
      <div className="flex items-baseline justify-between gap-2 px-2">
        <h3 className="text-[18px] font-semibold tracking-[-0.01em]">{copy.listHeading}</h3>
        <span className="text-[14px] text-slate">{viz.sequenceList.count(count ?? rows.length)}</span>
      </div>
      <ul ref={listRef} className="relative mt-2 flex max-h-[560px] flex-col gap-1 overflow-y-auto pr-1 lg:max-h-[640px]">
        {rows.map(({ key, seq }) => {
          const on = key === selectedKey;
          const clock = clockOf(seq.start_minute, seq.start_second);
          return (
            <li key={key}>
              <button
                type="button"
                aria-pressed={on}
                onClick={() => onSelect(key)}
                className={`grid w-full grid-cols-[minmax(0,1fr)_auto] items-baseline gap-x-3 rounded-[12px] px-3 py-2 text-left transition-colors ${
                  on ? "bg-spot" : "hover:bg-panel"
                }`}
              >
                <span className={`truncate text-[15px] font-semibold ${on ? "text-team" : ""}`}>{chainName(seq)}</span>
                <span className="text-[14px] whitespace-nowrap text-slate tabular-nums">
                  {seq.context ? `${seq.context} · ${clock}` : clock}
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
  );
}
