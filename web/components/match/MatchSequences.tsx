"use client";

/**
 * MatchSequences — screen 02's sequence block, on the light-blue spot panel:
 * one chain drawn large, and every chain of the match listed beside it.
 *
 * Renders: the block heading with the xT / VAEP switch; on the left the
 * selected chain (`ChainCard`), on the right the list (`SequenceList`),
 * sorted by the chosen metric. Stacked on a phone, the chain first.
 * Props: `sequences` in FILE order (the export's, best xT first): a map
 *        row's `s` indexes this array.
 * Data:  `matches/{match_id}.json` -> `sequences[]` (md/WEB_DATA.md §7.3).
 *
 * The layout is the handoff's block 6 (the big pitch, the list on the right),
 * redrawn in Pizarra on 24 Oct 2026; the halves are shared with the player
 * page (`components/sequences/SequenceParts`). It is the page's one spot
 * panel because it is where the maps above lead: a chain opened from a map
 * mark (`SequenceLink`) is selected here, scrolled into the list, and the
 * page scrolls to this block. Switching the metric keeps the selected chain.
 */

import { useEffect, useMemo, useRef, useState } from "react";

import { matchExplorer as copy, metricDefinitions, type SequenceMetric } from "@/lib/labels";

import { ChainCard, MetricSwitch, SequenceList } from "@/components/sequences/SequenceParts";
import type { SequenceEntry } from "@/components/viz/SequenceBrowser";

import { useSequenceLink } from "./SequenceLink";

export function MatchSequences({ sequences }: { sequences: SequenceEntry[] }) {
  const { opened } = useSequenceLink();
  const [selected, setSelected] = useState<string | null>(sequences[0]?.sequence_id ?? null);
  const [metric, setMetric] = useState<SequenceMetric>("xt");
  const sectionRef = useRef<HTMLDivElement>(null);
  const listRef = useRef<HTMLUListElement>(null);

  // A chain opened from a map: select it and bring the block up. Every new
  // open, even of the chain already showing, scrolls (the `nonce`).
  const nonce = opened?.nonce;
  useEffect(() => {
    if (!opened) return;
    if (sequences.some((s) => s.sequence_id === opened.sequence_id)) setSelected(opened.sequence_id);
    sectionRef.current?.scrollIntoView({ behavior: "smooth", block: "start" });
    // Only a new open should move the page, not a new `sequences` array.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [nonce]);

  const rows = useMemo(() => {
    const all = sequences.map((seq) => ({ key: seq.sequence_id, seq }));
    return metric === "xt" ? all : [...all].sort((a, b) => b.seq[metric] - a.seq[metric]);
  }, [sequences, metric]);

  // Keep the selected row in sight inside the list (not the page).
  useEffect(() => {
    const list = listRef.current;
    const row = list?.querySelector<HTMLElement>("[aria-pressed='true']");
    if (list && row && (row.offsetTop < list.scrollTop || row.offsetTop > list.scrollTop + list.clientHeight - 48)) {
      list.scrollTop = row.offsetTop - 8;
    }
  }, [selected, metric]);

  const chain = sequences.find((s) => s.sequence_id === selected) ?? null;

  return (
    <div ref={sectionRef} className="flex scroll-mt-24 flex-col gap-4">
      <div className="flex flex-wrap items-end justify-between gap-x-6 gap-y-3 px-2 pt-2">
        <div>
          <h2 id="jugadas-titulo" className="text-[26px] leading-tight font-semibold tracking-[-0.02em]">
            {copy.sequencesHeading(metricDefinitions[metric].abbr)}
          </h2>
          <p className="mt-2 max-w-[70ch] text-[15px] text-spot-text">{copy.sequencesDek(sequences.length)}</p>
        </div>
        <MetricSwitch value={metric} onChange={setMetric} />
      </div>

      {chain ? (
        <div className="grid gap-4 lg:grid-cols-[minmax(0,2fr)_minmax(0,1fr)]">
          <ChainCard chain={chain} />
          <SequenceList rows={rows} selectedKey={selected} onSelect={setSelected} metric={metric} listRef={listRef} />
        </div>
      ) : (
        <p className="rounded-row bg-white p-5 text-[15px] text-slate">{copy.chainEmpty}</p>
      )}
    </div>
  );
}
