"use client";

/**
 * DemoSequences — /demo's chain and list: the two `SequenceParts` halves with
 * the smallest state that joins them (which chain is selected, by which
 * metric the list is sorted).
 *
 * Renders: the metric switch, then the selected chain (`ChainCard`) beside
 * the list (`SequenceList`), as on the match and player pages.
 * Props: `sequences`, the fixture's chains.
 * Data:  `lib/viz/fixtures.ts` -> `demoSequenceEntries` only.
 */

import { useMemo, useState } from "react";

import type { SequenceMetric } from "@/lib/labels";
import type { SequenceEntry } from "@/lib/data/sequences";

import { ChainCard, MetricSwitch, SequenceList } from "./SequenceParts";

export function DemoSequences({ sequences }: { sequences: SequenceEntry[] }) {
  const [metric, setMetric] = useState<SequenceMetric>("xt");
  const [selected, setSelected] = useState<string | null>(sequences[0]?.sequence_id ?? null);
  const rows = useMemo(
    () => [...sequences].sort((a, b) => b[metric] - a[metric]).map((seq) => ({ key: seq.sequence_id, seq })),
    [sequences, metric],
  );
  const chain = sequences.find((s) => s.sequence_id === selected) ?? sequences[0];
  if (!chain) return null;

  return (
    <div className="flex flex-col gap-4">
      <div className="flex justify-end px-2">
        <MetricSwitch value={metric} onChange={setMetric} />
      </div>
      <div className="grid gap-4 lg:grid-cols-[minmax(0,2fr)_minmax(0,1fr)]">
        <ChainCard chain={chain} />
        <SequenceList rows={rows} selectedKey={chain.sequence_id} onSelect={setSelected} metric={metric} />
      </div>
    </div>
  );
}
