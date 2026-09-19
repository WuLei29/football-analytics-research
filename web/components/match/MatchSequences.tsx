"use client";

/**
 * MatchSequences — screen 02, block 6: the section head with the xT / VAEP
 * toggle, and the sequence browser sorted by whichever is chosen.
 *
 * Renders: `SectionHead` whose title and caption follow the metric ("Jugadas
 * por xT (Amenaza esperada)" / "Jugadas por VAEP (Valor de la acción)"), the
 * `MetricToggle` in its right slot, and `SequenceBrowser` in a card.
 * Props: `sequences` (as the export ordered them: best xT first) and the
 * marks `legend`, built by the page so this file holds no Spanish of its own.
 * Data:  `matches/{match_id}.json` -> `sequences[]` (md/WEB_DATA.md §7.3).
 *
 * WHY THIS EXISTS. The page is a server component and the title has to change
 * with the toggle, so the state that picks the metric must live above both
 * the head and the browser. This is the smallest client boundary that holds
 * both; the page stays server-rendered around it.
 *
 * The browser is keyed by the metric, so switching remounts it and the
 * selection goes back to the first row. Its selection is an index into the
 * list, and after a re-sort the same index would be a different play — a
 * pitch showing one sequence while the list highlights another.
 */

import { useMemo, useState, type ReactNode } from "react";

import { SectionHead } from "@/components/shell/SectionHead";
import { match as copy, type SequenceMetric } from "@/lib/labels";

import { MetricToggle } from "@/components/viz/MetricToggle";
import { SequenceBrowser, type SequenceEntry } from "@/components/viz/SequenceBrowser";

export function MatchSequences({
  sequences,
  legend,
}: {
  sequences: SequenceEntry[];
  legend: ReactNode;
}) {
  const [metric, setMetric] = useState<SequenceMetric>("xt");

  // The export already sorts by xT; VAEP is sorted here. `sort` mutates, so
  // copy first — the page's array is also what the xT view shows.
  const ordered = useMemo(
    () =>
      metric === "xt" ? sequences : [...sequences].sort((a, b) => b[metric] - a[metric]),
    [sequences, metric],
  );

  const title = copy.sequencesHeading(metric);

  return (
    <section className="mt-8">
      <SectionHead
        title={title}
        right={
          <div className="flex flex-wrap items-baseline gap-x-4 gap-y-1">
            <span className="font-mono text-[9px] tracking-[0.04em] text-faint">
              {copy.sequencesCaption(sequences.length, metric)}
            </span>
            <MetricToggle value={metric} onChange={setMetric} />
          </div>
        }
      />
      <div className="card mt-4">
        {ordered.length > 0 ? (
          <SequenceBrowser key={metric} sequences={ordered} title={title} legend={legend} />
        ) : (
          <p className="label">{copy.sequencesEmpty}</p>
        )}
      </div>
    </section>
  );
}
