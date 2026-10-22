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
 *
 * A chain opened from one of the maps (`SequenceLink`, 22 Oct 2026) remounts
 * the browser with that chain selected and scrolled into the list, and the
 * page scrolls to this block. Switching the metric drops it: the reader has
 * moved on, and the toggle's contract is "back to the first row".
 */

import { useEffect, useMemo, useRef, useState, type ReactNode } from "react";

import { SectionHead } from "@/components/shell/SectionHead";
import { match as copy, type SequenceMetric } from "@/lib/labels";

import { MetricToggle } from "@/components/viz/MetricToggle";
import { SequenceBrowser, type SequenceEntry } from "@/components/viz/SequenceBrowser";

import { useSequenceLink } from "./SequenceLink";

export function MatchSequences({
  sequences,
  legend,
}: {
  sequences: SequenceEntry[];
  legend: ReactNode;
}) {
  const [metric, setMetric] = useState<SequenceMetric>("xt");
  const { opened, clear } = useSequenceLink();
  const sectionRef = useRef<HTMLElement>(null);

  // Every new open, even of the chain already showing, brings the block up.
  const nonce = opened?.nonce;
  useEffect(() => {
    if (nonce !== undefined) {
      sectionRef.current?.scrollIntoView({ behavior: "smooth", block: "start" });
    }
  }, [nonce]);

  function chooseMetric(next: SequenceMetric) {
    clear();
    setMetric(next);
  }

  // The export already sorts by xT; VAEP is sorted here. `sort` mutates, so
  // copy first — the page's array is also what the xT view shows.
  const ordered = useMemo(
    () =>
      metric === "xt" ? sequences : [...sequences].sort((a, b) => b[metric] - a[metric]),
    [sequences, metric],
  );

  const title = copy.sequencesHeading(metric);
  const openedIndex = opened ? ordered.findIndex((s) => s.sequence_id === opened.sequence_id) : -1;

  return (
    <section ref={sectionRef} className="mt-8 scroll-mt-4">
      <SectionHead
        title={title}
        right={
          <div className="flex flex-wrap items-baseline gap-x-4 gap-y-1">
            <span className="font-mono text-[9px] tracking-[0.04em] text-faint">
              {copy.sequencesCaption(sequences.length, metric)}
            </span>
            <MetricToggle value={metric} onChange={chooseMetric} />
          </div>
        }
      />
      <div className="card mt-4">
        {ordered.length > 0 ? (
          <SequenceBrowser
            key={`${metric}:${opened?.nonce ?? 0}`}
            sequences={ordered}
            title={title}
            legend={legend}
            initialIndex={Math.max(openedIndex, 0)}
          />
        ) : (
          <p className="label">{copy.sequencesEmpty}</p>
        )}
      </div>
    </section>
  );
}
