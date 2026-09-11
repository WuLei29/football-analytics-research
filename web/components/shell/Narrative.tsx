/**
 * Narrative — the bordered prose block that closes screens 01 and 02.
 *
 * Renders: an ink-bordered panel on `--paper` with an eyebrow, one or more
 * 20px/1.42 paragraphs, and an optional mono provenance line.
 * Props: `eyebrow`, `narrative` (paragraphs + optional provenance), and an
 * optional `action` node for the pill button the design puts in the match
 * version ("Open in sequence lab").
 * Data:  `web/content/narratives.ts` — authored, never exported
 *        (`md/WEB_DATA.md` §3.7).
 *
 * The typography here is the one place on the site where a reader is expected
 * to read a paragraph rather than scan a figure, which is why it is the only
 * block set at 20px.
 */

import type { ReactNode } from "react";

import type { Narrative as NarrativeContent } from "@/content/narratives";

interface NarrativeProps {
  eyebrow: string;
  narrative: NarrativeContent;
  action?: ReactNode;
}

export function Narrative({ eyebrow, narrative, action }: NarrativeProps) {
  return (
    <section className="rounded-card border border-ink bg-paper p-5 md:p-6">
      <p className="eyebrow">{eyebrow}</p>

      <div className="mt-3 flex flex-col gap-3">
        {narrative.paragraphs.map((paragraph, index) => (
          <p
            // The paragraphs of one authored block are stable text with no id
            // of their own; the index is the identity here.
            key={index}
            className="max-w-[78ch] font-sans text-[17px] leading-[1.45] md:text-narrative"
          >
            {paragraph}
          </p>
        ))}
      </div>

      {(narrative.provenance || action) && (
        <div className="mt-4 flex flex-wrap items-center justify-between gap-3">
          {narrative.provenance ? (
            <p className="font-mono text-[9px] tracking-[0.14em] uppercase text-faint">
              {narrative.provenance}
            </p>
          ) : (
            <span />
          )}
          {action}
        </div>
      )}
    </section>
  );
}
