/**
 * SectionHead — the heading of a section or of a card: a display title with a
 * mono caption beside it.
 *
 * Renders: a baseline-aligned row. `rule="line"` (the default) underlines it
 * with the hairline `--line`; `rule="ink"` uses the hard ink rule the design
 * puts under a card head; `rule="none"` leaves it bare.
 * Props: `title`, optional `caption`, optional `right` node (a link, a chip),
 * and `rule`.
 * Data:  none.
 *
 * This is the third copy of a shape that was local to `/` and `/demo`
 * (WEB_PLAN.md §3.2: duplicate twice, extract on the third), and screens 01
 * and 02 have eleven of them between them.
 */

import type { ReactNode } from "react";

const RULES = {
  line: "border-b border-line pb-2",
  ink: "border-b border-ink pb-2",
  none: "",
} as const;

interface SectionHeadProps {
  title: string;
  caption?: string;
  right?: ReactNode;
  rule?: keyof typeof RULES;
  /** `h2` on a page section, `h3` inside a card. */
  as?: "h2" | "h3";
}

export function SectionHead({
  title,
  caption,
  right,
  rule = "line",
  as = "h2",
}: SectionHeadProps) {
  const Heading = as;

  return (
    <div
      className={`flex flex-wrap items-baseline justify-between gap-x-6 gap-y-1 ${RULES[rule]}`}
    >
      <Heading
        className={
          as === "h2"
            ? "font-display text-h2card font-bold"
            : "font-display text-[15px] font-bold"
        }
      >
        {title}
      </Heading>

      {right ??
        (caption ? (
          /* The caption is mono and lower case, not a label: it is a sentence
             about the block, and 0.16em tracking on a full sentence is
             unreadable. */
          <span className="font-mono text-[9px] tracking-[0.04em] text-faint">
            {caption}
          </span>
        ) : null)}
    </div>
  );
}
