/**
 * PageHeader — the top of a content page: eyebrow, H1 with its accent half,
 * and the italic dek to its right.
 *
 * Renders: a flex row over a hard ink rule (`border-bottom: 1px solid --ink`,
 * 12px padding), exactly the "Page header" block of screens 01 and 02.
 * Props: `eyebrow`, `title` ({ plain, accent }), `dek`, and an optional
 * `meta` node for the right-hand column when a page needs lines of mono
 * rather than a sentence (screen 02 puts competition, venue and xG there).
 * Data:  none — every string arrives from `lib/labels.ts` via the page.
 *
 * The crest tile of the design lives in the masthead hero, not here: this
 * header sits under it on every page, and repeating the tile twelve pixels
 * below itself looked like a mistake.
 */

import type { ReactNode } from "react";

interface PageHeaderProps {
  eyebrow: string;
  title: { plain: string; accent: string };
  /** The italic sentence on the right. Omit when `meta` is used instead. */
  dek?: string;
  /** Right-hand column for mono meta lines (screen 02). */
  meta?: ReactNode;
}

export function PageHeader({ eyebrow, title, dek, meta }: PageHeaderProps) {
  return (
    <header className="mt-8 flex flex-wrap items-end justify-between gap-x-8 gap-y-3 border-b border-ink pb-3">
      <div>
        <p className="eyebrow">{eyebrow}</p>
        <h1 className="mt-2 font-display text-h1 font-bold">
          {title.plain}
          <span className="text-blue">{title.accent}</span>
        </h1>
      </div>

      {meta ?? (dek ? <p className="dek max-w-[400px]">{dek}</p> : null)}
    </header>
  );
}
