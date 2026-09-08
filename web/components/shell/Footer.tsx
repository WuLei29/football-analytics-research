/**
 * Footer — provenance. Source credit, the "derived by the author" note, the
 * unaffiliated disclaimer, and when the data was last generated.
 *
 * Renders: three lines of mono/italic meta above a hard ink rule.
 * Props: `source` and `generated_at`, straight from the manifest envelope.
 * Data:  `manifest.json` (md/WEB_DATA.md §4).
 *
 * This is not decoration. WEB_PLAN.md §6 commits the site to naming its data
 * source and stating that every metric is the author's own derivation; that
 * commitment is what keeps the published shape defensible. The methodology
 * page expands it — this is the version that appears on every page.
 */

import { buildStamp } from "@/lib/format";
import { provenance } from "@/lib/labels";
import type { Manifest } from "@/lib/data/types";

export function Footer({
  source,
  generatedAt,
}: {
  source: Manifest["source"];
  generatedAt: string;
}) {
  const note =
    provenance.metricsNote[source.metrics_note] ??
    `⟨${source.metrics_note}⟩`;

  return (
    <footer className="mt-14 border-t border-ink pt-4">
      <p className="label">{provenance.sourceLine(source.provider)}</p>
      <p className="dek mt-2 max-w-[70ch]">{note}</p>
      <p className="dek mt-1 max-w-[70ch]">{provenance.unaffiliated}</p>
      <p className="mt-3 font-mono text-[9px] tracking-[0.16em] uppercase text-faint">
        {provenance.buildLine(buildStamp(generatedAt))}
      </p>
    </footer>
  );
}
