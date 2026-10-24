/**
 * BoardFooter — provenance and credits, in the Pizarra world.
 *
 * Renders: the data source, the "metrics are the author's" note, the
 * unaffiliated line, the stadium photo's CC BY credit, the author with links,
 * and when the data was generated.
 * Props: `source` and `generatedAt` from the manifest envelope; `photoCredit`
 *        false on a page that does not show the stadium photo (every page but
 *        the landing, for now: the credit belongs where the photo is).
 * Data:  `manifest.json` (md/WEB_DATA.md §4).
 *
 * Same commitments as the handoff `Footer` (WEB_PLAN.md §6: name the source,
 * say every metric is derived), plus two that the new world added: the photo
 * credit, which CC BY requires wherever the photo appears, and the author's
 * name on every page (PRODUCT.md, "Brand Commitments").
 */

import { brand } from "@/lib/brand";
import type { Manifest } from "@/lib/data/types";
import { buildStamp } from "@/lib/format";
import { landing, provenance } from "@/lib/labels";

export function BoardFooter({
  source,
  generatedAt,
  photoCredit = true,
}: {
  source: Manifest["source"];
  generatedAt: string;
  photoCredit?: boolean;
}) {
  const note = provenance.metricsNote[source.metrics_note] ?? `⟨${source.metrics_note}⟩`;
  const link = "font-medium text-ink underline decoration-chalk hover:decoration-team";

  return (
    <footer className="mt-16 grid gap-6 pb-10 text-[14px] text-slate md:grid-cols-[1.55fr_1fr]">
      <div className="max-w-[70ch] space-y-1.5">
        <p className="font-medium text-ink">{provenance.sourceLine(source.provider)}</p>
        <p>{note}</p>
        <p>{provenance.unaffiliated}</p>
        {photoCredit && (
          <p>
            <a href={brand.stadiumPhoto.creditHref} className={link} target="_blank" rel="noopener">
              {brand.stadiumPhoto.credit}
            </a>
          </p>
        )}
      </div>
      <div className="space-y-1.5 md:text-right">
        <p className="font-medium text-ink">{brand.author.name}</p>
        <p>
          <a href={brand.author.linkedin} className={link} target="_blank" rel="me noopener">
            {landing.linkedin}
          </a>
          {" · "}
          <a href={brand.author.github} className={link} target="_blank" rel="me noopener">
            {landing.github}
          </a>
        </p>
        <p>{provenance.buildLine(buildStamp(generatedAt))}</p>
      </div>
    </footer>
  );
}
