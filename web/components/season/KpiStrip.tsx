/**
 * KpiStrip — the five headline figures of screen 01, as raised cards.
 *
 * Renders: five white cards (2 / 3 / 5 across), each with the figure's name
 * and a rank chip on top, the large light figure, and the secondary value.
 * The chip is green in the top six, red in the bottom six, neutral between.
 * Props: `kpis` — `overview.kpis`, in the order the export wrote them.
 * Data:  `overview.json` -> `kpis[]` (md/WEB_DATA.md §6), whose source is
 *        `gold.team_season_stats` plus a rank computed over the 20 clubs.
 *
 * Two things the data has already settled, and this component must not
 * second-guess:
 *
 *   - **Rank direction.** The export ranks each metric in its own direction
 *     (`higher_is_better` in `src/export/config.py`). A rank here is always
 *     "1 is best", so the chip logic is one rule.
 *   - **Scaling.** `possession_pct` arrives as 41.7, not 0.417 (§5.1).
 *
 * Revised 24 Oct 2026 at the author's request: the figures were one row on a
 * flat panel and did not stand out. They are now separate raised cards
 * (white, hairline ring, the `raised` shadow: one of the two raised things in
 * the world, with the nav), and the handoff's green rank chip is back, with a
 * red counterpart for the bottom six (DESIGN.md, verdict colours). Figures
 * stay at weight 300 ("The Light Figure Rule").
 */

import { dec, num, orEmpty, signed } from "@/lib/format";
import { kpiLabels, label, season as copy } from "@/lib/labels";
import type { OverviewKpi } from "@/lib/data/types";

/** Green from the top down to here; red from `peer_n - TIER + 1` down. */
const TIER = 6;

/**
 * Secondary values that are a percentage. The file carries `25.6` for the
 * set-piece share (§6) with no unit of its own, so the sign is added here;
 * without it the note reads "25.6 de los goles", which is a count.
 */
const SHARE_SECONDARIES = new Set(["set_piece_goal_share"]);

/** Figures that carry a sign: a difference reads "+0.4" or "−0.1", never bare. */
const SIGNED = new Set(["xg_difference"]);

function chipClass(rank: number | null, peers: number): string {
  if (rank === null) return "bg-panel text-slate";
  if (rank <= TIER) return "bg-good/12 text-good";
  if (rank > peers - TIER) return "bg-bad/10 text-bad";
  return "bg-panel text-slate";
}

export function KpiStrip({ kpis }: { kpis: OverviewKpi[] }) {
  return (
    <dl className="grid grid-cols-2 gap-3 sm:grid-cols-3 sm:gap-4 lg:grid-cols-5">
      {kpis.map((kpi) => (
        <div
          key={kpi.key}
          className="flex flex-col rounded-panel bg-white p-5 shadow-raised ring-1 ring-ink/[0.06] odd:last:col-span-2 sm:p-6 sm:odd:last:col-span-1"
        >
          {/* DOM order is name, rank, figure, note (a <dl> group is dt then
              dd); `order` puts the figure under the name and chip. */}
          <dt className="text-[14px] leading-snug font-medium text-ink">{label(kpiLabels, kpi.key)}</dt>
          <dd className="order-first mb-3 self-start">
            <span
              className={`inline-block rounded-full px-2.5 py-1 text-[12px] leading-none font-semibold whitespace-nowrap ${chipClass(kpi.rank, kpi.peer_n)}`}
            >
              {kpi.rank === null ? copy.rankEmpty : copy.rank(kpi.rank, kpi.peer_n)}
            </span>
          </dd>
          <dd className="mt-4 text-[clamp(40px,3.6vw,56px)] leading-[0.9] font-light tracking-[-0.04em]">
            {/* Points, goals and set-piece goals are counts; everything else
                has a decimal. `decimals` is not in the file, so the integer
                test is the rule — the same one the export applied. */}
            {orEmpty(kpi.value, (v) =>
              SIGNED.has(kpi.key) ? signed(v, 1) : Number.isInteger(v) ? num(v) : dec(v, 1),
            )}
            {/* Only "%" is printed: "pts" and "xG" repeat what the name says. */}
            {kpi.unit === "%" && <span className="ml-1 text-[0.5em] tracking-normal">{kpi.unit}</span>}
          </dd>
          {kpi.secondary && (
            <dd className="mt-auto pt-3 text-[14px] leading-snug text-slate">
              {dec(kpi.secondary.value, 1)}
              {SHARE_SECONDARIES.has(kpi.secondary.key) ? " %" : ""} {label(kpiLabels, kpi.secondary.key)}
            </dd>
          )}
        </div>
      ))}
    </dl>
  );
}
