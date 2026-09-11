/**
 * KpiStrip — the five cards at the top of screen 01.
 *
 * Renders: `repeat(5, 1fr)` cards, each a mono label with a rank chip, a 44px
 * Outfit value with its unit suffix, and an italic note.
 * Props: `kpis` — `overview.kpis`, in the order the export wrote them.
 * Data:  `overview.json` -> `kpis[]` (md/WEB_DATA.md §6), whose source is
 *        `gold.team_season_stats` plus a rank computed over the 20 clubs.
 *
 * Two things the data has already settled, and this component must not
 * second-guess:
 *
 *   - **Rank direction.** PPDA ranks ascending (fewer opponent passes per
 *     defensive action is more pressing) and the export has already applied
 *     that. A rank here is always "1 is best", so the chip logic is one rule.
 *   - **Scaling.** `possession_pct` arrives as 41.7, not 0.417 (§5.1).
 *
 * The chip turns green in the top six, which is the design's rule and reads
 * as "European places" on a 20-team table without saying so.
 */

import { dec, num, orEmpty } from "@/lib/format";
import { kpiLabels, label, season as copy } from "@/lib/labels";
import type { OverviewKpi } from "@/lib/data/types";

/** Where the design switches the chip from grey to green. */
const TOP_TIER = 6;

/**
 * Secondary values that are a percentage. The file carries `25.6` for the
 * set-piece share (§6) with no unit of its own, so the sign is added here;
 * without it the note reads "25.6 de los goles", which is a count.
 */
const SHARE_SECONDARIES = new Set(["set_piece_goal_share"]);

export function KpiStrip({ kpis }: { kpis: OverviewKpi[] }) {
  return (
    <div className="mt-4 grid gap-3.5 sm:grid-cols-2 lg:grid-cols-5">
      {kpis.map((kpi) => (
        <article key={kpi.key} className="card flex flex-col gap-3">
          <div className="flex items-start justify-between gap-2">
            <span className="label">{label(kpiLabels, kpi.key)}</span>
            <span
              className={[
                "shrink-0 rounded-pill px-1.5 py-0.5 font-mono text-[9px] tracking-[0.08em]",
                kpi.rank !== null && kpi.rank <= TOP_TIER
                  ? "bg-pos text-on-dark"
                  : "bg-track text-mid",
              ].join(" ")}
            >
              {kpi.rank === null
                ? copy.rankEmpty
                : copy.rank(kpi.rank, kpi.peer_n)}
            </span>
          </div>

          <p className="font-display text-kpi font-bold">
            {/* Points and set-piece goals are counts; everything else has a
                decimal. `decimals` is not in the file, so the integer test is
                the rule — and it is the same one the export applied. */}
            {orEmpty(kpi.value, (v) => (Number.isInteger(v) ? num(v) : dec(v, 1)))}
            {kpi.unit ? (
              <span className="ml-1 align-baseline text-[16px] font-medium text-faint">
                {kpi.unit}
              </span>
            ) : null}
          </p>

          <p className="dek">
            {kpi.secondary
              ? `${dec(kpi.secondary.value, 1)}${SHARE_SECONDARIES.has(kpi.secondary.key) ? " %" : ""} ${label(kpiLabels, kpi.secondary.key)}`
              : /* No note in the file: keep the card height by printing the
                   peer count instead of an empty line. */
                `${kpi.peer_n} equipos`}
          </p>
        </article>
      ))}
    </div>
  );
}
