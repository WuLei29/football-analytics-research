/**
 * PlayerPercentiles — screen 03 block 4: four cards of six metrics, each a
 * league percentile among the players of the same role.
 *
 * Renders: README-PLAYER §4 — the section head ("Percentil vs LaLiga ·
 * Laterales derechos"), its caption with the peer count and the minutes
 * gate, the "polivalente" pill when the primary position is under 60 % of
 * minutes, and four panels built from `PercentileCard` / `PercentileRow`,
 * the page-01 team profile's own construction (Pizarra, 24 Oct 2026).
 * Props: `percentiles`, `minutes`, `position` (Spanish, for the chip).
 * Data:  `players/{player_id}.json` -> `percentiles` (WEB_DATA §3.6, §9),
 *        ranked by `src/export/player_percentiles.py` from
 *        `gold.player_season_stats` with `PLAYER_SPINE` and
 *        `gold.percentile_metrics`.
 *
 * Three states the data carries and this only renders: a row with
 * `percentile: null` (not ranked for the role, or under its attempt floor)
 * shows its value over an empty track; a ↓ row's percentile is already
 * flipped; and a player under the gate gets the "todavía pocos minutos"
 * sentence instead of bars that would rank him on 40 minutes.
 */

import { dec, EMPTY, num } from "@/lib/format";
import {
  label,
  peerGroupLabels,
  player as copy,
  spineCardLabels,
  spineMetricLabels,
} from "@/lib/labels";
import type { PercentileMetric, PlayerPercentiles as Percentiles } from "@/lib/data/types";

import { PercentileCard, PercentileRow } from "../season/TeamProfile";

function printed(m: PercentileMetric): string {
  if (m.value === null) return EMPTY;
  if (m.out_of !== undefined) return `${num(m.value)}/${m.out_of === null ? EMPTY : num(m.out_of)}`;
  return `${dec(m.value, m.decimals)}${m.scale === "pct" ? " %" : ""}`;
}

export function PlayerPercentiles({
  percentiles,
  minutes,
  isUtility,
  primaryPct,
  position,
}: {
  percentiles: Percentiles;
  minutes: number;
  isUtility: boolean;
  primaryPct: number | null;
  position: string;
}) {
  const group = label(peerGroupLabels, percentiles.peer_group);

  return (
    <section aria-labelledby="percentiles" className="mt-10 lg:mt-14">
      <div className="flex flex-wrap items-baseline gap-x-4 gap-y-2">
        <h2 id="percentiles" className="text-[22px] font-semibold tracking-[-0.02em]">
          {copy.percentileHeading(group)}
        </h2>
        {isUtility && primaryPct !== null && (
          <span className="rounded-full bg-panel-deep px-3 py-1 text-[13px] font-semibold">
            {copy.utilityChip(dec(primaryPct, 0), position.toLowerCase())}
          </span>
        )}
      </div>
      <p className="mt-1 max-w-[70ch] text-[15px] text-slate">
        {copy.percentileCaption(percentiles.peer_n, group, num(percentiles.min_minutes))}
      </p>

      {percentiles.eligible ? (
        <div className="mt-5 grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
          {percentiles.cards.map((card) => (
            <PercentileCard key={card.key} title={label(spineCardLabels, card.key)}>
              {card.metrics.map((m) => (
                <PercentileRow
                  key={m.key}
                  label={`${label(spineMetricLabels, m.key)}${m.higher_is_better ? "" : ` ${copy.lowerIsBetter}`}`}
                  printed={printed(m)}
                  percentile={m.out_of !== undefined ? null : m.percentile}
                />
              ))}
            </PercentileCard>
          ))}
        </div>
      ) : (
        <p className="mt-5 rounded-panel bg-panel p-6 text-[15px] text-slate">
          {copy.percentileIneligible(num(minutes), num(percentiles.min_minutes))}
        </p>
      )}
    </section>
  );
}
