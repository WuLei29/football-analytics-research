/**
 * PlayerPercentiles — screen 03 block 4: four cards of six metrics, each a
 * league percentile among the players of the same role.
 *
 * Renders: README-PLAYER §4 — the section head ("Percentil vs LaLiga ·
 * Laterales derechos"), its caption with the peer count and the minutes
 * gate, the POLIVALENTE chip when the primary position is under 60 % of
 * minutes, and `repeat(4, 1fr)` cards built from `PercentileCard` /
 * `PercentileRow` — the page-01 team profile's own construction.
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

import { SectionHead } from "../shell/SectionHead";
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
    <section className="mt-6">
      <SectionHead
        title={copy.percentileHeading(group)}
        rule="none"
        right={
          <div className="flex flex-wrap items-center gap-2">
            {isUtility && primaryPct !== null && (
              <span className="rounded-pill border border-line bg-paper px-2 py-0.5 font-mono text-[8.5px] tracking-[0.12em] text-mid">
                {copy.utilityChip(dec(primaryPct, 0), position.toUpperCase())}
              </span>
            )}
            <span className="font-mono text-[9px] tracking-[0.12em] uppercase text-faint">
              {copy.percentileCaption(percentiles.peer_n, group, num(percentiles.min_minutes))}
            </span>
          </div>
        }
      />

      {percentiles.eligible ? (
        <div className="mt-3 mb-5 grid gap-3.5 sm:grid-cols-2 xl:grid-cols-4">
          {percentiles.cards.map((card) => (
            <PercentileCard key={card.key} title={label(spineCardLabels, card.key)} unit={copy.percentileUnit}>
              {card.metrics.map((m) => (
                <PercentileRow
                  key={m.key}
                  label={`${label(spineMetricLabels, m.key)}${m.higher_is_better ? "" : ` ${copy.lowerIsBetter}`}`}
                  printed={printed(m)}
                  percentile={m.out_of !== undefined ? null : m.percentile}
                  tintNumber
                />
              ))}
            </PercentileCard>
          ))}
        </div>
      ) : (
        <p className="dek mt-3 mb-5">
          {copy.percentileIneligible(num(minutes), num(percentiles.min_minutes))}
        </p>
      )}
    </section>
  );
}
