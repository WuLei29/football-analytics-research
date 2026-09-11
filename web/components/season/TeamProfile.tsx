/**
 * TeamProfile — the four cards of screen 01, block 5: Defensa, Posesión,
 * Progresión, Finalización, six metrics each.
 *
 * Renders: `repeat(4, 1fr)` cards. Each card head is a 15px title with a mono
 * "VS LA LIGA" caption over a hard ink rule; each row is a label and a value
 * on the first line, then a 5px percentile track and a two-digit percentile on
 * the second.
 * Props: `metrics` (the 24 descriptors, in the design order) and `values` (one
 * club's `{v, p}` per key).
 * Data:  `league/{season}/table.json` -> `profile` (md/WEB_DATA.md §5.1),
 *        computed by the export from `gold.team_season_stats`.
 *
 * Three rules the data carries and this component only renders:
 *
 *   - **The values are per match** (`kind: "count"` was divided by matches
 *     played), so a club with 3 matches compares with one that has 38 on the
 *     same footing (WEB_PLAN.md §9.5). The caption above the cards says so.
 *   - **The shares are already scaled**: `possession_pct` is 41.7 (§5.1).
 *     `scale: "pct"` is what tells this component to print a % sign.
 *   - **The percentile always reads "higher is better"**, including on fouls,
 *     where the export inverted it. There is no `invert` branch here.
 *
 * A null percentile is a real state, not a bug: the export drops it when the
 * 20 clubs show no variance on a metric, because a full green bar on a column
 * where everyone scores the same is a lie (§5.1).
 */

import { dec } from "@/lib/format";
import { label, metricLabels, profileCardLabels, season as copy } from "@/lib/labels";
import type { ProfileCard, ProfileMetric, ProfileValue } from "@/lib/data/types";

/**
 * Three-step fill: top third green, middle gold, bottom red. The design's
 * middle step was blue, which on a page where blue already means "Espanyol"
 * read as a second accent rather than as "average"; gold is the traffic-light
 * middle and the only warm token the palette has.
 */
function percentileColor(p: number): string {
  if (p >= 70) return "var(--color-pos)";
  if (p >= 45) return "var(--color-gold)";
  return "var(--color-neg)";
}

const CARDS: ProfileCard[] = ["defensive", "possession", "progression", "finishing"];

export function TeamProfile({
  metrics,
  values,
}: {
  metrics: ProfileMetric[];
  values: Record<string, ProfileValue>;
}) {
  return (
    <div className="mt-4 grid gap-3.5 sm:grid-cols-2 xl:grid-cols-4">
      {CARDS.map((card) => (
        <article key={card} className="card">
          <div className="flex items-baseline justify-between gap-2 border-b border-ink pb-2">
            <h3 className="font-display text-[15px] font-bold">
              {profileCardLabels[card]}
            </h3>
            <span className="font-mono text-[8.5px] tracking-[0.16em] uppercase text-faint">
              {copy.profileVs}
            </span>
          </div>

          <dl className="mt-3 flex flex-col gap-2.5">
            {metrics
              .filter((m) => m.card === card)
              .map((metric) => (
                <Row
                  key={metric.key}
                  metric={metric}
                  value={values[metric.key] ?? { v: null, p: null }}
                />
              ))}
          </dl>
        </article>
      ))}
    </div>
  );
}

function Row({ metric, value }: { metric: ProfileMetric; value: ProfileValue }) {
  const printed =
    value.v === null
      ? copy.percentileEmpty
      : `${dec(value.v, metric.decimals as 0 | 1 | 2 | 3)}${metric.scale === "pct" ? " %" : ""}`;

  return (
    <div>
      <div className="flex items-baseline justify-between gap-3">
        <dt className="font-sans text-[12px] font-medium text-mid">
          {label(metricLabels, metric.key)}
        </dt>
        <dd className="font-mono text-[12px] font-medium text-ink">{printed}</dd>
      </div>

      <div className="mt-1 flex items-center gap-2">
        <div className="h-[5px] flex-1 rounded-[3px] bg-paper">
          {value.p !== null && (
            <div
              className="h-full rounded-[3px]"
              style={{
                width: `${value.p}%`,
                background: percentileColor(value.p),
              }}
            />
          )}
        </div>
        <span className="w-[18px] text-right font-mono text-[9px] text-faint">
          {value.p === null ? copy.percentileEmpty : value.p}
        </span>
      </div>
    </div>
  );
}
