/**
 * TeamProfile — the four panels of screen 01's profile: Defensa, Posesión,
 * Progresión, Finalización, six metrics each. Pizarra world.
 *
 * Renders: four panels (one column on a phone, two at sm, four at xl). Each
 * is an 18px title and six rows: a label and a value on the first line, then
 * a round-capped percentile track and the percentile on the second.
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
 *
 * Changed from the handoff (24 Oct 2026, DESIGN.md "Chart colour mapping"):
 * the tiers keep the handoff's green / amber / red (top ≥ 70, bottom < 45;
 * the author found them clearer than blue / slate / red, 24 Oct 2026), on a
 * chalk track drawn as a marker stroke;
 * no ink rule under the title and no "VS LA LIGA" caption, which the section
 * line already says. The thresholds are unchanged.
 *
 * `PercentileCard` and `PercentileRow` below are the same drawing for screen
 * 03 (`components/player/PlayerPercentiles.tsx`), moved to Pizarra with it
 * on 24 Oct 2026 (README-PLAYER §4: "same construction as the Team profile").
 */

import type { ReactNode } from "react";

import { dec } from "@/lib/format";
import { label, metricLabels, profileCardLabels, season as copy } from "@/lib/labels";
import type { ProfileCard, ProfileMetric, ProfileValue } from "@/lib/data/types";

/**
 * Three-step fill: top third green, middle gold, bottom red. The design's
 * middle step was blue, which on a page where blue already means "Espanyol"
 * read as a second accent rather than as "average"; gold is the traffic-light
 * middle and the only warm token the palette has.
 */
/** Pizarra tiers: the handoff's thresholds and its green / amber / red. */
function tierColor(p: number): string {
  if (p >= 70) return "var(--color-good)";
  if (p >= 45) return "var(--color-fair-fill)";
  return "var(--color-bad)";
}

/** The same tier as text: the amber fill is too light to read (`fair`). */
function tierText(p: number): string {
  return p >= 45 && p < 70 ? "var(--color-fair)" : tierColor(p);
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
    <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
      {CARDS.map((card) => (
        <article key={card} className="rounded-panel bg-panel p-6">
          <h3 className="text-[18px] font-semibold tracking-[-0.01em]">{profileCardLabels[card]}</h3>
          <dl className="mt-5 flex flex-col gap-4">
            {metrics
              .filter((m) => m.card === card)
              .map((metric) => {
                const value = values[metric.key] ?? { v: null, p: null };
                return (
                  <ProfileRow
                    key={metric.key}
                    label={label(metricLabels, metric.key)}
                    printed={
                      value.v === null
                        ? copy.percentileEmpty
                        : `${dec(value.v, metric.decimals as 0 | 1 | 2 | 3)}${metric.scale === "pct" ? " %" : ""}`
                    }
                    percentile={value.p}
                  />
                );
              })}
          </dl>
        </article>
      ))}
    </div>
  );
}

/** One profile row: the shared `PercentileRow`. */
function ProfileRow({ label: text, printed, percentile }: { label: string; printed: string; percentile: number | null }) {
  return <PercentileRow label={text} printed={printed} percentile={percentile} />;
}

/**
 * One card of percentile rows: a panel, the 18px title and, on the player
 * page, the unit in slate beside it.
 */
export function PercentileCard({
  title,
  unit,
  children,
}: {
  title: string;
  unit?: string;
  children: ReactNode;
}) {
  return (
    <article className="rounded-panel bg-panel p-6">
      <div className="flex items-baseline justify-between gap-2">
        <h3 className="text-[18px] font-semibold tracking-[-0.01em]">{title}</h3>
        {unit && <span className="text-[13px] text-slate">{unit}</span>}
      </div>
      <dl className="mt-5 flex flex-col gap-4">{children}</dl>
    </article>
  );
}

/**
 * A label and value, then the chalk track filled to the percentile in its
 * tier (green ≥ 70, amber, red < 45), the number in the tier's text colour.
 * A null percentile keeps the track empty and prints "—": no confident bar
 * that describes nothing (a metric not ranked for this player).
 */
export function PercentileRow({ label: text, printed, percentile }: { label: string; printed: string; percentile: number | null }) {
  return (
    <div>
      <div className="flex items-baseline justify-between gap-3 text-[14px]">
        <dt className="text-slate">{text}</dt>
        <dd className="shrink-0 font-medium text-ink tabular-nums">{printed}</dd>
      </div>
      <div className="mt-1.5 flex items-center gap-3">
        <svg viewBox="0 0 100 6" preserveAspectRatio="none" className="h-1.5 flex-1" aria-hidden="true">
          <line x1="3" y1="3" x2="97" y2="3" stroke="var(--color-chalk)" strokeWidth="6" strokeLinecap="round" vectorEffect="non-scaling-stroke" />
          {percentile !== null && percentile > 0 && (
            <line
              x1="3"
              y1="3"
              x2={3 + (94 * percentile) / 100}
              y2="3"
              stroke={tierColor(percentile)}
              strokeWidth="6"
              strokeLinecap="round"
              vectorEffect="non-scaling-stroke"
            />
          )}
        </svg>
        <span
          className="w-[3ch] text-right text-[13px] tabular-nums"
          style={{ color: percentile === null ? "var(--color-slate)" : tierText(percentile) }}
        >
          {percentile === null ? copy.percentileEmpty : percentile}
        </span>
      </div>
    </div>
  );
}
