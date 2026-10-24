"use client";

/**
 * SeasonTrend — screen 01's trend chart: every match's difference as a bar,
 * the rolling mean as a line, and a readout of the match under the cursor.
 *
 * Renders: an xG / xT switch (two pills), then a chart with one bar per match
 * (xG for − xG against, or xT; green above zero, red below), the club's
 * rolling mean drawn over it as a team-blue round-capped line ending in a
 * dot, a tick ladder and a matchday strip in HTML at a real 12 px, and a
 * readout card naming the selected match: opponent, venue, score, the two
 * values, the net and the rolling mean at that point.
 * Props: `rows` — one per match in season order, already joined by the page
 *        (match fields + the rolling value at that matchday, null before the
 *        window fills); `window` — the rolling window (5).
 * Data:  `overview.json` -> `matches[]` and `rolling.points` (md/WEB_DATA.md
 *        §6), from `gold.team_match_stats`. Nothing is computed here but the
 *        subtraction for/against, which is the definition of the bar.
 *
 * Interaction: hover or tap a match column to read it; or Tab to the chart
 * and use ← → (Home / End) to walk the season. The latest match is selected
 * by default, so the readout is never empty and a phone needs no hover. The
 * readout is `aria-live`, so a screen reader hears each match as it moves.
 *
 * Replaces the two `RollingArea` charts the page had (24 Oct 2026, at the
 * author's request: they read as too simple to stop at). The rolling line is
 * the same series; the bars add the match-by-match values the mean hides, and
 * the readout makes every bar answerable. The plot stretches to a fixed CSS
 * height with non-scaling strokes, so it is tall enough on a phone.
 */

import { useState, type KeyboardEvent } from "react";

import { dec, signed } from "@/lib/format";
import { matchList, season as copy } from "@/lib/labels";
import { symmetricTicks } from "@/lib/viz/scale";
import type { OverviewMatch } from "@/lib/data/types";

export interface TrendRow {
  matchday: number;
  opponent: string;
  abbr: string;
  isHome: boolean;
  goalsFor: number;
  goalsAgainst: number;
  result: OverviewMatch["result"];
  xgFor: number;
  xgAgainst: number;
  xtFor: number;
  xtAgainst: number;
  rollXg: number | null;
  rollXt: number | null;
}

type Metric = "xg" | "xt";

const W = 600;
const H = 220;
const PAD = 22;

export function SeasonTrend({ rows, window }: { rows: TrendRow[]; window: number }) {
  const [metric, setMetric] = useState<Metric>("xg");
  const [selected, setSelected] = useState(rows.length - 1);

  if (rows.length === 0) return <p className="text-[15px] text-slate">{copy.formEmpty}</p>;

  const net = (r: TrendRow) => (metric === "xg" ? r.xgFor - r.xgAgainst : r.xtFor - r.xtAgainst);
  const roll = (r: TrendRow) => (metric === "xg" ? r.rollXg : r.rollXt);

  const peak = Math.max(...rows.map((r) => Math.max(Math.abs(net(r)), Math.abs(roll(r) ?? 0))));
  const { ticks, max, decimals } = symmetricTicks(peak);
  const zeroY = H / 2;
  const yAt = (v: number) => zeroY - (v / max) * (zeroY - PAD);
  const step = W / rows.length;
  const xAt = (i: number) => step * (i + 0.5);
  const barW = Math.min(step * 0.56, 40);

  const line = rows
    .map((r, i) => ({ r: roll(r), i }))
    .filter((p): p is { r: number; i: number } => p.r !== null)
    .map((p) => ({ x: xAt(p.i), y: yAt(p.r) }));
  const linePath = line.map((p, k) => `${k === 0 ? "M" : "L"}${p.x.toFixed(2)} ${p.y.toFixed(2)}`).join(" ");
  const last = line.at(-1);

  const labelEvery = rows.length > 12 ? 5 : 1;
  const pct = (v: number, of: number) => `${(v / of) * 100}%`;
  const sel = rows[Math.min(selected, rows.length - 1)];
  const selNet = net(sel);
  const selRoll = roll(sel);
  const metricName = metric === "xg" ? copy.trendXg : copy.trendXt;

  const onKey = (e: KeyboardEvent) => {
    const next =
      e.key === "ArrowLeft" ? selected - 1 : e.key === "ArrowRight" ? selected + 1 : e.key === "Home" ? 0 : e.key === "End" ? rows.length - 1 : null;
    if (next === null) return;
    e.preventDefault();
    setSelected(Math.max(0, Math.min(rows.length - 1, next)));
  };

  return (
    <div>
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div role="group" aria-label={copy.trendSwitch} className="flex gap-1 rounded-full bg-spot p-1">
          {(["xg", "xt"] as const).map((m) => (
            <button
              key={m}
              type="button"
              aria-pressed={metric === m}
              onClick={() => setMetric(m)}
              className={`rounded-full px-4 py-1.5 text-[14px] font-semibold transition-colors ${
                metric === m ? "bg-team text-white" : "text-team hover:bg-spot-deep"
              }`}
            >
              {m === "xg" ? copy.trendXg : copy.trendXt}
            </button>
          ))}
        </div>
        <p className="text-[13px] text-slate">{copy.trendLegend(window)}</p>
      </div>

      <div
        tabIndex={0}
        role="group"
        aria-label={copy.trendChartLabel(metricName, rows.length)}
        onKeyDown={onKey}
        className="relative mt-5 rounded-row pl-12 outline-offset-4"
      >
        {ticks.map((t) => (
          <span
            key={t}
            aria-hidden="true"
            className="absolute left-0 w-10 -translate-y-1/2 text-right text-[12px] text-slate tabular-nums"
            style={{ top: pct(yAt(t), H) }}
          >
            {signed(t, decimals)}
          </span>
        ))}
        <svg viewBox={`0 0 ${W} ${H}`} preserveAspectRatio="none" className="block h-[200px] w-full lg:h-[230px]" aria-hidden="true">
          {ticks
            .filter((t) => t !== 0)
            .map((t) => (
              <line key={t} x1={0} y1={yAt(t)} x2={W} y2={yAt(t)} stroke="var(--color-chalk)" strokeOpacity={0.7} strokeWidth={1} vectorEffect="non-scaling-stroke" />
            ))}
          {/* The selected match: a soft column behind its bar. */}
          <rect x={xAt(selected) - step / 2} y={0} width={step} height={H} fill="var(--color-spot)" />
          {rows.map((r, i) => {
            const v = net(r);
            const y = yAt(v);
            return (
              <rect
                key={i}
                x={xAt(i) - barW / 2}
                y={Math.min(y, zeroY)}
                width={barW}
                height={Math.max(Math.abs(y - zeroY), 0.5)}
                fill={v >= 0 ? "var(--color-good)" : "var(--color-bad)"}
                fillOpacity={i === selected ? 0.9 : 0.38}
              />
            );
          })}
          <line x1={0} y1={zeroY} x2={W} y2={zeroY} stroke="var(--color-slate)" strokeWidth={1.25} vectorEffect="non-scaling-stroke" />
          {linePath && (
            <path d={linePath} fill="none" stroke="var(--color-team)" strokeWidth={3} strokeLinecap="round" strokeLinejoin="round" vectorEffect="non-scaling-stroke" />
          )}
          {last && (
            <line x1={last.x} y1={last.y} x2={last.x} y2={last.y} stroke="var(--color-team)" strokeWidth={11} strokeLinecap="round" vectorEffect="non-scaling-stroke" />
          )}
          {/* Hit columns: the whole height of each match, for pointer and tap. */}
          {rows.map((_, i) => (
            <rect
              key={`hit-${i}`}
              x={xAt(i) - step / 2}
              y={0}
              width={step}
              height={H}
              fill="transparent"
              className="cursor-pointer"
              onMouseEnter={() => setSelected(i)}
              onClick={() => setSelected(i)}
            />
          ))}
        </svg>
        <div className="relative mt-2 h-[18px] text-[12px] text-slate tabular-nums" aria-hidden="true">
          <span className="absolute -left-12 w-10 text-right">{copy.trendMatchdayShort}</span>
          {rows.map((r, i) =>
            i % labelEvery === 0 || i === rows.length - 1 ? (
              <span
                key={i}
                className={`absolute -translate-x-1/2 ${i === selected ? "font-semibold text-team" : ""}`}
                style={{ left: pct(xAt(i), W) }}
              >
                {r.matchday}
              </span>
            ) : null,
          )}
        </div>
      </div>

      {/* The readout: the selected match, in words and figures. */}
      <div aria-live="polite" className="mt-5 grid gap-4 rounded-row bg-spot px-5 py-4 sm:grid-cols-[1.4fr_1fr_1fr]">
        <div>
          <p className="text-[13px] text-spot-text">{copy.trendMatchday(sel.matchday)}</p>
          <p className="text-[17px] font-semibold">
            {sel.opponent}{" "}
            <span className="font-normal text-spot-text">
              {sel.isHome ? matchList.homeShort : matchList.awayShort}
            </span>{" "}
            · {sel.goalsFor}–{sel.goalsAgainst}{" "}
            <span className="text-[14px] font-medium text-spot-text">{copy.resultWord[sel.result]}</span>
          </p>
        </div>
        <div>
          <p className="text-[13px] text-spot-text">{copy.trendForAgainst(metricName)}</p>
          <p className="text-[17px] tabular-nums">
            {metric === "xg"
              ? `${dec(sel.xgFor, 2)} – ${dec(sel.xgAgainst, 2)}`
              : `${dec(sel.xtFor, 2)} – ${dec(sel.xtAgainst, 2)}`}{" "}
            <span className={`font-semibold ${selNet >= 0 ? "text-good" : "text-bad"}`}>({signed(selNet, 2)})</span>
          </p>
        </div>
        <div>
          <p className="text-[13px] text-spot-text">{copy.trendRolling(window)}</p>
          <p className="text-[17px] font-semibold text-team tabular-nums">
            {selRoll === null ? (
              <span className="text-[14px] font-normal text-spot-text">{copy.trendRollingNone(window)}</span>
            ) : (
              signed(selRoll, 2)
            )}
          </p>
        </div>
      </div>
      <p className="mt-2 text-[13px] text-slate">{copy.trendHint}</p>
    </div>
  );
}
