/**
 * MatchTotals — the eighteen stat rows of screen 02.
 *
 * Renders: a grid of rows (one column on a phone, two from md). Each row is [home value] [label] [away value] over a 6px round-capped
 * chalk track holding two strokes that meet: the home share from the left,
 * the away share from the right, in team blue for the published club and
 * rival red for the opponent.
 * Props: `totals` (the eighteen rows in the export order) and `teamSide`, so
 * the published club is the blue one.
 * Data:  `matches/{match_id}.json` -> `totals[]` (md/WEB_DATA.md §7.2), from
 *        both rows of `gold.team_match_stats`.
 *
 * The share is computed here, not exported: `home / (home + away)` is a
 * rendering decision about a bar, not a number about the match. A row where
 * both sides are 0 (red cards, usually) draws an empty track: "neither", not
 * "one each".
 *
 * Pizarra (24 Oct 2026): the handoff's mono label, bold display figures and
 * one-colour bar became 14px slate labels, light figures and a two-ink bar,
 * so the opponent's share is drawn and not only implied by the gap. Which
 * side is which is said once, in the panel's caption, rather than by an
 * abbreviation row that three columns made ambiguous.
 */

import { dec, num, orEmpty } from "@/lib/format";
import { label, matchTotalLabels } from "@/lib/labels";
import type { MatchTotal } from "@/lib/data/types";

/**
 * Decimal places per row, for the rows that are not plain counts. xT is the
 * one value small enough to need three; the two shares are printed to one,
 * as the export rounds them; everything else (the xG rows) reads to two,
 * whatever its size — "0.66", not "0.660".
 */
const PLACES: Record<string, 1 | 2 | 3> = {
  xt_created: 3,
  possession_pct: 1,
  pass_completion_pct: 1,
};

/** Values that are counts print as integers; the rest keep their decimals. */
function print(key: string, value: number | null): string {
  return orEmpty(value, (v) => (Number.isInteger(v) ? num(v) : dec(v, PLACES[key] ?? 2)));
}

export function MatchTotals({
  totals,
  teamSide,
}: {
  totals: MatchTotal[];
  teamSide: "home" | "away";
}) {
  const ink = (side: "home" | "away") => (side === teamSide ? "var(--color-team)" : "var(--color-rival)");

  return (
    <>
      <div className="grid gap-x-10 gap-y-4 md:grid-cols-2">
        {totals.map((total) => {
          const home = total.home ?? 0;
          const away = total.away ?? 0;
          const sum = home + away;
          const homeShare = sum > 0 ? (home / sum) * 100 : 0;
          const awayShare = sum > 0 ? 100 - homeShare : 0;

          return (
            <div key={total.key}>
              <div className="flex items-baseline justify-between gap-3">
                <span className="text-[20px] leading-none font-light tracking-[-0.02em] tabular-nums">
                  {print(total.key, total.home)}
                </span>
                <span className="flex-1 text-center text-[14px] text-slate">{label(matchTotalLabels, total.key)}</span>
                <span className="text-[20px] leading-none font-light tracking-[-0.02em] tabular-nums">
                  {print(total.key, total.away)}
                </span>
              </div>

              {/* Two round-capped strokes on one chalk track, meeting at
                  the split with a small gap so they read as two. */}
              <div className="mt-2 flex h-1.5 gap-[3px] rounded-full bg-chalk" aria-hidden="true">
                {homeShare > 0 && (
                  <div className="h-full rounded-full" style={{ width: `${homeShare}%`, background: ink("home") }} />
                )}
                {awayShare > 0 && (
                  <div className="h-full flex-1 rounded-full" style={{ background: ink("away") }} />
                )}
              </div>
            </div>
          );
        })}
      </div>
    </>
  );
}
