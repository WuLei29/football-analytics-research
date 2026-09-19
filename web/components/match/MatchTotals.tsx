/**
 * MatchTotals — the eighteen stat rows of screen 02, block 3.
 *
 * Renders: a two-column grid of rows. Each row is [home value] [centred mono
 * label] [away value] over a 7px rounded track filled to the home share.
 * Props: `totals` (the eighteen rows in the export order) and the two
 * `abbr`eviations, plus `teamSide` so the published club is the blue one.
 * Data:  `matches/{match_id}.json` -> `totals[]` (md/WEB_DATA.md §7.2), from
 *        both rows of `gold.team_match_stats`.
 *
 * The share the track is filled to is computed here, not exported: it is
 * `home / (home + away)`, which is a rendering decision about a bar and not a
 * number about the match. `possession_pct` already sums to 100 and lands in
 * the same place either way.
 *
 * A row where both sides are 0 (red cards, usually) fills nothing rather than
 * half: an empty track reads as "neither", a half-full one as "one each".
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
  abbr,
  teamSide,
}: {
  totals: MatchTotal[];
  abbr: { home: string; away: string };
  teamSide: "home" | "away";
}) {
  const teamColor = "var(--color-blue)";
  const oppColor = "var(--color-mid)";

  return (
    <>
      <div className="mt-3 flex items-baseline justify-between font-mono text-[9px] tracking-[0.16em] uppercase">
        <span style={{ color: teamSide === "home" ? teamColor : oppColor }}>
          {abbr.home}
        </span>
        <span style={{ color: teamSide === "away" ? teamColor : oppColor }}>
          {abbr.away}
        </span>
      </div>

      <div className="mt-2 grid gap-x-7 gap-y-3.5 md:grid-cols-2">
        {totals.map((total) => {
          const home = total.home ?? 0;
          const away = total.away ?? 0;
          const sum = home + away;
          const homeShare = sum > 0 ? (home / sum) * 100 : 0;

          return (
            <div key={total.key}>
              <div className="flex items-baseline justify-between gap-2">
                <span
                  className="font-display text-[17px] font-bold"
                  style={{ color: teamSide === "home" ? teamColor : oppColor }}
                >
                  {print(total.key, total.home)}
                </span>

                <span className="label flex-1 text-center">
                  {label(matchTotalLabels, total.key)}
                </span>

                <span
                  className="font-display text-[17px] font-bold"
                  style={{ color: teamSide === "away" ? teamColor : oppColor }}
                >
                  {print(total.key, total.away)}
                </span>
              </div>

              <div className="mt-1 h-[7px] rounded-pill bg-track">
                <div
                  className="h-full rounded-pill"
                  style={{
                    width: `${homeShare}%`,
                    // The bar is always filled from the home end, so the two
                    // rows of the page cannot disagree about which side is
                    // which; the colour follows the published club.
                    background: teamSide === "home" ? teamColor : "var(--color-opp-strong)",
                  }}
                />
              </div>
            </div>
          );
        })}
      </div>
    </>
  );
}
