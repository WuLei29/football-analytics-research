/**
 * MatchPlayers — the four boxes of screen 02, block 7: Recuperaciones, Pases
 * completados, Pases al último tercio, xG + xA.
 *
 * Renders: `repeat(4, 1fr)` cards of six rows. Each row is a team chip, a
 * name, and a value; row 1 is tinted with the selected-row blue.
 * Props: `boxes` (`players[]`) and `teamSide`, so the published club's chip is
 * blue and the opponent's grey.
 * Data:  `matches/{match_id}.json` -> `players[]` (md/WEB_DATA.md §7), from
 *        `gold.player_match_stats` for BOTH teams.
 *
 * Both teams on purpose: this is the one block of the match page that is not
 * about the published club, and a match where the opponent's holding midfielder
 * made 14 recoveries is a match you cannot read from one side.
 */

import { dec, num } from "@/lib/format";
import { label, matchPlayerBoxLabels } from "@/lib/labels";
import type { MatchPlayerBox } from "@/lib/data/types";

export function MatchPlayers({
  boxes,
  teamSide,
}: {
  boxes: MatchPlayerBox[];
  teamSide: "home" | "away";
}) {
  return (
    <div className="mt-4 grid gap-3.5 sm:grid-cols-2 xl:grid-cols-4">
      {boxes.map((box) => (
        <article key={box.key} className="card">
          <h3 className="border-b border-ink pb-2 font-display text-[15px] font-bold">
            {label(matchPlayerBoxLabels, box.key)}
          </h3>

          <ol className="mt-3 flex flex-col gap-1.5">
            {box.rows.map((row, index) => (
              <li
                key={`${box.key}-${row.player_id}`}
                className="flex items-baseline justify-between gap-2 rounded-chip px-2 py-1.5"
                style={
                  index === 0 ? { background: "rgba(11,76,158,.10)" } : undefined
                }
              >
                <span className="flex min-w-0 items-baseline gap-2">
                  <span
                    className="shrink-0 rounded-[4px] px-1 py-0.5 font-mono text-[8px] text-on-dark"
                    style={{
                      background:
                        row.side === teamSide
                          ? "var(--color-blue)"
                          : "var(--color-faint)",
                    }}
                  >
                    {row.abbr}
                  </span>
                  <span className="truncate font-sans text-[12px] font-medium">
                    {row.name}
                  </span>
                </span>

                <span className="shrink-0 font-mono text-[12px] font-medium">
                  {/* xG + xA is a sum of two decimals; the other three boxes
                      are counts. */}
                  {Number.isInteger(row.value) ? num(row.value) : dec(row.value, 2)}
                </span>
              </li>
            ))}
          </ol>
        </article>
      ))}
    </div>
  );
}
