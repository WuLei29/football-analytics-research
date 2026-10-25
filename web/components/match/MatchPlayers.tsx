/**
 * MatchPlayers — the four boxes of screen 02: Recuperaciones, Pases
 * completados, Pases al último tercio, xG + xA.
 *
 * Renders: four panels (one, two or four across) of six rows. Each row is
 * the team's abbreviation in its ink, a name, and a light figure; the leader
 * row is white, as the season page's leader rows are.
 * Props: `boxes` (`players[]`) and `teamSide`, so the published club's
 * abbreviation is team blue and the opponent's rival red.
 * Data:  `matches/{match_id}.json` -> `players[]` (md/WEB_DATA.md §7), from
 *        `gold.player_match_stats` for BOTH teams.
 *
 * Both teams on purpose: this is the one block of the match page that is not
 * about the published club, and a match where the opponent's holding midfielder
 * made 14 recoveries is a match you cannot read from one side.
 *
 * Pizarra (24 Oct 2026): the handoff's filled mono chips and ruled card
 * heads became coloured abbreviations and panel headings.
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
    <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
      {boxes.map((box) => (
        <article key={box.key} className="rounded-panel bg-panel p-4 sm:p-5">
          <h3 className="px-2 text-[18px] font-semibold tracking-[-0.01em]">{label(matchPlayerBoxLabels, box.key)}</h3>

          <ol className="mt-3 flex flex-col gap-1">
            {box.rows.map((row, index) => (
              <li
                key={`${box.key}-${row.player_id}`}
                className={`grid grid-cols-[36px_minmax(0,1fr)_auto] items-baseline gap-2 rounded-[12px] px-2 py-1.5 ${
                  index === 0 ? "bg-white" : ""
                }`}
              >
                <span
                  className={`text-[13px] font-semibold ${row.side === teamSide ? "text-team" : "text-rival"}`}
                >
                  {row.abbr}
                </span>
                <span className="truncate text-[15px] font-medium">{row.name}</span>
                <span className="text-[17px] font-light tabular-nums">
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
