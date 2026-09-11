/**
 * FormStrip — "Forma · últimos 10" on screen 01, block 3, right column.
 *
 * Renders: up to ten equal cells, each an opponent code, a 23x23 result badge
 * (W green, D grey, L red), the score, and the match xG difference. Every cell
 * is a link to that match page.
 * Props: `matches` — the whole season; the component takes the last `count`.
 * Data:  `overview.json` -> `matches[]` (md/WEB_DATA.md §6), from
 *        `gold.team_match_stats` joined to `silver.teams` for the opponent.
 *
 * `matches` is the only place per-match rows live in the file, so the strip
 * slices it rather than reading a block of its own (§6).
 *
 * The design's cells are static text. They are links here because this strip
 * and the match list are the only two ways into a match page, and a reader who
 * has just seen a 3-0 wants to click it.
 */

import Link from "next/link";

import { signed } from "@/lib/format";
import { matchList, season as copy } from "@/lib/labels";
import { routes } from "@/lib/routes";
import type { OverviewMatch } from "@/lib/data/types";

const RESULT_COLOR: Record<OverviewMatch["result"], string> = {
  W: "var(--color-pos)",
  D: "var(--color-faint)",
  L: "var(--color-neg)",
};

export function FormStrip({
  matches,
  team,
  count = 10,
}: {
  matches: OverviewMatch[];
  /** Team slug, for the match links. */
  team: string;
  count?: number;
}) {
  const recent = matches.slice(-count);

  if (recent.length === 0) {
    return <p className="dek mt-3">{copy.formEmpty}</p>;
  }

  return (
    <div className="mt-3 grid gap-1.5" style={{ gridTemplateColumns: `repeat(${recent.length}, minmax(0,1fr))` }}>
      {recent.map((m) => {
        const xgd = m.xg_for - m.xg_against;

        return (
          <Link
            key={m.match_id}
            href={routes.match(team, m.match_id)}
            className="flex flex-col items-center gap-1 rounded-chip border border-transparent bg-paper px-1 py-2 hover:border-blue"
          >
            <span className="font-mono text-[8px] tracking-[0.06em] text-mid">
              {m.opponent_abbr}
              {/* Home or away, in one character: the design puts the venue in
                  the opponent cell rather than in a column of its own. */}
              <span className="text-faint">
                {" "}
                {m.is_home ? matchList.homeShort : matchList.awayShort}
              </span>
            </span>

            <span
              className="flex size-[23px] items-center justify-center rounded-[7px] font-mono text-[11px] text-on-dark"
              style={{ background: RESULT_COLOR[m.result] }}
            >
              {m.result}
            </span>

            <span className="font-mono text-[9.5px] text-ink">
              {m.goals_for}–{m.goals_against}
            </span>

            <span
              className="font-mono text-[8px]"
              style={{
                color: xgd >= 0 ? "var(--color-pos)" : "var(--color-neg)",
              }}
            >
              {signed(xgd, 1)}
            </span>
          </Link>
        );
      })}
    </div>
  );
}
