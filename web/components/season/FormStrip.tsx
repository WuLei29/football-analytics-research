/**
 * FormStrip — the last ten matches, inside screen 01's spot panel.
 *
 * Renders: up to ten equal cells, each the opponent's crest (24px; its
 * colour chip when none is supplied, 2 Nov 2026), its code with the venue, a
 * round result badge (G / E / P), the score, the match xG difference, and a
 * pair of round-capped bars (xG for in good green, xG against in rival red,
 * on one scale for the whole strip). Every cell is a link to that match
 * page, named in full for a screen reader ("Jornada 7, en casa ante Elche:
 * derrota 1–3, xG neto +0.1").
 * Props: `matches` — the whole season; the component takes the last `count`.
 *        `team` — the slug, for the links.
 * Data:  `overview.json` -> `matches[]` (md/WEB_DATA.md §6), from
 *        `gold.team_match_stats` joined to `silver.teams` for the opponent.
 *
 * `matches` is the only place per-match rows live in the file, so the strip
 * slices it rather than reading a block of its own (§6).
 *
 * Revised 24 Oct 2026 at the author's request: results are green / amber /
 * red again (the handoff's verdict colours, which read faster than two inks),
 * and each cell gained the xG pair, so a 1–3 with 0.9 against 0.8 reads as
 * the near-even match it was. The letters are Spanish (G / E / P, the table's
 * columns), not the handoff's W / D / L. On a phone the cells wrap five to a
 * row instead of shrinking past legibility.
 *
 * Revised 1 Nov 2026 (author's review): the xG difference is the figure a
 * reader comes for, so it moved above the bars at 18px / 600; the xG-for bar
 * is `good` green rather than team blue (for and against read as good and
 * bad, the way the difference beneath them is coloured); and a chalk
 * hairline separates the matches.
 */

import Link from "next/link";
import type { CSSProperties } from "react";

import { Crest } from "@/components/shell/Crest";
import { signed } from "@/lib/format";
import { matchList, season as copy } from "@/lib/labels";
import { routes } from "@/lib/routes";
import type { OverviewMatch } from "@/lib/data/types";

const BADGE: Record<OverviewMatch["result"], string> = {
  W: "bg-good text-white",
  D: "bg-fair-fill text-ink",
  L: "bg-bad text-white",
};

/** The xG bars never scale below this, so a strip of 0.3–0.5 matches is not
    drawn as if every one were a rout. */
const MIN_SCALE = 2;
const BAR = { h: 40, top: 3 };

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
    return <p className="text-[15px] text-slate">{copy.formEmpty}</p>;
  }

  const scale = Math.max(MIN_SCALE, ...recent.flatMap((m) => [m.xg_for, m.xg_against]));
  const barY = (v: number) => BAR.h - BAR.top - (v / scale) * (BAR.h - 2 * BAR.top);

  return (
    <ol
      className="grid grid-cols-5 gap-y-1.5 sm:[grid-template-columns:repeat(var(--n),minmax(0,1fr))]"
      style={{ "--n": recent.length } as CSSProperties}
    >
      {recent.map((m) => {
        const xgd = m.xg_for - m.xg_against;

        return (
          // A chalk hairline between matches; none at the start of a row
          // (every cell but the first from sm, every fifth on a phone).
          <li
            key={m.match_id}
            className="border-chalk px-0.5 max-sm:[&:not(:nth-child(5n+1))]:border-l sm:[&:not(:first-child)]:border-l"
          >
            <Link
              href={routes.match(team, m.match_id)}
              aria-label={copy.formCellLabel(
                m.matchday,
                m.opponent_short_name,
                m.is_home,
                copy.resultWord[m.result],
                m.goals_for,
                m.goals_against,
                signed(xgd, 1),
              )}
              className="flex flex-col items-center gap-1.5 rounded-row px-1 py-3 transition-colors hover:bg-spot focus-visible:bg-spot"
            >
              <Crest crest={m.opponent_crest} color={m.opponent_color} abbr={m.opponent_abbr} size={24} />
              <span className="text-[12px] whitespace-nowrap text-slate" aria-hidden="true">
                {m.opponent_abbr} {m.is_home ? matchList.homeShort : matchList.awayShort}
              </span>
              <span
                aria-hidden="true"
                className={`flex size-7 items-center justify-center rounded-full text-[13px] font-semibold ${BADGE[m.result]}`}
              >
                {copy.resultLetter[m.result]}
              </span>
              <span className="text-[16px] font-medium tabular-nums" aria-hidden="true">
                {m.goals_for}–{m.goals_against}
              </span>
              <span
                aria-hidden="true"
                className={`mt-1 text-[18px] leading-none font-semibold tracking-[-0.01em] tabular-nums ${xgd >= 0 ? "text-good" : "text-bad"}`}
              >
                {signed(xgd, 1)}
              </span>
              <svg viewBox={`0 0 20 ${BAR.h}`} width="20" height={BAR.h} aria-hidden="true">
                <line x1="6" y1={BAR.h - BAR.top} x2="14" y2={BAR.h - BAR.top} stroke="var(--color-chalk)" strokeWidth="1.5" strokeLinecap="round" />
                <line x1="6" y1={BAR.h - BAR.top} x2="6" y2={barY(m.xg_for)} stroke="var(--color-good)" strokeWidth="4" strokeLinecap="round" />
                <line x1="14" y1={BAR.h - BAR.top} x2="14" y2={barY(m.xg_against)} stroke="var(--color-rival)" strokeWidth="4" strokeLinecap="round" />
              </svg>
            </Link>
          </li>
        );
      })}
    </ol>
  );
}
