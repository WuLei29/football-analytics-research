/**
 * MatchIndex — every match of the season as one whole-row link, newest first.
 *
 * Renders: per row, the matchday, date and venue (slate), the opponent as
 * the row title, a result disc with the table's letter (G good, E fair-fill
 * with an ink letter, P bad) and the score, a pair of round-capped xG strokes
 * (for in team blue over against in rival red, one scale for the whole list,
 * never under 2.0) with the two values and the net in good / bad, then a
 * small pitch of Espanyol's shots and the drawn arrow. Hover and keyboard
 * focus step the row to panel-deep, turn the title blue and slide the arrow
 * in, as in the depth index.
 * Props: `rows` — the season's matches (overview order) each with its
 *        Espanyol shots; `team` — the slug, for the links.
 * Data:  `overview.json` -> `matches[]` (md/WEB_DATA.md §6) and each match
 *        file's `shots[]` (§7.1), joined by the page at build time.
 *
 * The handoff's row was a mono line of chips; this keeps its fields and the
 * form strip's encodings (result colours, the xG pair) and redraws it as a
 * Pizarra index row. The xG pair lies flat here because a row is wide and
 * short; in the form strip it stands up because a cell is narrow and tall.
 * On a phone the shot pitch is dropped and the xG line moves under the title.
 */

import Link from "next/link";

import { dec, shortDate, signed } from "@/lib/format";
import { matchList as copy, season as seasonCopy } from "@/lib/labels";
import { routes } from "@/lib/routes";
import type { OverviewMatch } from "@/lib/data/types";

import { ShotsThumb } from "@/components/landing/Thumbs";
import { Arrow } from "@/components/shell/Arrow";

export interface MatchIndexRow {
  match: OverviewMatch;
  shots: { x: number; y: number; outcome: string }[];
}

const DISC: Record<OverviewMatch["result"], string> = {
  W: "bg-good text-white",
  D: "bg-fair-fill text-ink",
  L: "bg-bad text-white",
};

/** The xG strokes never scale below this (the form strip's floor). */
const MIN_SCALE = 2;

function XgPair({ m, scale }: { m: OverviewMatch; scale: number }) {
  const W = 120;
  const x = (v: number) => 2 + (v / scale) * (W - 4);
  const xgd = m.xg_for - m.xg_against;
  return (
    <span className="flex items-center gap-3" aria-hidden="true">
      <svg viewBox={`0 0 ${W} 14`} width={W} height="14" className="shrink-0">
        <line x1="2" y1="4" x2={x(m.xg_for)} y2="4" stroke="var(--color-team)" strokeWidth="4" strokeLinecap="round" />
        <line x1="2" y1="10" x2={x(m.xg_against)} y2="10" stroke="var(--color-rival)" strokeWidth="4" strokeLinecap="round" />
      </svg>
      <span className="text-[14px] whitespace-nowrap text-slate tabular-nums">
        {dec(m.xg_for, 1)}–{dec(m.xg_against, 1)}
      </span>
      <span className={`w-[3.2em] text-[14px] font-medium tabular-nums ${xgd >= 0 ? "text-good" : "text-bad"}`}>
        {signed(xgd, 1)}
      </span>
    </span>
  );
}

export function MatchIndex({ rows, team }: { rows: MatchIndexRow[]; team: string }) {
  const scale = Math.max(MIN_SCALE, ...rows.flatMap((r) => [r.match.xg_for, r.match.xg_against]));

  return (
    <ol className="flex flex-col gap-2">
      {/* Newest first: the match a reader wants is almost always the last one
          played. */}
      {[...rows].reverse().map(({ match: m, shots }) => (
        <li key={m.match_id}>
          <Link
            href={routes.match(team, m.match_id)}
            aria-label={copy.rowLabel(
              m.matchday,
              shortDate(m.date),
              m.opponent_short_name,
              m.is_home,
              seasonCopy.resultWord[m.result],
              m.goals_for,
              m.goals_against,
              dec(m.xg_for, 1),
              dec(m.xg_against, 1),
            )}
            className="group grid grid-cols-[minmax(0,1fr)_auto] items-center gap-x-4 gap-y-2 rounded-row bg-panel px-4 py-3.5 transition-colors hover:bg-panel-deep focus-visible:bg-panel-deep sm:px-5 md:grid-cols-[minmax(0,1fr)_auto_auto] lg:grid-cols-[minmax(0,280px)_120px_minmax(0,1fr)_112px_20px] lg:gap-x-8"
          >
            <span className="min-w-0" aria-hidden="true">
              <span className="block text-[14px] text-slate">
                {copy.matchdayShort(m.matchday)} · {shortDate(m.date)} · {m.is_home ? copy.home : copy.away}
              </span>
              <span className="mt-0.5 block truncate text-[18px] font-semibold tracking-[-0.01em] transition-colors group-hover:text-team group-focus-visible:text-team">
                {m.opponent_short_name}
              </span>
            </span>

            <span className="flex items-center gap-2.5" aria-hidden="true">
              <span
                className={`flex size-7 items-center justify-center rounded-full text-[13px] font-semibold ${DISC[m.result]}`}
              >
                {seasonCopy.resultLetter[m.result]}
              </span>
              <span className="text-[22px] leading-none font-light tracking-[-0.02em] tabular-nums">
                {m.goals_for}–{m.goals_against}
              </span>
            </span>

            <span className="col-span-2 md:col-span-1">
              <XgPair m={m} scale={scale} />
            </span>

            <span className="hidden lg:block" aria-hidden="true">
              {shots.length > 0 && <ShotsThumb shots={shots} title={copy.thumbLabel(m.opponent_short_name)} />}
            </span>

            <span className="hidden text-[18px] text-team lg:block">
              <Arrow className="-translate-x-1 opacity-0 transition-[opacity,translate] duration-200 group-hover:translate-x-0 group-hover:opacity-100 group-focus-visible:translate-x-0 group-focus-visible:opacity-100" />
            </span>
          </Link>
        </li>
      ))}
    </ol>
  );
}
