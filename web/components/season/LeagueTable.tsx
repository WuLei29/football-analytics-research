/**
 * LeagueTable — the 20-row La Liga table of screen 01, in the Pizarra world.
 *
 * Renders: a table with `# / Club / PJ / G / E / P / DG / DxG / Pts`, a short
 * round-capped zone bar beside each position, and the published club on a
 * light-blue (spot) band with its name in team ink. On a phone the four record
 * columns (PJ G E P) are hidden: the five that stay fit 320 px without
 * scrolling, and the record is in the line under the page's h1.
 * Props: `rows` (`table[]`), `highlight` — the team_id to band, and `season`.
 * Data:  `league/{season}/table.json` -> `table[]` (md/WEB_DATA.md §5), from
 *        `gold.team_season_stats` for all 20 clubs.
 *
 * The zone bars are presentation and deliberately not in the data (§5):
 * 1-4 Champions League, 5 Europa League, 18-20 relegation. If the league
 * changes its allocation, it changes here.
 *
 * Changed from the handoff (24 Oct 2026, DESIGN.md "Chart colour mapping"):
 * no ink rule or dotted row rules (Pizarra draws no lines that are not data);
 * the zone bars are good green (Champions; Europa League at half strength)
 * and bad red (relegation); signed differences are good when positive and
 * bad when negative, on the club's band too (both pass 4.5:1 on spot). Mono
 * cells became the board face with tabular figures. Blue is kept for the
 * club itself (DESIGN.md, verdict colours, revised 24 Oct 2026).
 */

import Link from "next/link";

import { num, signed } from "@/lib/format";
import { season as copy } from "@/lib/labels";
import { routes } from "@/lib/routes";
import type { LeagueTableRow } from "@/lib/data/types";

/** The zone bar beside a position: colour and strength, or nothing. */
function zone(position: number, total: number): { color: string; opacity: number } | null {
  if (position <= 4) return { color: "var(--color-good)", opacity: 1 };
  if (position === 5) return { color: "var(--color-good)", opacity: 0.45 };
  if (position > total - 3) return { color: "var(--color-bad)", opacity: 1 };
  return null;
}

/** Signed values are coloured by sign, which is why they never print bare. */
function signClass(value: number): string {
  if (value > 0) return "text-good";
  if (value < 0) return "text-bad";
  return "text-slate";
}

/** The record columns that a phone drops. */
const RECORD = "hidden sm:table-cell";

export function LeagueTable({
  rows,
  highlight,
  season,
}: {
  rows: LeagueTableRow[];
  highlight: number;
  /** Season slug, for the link on the published club. */
  season: string;
}) {
  const columns = copy.tableColumns;
  const th = "pb-2 text-[13px] font-medium text-slate";

  return (
    /* The wrapper cancels the panel's side padding by 12px so the team band
       reaches a little past the text on both sides, as a marker would. */
    <div className="-mx-3">
      <table className="w-full border-separate border-spacing-0 tabular-nums">
        <thead>
          <tr>
            <th className={`${th} pl-3 text-left`}>{columns.position}</th>
            <th className={`${th} pl-2 text-left`}>{columns.club}</th>
            <th className={`${th} ${RECORD} text-right`}>{columns.played}</th>
            <th className={`${th} ${RECORD} text-right`}>{columns.won}</th>
            <th className={`${th} ${RECORD} text-right`}>{columns.drawn}</th>
            <th className={`${th} ${RECORD} text-right`}>{columns.lost}</th>
            <th className={`${th} text-right`}>{columns.goalDifference}</th>
            <th className={`${th} text-right`}>{columns.xgDifference}</th>
            <th className={`${th} pr-3 text-right`}>{columns.points}</th>
          </tr>
        </thead>

        <tbody className="text-[14px]">
          {rows.map((row) => {
            const isTeam = row.team_id === highlight;
            const z = zone(row.position, rows.length);
            // The band is drawn on the cells (a <tr> cannot round its
            // corners); the first and last cell carry the radius.
            const band = isTeam ? "bg-spot" : "";
            const td = `py-[7px] ${band}`;

            return (
              <tr key={row.team_id} aria-current={isTeam ? "true" : undefined}>
                <td className={`${td} rounded-l-full pl-3`}>
                  <span className="flex items-center gap-2.5">
                    <svg viewBox="0 0 4 16" width="4" height="16" aria-hidden="true" className="shrink-0">
                      {z && (
                        <line
                          x1="2"
                          y1="2"
                          x2="2"
                          y2="14"
                          stroke={z.color}
                          strokeOpacity={z.opacity}
                          strokeWidth="3"
                          strokeLinecap="round"
                        />
                      )}
                    </svg>
                    <span className="w-[2ch] text-right text-slate">{row.position}</span>
                  </span>
                </td>

                <td className={`${td} pl-2 ${isTeam ? "font-semibold text-team" : "font-medium"}`}>
                  {row.slug ? (
                    <Link
                      href={routes.season(row.slug, season)}
                      className="underline decoration-transparent underline-offset-4 hover:decoration-current"
                    >
                      {row.short_name}
                    </Link>
                  ) : (
                    row.short_name
                  )}
                </td>

                <td className={`${td} ${RECORD} pl-2 text-right text-slate`}>{num(row.played)}</td>
                <td className={`${td} ${RECORD} pl-2 text-right text-slate`}>{num(row.won)}</td>
                <td className={`${td} ${RECORD} pl-2 text-right text-slate`}>{num(row.drawn)}</td>
                <td className={`${td} ${RECORD} pl-2 text-right text-slate`}>{num(row.lost)}</td>
                <td className={`${td} pl-3 text-right ${signClass(row.goal_difference)}`}>
                  {signed(row.goal_difference, 0)}
                </td>
                <td className={`${td} pl-3 text-right ${signClass(row.xg_difference)}`}>
                  {signed(row.xg_difference, 1)}
                </td>
                <td className={`${td} rounded-r-full pr-3 pl-3 text-right font-semibold`}>{num(row.points)}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
