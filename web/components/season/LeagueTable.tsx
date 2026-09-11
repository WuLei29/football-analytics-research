/**
 * LeagueTable — the 20-row La Liga table of screen 01, block 3.
 *
 * Renders: a table with `# / Club / PJ / G / E / P / DG / DxG / Pts`, a hard
 * ink rule under the head, dotted `--line` rules between rows, a 3px zone
 * marker on the position cell, and the published club highlighted.
 * Props: `rows` (`table[]`) and `highlight` — the team_id to tint.
 * Data:  `league/{season}/table.json` -> `table[]` (md/WEB_DATA.md §5), from
 *        `gold.team_season_stats` for all 20 clubs.
 *
 * The zone markers are presentation and deliberately not in the data (§5):
 * 1-4 Champions League, 5 Europa League, 18-20 relegation. If the league
 * changes its allocation, it changes here.
 *
 * Rows are text, not links, except the published club: `slug` is non-null
 * only for clubs this site has pages for, which is one today.
 */

import Link from "next/link";

import { num, signed } from "@/lib/format";
import { season as copy } from "@/lib/labels";
import { routes } from "@/lib/routes";
import type { LeagueTableRow } from "@/lib/data/types";

/** The 3px marker on the position cell. Colour by zone, else nothing. */
function zoneColor(position: number, total: number): string {
  if (position <= 4) return "var(--color-pos)";
  if (position === 5) return "var(--color-blue2)";
  if (position > total - 3) return "var(--color-neg)";
  return "transparent";
}

/** Signed values are coloured by sign, which is why they never print bare. */
function signColor(value: number): string {
  if (value > 0) return "var(--color-pos)";
  if (value < 0) return "var(--color-neg)";
  return "var(--color-mid)";
}

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

  return (
    /* The table is the one block that may scroll sideways on a phone: nine
       columns of numbers cannot collapse into one (WEB_PLAN.md §3.3 point 5).

       The wrapper cancels the card's 16px padding and the first and last cells
       put it back, so the highlighted row's tint runs edge to edge of the card
       instead of stopping 16px short of it on both sides. */
    <div className="-mx-4 overflow-x-auto">
      <table className="w-full min-w-[460px] border-collapse">
        <thead>
          <tr className="border-b border-ink">
            <th className="label py-2 pl-4 pr-2 text-left font-normal">{columns.position}</th>
            <th className="label py-2 pr-2 text-left font-normal">{columns.club}</th>
            <th className="label py-2 pr-1 text-right font-normal">{columns.played}</th>
            <th className="label py-2 pr-1 text-right font-normal">{columns.won}</th>
            <th className="label py-2 pr-1 text-right font-normal">{columns.drawn}</th>
            <th className="label py-2 pr-1 text-right font-normal">{columns.lost}</th>
            <th className="label py-2 pr-1 text-right font-normal">
              {columns.goalDifference}
            </th>
            <th className="label py-2 pr-1 text-right font-normal">
              {columns.xgDifference}
            </th>
            <th className="label py-2 pr-4 text-right font-normal">{columns.points}</th>
          </tr>
        </thead>

        <tbody>
          {rows.map((row) => {
            const isTeam = row.team_id === highlight;

            return (
              <tr
                key={row.team_id}
                className="border-b border-dotted border-line last:border-b-0"
                style={
                  isTeam
                    ? { background: "rgba(11,76,158,.10)", color: "var(--color-blue)" }
                    : undefined
                }
              >
                <td className="py-[7px] pl-4 pr-2 font-mono text-[11px]">
                  {/* The zone marker is a bar inside the cell rather than a
                      border on it, so the cell can carry the card gutter. */}
                  <span
                    className="inline-block h-[14px] w-[3px] translate-y-[3px]"
                    style={{ background: zoneColor(row.position, rows.length) }}
                  />
                  <span className="ml-[7px]">{row.position}</span>
                </td>

                <td
                  className={`py-[7px] pr-2 font-sans text-[12.5px] ${isTeam ? "font-bold" : "font-medium"}`}
                >
                  {row.slug ? (
                    <Link
                      href={routes.season(row.slug, season)}
                      className="hover:underline"
                    >
                      {row.short_name}
                    </Link>
                  ) : (
                    row.short_name
                  )}
                </td>

                <Num>{num(row.played)}</Num>
                <Num>{num(row.won)}</Num>
                <Num>{num(row.drawn)}</Num>
                <Num>{num(row.lost)}</Num>
                <Num color={isTeam ? undefined : signColor(row.goal_difference)}>
                  {signed(row.goal_difference, 0)}
                </Num>
                <Num color={isTeam ? undefined : signColor(row.xg_difference)}>
                  {signed(row.xg_difference, 1)}
                </Num>

                <td
                  className={`py-[7px] pr-4 text-right font-display text-[14px] ${isTeam ? "font-bold" : "font-semibold"}`}
                >
                  {num(row.points)}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

/** A numeric cell: DM Mono 11px, right-aligned, coloured only when signed. */
function Num({ children, color }: { children: string; color?: string }) {
  return (
    <td className="py-[7px] pr-1 text-right font-mono text-[11px]" style={{ color }}>
      {children}
    </td>
  );
}
