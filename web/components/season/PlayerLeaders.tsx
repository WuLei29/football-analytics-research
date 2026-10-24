/**
 * PlayerLeaders — the four lists of screen 01: Goles, VAEP por 90, Pases
 * progresivos, Conducciones al último tercio. Pizarra world.
 *
 * Renders: four panels (one column on a phone, two at sm, four at xl) of six
 * rows each, sorted descending. Row 1 is a team-filled row with white text
 * (hover team-deep); the rest sit on the panel and step to panel-deep on hover and focus.
 * Props: `leaders` — `overview.leaders`, four entries in the export order.
 * Data:  `overview.json` -> `leaders[]` (md/WEB_DATA.md §6), from
 *        `gold.player_season_stats` for that club and season.
 *
 * The design makes every row a button into screen 03. A row links only when
 * that player has a page: `linkable` is the player ids of `squad.json`, and
 * a static export has no useful 404, so a row whose page was not built stays
 * inert — the posture the masthead takes with its `ready` flags. Before the
 * export has run its `squad` step the set is empty and no row links.
 *
 * Changed from the handoff (24 Oct 2026): no ink rule under the title, the
 * rows are the board's rounded rows rather than paper chips, mono became the
 * board face with tabular figures, and hover is one tone step (DESIGN.md
 * "The Tone Step Rule") instead of an opacity fade.
 */

import Link from "next/link";

import { dec, num } from "@/lib/format";
import { label, leaderLabels } from "@/lib/labels";
import { routes } from "@/lib/routes";
import type { OverviewLeaders } from "@/lib/data/types";

export function PlayerLeaders({
  leaders,
  team,
  season,
  linkable,
}: {
  leaders: OverviewLeaders[];
  /** Team and season slugs, for the player links. */
  team: string;
  season: string;
  /** Player ids with a page this season (`squad.json`). */
  linkable: ReadonlySet<number>;
}) {
  return (
    <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
      {leaders.map((box) => (
        <article key={box.key} className="rounded-panel bg-panel p-6">
          <h3 className="text-[18px] font-semibold tracking-[-0.01em]">{label(leaderLabels, box.key)}</h3>

          <ol className="-mx-3 mt-4 flex flex-col gap-1">
            {box.rows.map((row, index) => {
              const leading = index === 0;

              const content = (
                <>
                  <span className="flex min-w-0 items-baseline gap-2.5">
                    <span
                      className={`w-[2ch] shrink-0 text-right text-[13px] tabular-nums ${leading ? "text-white/80" : "text-slate"}`}
                    >
                      {row.shirt_number ?? index + 1}
                    </span>
                    {/* Names are long ("Luca Warrick Daeovie Koleosho"), the
                        panels are a quarter of the page wide, and the value
                        on the right must never be pushed out: one line,
                        clipped. */}
                    <span className={`truncate text-[15px] ${leading ? "font-semibold" : "font-medium"}`}>
                      {row.name}
                    </span>
                  </span>

                  <span className={`shrink-0 text-[15px] tabular-nums ${leading ? "font-semibold" : ""}`}>
                    {Number.isInteger(row.value) ? num(row.value) : dec(row.value, 2)}
                  </span>
                </>
              );

              const rowClass = `flex items-baseline justify-between gap-3 rounded-row px-3 py-2 ${
                leading ? "bg-team text-white" : ""
              }`;

              return (
                <li key={row.player_id}>
                  {linkable.has(row.player_id) ? (
                    <Link
                      href={routes.player(team, season, row.player_id)}
                      className={`${rowClass} transition-colors ${
                        leading ? "hover:bg-team-deep focus-visible:bg-team-deep" : "hover:bg-panel-deep focus-visible:bg-panel-deep"
                      }`}
                    >
                      {content}
                    </Link>
                  ) : (
                    <div className={rowClass}>{content}</div>
                  )}
                </li>
              );
            })}
          </ol>
        </article>
      ))}
    </div>
  );
}
