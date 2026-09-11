/**
 * PlayerLeaders — the four boxes of screen 01, block 6: Goles, xT por 90,
 * Pases progresivos, Conducciones al último tercio.
 *
 * Renders: `repeat(4, 1fr)` cards of six rows each, sorted descending, with
 * row 1 inverted (blue ground, white text) and the rest on `--paper`.
 * Props: `leaders` — `overview.leaders`, four entries in the export order.
 * Data:  `overview.json` -> `leaders[]` (md/WEB_DATA.md §6), from
 *        `gold.player_season_stats` for that club and season.
 *
 * The design makes every row a button into screen 03. That page is Phase 5b,
 * and a static export has no useful 404, so the rows are inert until it lands:
 * flip `LINK_TO_PLAYER` in one place when `/{equipo}/jugador/{id}` exists —
 * the same posture the masthead takes with its `ready` flags.
 */

import Link from "next/link";

import { dec, num } from "@/lib/format";
import { label, leaderLabels } from "@/lib/labels";
import { routes } from "@/lib/routes";
import type { OverviewLeaders } from "@/lib/data/types";

/**
 * Screen 03 is not built (WEB_PLAN.md phase 5b). While this is false the rows
 * render as plain rows; when the player page lands, set it to true and the
 * design's behaviour returns.
 */
const LINK_TO_PLAYER = false;

export function PlayerLeaders({
  leaders,
  team,
}: {
  leaders: OverviewLeaders[];
  /** Team slug, for the player links once screen 03 exists. */
  team: string;
}) {
  return (
    <div className="mt-4 grid gap-3.5 sm:grid-cols-2 xl:grid-cols-4">
      {leaders.map((box) => (
        <article key={box.key} className="card">
          <h3 className="border-b border-ink pb-2 font-display text-[15px] font-bold">
            {label(leaderLabels, box.key)}
          </h3>

          <ol className="mt-3 flex flex-col gap-1.5">
            {box.rows.map((row, index) => {
              const leading = index === 0;

              const content = (
                <>
                  <span className="flex min-w-0 items-baseline gap-2">
                    <span
                      className="font-mono text-[8px]"
                      style={{ color: leading ? "rgba(246,239,230,.7)" : "var(--color-faint)" }}
                    >
                      {row.shirt_number ?? index + 1}
                    </span>
                    {/* Names are long ("Luca Warrick Daeovie Koleosho"), the
                        cards are a quarter of the page wide, and the value on
                        the right must never be pushed out: one line, clipped. */}
                    <span className="truncate font-sans text-[12px] font-medium">
                      {row.name}
                    </span>
                  </span>

                  <span className="shrink-0 font-mono text-[12px] font-medium">
                    {Number.isInteger(row.value) ? num(row.value) : dec(row.value, 2)}
                  </span>
                </>
              );

              const rowClass =
                "flex items-baseline justify-between gap-2 rounded-chip px-2.5 py-1.5";
              const rowStyle = {
                background: leading ? "var(--color-blue)" : "var(--color-paper)",
                color: leading ? "var(--color-on-dark)" : undefined,
              };

              return (
                <li key={row.player_id}>
                  {LINK_TO_PLAYER ? (
                    <Link
                      href={routes.player(team, row.player_id)}
                      className={`${rowClass} hover:opacity-85`}
                      style={rowStyle}
                    >
                      {content}
                    </Link>
                  ) : (
                    <div className={rowClass} style={rowStyle}>
                      {content}
                    </div>
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
