"use client";

/**
 * PlayerTabs — one pill per squad player who has played this season.
 *
 * Renders: README-PLAYER §2 — wrapping mono pills, the current player in
 * `--blue`, the rest on `--paper`; each a link to that player's page.
 * Props: `players` (squad order), `currentId`, `team`, `season`, and
 *        `demo` on /demo, where the pills are drawn but link nowhere.
 * Data:  `squad.json` -> `players` (WEB_DATA §8): GK -> CB -> FB -> MID ->
 *        W -> ST, then minutes, ordered by the export.
 *
 * Client component for one reason: every link carries the current event-map
 * mode (`?vista=`), so a reader comparing two players keeps the same view.
 * The mode lives in the URL (`lib/vista.ts`); the links re-render when it
 * changes. Without JavaScript they are plain links to the default view.
 */

import Link from "next/link";

import { player as copy } from "@/lib/labels";
import { routes } from "@/lib/routes";
import { DEFAULT_VISTA, useVista } from "@/lib/vista";
import type { SquadPlayer } from "@/lib/data/types";

export function PlayerTabs({
  players,
  currentId,
  team,
  season,
  demo = false,
}: {
  players: SquadPlayer[];
  currentId: number;
  team: string;
  season: string;
  /** /demo: the pills are drawn but go nowhere (there are no fixture pages). */
  demo?: boolean;
}) {
  const vista = useVista();

  return (
    <nav aria-label={copy.tabsLabel} className="mt-4 mb-4 flex flex-wrap gap-1.5">
      {players.map((p) => {
        const current = p.player_id === currentId;
        const className =
          "rounded-pill border px-3 py-1.5 font-mono text-[10px] tracking-[0.1em] uppercase hover:border-blue";
        const style = current
          ? { background: "var(--color-blue)", borderColor: "var(--color-blue)", color: "#fff" }
          : { background: "var(--color-paper)", borderColor: "var(--color-line)", color: "var(--color-mid)" };
        if (demo || current) {
          return (
            <span key={p.player_id} className={className} style={style} aria-current={current ? "page" : undefined}>
              {p.name.last}
            </span>
          );
        }
        return (
          <Link
            key={p.player_id}
            href={routes.player(team, season, p.player_id, vista === DEFAULT_VISTA ? undefined : vista)}
            className={className}
            style={style}
          >
            {p.name.last}
          </Link>
        );
      })}
    </nav>
  );
}
