/**
 * PlayerHeader — the top of screen 03, in the Pizarra world.
 *
 * Renders: the page's <h1>, two lines: the player's name, then his position
 * and shirt number in team blue; under it his age and minutes; and the
 * season with the matchday the data runs to, locked top-right as on the
 * other Pizarra pages.
 * Props: `player`, `seasonLabel`, `throughMatchday`.
 * Data:  `players/{player_id}.json` -> `player`, `season` (README-PLAYER
 *        "Data contract"), from `silver.players` and
 *        `gold.player_season_stats`.
 *
 * Pizarra (24 Oct 2026): the handoff's eyebrow, ink-boxed shirt, blue mono
 * given name and upper-case mono meta lines became the season page's
 * heading: a two-line h1, a slate line, the status corner. Same facts.
 */

import { num } from "@/lib/format";
import { label, player as copy, positionLabels } from "@/lib/labels";
import type { PlayerBio } from "@/lib/data/types";

export function PlayerHeader({
  player,
  seasonLabel,
  throughMatchday,
}: {
  player: PlayerBio;
  seasonLabel: string;
  throughMatchday: number | null;
}) {
  const position = player.position ? label(positionLabels, player.position) : null;

  return (
    <header className="flex flex-col gap-4 lg:flex-row-reverse lg:items-start lg:justify-between lg:gap-10">
      <p className="text-[14px] text-slate lg:pt-3 lg:text-right">{copy.status(seasonLabel, throughMatchday)}</p>
      <div>
        <h1 className="text-[clamp(34px,3.9vw,58px)] leading-[1.02] font-semibold tracking-[-0.035em] text-balance">
          {player.name.full}
          {(position || player.shirt_number !== null) && (
            <>
              <br />
              <span className="text-team">{copy.roleLine(position, player.shirt_number)}</span>
            </>
          )}
        </h1>
        <p className="mt-4 max-w-[62ch] text-[17px] leading-[1.5] text-slate">{copy.lead(player.age, num(player.minutes))}</p>
      </div>
    </header>
  );
}
