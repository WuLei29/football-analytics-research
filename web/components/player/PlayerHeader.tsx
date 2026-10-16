/**
 * PlayerHeader — the top of screen 03: eyebrow, shirt block, name block, and
 * two mono meta lines on the right.
 *
 * Renders: README-PLAYER §1 — a flex row over the hard ink rule; the shirt
 * number in a 2px ink box with its DORSAL caption; the given name in blue
 * mono above the 52px surname; position, age and minutes, then the season
 * and the matchday the data runs to.
 * Props: `player`, `seasonLabel`, `throughMatchday`.
 * Data:  `players/{player_id}.json` -> `player`, `season` (README-PLAYER
 *        "Data contract"), from `silver.players` and
 *        `gold.player_season_stats`.
 *
 * Not `PageHeader`: that one's H1 is a plain + accent sentence at 46px, and
 * this one is a shirt and a name at 52px. Same rule, same spacing, same
 * eyebrow underneath.
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
  const position = player.position ? label(positionLabels, player.position) : "—";

  return (
    <header className="mt-8 flex flex-wrap items-end justify-between gap-x-6 gap-y-3 border-b border-ink pb-3">
      <div>
        <p className="eyebrow mb-1.5">{copy.eyebrow}</p>
        <div className="flex items-end gap-4">
          <div className="rounded-[11px] border-2 border-ink bg-card px-3 pt-1.5 pb-1 text-center">
            <div className="font-display text-[34px] font-bold leading-[0.86]">
              {player.shirt_number ?? "—"}
            </div>
            <div className="mt-1 font-mono text-[7px] tracking-[0.16em] text-faint">
              {copy.shirtCaption}
            </div>
          </div>
          <div>
            {player.name.first && (
              <div className="font-mono text-[10px] tracking-[0.22em] uppercase text-blue">
                {player.name.first}
              </div>
            )}
            <h1
              className="font-display font-bold"
              style={{ fontSize: 52, lineHeight: 0.95, letterSpacing: "-0.04em" }}
            >
              {player.name.last}
            </h1>
          </div>
        </div>
      </div>

      <div
        className="font-mono text-[10px] tracking-[0.12em] uppercase text-mid md:text-right"
        style={{ lineHeight: 1.7 }}
      >
        <div>{copy.metaLine(position, player.age, num(player.minutes))}</div>
        <div>{copy.seasonLine(seasonLabel, throughMatchday)}</div>
      </div>
    </header>
  );
}
