/**
 * PlayerView — screen 03 between the masthead and the footer.
 *
 * Renders: README-PLAYER "Page structure", top to bottom — page header, the
 * toolbar (player picker, match filter), profile card beside the event map,
 * the percentile cards, and the sequence block.
 * Props: the player file, the squad (for the picker), the team and season
 *        slugs, an optional photo URL, and `demo` for the /demo fixture page.
 * Data:  `players/{player_id}.json` and `squad.json` (WEB_DATA §8, §9),
 *        passed down by the page, which read them at build time.
 *
 * A server component. Everything the match filter reaches lives in the
 * client island `PlayerBody`; the percentile cards, which the filter must
 * not touch, are rendered here and handed in. It exists so the player
 * route, the Plantilla route and the /demo fixture page draw the same page
 * from the same code.
 */

import { label, player as copy, positionLabels } from "@/lib/labels";
import type { PlayerFile, SquadPlayer } from "@/lib/data/types";

import { PlayerBody } from "./PlayerBody";
import { PlayerHeader } from "./PlayerHeader";
import { PlayerPercentiles } from "./PlayerPercentiles";

export function PlayerView({
  file,
  squad,
  team,
  season,
  photo = null,
  demo = false,
}: {
  file: PlayerFile;
  squad: SquadPlayer[];
  team: string;
  season: string;
  photo?: string | null;
  demo?: boolean;
}) {
  const p = file.player;

  return (
    <>
      <PlayerHeader
        player={p}
        seasonLabel={file.season.label}
        throughMatchday={file.season.through_matchday}
      />

      {demo && (
        <p className="mt-3 rounded-chip border border-line bg-card px-3 py-2 font-mono text-[10px] tracking-[0.08em] text-neg">
          {copy.demoWarning}
        </p>
      )}

      <PlayerBody
        file={file}
        squad={squad}
        team={team}
        season={season}
        photo={photo}
        demo={demo}
        percentiles={
          file.percentiles && (
            <PlayerPercentiles
              percentiles={file.percentiles}
              minutes={p.minutes}
              isUtility={p.is_utility}
              primaryPct={p.primary_position_pct}
              position={p.position ? label(positionLabels, p.position) : ""}
            />
          )
        }
      />
    </>
  );
}
