/**
 * PlayerView — screen 03 between the masthead and the footer.
 *
 * Renders: README-PLAYER "Page structure", top to bottom — page header, tab
 * strip, profile card beside the event map, the percentile cards, and the
 * sequence block.
 * Props: the player file, the squad (for the tab strip), the team and season
 *        slugs, an optional photo URL, and `demo` for the /demo fixture page.
 * Data:  `players/{player_id}.json` and `squad.json` (WEB_DATA §8, §9),
 *        passed down by the page, which read them at build time.
 *
 * A server component that places two client islands (the event map, the
 * sequence block) and the tab strip. It exists so the real route and the
 * /demo fixture page draw the same page from the same code.
 */

import { label, player as copy, positionLabels } from "@/lib/labels";
import type { PlayerFile, SquadPlayer } from "@/lib/data/types";

import { EventMap } from "./EventMap";
import { PlayerHeader } from "./PlayerHeader";
import { PlayerPercentiles } from "./PlayerPercentiles";
import { PlayerSequences } from "./PlayerSequences";
import { PlayerTabs } from "./PlayerTabs";
import { ProfileCard } from "./ProfileCard";

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

      <PlayerTabs players={squad} currentId={p.player_id} team={team} season={season} demo={demo} />

      {/* 340px card beside the pitch; the card's photo flexes to the pitch
          card's height so the two columns bottom-align. Stacks below lg. */}
      <section className="grid items-stretch gap-4 lg:grid-cols-[minmax(0,340px)_minmax(0,1fr)]">
        <ProfileCard player={p} kpis={file.kpis} photo={photo} />
        <EventMap maps={file.maps} playerName={p.name.full} teamAbbr={file.team.abbr} />
      </section>

      {file.percentiles && (
        <PlayerPercentiles
          percentiles={file.percentiles}
          minutes={p.minutes}
          isUtility={p.is_utility}
          primaryPct={p.primary_position_pct}
          position={p.position ? label(positionLabels, p.position) : ""}
        />
      )}

      <PlayerSequences sequences={file.sequences} surname={p.name.last} />
    </>
  );
}
