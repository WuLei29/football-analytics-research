/**
 * PlayerRoute — a whole screen-03 page: masthead, `PlayerView`, footer.
 *
 * Renders: the page for one (club, season, player), with the Plantilla tab
 * of the masthead active; or, when `playerId` is null, the "en preparación"
 * placeholder the routes build before the export has written a squad.json.
 * Props: `equipo`, `season` slugs, `playerId`.
 * Data:  `manifest.json`, `squad.json`, `players/{player_id}.json`, read at
 *        build time.
 *
 * Shared by `/{equipo}/jugador/{season}/{player_id}` and the Plantilla tab,
 * `/{equipo}/plantilla/{season}`, which opens on the first player of the
 * squad (16 Oct 2026). One page, two addresses, so they cannot drift.
 */

import { access } from "node:fs/promises";
import path from "node:path";

import { Footer } from "@/components/shell/Footer";
import { Masthead } from "@/components/shell/Masthead";
import { PageHeader } from "@/components/shell/PageHeader";
import { VizDefs } from "@/components/viz/VizDefs";
import { getManifest, getPlayer, getSquad } from "@/lib/data/load";
import { player as copy, site } from "@/lib/labels";

import { PlayerView } from "./PlayerView";

/** A hand-supplied, licensed photo, if one was dropped in public/players. */
async function photoFor(playerId: number): Promise<string | null> {
  try {
    await access(path.join(process.cwd(), "public", "players", `${playerId}.jpg`));
    return `/players/${playerId}.jpg`;
  } catch {
    return null;
  }
}

export async function PlayerRoute({
  equipo,
  season,
  playerId,
}: {
  equipo: string;
  season: string;
  playerId: number | null;
}) {
  const manifest = await getManifest();
  const masthead = (
    <Masthead
      eyebrow={site.eyebrow}
      title={site.homeTitle}
      dek={site.homeDek}
      seasons={manifest.seasons}
      team={equipo}
      season={season}
      active="squad"
    />
  );

  if (playerId === null) {
    return (
      <>
        {masthead}
        <PageHeader eyebrow={copy.eyebrow} title={copy.pendingHeading} dek={copy.pendingDek} />
        <Footer source={manifest.source} generatedAt={manifest.generated_at} />
      </>
    );
  }

  const squad = await getSquad(equipo, season);
  const file = await getPlayer(equipo, season, playerId);

  return (
    <>
      <VizDefs />
      {masthead}
      <PlayerView
        file={file}
        squad={squad?.players ?? []}
        team={equipo}
        season={season}
        photo={await photoFor(playerId)}
      />
      <Footer source={manifest.source} generatedAt={file.generated_at} />
    </>
  );
}
