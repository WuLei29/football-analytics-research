/**
 * /{equipo}/jugador/{season}/{player_id} — screen 03, the player profile.
 *
 * Renders: the masthead, `PlayerView` (header, tab strip, profile card and
 * event map, percentile cards, sequences), the footer.
 * Data:  `squad.json` and `players/{player_id}.json` (WEB_DATA §8, §9), read
 *        at build time. One static page per (club, season, squad row).
 *
 * The season is in the route (README-PLAYER open decision 1): minutes,
 * percentiles and maps are all season-scoped, and a player who stays two
 * seasons has two pages. `?vista=` is read on the client only, by the event
 * map and the tab strip; the HTML is the default view.
 *
 * Until the export has run its `squad` step there is no squad.json. A static
 * export refuses a dynamic route with no params at all, so the route then
 * builds ONE placeholder page (`PENDING`) that says the profiles are not
 * generated yet. Nothing links to it: the leader rows only link to players
 * squad.json lists. The rest of the site is unaffected either way.
 */

import { access } from "node:fs/promises";
import path from "node:path";

import { PlayerView } from "@/components/player/PlayerView";
import { Footer } from "@/components/shell/Footer";
import { Masthead } from "@/components/shell/Masthead";
import { PageHeader } from "@/components/shell/PageHeader";
import { VizDefs } from "@/components/viz/VizDefs";
import { getManifest, getPlayer, getSquad } from "@/lib/data/load";
import { player as copy, site } from "@/lib/labels";

/** Only the pages generated below exist; anything else is a 404. */
export const dynamicParams = false;

/** The one page built before any squad.json exists (see the header). */
const PENDING = "pendiente";

export async function generateStaticParams() {
  const manifest = await getManifest();
  const params: { equipo: string; season: string; player_id: string }[] = [];
  for (const team of manifest.teams) {
    for (const s of team.seasons) {
      const squad = await getSquad(team.slug, s.slug);
      for (const p of squad?.players ?? []) {
        params.push({ equipo: team.slug, season: s.slug, player_id: String(p.player_id) });
      }
    }
  }
  if (params.length === 0) {
    params.push({ equipo: manifest.default.team, season: manifest.default.season, player_id: PENDING });
  }
  return params;
}

interface PageProps {
  params: Promise<{ equipo: string; season: string; player_id: string }>;
}

export async function generateMetadata({ params }: PageProps) {
  const { equipo, season, player_id } = await params;
  if (player_id === PENDING) return { title: copy.pendingTitle, description: site.tagline };
  const file = await getPlayer(equipo, season, Number(player_id));
  return {
    title: `${file.player.name.full} · ${file.season.label}`,
    description: site.tagline,
  };
}

/** A hand-supplied, licensed photo, if one was dropped in public/players. */
async function photoFor(playerId: number): Promise<string | null> {
  try {
    await access(path.join(process.cwd(), "public", "players", `${playerId}.jpg`));
    return `/players/${playerId}.jpg`;
  } catch {
    return null;
  }
}

export default async function PlayerPage({ params }: PageProps) {
  const { equipo, season, player_id } = await params;
  const playerId = Number(player_id);

  const manifest = await getManifest();
  if (player_id === PENDING) {
    return (
      <>
        <Masthead
          eyebrow={site.eyebrow}
          title={site.homeTitle}
          dek={site.homeDek}
          seasons={manifest.seasons}
          team={equipo}
          season={season}
        />
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
      <Masthead
        eyebrow={site.eyebrow}
        title={site.homeTitle}
        dek={site.homeDek}
        seasons={manifest.seasons}
        team={equipo}
        season={season}
      />
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
