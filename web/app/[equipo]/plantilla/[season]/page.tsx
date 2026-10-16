/**
 * /{equipo}/plantilla/{season} — the Plantilla tab of the masthead.
 *
 * Renders: screen 03 opened on the first player of the squad (GK first, the
 * export's order), via `PlayerRoute`. The player picker moves on from there.
 * Data:  `squad.json` and the first player's `players/{player_id}.json`.
 *
 * The tab was "en construcción" until 16 Oct 2026, waiting for the sortable
 * squad table of WEB_PLAN phase 5b. The review asked for the tab to open the
 * player pages instead; the table is still not built. A season without a
 * squad.json gets the same "en preparación" placeholder as the player route.
 */

import { PlayerRoute } from "@/components/player/PlayerRoute";
import { getManifest, getSquad } from "@/lib/data/load";
import { player as copy, site } from "@/lib/labels";

export const dynamicParams = false;

export async function generateStaticParams() {
  const manifest = await getManifest();
  return manifest.teams.flatMap((team) =>
    team.seasons.map((s) => ({ equipo: team.slug, season: s.slug })),
  );
}

interface PageProps {
  params: Promise<{ equipo: string; season: string }>;
}

async function firstPlayer(equipo: string, season: string) {
  const squad = await getSquad(equipo, season);
  return squad?.players[0] ?? null;
}

export async function generateMetadata({ params }: PageProps) {
  const { equipo, season } = await params;
  const first = await firstPlayer(equipo, season);
  return { title: first ? `${first.name.full} · ${season}` : copy.pendingTitle, description: site.tagline };
}

export default async function SquadPage({ params }: PageProps) {
  const { equipo, season } = await params;
  const first = await firstPlayer(equipo, season);
  return <PlayerRoute equipo={equipo} season={season} playerId={first?.player_id ?? null} />;
}
