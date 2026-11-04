/**
 * /{equipo}/inicio/{season} — the landing for a chosen season, opened from the
 * nav's season menu (`LandingPage`). Only seasons with a match played get one:
 * a landing that opens on "0 puntos" proves nothing, and the menu does not
 * offer them from here.
 */

import { LandingPage } from "@/components/landing/LandingPage";
import { getManifest, getSeason, getTeam } from "@/lib/data/load";
import { site } from "@/lib/labels";

export async function generateStaticParams() {
  const manifest = await getManifest();
  return manifest.teams.flatMap((team) =>
    team.seasons.filter((s) => s.matches_played > 0).map((s) => ({ equipo: team.slug, season: s.slug })),
  );
}

interface PageProps {
  params: Promise<{ equipo: string; season: string }>;
}

export async function generateMetadata({ params }: PageProps) {
  const { equipo, season } = await params;
  const [team, meta] = await Promise.all([getTeam(equipo), getSeason(season)]);
  return {
    title: `${team.short_name} ${meta.label}`,
    description: site.tagline,
  };
}

export default async function LandingSeasonPage({ params }: PageProps) {
  const { equipo, season } = await params;
  return <LandingPage teamSlug={equipo} seasonSlug={season} />;
}
