/**
 * page.tsx — the landing page, `/`: `LandingPage` on the home season
 * (`getHomeSeason`). The other seasons' landings are
 * `/{equipo}/inicio/{season}`.
 */

import { LandingPage } from "@/components/landing/LandingPage";
import { getHomeSeason, getManifest } from "@/lib/data/load";

export default async function HomePage() {
  const manifest = await getManifest();
  const team = manifest.default.team;
  const season = await getHomeSeason(team);
  return <LandingPage teamSlug={team} seasonSlug={season.slug} />;
}
