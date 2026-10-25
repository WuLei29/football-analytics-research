/**
 * /demo/jugador — screen 03 drawn from a fixture.
 *
 * Renders: the whole player page (`PlayerView`) for an invented right back,
 * with the warning that every number on it is fake.
 * Data:  none from the export: `demoPlayerFile` and `demoSquad` in
 *        `lib/viz/fixtures.ts`, seeded and deterministic.
 *
 * The /demo idiom applied to a whole page: the new layers (ActionArrows, the
 * reception dot layer, the sequence browser with the gold player node, the
 * percentile cards, the match filter) are checked here against a fixture,
 * and the fixture is the contract check — it is typed as `PlayerFile`. The
 * player picker's rows render but link nowhere: there are no fixture pages
 * behind them. Every chain ships in the fixture, so the sequences never
 * fetch a match file here.
 */

import { PlayerView } from "@/components/player/PlayerView";
import { Footer } from "@/components/shell/Footer";
import { Masthead } from "@/components/shell/Masthead";
import { VizDefs } from "@/components/viz/VizDefs";
import { getManifest } from "@/lib/data/load";
import { site } from "@/lib/labels";
import { demoPlayerFile, demoSquad } from "@/lib/viz/fixtures";

export const metadata = { title: "Jugador de demostración", description: site.tagline };

export default async function DemoPlayerPage() {
  const manifest = await getManifest();
  return (
    <>
      <div className="pizarra-viz">
        <VizDefs />
      </div>
      <Masthead
        eyebrow={site.eyebrow}
        title={site.homeTitle}
        dek={site.homeDek}
        seasons={manifest.seasons}
        team={manifest.default.team}
      />
      {/* The player view moved to Pizarra on 24 Oct 2026; this fixture page
          moves with the rest of /demo (md/REDESIGN_HANDOFF.md step 5). */}
      <div className="pizarra mt-8">
        <PlayerView
          file={demoPlayerFile}
          squad={demoSquad.players}
          team={demoSquad.team}
          season={demoSquad.season}
          demo
        />
      </div>
      <Footer source={manifest.source} generatedAt={demoPlayerFile.generated_at} />
    </>
  );
}
