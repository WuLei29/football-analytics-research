/**
 * /demo/jugador — screen 03 drawn from a fixture.
 *
 * Renders: the whole player page (`PlayerView`) for an invented right back,
 * with the warning that every number on it is fake.
 * Data:  none from the export: `demoPlayerFile` and `demoSquad` in
 *        `lib/viz/fixtures.ts`, seeded and deterministic.
 *
 * The /demo idiom applied to a whole page: the new layers (ActionArrows, the
 * reception dot layer, the player variant of SequenceTraces, the percentile
 * cards) are checked here before a real `players/{id}.json` exists, and the
 * fixture is the contract check — it is typed as `PlayerFile`. The tab-strip
 * pills render but link nowhere: there are no fixture pages behind them.
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
      <VizDefs />
      <Masthead
        eyebrow={site.eyebrow}
        title={site.homeTitle}
        dek={site.homeDek}
        seasons={manifest.seasons}
        team={manifest.default.team}
      />
      <PlayerView
        file={demoPlayerFile}
        squad={demoSquad.players}
        team={demoSquad.team}
        season={demoSquad.season}
        demo
      />
      <Footer source={manifest.source} generatedAt={demoPlayerFile.generated_at} />
    </>
  );
}
