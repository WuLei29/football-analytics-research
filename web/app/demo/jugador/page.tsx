/**
 * /demo/jugador — screen 03 drawn from a fixture.
 *
 * Renders: the whole player page (`PlayerView`) for an invented right back,
 * with the warning that every number on it is fake, in the Pizarra shell
 * (pill nav and footer; no exits: nothing links here). Pizarra since 24 Oct
 * 2026.
 * Data:  none from the export: `demoPlayerFile` and `demoSquad` in
 *        `lib/viz/fixtures.ts`, seeded and deterministic.
 *
 * The /demo idiom applied to a whole page: the event map, the reception dot
 * layer, the sequence block with the player's light-blue node, the
 * percentile cards and the match filter are checked here against a fixture,
 * and the fixture is the contract check — it is typed as `PlayerFile`. The
 * player picker's rows render but link nowhere: there are no fixture pages
 * behind them. Every chain ships in the fixture, so the sequences never
 * fetch a match file here.
 */

import { PlayerView } from "@/components/player/PlayerView";
import { BoardFooter } from "@/components/shell/BoardFooter";
import { PillNav } from "@/components/shell/PillNav";
import { VizDefs } from "@/components/viz/VizDefs";
import { getManifest } from "@/lib/data/load";
import { site } from "@/lib/labels";
import { demoPlayerFile, demoSquad } from "@/lib/viz/fixtures";

export const metadata = { title: "Jugador de demostración", description: site.tagline };

export default async function DemoPlayerPage() {
  const manifest = await getManifest();
  return (
    <div className="pizarra">
      <PillNav team={manifest.default.team} season={manifest.default.season} seasonLabel={manifest.default.season.replace("-", "/")} />
      <main className="mx-auto max-w-[1440px] px-4 pt-[88px] sm:px-8 sm:pt-[104px] lg:px-10">
        <VizDefs />
        <div className="mx-auto max-w-[1360px]">
          <PlayerView
            file={demoPlayerFile}
            squad={demoSquad.players}
            team={demoSquad.team}
            season={demoSquad.season}
            demo
          />
          <BoardFooter source={manifest.source} generatedAt={demoPlayerFile.generated_at} photoCredit={false} />
        </div>
      </main>
    </div>
  );
}
