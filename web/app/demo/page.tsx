/**
 * /demo — the catalogue of drawing primitives, in the Pizarra world.
 *
 * Renders: every pitch layer and chart the site draws, each on its own
 * panel, with the invented data of `lib/viz/fixtures.ts`: both pitches, the
 * three event maps of the match page (`MatchMap`), the xT surface and a zone
 * map (`ZoneHeatmap`), the pass network, one possession chain with the list
 * beside it (`SequenceParts`), the season trend (`SeasonTrend`) and the
 * momentum chart. The page says at the top that the numbers are fake.
 * Data:  none from the export; nothing here reads `public/data/`.
 *
 * Not in the nav and not linked from anywhere. It exists so the pieces can be
 * looked at in isolation, and it is the fastest way to see whether a change
 * to `lib/viz/pitch.ts` broke something. `/demo/jugador` draws the whole
 * player page from a fixture.
 *
 * Moved to Pizarra on 24 Oct 2026, last of the pages (md/CHANGELOG.md): it
 * now shows the components the live pages use, so the handoff-only ones
 * (`ShotMap`, `DefensiveActions`, `ProgressionArrows`, `SequenceBrowser`,
 * `RollingArea`, `Legend`) could go. A server component; each chart that
 * answers a mouse or the keyboard declares its own client boundary.
 */

import type { ReactNode } from "react";

import { MatchMap } from "@/components/match/MatchMap";
import { DemoSequences } from "@/components/sequences/DemoSequences";
import { SeasonTrend } from "@/components/season/SeasonTrend";
import { BoardFooter } from "@/components/shell/BoardFooter";
import { PillNav } from "@/components/shell/PillNav";
import { Momentum } from "@/components/viz/Momentum";
import { PassNetwork } from "@/components/viz/PassNetwork";
import { Pitch } from "@/components/viz/Pitch";
import { VizDefs } from "@/components/viz/VizDefs";
import { ZoneHeatmap } from "@/components/viz/ZoneHeatmap";
import { getManifest } from "@/lib/data/load";
import { demo, viz } from "@/lib/labels";
import { xtCellsToZones } from "@/lib/viz/pitch";
import {
  demoDefence,
  demoMatch,
  demoMomentumBins,
  demoMomentumMarkers,
  demoNetworkEdges,
  demoNetworkNodes,
  demoProgression,
  demoSequenceEntries,
  demoShots,
  demoTrendRows,
  demoXtCells,
  demoZoneCells,
} from "@/lib/viz/fixtures";

export const metadata = { title: demo.metaTitle };

export default async function DemoPage() {
  const manifest = await getManifest();
  const markers = demoMomentumMarkers.map((m) => ({
    ...m,
    label: viz.marker[m.type](m.minute),
  }));
  const ramp = (
    <div className="mx-auto mt-4 flex max-w-[320px] items-center gap-3 text-[13px] text-slate" aria-hidden="true">
      <span>{viz.legend.low}</span>
      <span className="h-1.5 flex-1 rounded-full bg-[linear-gradient(90deg,var(--color-panel-deep),var(--color-team))]" />
      <span>{viz.legend.high}</span>
    </div>
  );

  return (
    <div className="pizarra">
      <PillNav team={manifest.default.team} season={manifest.default.season} seasonLabel={manifest.default.season.replace("-", "/")} />

      <main className="mx-auto max-w-[1440px] px-4 pt-[88px] sm:px-8 sm:pt-[104px] lg:px-10">
        <VizDefs />

        <div className="mx-auto max-w-[1360px]">
          <header>
            <h1 className="text-[clamp(34px,3.9vw,58px)] leading-[1.02] font-semibold tracking-[-0.035em] text-balance">
              {demo.title.plain}
              <span className="text-team">{demo.title.accent}</span>
            </h1>
            <p className="mt-4 max-w-[62ch] text-[17px] leading-[1.5] text-slate">{demo.dek}</p>
            <p className="mt-3 max-w-[62ch] text-[15px] font-medium text-bad">{demo.warning}</p>
          </header>

          <div className="mt-8 grid gap-6 lg:mt-10 lg:grid-cols-2 lg:gap-8">
            <Block title={demo.blocks.pitchH} dek={demo.blocks.pitchHDek}>
              <Pitch orientation="horizontal" title={viz.pitchTitle} />
            </Block>

            <Block title={demo.blocks.pitchV} dek={demo.blocks.pitchVDek}>
              <div className="mx-auto max-w-[306px]">
                <Pitch orientation="vertical" title={viz.pitchTitle} />
              </div>
            </Block>

            <Block title={demo.blocks.shots} dek={demo.blocks.shotsDek}>
              <MatchMap
                mode="shots"
                shots={demoShots}
                teamSide={demoMatch.teamSide}
                abbr={{ team: demoMatch.team.abbr, rival: demoMatch.opponent.abbr }}
              />
            </Block>

            {/* The xT surface on the 30 zones rather than its own 12 x 8
                grid, so it can be compared with the zone map beside it.
                `xtCellsToZones` explains what that costs. */}
            <Block title={demo.blocks.xtSurface} dek={demo.blocks.xtSurfaceDek}>
              <div className="mx-auto max-w-[306px]">
                <ZoneHeatmap cells={xtCellsToZones(demoXtCells)} orientation="vertical" title={demo.blocks.xtSurface} showGrid />
              </div>
              {ramp}
            </Block>

            <Block title={demo.blocks.progression} dek={demo.blocks.progressionDek}>
              <MatchMap mode="progression" actions={demoProgression} />
            </Block>

            <Block title={demo.blocks.defence} dek={demo.blocks.defenceDek}>
              <MatchMap mode="defence" actions={demoDefence.actions} lineX={demoDefence.line_x} />
            </Block>

            <Block title={demo.blocks.zones} dek={demo.blocks.zonesDek}>
              <ZoneHeatmap cells={demoZoneCells} title={demo.blocks.zones} showGrid />
              {ramp}
            </Block>

            <Block title={demo.blocks.network} dek={demo.blocks.networkDek}>
              <div className="mx-auto max-w-[306px]">
                <PassNetwork nodes={demoNetworkNodes} edges={demoNetworkEdges} title={demo.blocks.network} />
              </div>
              <p className="mt-3 text-[13px] text-slate">{viz.legend.nodeSize}</p>
            </Block>
          </div>

          {/* The one spot panel: a chain and the list it is chosen from. */}
          <section aria-labelledby="demo-jugadas" className="mt-6 rounded-panel bg-spot p-4 sm:p-6 lg:mt-8">
            <div className="px-2 pt-2 pb-4">
              <h2 id="demo-jugadas" className="text-[26px] leading-tight font-semibold tracking-[-0.02em]">
                {demo.blocks.sequenceDetail}
              </h2>
              <p className="mt-2 text-[15px] text-spot-text">{demo.blocks.sequenceDetailDek}</p>
            </div>
            <DemoSequences sequences={demoSequenceEntries} />
          </section>

          <div className="mt-6 grid gap-6 lg:mt-8 lg:gap-8">
            <Block title={demo.blocks.rolling} dek={demo.blocks.rollingDek}>
              <div className="rounded-row bg-white p-4 sm:p-5">
                <SeasonTrend rows={demoTrendRows} window={5} />
              </div>
            </Block>

            <Block title={demo.blocks.momentum} dek={demo.blocks.momentumDek}>
              <div className="-mx-2 overflow-x-auto px-2">
                <div className="min-w-[820px]">
                  <Momentum
                    bins={demoMomentumBins}
                    markers={markers}
                    teamSide={demoMatch.teamSide}
                    teamName={demoMatch.team.name}
                    opponentName={demoMatch.opponent.name}
                  />
                </div>
              </div>
            </Block>
          </div>

          <BoardFooter source={manifest.source} generatedAt={manifest.generated_at} photoCredit={false} />
        </div>
      </main>
    </div>
  );
}

/** One primitive on its own panel, with what it should show. */
function Block({ title, dek, children }: { title: string; dek: string; children: ReactNode }) {
  return (
    // min-w-0: a grid item otherwise grows to the momentum chart's scroll width.
    <section className="min-w-0 rounded-panel bg-panel p-6 sm:p-8">
      <h2 className="text-[22px] font-semibold tracking-[-0.02em]">{title}</h2>
      <p className="mt-1 text-[14px] text-slate">{dek}</p>
      <div className="mt-5">{children}</div>
    </section>
  );
}
