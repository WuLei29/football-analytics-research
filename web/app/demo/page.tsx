/**
 * /demo — the catalogue of drawing primitives.
 *
 * Renders: every pitch layer and both chart primitives of Phase 4, each in its
 * own card, with the invented data of `lib/viz/fixtures.ts`.
 * Data:  none from the export. The fixtures are seeded and fake, and the page
 *        says so at the top; nothing here reads `public/data/`.
 *
 * Not in the nav and not linked from anywhere. It exists so the pieces can be
 * looked at in isolation while screens 01 and 02 are built on top of them
 * (`md/WEB_PLAN.md` §5, Phase 4), and it is the fastest way to see whether a
 * change to `lib/viz/pitch.ts` broke something.
 *
 * It is a server component, like every other page here (`web/README.md` §2).
 * Three things on it answer a mouse — the shot map, the defensive actions and
 * the sequence browser — and each declares its own client boundary rather than
 * pulling the page across it. That is the pattern to copy.
 */

import { DefensiveActions } from "@/components/viz/DefensiveActions";
import { Momentum } from "@/components/viz/Momentum";
import { PassNetwork } from "@/components/viz/PassNetwork";
import { Pitch } from "@/components/viz/Pitch";
import { ProgressionArrows } from "@/components/viz/ProgressionArrows";
import { RollingArea } from "@/components/viz/RollingArea";
import { SequenceBrowser } from "@/components/viz/SequenceBrowser";
import { ShotMap } from "@/components/viz/ShotMap";
import { VizDefs } from "@/components/viz/VizDefs";
import { Legend, Ramp } from "@/components/viz/Legend";
import { ZoneHeatmap } from "@/components/viz/ZoneHeatmap";
import { dec } from "@/lib/format";
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
  demoRollingXgd,
  demoRollingXt,
  demoSequenceEntries,
  demoShots,
  demoXtCells,
  demoZoneCells,
} from "@/lib/viz/fixtures";

export default function DemoPage() {
  const markers = demoMomentumMarkers.map((m) => ({
    ...m,
    label: viz.marker[m.type](m.minute),
  }));

  return (
    <>
      {/* The arrowhead markers live here, once, for the whole page. */}
      <VizDefs />

      <header style={{ borderBottom: "1px solid var(--color-ink)", paddingBottom: 12 }}>
        <p className="eyebrow">{demo.eyebrow}</p>
        <h1 className="mt-2 font-[family-name:var(--font-display)] text-h1 font-bold">
          {demo.title.plain}
          <span className="text-blue">{demo.title.accent}</span>
        </h1>
        <p className="dek mt-3 max-w-[520px]">{demo.dek}</p>
        <p className="label mt-3">{demo.warning}</p>
      </header>

      <div className="mt-6 grid gap-4 lg:grid-cols-2">
        <Block title={demo.blocks.pitchH} dek={demo.blocks.pitchHDek}>
          <Pitch orientation="horizontal" title={viz.pitchTitle} />
        </Block>

        <Block title={demo.blocks.pitchV} dek={demo.blocks.pitchVDek}>
          <div className="mx-auto max-w-[306px]">
            <Pitch orientation="vertical" title={viz.pitchTitle} />
          </div>
        </Block>

        <Block title={demo.blocks.shots} dek={demo.blocks.shotsDek}>
          <ShotMap
            shots={demoShots}
            teamSide={demoMatch.teamSide}
            abbr={{ home: demoMatch.team.abbr, away: demoMatch.opponent.abbr }}
          />
          {/* One row per team, the same three marks in the same order in each,
              so the row reads as a comparison rather than a list. The middle
              mark is the whole point of the change: a save and a shot into the
              stands used to look identical. */}
          <Legend
            rows={[
              {
                label: demoMatch.team.name,
                items: [
                  { swatch: "var(--color-blue)", text: viz.legend.shotGoal },
                  { swatch: "rgba(11,76,158,.45)", outline: "var(--color-blue)", text: viz.legend.shotOn },
                  { outline: "var(--color-blue)", text: viz.legend.shotOff },
                ],
              },
              {
                label: demoMatch.opponent.name,
                items: [
                  { swatch: "var(--color-opp-strong)", text: viz.legend.shotGoal },
                  { swatch: "rgba(201,185,166,.60)", outline: "var(--color-faint)", text: viz.legend.shotOn },
                  { outline: "var(--color-faint)", text: viz.legend.shotOff },
                ],
              },
            ]}
          />
          <p className="label mt-1">{viz.legend.shotArea}</p>
        </Block>

        {/* The xT surface on the 30 zones rather than its own 12 x 8 grid, so
            it can be compared with the zone map beside it. `xtCellsToZones`
            explains what that costs. */}
        <Block title={demo.blocks.xtSurface} dek={demo.blocks.xtSurfaceDek}>
          <div className="mx-auto max-w-[306px]">
            <ZoneHeatmap
              cells={xtCellsToZones(demoXtCells)}
              orientation="vertical"
              title={demo.blocks.xtSurface}
              showGrid
            />
          </div>
          <Ramp low={viz.legend.low} high={viz.legend.high} />
        </Block>

        <Block title={demo.blocks.zones} dek={demo.blocks.zonesDek}>
          <ZoneHeatmap cells={demoZoneCells} title={demo.blocks.zones} showGrid />
          <Ramp low={viz.legend.low} high={viz.legend.high} />
        </Block>

        <Block title={demo.blocks.network} dek={demo.blocks.networkDek}>
          <div className="mx-auto max-w-[306px]">
            <PassNetwork
              nodes={demoNetworkNodes}
              edges={demoNetworkEdges}
              title={demo.blocks.network}
            />
          </div>
          <p className="label mt-2">{viz.legend.nodeSize}</p>
        </Block>

        <Block title={demo.blocks.progression} dek={demo.blocks.progressionDek}>
          <ProgressionArrows actions={demoProgression} title={demo.blocks.progression} />
          <Legend
            items={[
              { bar: "var(--color-blue)", text: viz.legend.progressivePass },
              { bar: "var(--color-gold)", text: viz.legend.carry },
            ]}
          />
        </Block>

        <Block title={demo.blocks.defence} dek={demo.blocks.defenceDek}>
          <DefensiveActions
            actions={demoDefence.actions}
            lineX={demoDefence.line_x}
            // Built from `dec()`, not typed: the separator is a setting, and a
            // caption with the digits baked in is the one that gets it wrong.
            lineLabel={demo.blocks.defLine(dec(demoDefence.line_x, 1))}
            title={demo.blocks.defence}
          />
          <Legend
            items={[
              { swatch: "var(--color-blue)", text: viz.legend.regain, diamond: true },
              { swatch: "var(--color-opp)", text: viz.legend.duelLost, diamond: true },
            ]}
          />
          <p className="label mt-1">{viz.legend.defenceHover}</p>
        </Block>

        <Block title={demo.blocks.sequenceDetail} dek={demo.blocks.sequenceDetailDek} wide>
          {/* Pitch and list side by side: the pitch is 105 x 68, so given the
              full width of a two-column card it grows taller than the viewport.
              Handing a third of the row to the list of plays keeps the whole
              pitch on screen and puts the choice next to the drawing.

              The marks legend is passed in rather than built inside the
              component, so the component still holds no Spanish. */}
          <SequenceBrowser
            sequences={demoSequenceEntries}
            title={demo.blocks.sequenceDetail}
            legend={
              <div className="mt-2">
                <Legend
                  items={[
                    { outline: "var(--color-blue)", text: viz.legend.participants },
                    { swatch: "var(--color-blue)", text: viz.legend.finisher },
                    { bar: "var(--color-mid)", text: viz.legend.pass },
                    { dotted: "var(--color-mid)", text: viz.legend.carry },
                    { curved: "var(--color-gold)", text: viz.legend.cross },
                    { swatch: "var(--color-blue)", text: viz.legend.takeOn },
                    { bar: "var(--color-blue)", text: viz.legend.shot },
                  ]}
                />
              </div>
            }
          />
        </Block>

        <Block title={demo.blocks.rolling} dek={demo.blocks.rollingDek} wide>
          <div className="grid gap-4 md:grid-cols-2">
            <RollingArea
              points={demoRollingXgd}
              axisLabel={viz.axis.rollingXgd}
              xCaption={viz.axis.matchday}
            />
            <RollingArea
              points={demoRollingXt}
              axisLabel={viz.axis.rollingXt}
              xCaption={viz.axis.matchday}
            />
          </div>
        </Block>

        <Block title={demo.blocks.momentum} dek={demo.blocks.momentumDek} wide>
          <Momentum
            bins={demoMomentumBins}
            markers={markers}
            teamSide={demoMatch.teamSide}
            teamName={demoMatch.team.name}
            opponentName={demoMatch.opponent.name}
          />
        </Block>
      </div>
    </>
  );
}

/* --------------------------------------------------------------------------
 * Page furniture. Local to /demo — screens 01 and 02 have their own headers
 * from the design and will not import these.
 * ------------------------------------------------------------------------ */

function Block({
  title,
  dek,
  wide,
  children,
}: {
  title: string;
  dek: string;
  wide?: boolean;
  children: React.ReactNode;
}) {
  return (
    <section className={`card ${wide ? "lg:col-span-2" : ""}`}>
      <div className="mb-2 flex items-baseline justify-between gap-3">
        <h2 className="font-[family-name:var(--font-display)] text-h3 font-bold">{title}</h2>
        <span className="label" style={{ letterSpacing: 0, textTransform: "none" }}>
          {dek}
        </span>
      </div>
      {children}
    </section>
  );
}
