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
          <Ramp />
        </Block>

        <Block title={demo.blocks.zones} dek={demo.blocks.zonesDek}>
          <ZoneHeatmap cells={demoZoneCells} title={demo.blocks.zones} showGrid />
          <Ramp />
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
              pxPerUnit={60}
              ticks={[1, 0.5, 0, -0.5, -1]}
              axisLabel={viz.axis.rollingXgd}
              xCaption={viz.axis.matchday}
            />
            <RollingArea
              points={demoRollingXt}
              pxPerUnit={150}
              ticks={[0.4, 0.2, 0, -0.2, -0.4]}
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

interface LegendItem {
  swatch?: string;
  outline?: string;
  bar?: string;
  /** A dashed rule — the carry mark. */
  dotted?: string;
  /** A curved rule — the cross mark; the colour is its origin dot. */
  curved?: string;
  diamond?: boolean;
  text: string;
}

/**
 * The legend row of the design: real marks, in HTML, under the figure.
 *
 * Pass `items` for the usual single wrapping row, or `rows` when the marks
 * divide into named groups — the shot map, where the same three marks exist
 * once per team. Six marks in one wrapping row put the split wherever the
 * container width happens to fall, which is never the team boundary; a row
 * each makes the grouping the reader's first read instead of a puzzle.
 */
function Legend({
  items,
  rows,
}: {
  items?: LegendItem[];
  /** One line per group, each with a name in the gutter. */
  rows?: { label: string; items: LegendItem[] }[];
}) {
  if (rows) {
    return (
      <div className="label mt-2 flex flex-col gap-[5px]">
        {rows.map((row) => (
          <div key={row.label} className="flex flex-wrap items-center gap-x-3 gap-y-[5px]">
            {/* Fixed gutter so the first mark of every row starts at the same
                x — the alignment is what makes the block read as a table. */}
            <span style={{ minWidth: 68, color: "var(--color-ink)", fontWeight: 600 }}>
              {row.label}
            </span>
            <LegendMarks items={row.items} />
          </div>
        ))}
      </div>
    );
  }

  return (
    <div className="label mt-2 flex flex-wrap gap-3">
      <LegendMarks items={items ?? []} />
    </div>
  );
}

/** The marks themselves, shared by both arrangements. */
function LegendMarks({ items }: { items: LegendItem[] }) {
  return (
    <>
      {items.map((item) => (
        <span key={item.text} className="flex items-center gap-[5px]">
          {item.dotted ? (
            <span
              style={{
                display: "inline-block",
                width: 14,
                height: 0,
                borderTop: `2px dotted ${item.dotted}`,
              }}
            />
          ) : item.curved ? (
            <svg width={14} height={9} viewBox="0 0 14 9" aria-hidden="true">
              <path d="M1,8 Q7,0 13,5" fill="none" stroke="var(--color-mid)" strokeWidth={1.2} />
              <circle cx={1} cy={8} r={1.6} fill={item.curved} />
            </svg>
          ) : (
            <span
              style={{
                display: "inline-block",
                width: item.bar ? 14 : 9,
                height: item.bar ? 2 : 9,
                borderRadius: item.bar || item.diamond ? 0 : "50%",
                transform: item.diamond ? "rotate(45deg)" : undefined,
                background: item.swatch ?? item.bar ?? "transparent",
                border: item.outline ? `1px solid ${item.outline}` : undefined,
              }}
            />
          )}
          {item.text}
        </span>
      ))}
    </>
  );
}

/** The LOW -> HIGH gradient the design puts under every heat layer. */
function Ramp() {
  return (
    <div className="mt-2 flex items-center gap-[6px]">
      <span className="label">{viz.legend.low}</span>
      <div
        style={{
          flex: 1,
          height: 8,
          background: "linear-gradient(90deg,rgba(11,76,158,.06),rgba(11,76,158,1))",
        }}
      />
      <span className="label">{viz.legend.high}</span>
    </div>
  );
}
