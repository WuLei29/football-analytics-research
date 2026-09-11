/**
 * /{equipo}/partido/{match_id} — screen 02, the match analysis.
 *
 * Renders: the eight blocks of the design in order — header, momentum, pass
 * network beside the fourteen totals, both-team shot map beside the xT
 * surface, progression beside the defensive actions, the sequence browser,
 * the four player boxes, and the reading of the match when one is written.
 * Data:  `matches/{match_id}.json` only (md/WEB_DATA.md §2.1), read at build
 *        time. It is the largest file on the site (200-380 KB), which is why a
 *        match page reads its own file and never a tree of them.
 *
 * Server component. Two things on the page answer a mouse and each declares
 * its own client boundary rather than pulling the page across it: the shot map
 * (hover tooltip) and the sequence block (selection).
 *
 * The header names the sides home-first, as a scoreline is written, and the
 * published club is the blue one wherever a colour distinguishes them —
 * `match.team_side` says which side that is, so no component works it out
 * from a team id (§7).
 */

import Link from "next/link";

import { MatchPlayers } from "@/components/match/MatchPlayers";
import { MatchTotals } from "@/components/match/MatchTotals";
import { Footer } from "@/components/shell/Footer";
import { Masthead } from "@/components/shell/Masthead";
import { Narrative } from "@/components/shell/Narrative";
import { PageHeader } from "@/components/shell/PageHeader";
import { SectionHead } from "@/components/shell/SectionHead";
import { DefensiveActions } from "@/components/viz/DefensiveActions";
import { Legend, Ramp } from "@/components/viz/Legend";
import { Momentum } from "@/components/viz/Momentum";
import { PassNetwork } from "@/components/viz/PassNetwork";
import { ProgressionArrows } from "@/components/viz/ProgressionArrows";
import { SequenceBrowser } from "@/components/viz/SequenceBrowser";
import { ShotMap } from "@/components/viz/ShotMap";
import { VizDefs } from "@/components/viz/VizDefs";
import { ZoneHeatmap } from "@/components/viz/ZoneHeatmap";
import { matchReading } from "@/content/narratives";
import { getManifest, getMatch, getOverview, getTeam } from "@/lib/data/load";
import { dec, longDate } from "@/lib/format";
import { match as copy, site, viz } from "@/lib/labels";
import { routes } from "@/lib/routes";

/** The rolling window the momentum chart smooths with, and its caption. */
const MOMENTUM_WINDOW = 5;

/**
 * One static page per match of every published club. The match ids come from
 * `overview.json`, which is the only place per-match rows live (§6) — so this
 * cannot drift from the form strip or the match list.
 */
export async function generateStaticParams() {
  const manifest = await getManifest();

  const params: { equipo: string; match_id: string }[] = [];
  for (const team of manifest.teams) {
    for (const season of team.seasons) {
      const overview = await getOverview(team.slug, season.slug);
      for (const m of overview.matches) {
        params.push({ equipo: team.slug, match_id: String(m.match_id) });
      }
    }
  }
  return params;
}

interface PageProps {
  params: Promise<{ equipo: string; match_id: string }>;
}

/**
 * A match page is addressed by match id alone, and the season is not in the
 * URL — but the file lives under the season. The manifest is small, so the
 * page finds the season by asking which of the club's seasons contains the id.
 */
async function findMatch(equipo: string, matchId: number) {
  const team = await getTeam(equipo);

  for (const season of team.seasons) {
    const overview = await getOverview(equipo, season.slug);
    const row = overview.matches.find((m) => m.match_id === matchId);
    if (row) {
      return { team, season: season.slug, row, overview };
    }
  }

  throw new Error(
    `El partido ${matchId} no pertenece a ninguna temporada publicada de ` +
      `"${equipo}". Comprueba que "python -m src.export" se ha ejecutado ` +
      `despues de cargar la jornada.`,
  );
}

export async function generateMetadata({ params }: PageProps) {
  const { equipo, match_id } = await params;
  const { row } = await findMatch(equipo, Number(match_id));
  return {
    title: `${row.opponent_short_name} · J${row.matchday}`,
    description: site.tagline,
  };
}

export default async function MatchPage({ params }: PageProps) {
  const { equipo, match_id } = await params;
  const matchId = Number(match_id);

  const manifest = await getManifest();
  const { season } = await findMatch(equipo, matchId);
  const file = await getMatch(equipo, season, matchId);

  const { match: header, momentum, network, totals, shots, xt_zones, progression, defence } = file;
  const teamSide = header.team_side;
  const published = header[teamSide];
  const opponent = header[teamSide === "home" ? "away" : "home"];
  const reading = matchReading(matchId);

  // The markers are keys in the file; the sentence is built here (§3.7).
  //
  // Cards are exported but NOT drawn: the design's momentum block has three
  // marker kinds (goals solid, subs dashed 3 3, half time dashed 2 3), and a
  // real match adds four or five cards to seven substitutions — thirteen
  // vertical rules over a chart whose subject is the shape of the xT curve.
  // The card minutes stay in the file for a later block that wants them.
  const markers = momentum.markers
    .filter((m) => m.type !== "card")
    .map((m) => ({
      minute: m.minute,
      type: m.type,
      side: m.side,
      label: viz.marker[m.label_key]?.(m.minute) ?? `${m.minute}`,
    }));

  return (
    <>
      {/* The arrowhead markers the progression arrows reference, once. */}
      <VizDefs />

      <Masthead
        eyebrow={site.eyebrow}
        title={site.homeTitle}
        dek={site.homeDek}
        seasons={manifest.seasons}
        team={equipo}
        season={season}
        active="matches"
      />

      <PageHeader
        eyebrow={copy.eyebrow}
        title={{
          plain: `${header.home.short_name} `,
          // The score is the accent half of the H1, as in the design.
          accent: `${header.home.goals}–${header.away.goals}`,
        }}
        meta={
          <div className="flex flex-col items-start gap-1 md:items-end">
            <span className="font-display text-h2card font-bold">
              {header.away.short_name}
            </span>
            <span className="font-mono text-[9.5px] tracking-[0.14em] uppercase text-mid">
              {copy.meta(header.competition_name, header.matchday)}
            </span>
            <span className="font-mono text-[9.5px] tracking-[0.14em] uppercase text-faint">
              {copy.venueLine(header.venue, longDate(header.date))}
            </span>
            <span className="font-mono text-[9.5px] tracking-[0.14em] uppercase text-faint">
              {copy.xgLine(dec(header.home.xg, 2), dec(header.away.xg, 2))}
              {header.home.goals_ht !== null && header.away.goals_ht !== null
                ? ` · ${copy.halfTime(header.home.goals_ht, header.away.goals_ht)}`
                : ""}
            </span>
            <Link
              href={routes.matches(equipo, season)}
              className="font-mono text-[9px] tracking-[0.14em] uppercase text-blue hover:underline"
            >
              {copy.backToList}
            </Link>
          </div>
        }
      />

      {/* --- Block 2: momentum, full width -------------------------------- */}
      <section className="card mt-5">
        <SectionHead
          title={copy.momentumHeading}
          caption={copy.momentumCaption(MOMENTUM_WINDOW)}
          rule="none"
        />
        <div className="mt-2">
          <Momentum
            bins={momentum.bins}
            markers={markers}
            teamSide={teamSide}
            teamName={published.short_name}
            opponentName={opponent.short_name}
            windowMinutes={MOMENTUM_WINDOW}
          />
        </div>
      </section>

      {/* --- Block 3: pass network beside the fourteen totals ------------- */}
      <section className="mt-4 grid gap-4 lg:grid-cols-[minmax(0,306px)_minmax(0,1fr)]">
        <div className="card">
          <SectionHead
            title={copy.networkHeading}
            caption={copy.networkCaption(network.min_combinations)}
            rule="none"
            as="h3"
          />
          <div className="mt-2">
            <PassNetwork
              nodes={network.nodes}
              edges={network.edges}
              title={copy.networkHeading}
            />
          </div>
          <p className="label mt-2">{viz.legend.nodeSize}</p>
        </div>

        <div className="card">
          <SectionHead
            title={copy.totalsHeading}
            caption={copy.totalsCaption}
            rule="ink"
            as="h3"
          />
          <MatchTotals
            totals={totals}
            abbr={{ home: header.home.abbr, away: header.away.abbr }}
            teamSide={teamSide}
          />
        </div>
      </section>

      {/* --- Block 4: both-team shot map beside the xT surface ------------ */}
      {/* 2.1 : 1, not 1fr : 306px. A horizontal pitch is 109 : 72 and a
          vertical one 73 : 110, so at that ratio the two figures — the shot
          map with its two legend rows, the zone map with its ramp — come out
          the same height at any viewport width, and neither card has a blank
          band under it. At the design's fixed 306px column the shot map was
          some 250px taller than the surface beside it. */}
      <section className="mt-4 grid gap-4 lg:grid-cols-[minmax(0,2.1fr)_minmax(0,1fr)]">
        <div className="card">
          <SectionHead
            title={copy.shotsHeading}
            caption={copy.shotsCaption}
            rule="none"
            as="h3"
          />
          <div className="mt-2">
            {/* Coordinates are final: the export mirrored the opponent so the
                two sides shoot at opposite goals (§3.1). */}
            <ShotMap
              shots={shots}
              teamSide={teamSide}
              abbr={{ home: header.home.abbr, away: header.away.abbr }}
            />
          </div>
          <Legend
            rows={[
              {
                label: published.short_name,
                items: [
                  { swatch: "var(--color-blue)", text: viz.legend.shotGoal },
                  { swatch: "rgba(11,76,158,.45)", outline: "var(--color-blue)", text: viz.legend.shotOn },
                  { outline: "var(--color-blue)", text: viz.legend.shotOff },
                ],
              },
              {
                label: opponent.short_name,
                items: [
                  { swatch: "var(--color-opp-strong)", text: viz.legend.shotGoal },
                  { swatch: "rgba(201,185,166,.60)", outline: "var(--color-faint)", text: viz.legend.shotOn },
                  { outline: "var(--color-faint)", text: viz.legend.shotOff },
                ],
              },
            ]}
          />
          <p className="label mt-1">{viz.legend.shotArea}</p>
        </div>

        <div className="card">
          <SectionHead
            title={copy.xtHeading}
            caption={copy.xtCaption(published.short_name)}
            rule="none"
            as="h3"
          />
          <div className="mt-2">
            {/* The 30 zones of `gold.pitch_zones`, summed by the export in
                SQL — the same grid every other spatial block of the site
                reads, so a zone here is the same zone as in the sequence
                start-zone filter. The model's own 12 x 8 grid is still in
                the file as `xt_grid` for anyone who wants it. */}
            <ZoneHeatmap
              cells={xt_zones.cells}
              orientation="vertical"
              title={copy.xtHeading}
              showGrid
            />
          </div>
          <Ramp low={viz.legend.low} high={viz.legend.high} />
        </div>
      </section>

      {/* --- Block 5: progression beside the defensive actions ------------ */}
      <section className="mt-4 grid gap-4 lg:grid-cols-2">
        <div className="card">
          <SectionHead
            title={copy.progressionHeading}
            caption={copy.progressionCaption(progression.length)}
            rule="none"
            as="h3"
          />
          <div className="mt-2">
            <ProgressionArrows
              actions={progression}
              title={copy.progressionHeading}
            />
          </div>
          <Legend
            items={[
              { bar: "var(--color-blue)", text: viz.legend.progressivePass },
              { bar: "var(--color-gold)", text: viz.legend.carry },
            ]}
          />
        </div>

        <div className="card">
          <SectionHead
            title={copy.defenceHeading}
            caption={copy.defenceCaption(defence.actions.length)}
            rule="none"
            as="h3"
          />
          <div className="mt-2">
            <DefensiveActions
              actions={defence.actions}
              lineX={defence.line_x ?? undefined}
              // Built from `dec()`, never typed: the decimal separator is a
              // setting, and a caption with the digits baked in gets it wrong.
              lineLabel={
                defence.line_x === null
                  ? undefined
                  : copy.defenceLine(dec(defence.line_x, 1))
              }
              title={copy.defenceHeading}
            />
          </div>
          <Legend
            items={[
              { swatch: "var(--color-blue)", text: viz.legend.regain, diamond: true },
              { swatch: "var(--color-opp)", text: viz.legend.duelLost, diamond: true },
            ]}
          />
          <p className="label mt-1">{viz.legend.defenceHover}</p>
        </div>
      </section>

      {/* --- Block 6: the sequence browser -------------------------------- */}
      {/* The demo's layout, verbatim: one detailed pitch, and beside it the
          list of every sequence of the match with three or more actions,
          best xT first as the export ordered them. The file's `rank` (the
          design's "twelve best") is not used here — the list IS the ranking.
          The marks legend is built here so the component holds no Spanish. */}
      <section className="mt-8">
        <SectionHead
          title={copy.sequencesHeading}
          caption={copy.sequencesCaption(file.sequences.length)}
        />
        <div className="card mt-4">
          {file.sequences.length > 0 ? (
            <SequenceBrowser
              sequences={file.sequences.map((s) => ({
                sequence_id: s.sequence_id,
                primary_phase: s.primary_phase ?? "chaotic",
                outcome: s.outcome ?? "turnover",
                start_minute: s.minute,
                start_second: s.second,
                duration_s: s.duration_s,
                event_count: s.events,
                xt: s.xt ?? 0,
                vaep: s.vaep ?? 0,
                actions: s.actions,
              }))}
              title={copy.sequencesHeading}
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
          ) : (
            <p className="dek">{copy.sequencesEmpty}</p>
          )}
        </div>
      </section>

      {/* --- Block 7: the four player boxes, both teams ------------------- */}
      <section className="mt-8">
        <SectionHead title={copy.playersHeading} caption={copy.playersCaption} />
        <MatchPlayers boxes={file.players} teamSide={teamSide} />
      </section>

      {/* --- Block 8: the reading of the match, when one is written ------- */}
      {reading && (
        <section className="mt-8">
          <Narrative eyebrow={copy.readEyebrow} narrative={reading} />
        </section>
      )}

      <Footer source={manifest.source} generatedAt={file.generated_at} />
    </>
  );
}
