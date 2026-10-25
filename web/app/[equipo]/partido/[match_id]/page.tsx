/**
 * /{equipo}/partido/{match_id} — screen 02, the match analysis. Pizarra world.
 *
 * Renders, top to bottom (the handoff's block order, kept at the author's
 * request on 24 Oct 2026):
 *   1. the floating pill nav (Partidos marked) and the board, no photo;
 *   2. the page's <h1> (the scoreline, home first; then the result read
 *      against the xG in team blue), the competition / venue / date / xG
 *      line, and the match picker with the way back to the list, top-right;
 *   3. momentum, full width;
 *   4. the eighteen totals beside the pass network;
 *   5. the shot map beside the xT surface;
 *   6. progression beside the defensive actions;
 *   7. the light-blue spot panel: the sequence block, one chain drawn large
 *      and the list on the right; a click (or Enter) on a map mark above
 *      opens its chain here (`SequenceLink`);
 *   8. the four player boxes, both teams; the reading, when one is written;
 *   9. the numbered exits and the footer.
 * Data:  `matches/{match_id}.json` and `overview.json` (for the picker and
 *        the season) (md/WEB_DATA.md §2.1), read at build time. The match
 *        file is the largest on the site (200-400 KB), which is why a match
 *        page reads its own file and never a tree of them.
 *
 * Built in Pizarra on 24 Oct 2026. A first version regrouped the page
 * (figures and reading first, one map with a switch on the spot); the author
 * preferred the handoff's distribution, so this keeps its blocks and order
 * and changes only the interface. The maps are `MatchMap` (keyboard-
 * reachable, labelled marks); the shared charts (Momentum, PassNetwork,
 * ZoneHeatmap, SequenceDetail) are recoloured by the `.pizarra-viz` adapter
 * in globals.css, because the player page still draws them in the handoff.
 *
 * The published club is the blue one wherever a colour distinguishes the
 * sides; `match.team_side` says which side that is (§7).
 */

import Link from "next/link";

import { MatchMap } from "@/components/match/MatchMap";
import { MatchPicker } from "@/components/match/MatchPicker";
import { MatchPlayers } from "@/components/match/MatchPlayers";
import { MatchSequences } from "@/components/match/MatchSequences";
import { MatchTotals } from "@/components/match/MatchTotals";
import { SequenceLinkProvider } from "@/components/match/SequenceLink";
import { BoardFooter } from "@/components/shell/BoardFooter";
import { Exits, type ExitRow } from "@/components/shell/Exits";
import { PillNav } from "@/components/shell/PillNav";
import { Momentum } from "@/components/viz/Momentum";
import { PassNetwork } from "@/components/viz/PassNetwork";
import { VizDefs } from "@/components/viz/VizDefs";
import { ZoneHeatmap } from "@/components/viz/ZoneHeatmap";
import { matchReading } from "@/content/narratives";
import { getManifest, getMatch, getOverview, getSquad, getTeam } from "@/lib/data/load";
import { sequenceEntry } from "@/lib/data/sequences";
import { dec, longDate } from "@/lib/format";
import { matchPage as copy, landing, site, viz } from "@/lib/labels";
import { routes } from "@/lib/routes";

/** The rolling window the momentum chart smooths with, and its caption. */
const MOMENTUM_WINDOW = 5;

/**
 * One static page per match of every published club. The match ids come from
 * `overview.json`, the only place per-match rows live (§6), so this cannot
 * drift from the form strip or the match list.
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
 * URL, but the file lives under the season. The page finds the season by
 * asking which of the club's seasons contains the id.
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
  const { team, season, row, overview } = await findMatch(equipo, matchId);
  const file = await getMatch(equipo, season, matchId);
  const squad = await getSquad(equipo, season);

  const { match: header, momentum, network, totals, shots, xt_zones, progression, defence } = file;
  const teamSide = header.team_side;
  const rivalSide = teamSide === "home" ? "away" : "home";
  const published = header[teamSide];
  const opponent = header[rivalSide];
  const reading = matchReading(matchId);
  const xgd = published.xg - opponent.xg;

  // The markers are keys in the file; the sentence is built here (§3.7).
  // Cards are exported but not drawn: thirteen vertical rules over a chart
  // whose subject is the shape of the xT curve (handoff decision, kept).
  const markers = momentum.markers
    .filter((m) => m.type !== "card")
    .map((m) => ({
      period: m.period,
      minute: m.minute,
      type: m.type,
      side: m.side,
      label: viz.marker[m.label_key]?.(m.minute) ?? `${m.minute}`,
    }));

  const ht =
    header.home.goals_ht !== null && header.away.goals_ht !== null
      ? `${header.home.goals_ht}–${header.away.goals_ht}`
      : null;

  const exits: ExitRow[] = [
    {
      key: "season",
      title: landing.chapters.season.title,
      dek: landing.chapters.season.dek(team.seasons.length),
      href: routes.season(equipo, season),
    },
    {
      key: "match",
      title: landing.chapters.match.title,
      dek: landing.chapters.match.dek(overview.matches.length),
      href: routes.matches(equipo, season),
    },
    {
      key: "sequence",
      title: landing.chapters.sequence.title,
      dek: landing.chapters.sequence.dek,
      href: `${routes.match(equipo, matchId)}#jugadas`,
    },
    {
      key: "player",
      title: landing.chapters.player.title,
      dek: landing.chapters.player.dek(squad?.players.length ?? 0),
      href: squad ? routes.squad(equipo, season) : null,
    },
    { key: "method", title: landing.chapters.method.title, dek: landing.chapters.method.dek, href: null },
  ];

  return (
    <div className="pizarra">
      <PillNav team={equipo} season={season} seasonLabel={season.replace("-", "/")} active="matches" />

      <main className="mx-auto max-w-[1440px] px-4 pt-[88px] sm:px-8 sm:pt-[104px] lg:px-10">
        {/* The arrowheads `SequenceDetail` references, once, inside the
            adapter so they take the Pizarra inks. */}
        <div className="pizarra-viz">
          <VizDefs />
        </div>

        <div className="mx-auto max-w-[1360px]">
          {/* --- The heading ------------------------------------------------ */}
          <header className="flex flex-col gap-5 lg:flex-row lg:items-start lg:justify-between lg:gap-10">
            <div>
              <h1 className="text-[clamp(34px,3.9vw,58px)] leading-[1.02] font-semibold tracking-[-0.035em] text-balance">
                {header.home.short_name} {header.home.goals}–{header.away.goals} {header.away.short_name}
                <br />
                <span className="text-team">{copy.verdict(row.result, xgd)}</span>
              </h1>
              <p className="mt-4 max-w-[62ch] text-[17px] leading-[1.5] text-slate">
                {copy.lead(header.competition_name, header.matchday, header.venue, longDate(header.date), ht, `${dec(header.home.xg, 2)}–${dec(header.away.xg, 2)}`)}
              </p>
            </div>
            <div className="flex shrink-0 flex-wrap items-center gap-2 lg:justify-end lg:pt-2">
              <MatchPicker team={equipo} matches={overview.matches} current={matchId} />
              <Link
                href={routes.matches(equipo, season)}
                className="rounded-full bg-panel-deep px-4 py-2.5 text-[14px] font-semibold whitespace-nowrap transition-colors hover:bg-chalk"
              >
                {copy.backToList}
              </Link>
            </div>
          </header>

          {/* --- Momentum --------------------------------------------------- */}
          <section aria-labelledby="momentum" className="mt-8 rounded-panel bg-panel p-6 sm:p-8 lg:mt-10">
            <h2 id="momentum" className="text-[22px] font-semibold tracking-[-0.02em]">
              {copy.momentumHeading}
            </h2>
            <p className="mt-1 text-[14px] text-slate">
              {copy.momentumCaption(MOMENTUM_WINDOW, published.short_name, opponent.short_name)}
            </p>
            {/* On a phone the chart keeps a readable width and scrolls inside
                its panel rather than shrinking its labels to specks. */}
            <div className="-mx-2 mt-4 overflow-x-auto px-2">
              <div className="pizarra-viz on-panel min-w-[820px]">
                <Momentum
                  bins={momentum.bins}
                  markers={markers}
                  teamSide={teamSide}
                  teamName={published.short_name}
                  opponentName={opponent.short_name}
                  windowMinutes={MOMENTUM_WINDOW}
                />
              </div>
            </div>
          </section>

          {/* --- Totals beside the pass network (handoff block 3) ---------- */}
          <div className="mt-6 grid gap-6 lg:mt-8 lg:grid-cols-[minmax(0,1fr)_minmax(0,380px)] lg:gap-8">
            <section aria-labelledby="datos" className="rounded-panel bg-panel p-6 sm:p-8">
              <h2 id="datos" className="text-[22px] font-semibold tracking-[-0.02em]">
                {copy.totalsHeading}
              </h2>
              <p className="mt-1 text-[14px] text-slate">{copy.totalsCaption(header.home.short_name, header.away.short_name)}</p>
              <div className="mt-5">
                <MatchTotals totals={totals} teamSide={teamSide} />
              </div>
            </section>

            <section aria-labelledby="red" className="rounded-panel bg-panel p-6 sm:p-8">
              <h2 id="red" className="text-[22px] font-semibold tracking-[-0.02em]">
                {copy.networkHeading}
              </h2>
              <p className="mt-1 text-[14px] text-slate">{copy.networkCaption(network.min_combinations)}</p>
              <div className="pizarra-viz on-panel mx-auto mt-5 max-w-[320px]">
                <PassNetwork nodes={network.nodes} edges={network.edges} title={copy.networkHeading} />
              </div>
            </section>
          </div>

          {/* Blocks 5-7: a click on a map mark opens its chain in the spot. */}
          <SequenceLinkProvider ids={file.sequences.map((s) => s.sequence_id)}>
            {/* --- Shot map beside the xT surface (handoff block 4) -------- */}
            {/* 2.1 : 1, the handoff's ratio: a horizontal pitch beside a
                vertical one come out about the same height at any width. */}
            <div className="mt-6 grid gap-6 lg:mt-8 lg:grid-cols-[minmax(0,2.1fr)_minmax(0,1fr)] lg:gap-8">
              <section aria-labelledby="tiros" className="rounded-panel bg-panel p-6 sm:p-8">
                <h2 id="tiros" className="text-[22px] font-semibold tracking-[-0.02em]">
                  {copy.shotsHeading}
                </h2>
                <p className="mt-1 text-[14px] text-slate">{copy.shotsCaption}</p>
                <div className="mt-5">
                  {/* Coordinates are final: the export mirrored the opponent
                      so the two sides shoot at opposite goals (§3.1). */}
                  <MatchMap
                    mode="shots"
                    shots={shots.map((s) => ({
                      shot_id: s.shot_id,
                      side: s.side,
                      name: s.name,
                      minute: s.minute,
                      x: s.x,
                      y: s.y,
                      xg: s.xg,
                      body_part: s.body_part,
                      outcome: s.outcome,
                      s: s.s ?? null,
                    }))}
                    teamSide={teamSide}
                    abbr={{ team: published.abbr, rival: opponent.abbr }}
                  />
                </div>
              </section>

              <section aria-labelledby="superficie" className="rounded-panel bg-panel p-6 sm:p-8">
                <h2 id="superficie" className="text-[22px] font-semibold tracking-[-0.02em]">
                  {copy.xtHeading}
                </h2>
                <p className="mt-1 text-[14px] text-slate">{copy.xtCaption(published.short_name)}</p>
                <div className="pizarra-viz on-panel mx-auto mt-5 max-w-[320px]">
                  <ZoneHeatmap cells={xt_zones.cells} orientation="vertical" title={copy.xtHeading} showGrid />
                </div>
                <div className="mx-auto mt-4 flex max-w-[320px] items-center gap-3 text-[13px] text-slate" aria-hidden="true">
                  <span>{viz.legend.low}</span>
                  <span className="h-1.5 flex-1 rounded-full bg-[linear-gradient(90deg,var(--color-panel-deep),var(--color-team))]" />
                  <span>{viz.legend.high}</span>
                </div>
              </section>
            </div>

            {/* --- Progression beside the defensive actions (block 5) ------ */}
            <div className="mt-6 grid gap-6 lg:mt-8 lg:grid-cols-2 lg:gap-8">
              <section aria-labelledby="progresion" className="rounded-panel bg-panel p-6 sm:p-8">
                <h2 id="progresion" className="text-[22px] font-semibold tracking-[-0.02em]">
                  {copy.progressionHeading}
                </h2>
                <p className="mt-1 text-[14px] text-slate">{copy.progressionCaption(progression.length)}</p>
                <div className="mt-5">
                  <MatchMap mode="progression" actions={progression} />
                </div>
              </section>

              <section aria-labelledby="defensa" className="rounded-panel bg-panel p-6 sm:p-8">
                <h2 id="defensa" className="text-[22px] font-semibold tracking-[-0.02em]">
                  {copy.defenceHeading}
                </h2>
                <p className="mt-1 text-[14px] text-slate">{copy.defenceCaption(defence.actions.length)}</p>
                <div className="mt-5">
                  <MatchMap mode="defence" actions={defence.actions} lineX={defence.line_x} />
                </div>
              </section>
            </div>

            {/* --- The sequence block, on the spot (block 6) --------------- */}
            <section
              id="jugadas"
              aria-labelledby="jugadas-titulo"
              className="mt-6 scroll-mt-24 rounded-panel bg-spot p-4 sm:p-6 lg:mt-8"
            >
              <MatchSequences sequences={file.sequences.map((s) => sequenceEntry(s))} />
            </section>
          </SequenceLinkProvider>

          {/* --- Players ---------------------------------------------------- */}
          <section aria-labelledby="destacados" className="mt-10 lg:mt-14">
            <h2 id="destacados" className="text-[22px] font-semibold tracking-[-0.02em]">
              {copy.playersHeading}
            </h2>
            <p className="mt-1 text-[15px] text-slate">{copy.playersCaption}</p>
            <div className="mt-5">
              <MatchPlayers boxes={file.players} teamSide={teamSide} />
            </div>
          </section>

          {/* --- The reading, when one is written --------------------------- */}
          {reading && (
            <section
              aria-labelledby="lectura"
              className="mt-10 grid gap-6 rounded-panel bg-panel p-6 sm:p-8 lg:mt-14 lg:grid-cols-[minmax(220px,1fr)_3fr] lg:gap-10 lg:p-10"
            >
              <div>
                <svg viewBox="0 0 48 40" width="44" height="37" aria-hidden="true" className="text-team">
                  <path
                    d="M6 34c0-12 4-20 14-26M28 34c0-12 4-20 14-26"
                    fill="none"
                    stroke="currentColor"
                    strokeWidth="5"
                    strokeLinecap="round"
                  />
                  <circle cx="9" cy="31" r="6" fill="currentColor" />
                  <circle cx="31" cy="31" r="6" fill="currentColor" />
                </svg>
                <h2 id="lectura" className="mt-4 text-[22px] font-semibold tracking-[-0.02em]">
                  {copy.readingHeading}
                </h2>
                {reading.provenance && <p className="mt-2 text-[14px] text-slate">{reading.provenance}</p>}
              </div>
              <div className="gap-10 xl:columns-2">
                {reading.paragraphs.map((paragraph, index) => (
                  <p key={index} className="mb-4 break-inside-avoid text-[17px] leading-[1.6] last:mb-0">
                    {paragraph}
                  </p>
                ))}
              </div>
            </section>
          )}

          <Exits rows={exits} current="match" />

          <BoardFooter source={manifest.source} generatedAt={file.generated_at} photoCredit={false} />
        </div>
      </main>
    </div>
  );
}
