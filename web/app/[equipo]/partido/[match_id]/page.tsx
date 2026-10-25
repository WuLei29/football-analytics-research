/**
 * /{equipo}/partido/{match_id} — screen 02, the match analysis. Pizarra world.
 *
 * Renders, top to bottom:
 *   1. the floating pill nav (Partidos marked) and the board, no photo;
 *   2. the page's <h1> (the scoreline, home first; then the result read
 *      against the xG in team blue), the competition / venue / date line,
 *      and the match picker with the way back to the list, top-right;
 *   3. five headline figures as raised cards, then the authored reading of
 *      the match on its own card when one is written;
 *   4. momentum, full width;
 *   5. the light-blue spot panel: a map (shots, progression or defensive
 *      actions) whose marks open the possession they belong to, the chain
 *      drawn beside it, and every chain of the match (`MatchExplorer`);
 *   6. the eighteen totals; the pass network beside the xT surface;
 *   7. the four player boxes, both teams;
 *   8. the numbered exits and the footer.
 * Data:  `matches/{match_id}.json` and `overview.json` (for the picker and
 *        the season) (md/WEB_DATA.md §2.1), read at build time. The match
 *        file is the largest on the site (200-400 KB), which is why a match
 *        page reads its own file and never a tree of them.
 *
 * Structure agreed with the author on 24 Oct 2026 (`/impeccable shape`):
 * reading first, as on screen 01 (PRODUCT.md principle 3), and one map with a
 * switch on the spot instead of three map cards, so the map, the opened chain
 * and the list sit in one panel and a click needs no cross-page link. The
 * data and chart encodings are the handoff's; only the interface changed
 * (md/REDESIGN_HANDOFF.md). The shared charts (Momentum, PassNetwork,
 * ZoneHeatmap, SequenceDetail) are recoloured by the `.pizarra-viz` adapter
 * in globals.css, because the player page still draws them in the handoff.
 *
 * The published club is the blue one wherever a colour distinguishes the
 * sides; `match.team_side` says which side that is (§7).
 */

import Link from "next/link";

import { MatchExplorer } from "@/components/match/MatchExplorer";
import { MatchPicker } from "@/components/match/MatchPicker";
import { MatchPlayers } from "@/components/match/MatchPlayers";
import { MatchTotals } from "@/components/match/MatchTotals";
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
import { dec, longDate, num, orEmpty } from "@/lib/format";
import { matchExplorer, matchPage as copy, landing, site, viz } from "@/lib/labels";
import { routes } from "@/lib/routes";
import type { MatchTotal } from "@/lib/data/types";

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

/** One total by key, both sides. */
function total(totals: MatchTotal[], key: string) {
  return totals.find((t) => t.key === key) ?? null;
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

  // The five headline figures: the club's value large, the rival's under it.
  const pick = (key: string) => {
    const t = total(totals, key);
    return t ? { team: t[teamSide], rival: t[rivalSide] } : null;
  };
  const shotsOn = pick("shots_on_target");
  const figures = [
    { key: "xg", name: copy.figure.xg, v: pick("xg"), fmt: (v: number) => dec(v, 2) },
    { key: "xt", name: copy.figure.xt, v: pick("xt_created"), fmt: (v: number) => dec(v, 2) },
    { key: "possession", name: copy.figure.possession, v: pick("possession_pct"), fmt: (v: number) => dec(v, 1), unit: "%" },
    {
      key: "shots",
      name: copy.figure.shots,
      v: pick("shots"),
      fmt: (v: number) => num(v),
      note: shotsOn?.team != null ? copy.figure.shotsOnTarget(num(shotsOn.team)) : null,
    },
    { key: "big", name: copy.figure.bigChances, v: pick("big_chances"), fmt: (v: number) => num(v) },
  ];

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
                {copy.lead(header.competition_name, header.matchday, header.venue, longDate(header.date), ht)}
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

          {/* --- The five figures, raised ---------------------------------- */}
          <section aria-labelledby="cifras" className="mt-8 lg:mt-10">
            <h2 id="cifras" className="sr-only">
              {copy.figuresHeading}
            </h2>
            <dl className="grid grid-cols-2 gap-3 sm:grid-cols-3 sm:gap-4 lg:grid-cols-5">
              {figures.map((f) => (
                <div
                  key={f.key}
                  className="flex flex-col rounded-panel bg-white p-5 shadow-raised ring-1 ring-ink/[0.06] odd:last:col-span-2 sm:p-6 sm:odd:last:col-span-1"
                >
                  <dt className="text-[14px] leading-snug font-medium">{f.name}</dt>
                  <dd className="mt-4 text-[clamp(40px,3.6vw,56px)] leading-[0.9] font-light tracking-[-0.04em] text-team">
                    {orEmpty(f.v?.team ?? null, f.fmt)}
                    {f.unit && <span className="ml-1 text-[0.5em] tracking-normal">{f.unit}</span>}
                  </dd>
                  <dd className="mt-auto pt-3 text-[14px] leading-snug text-slate">
                    {f.note && <span className="block">{f.note}</span>}
                    {copy.rivalValue(opponent.abbr, orEmpty(f.v?.rival ?? null, f.fmt) + (f.unit ? ` ${f.unit}` : ""))}
                  </dd>
                </div>
              ))}
            </dl>
          </section>

          {/* --- The reading, when one is written --------------------------- */}
          {reading && (
            <section
              aria-labelledby="lectura"
              className="mt-6 grid gap-6 rounded-panel bg-panel p-6 sm:p-8 lg:mt-8 lg:grid-cols-[minmax(220px,1fr)_3fr] lg:gap-10 lg:p-10"
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

          {/* --- Momentum --------------------------------------------------- */}
          <section aria-labelledby="momentum" className="mt-6 rounded-panel bg-panel p-6 sm:p-8 lg:mt-8">
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

          {/* --- The spot: from a mark to its possession -------------------- */}
          <section
            id="jugadas"
            aria-labelledby="explorar"
            className="mt-6 flex scroll-mt-24 flex-col gap-4 rounded-panel bg-spot p-4 sm:p-6 lg:mt-8"
          >
            <div className="px-2 pt-2">
              <h2 id="explorar" className="text-[26px] leading-tight font-semibold tracking-[-0.02em]">
                {matchExplorer.heading}
              </h2>
              <p className="mt-2 max-w-[70ch] text-[15px] text-spot-text">{matchExplorer.dek}</p>
            </div>
            <MatchExplorer
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
              progression={progression}
              defence={defence}
              sequences={file.sequences.map((s) => sequenceEntry(s))}
              teamSide={teamSide}
              abbr={{ team: published.abbr, rival: opponent.abbr }}
            />
          </section>

          {/* --- Totals ----------------------------------------------------- */}
          <section aria-labelledby="datos" className="mt-6 rounded-panel bg-panel p-6 sm:p-8 lg:mt-8">
            <h2 id="datos" className="text-[22px] font-semibold tracking-[-0.02em]">
              {copy.totalsHeading}
            </h2>
            <p className="mt-1 text-[14px] text-slate">{copy.totalsCaption(header.home.short_name, header.away.short_name)}</p>
            <div className="mt-5">
              <MatchTotals totals={totals} teamSide={teamSide} />
            </div>
          </section>

          {/* --- Pass network beside the xT surface ------------------------- */}
          <div className="mt-6 grid gap-6 lg:mt-8 lg:grid-cols-2 lg:gap-8">
            <section aria-labelledby="red" className="rounded-panel bg-panel p-6 sm:p-8">
              <h2 id="red" className="text-[22px] font-semibold tracking-[-0.02em]">
                {copy.networkHeading}
              </h2>
              <p className="mt-1 text-[14px] text-slate">{copy.networkCaption(network.min_combinations)}</p>
              <div className="pizarra-viz on-panel mx-auto mt-5 max-w-[380px]">
                <PassNetwork nodes={network.nodes} edges={network.edges} title={copy.networkHeading} />
              </div>
            </section>

            <section aria-labelledby="superficie" className="rounded-panel bg-panel p-6 sm:p-8">
              <h2 id="superficie" className="text-[22px] font-semibold tracking-[-0.02em]">
                {copy.xtHeading}
              </h2>
              <p className="mt-1 text-[14px] text-slate">{copy.xtCaption(published.short_name)}</p>
              <div className="pizarra-viz on-panel mx-auto mt-5 max-w-[380px]">
                <ZoneHeatmap cells={xt_zones.cells} orientation="vertical" title={copy.xtHeading} showGrid />
              </div>
              <div className="mx-auto mt-4 flex max-w-[380px] items-center gap-3 text-[13px] text-slate" aria-hidden="true">
                <span>{viz.legend.low}</span>
                <span className="h-1.5 flex-1 rounded-full bg-[linear-gradient(90deg,var(--color-panel-deep),var(--color-team))]" />
                <span>{viz.legend.high}</span>
              </div>
            </section>
          </div>

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

          <Exits rows={exits} current="match" />

          <BoardFooter source={manifest.source} generatedAt={file.generated_at} photoCredit={false} />
        </div>
      </main>
    </div>
  );
}
