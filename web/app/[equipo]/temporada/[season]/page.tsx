/**
 * /{equipo}/temporada/{season} — screen 01, the season overview. Pizarra world.
 *
 * Renders, top to bottom:
 *   1. the floating pill nav (Temporada marked) and the board, no photo;
 *   2. the page's <h1>, two facts (table position and points; the
 *      xG-difference rank in team blue), the record line, the season pills,
 *      and the data's status locked top-right;
 *   3. the five figures as raised cards, then the authored reading in its
 *      own full-width card;
 *   4. the league table beside the light-blue spot panel, "partido a
 *      partido": the form strip and the trend chart, each on a white card;
 *   5. the team profile (24 percentile rows) and the player leaders;
 *   6. the numbered exits and the footer.
 * Data:  `manifest.json`, `overview.json`, `league/{season}/table.json` and
 *        `squad.json` (md/WEB_DATA.md §2.1). All read at build time; the
 *        only JavaScript is the trend chart (`SeasonTrend`).
 *
 * Structure agreed with the author on 24 Oct 2026 (`/impeccable shape`,
 * "reading first"): interpretation frames the charts (PRODUCT.md principle
 * 3), so the reading sits with the figures at the top instead of last. The
 * data and chart encodings are the handoff's; only the interface changed
 * (md/REDESIGN_HANDOFF.md). The fixtures block of the handoff stays absent:
 * no fixture data exists in silver (WEB_PLAN.md §9.4 decision B).
 *
 * Revised the same day after the author's review: figures raised and
 * separated, the reading on its own card, the dark panel replaced by the
 * light-blue spot, green / amber / red for every verdict, and an interactive
 * trend chart in place of the two static rolling charts.
 */

import Link from "next/link";

import { FormStrip } from "@/components/season/FormStrip";
import { KpiStrip } from "@/components/season/KpiStrip";
import { LeagueTable } from "@/components/season/LeagueTable";
import { PlayerLeaders } from "@/components/season/PlayerLeaders";
import { SeasonTrend, type TrendRow } from "@/components/season/SeasonTrend";
import { TeamProfile } from "@/components/season/TeamProfile";
import { Arrow } from "@/components/shell/Arrow";
import { BoardFooter } from "@/components/shell/BoardFooter";
import { Exits, type ExitRow } from "@/components/shell/Exits";
import { PillNav } from "@/components/shell/PillNav";
import { seasonReading } from "@/content/narratives";
import {
  getLeagueTable,
  getManifest,
  getOverview,
  getSeason,
  getSquad,
  getTeam,
} from "@/lib/data/load";
import { longDate, num } from "@/lib/format";
import { season as copy, landing, site } from "@/lib/labels";
import { routes } from "@/lib/routes";

/**
 * Every published (club, season) pair becomes one static page. The manifest is
 * the authority on which pairs exist, so adding a club to `EXPORT_TEAMS` in
 * Python is all it takes to get its pages built.
 */
export async function generateStaticParams() {
  const manifest = await getManifest();
  return manifest.teams.flatMap((team) =>
    team.seasons.map((s) => ({ equipo: team.slug, season: s.slug })),
  );
}

interface PageProps {
  params: Promise<{ equipo: string; season: string }>;
}

export async function generateMetadata({ params }: PageProps) {
  const { equipo, season } = await params;
  const [team, meta] = await Promise.all([getTeam(equipo), getSeason(season)]);
  return {
    title: `${team.short_name} ${meta.label}`,
    description: site.tagline,
  };
}

export default async function SeasonPage({ params }: PageProps) {
  const { equipo, season } = await params;

  const manifest = await getManifest();
  const team = await getTeam(equipo);
  const seasonMeta = await getSeason(season);
  const overview = await getOverview(equipo, season);
  const table = await getLeagueTable(season);
  // Which leader rows can link to a player page: the ones squad.json lists.
  const squad = await getSquad(equipo, season);
  const linkable = new Set(squad?.players.map((p) => p.player_id) ?? []);

  // The published club's own row of the profile block. The file carries all 20
  // clubs so that a comparison later needs no new file (§5).
  const profileValues =
    table.profile.values.find((v) => v.team_id === team.team_id)?.metrics ?? {};

  const reading = seasonReading(equipo, season);
  const { record } = overview;
  const played = overview.season.matches_played;
  const window = overview.rolling.window;
  const xgKpi = overview.kpis.find((k) => k.key === "xg_difference") ?? null;
  const latest = overview.matches.at(-1) ?? null;

  // The seasons this club has pages for, newest first (manifest order).
  const seasons = manifest.seasons.filter((s) => team.seasons.some((t) => t.slug === s.slug));
  const short = (slug: string) => slug.replace("-", "/");

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
      dek: landing.chapters.match.dek(played),
      href: played > 0 ? routes.matches(equipo, season) : null,
    },
    {
      key: "sequence",
      title: landing.chapters.sequence.title,
      dek: landing.chapters.sequence.dek,
      href: latest ? routes.match(equipo, latest.match_id) : null,
    },
    {
      key: "player",
      title: landing.chapters.player.title,
      dek: landing.chapters.player.dek(squad?.players.length ?? 0),
      href: squad ? routes.squad(equipo, season) : null,
    },
    { key: "method", title: landing.chapters.method.title, dek: landing.chapters.method.dek, href: null },
  ];

  // The trend chart's rows: each match, with the rolling mean at its matchday
  // (null before the window fills). Joined here so the client island gets a
  // few kilobytes, not the file.
  const rolling = new Map(overview.rolling.points.map((p) => [p.matchday, p]));
  const trendRows: TrendRow[] = overview.matches.map((m) => ({
    matchday: m.matchday,
    opponent: m.opponent_short_name,
    abbr: m.opponent_abbr,
    isHome: m.is_home,
    goalsFor: m.goals_for,
    goalsAgainst: m.goals_against,
    result: m.result,
    xgFor: m.xg_for,
    xgAgainst: m.xg_against,
    xtFor: m.xt_for,
    xtAgainst: m.xt_against,
    rollXg: rolling.get(m.matchday)?.xg_difference ?? null,
    rollXt: rolling.get(m.matchday)?.xt_difference ?? null,
  }));

  return (
    <div className="pizarra">
      <PillNav team={equipo} season={season} seasonLabel={short(season)} active="season" />

      <main className="mx-auto max-w-[1440px] px-4 pt-[88px] sm:px-8 sm:pt-[104px] lg:px-10">
        <div className="mx-auto max-w-[1360px]">
          {/* --- The heading ------------------------------------------------ */}
          <header className="flex flex-col gap-4 lg:flex-row-reverse lg:items-start lg:justify-between lg:gap-10">
            <p className="text-[14px] text-slate lg:pt-3 lg:text-right">
              {landing.status(
                seasonMeta.last_matchday_loaded,
                longDate(seasonMeta.through_match_date ?? overview.generated_at.slice(0, 10)),
              )}
            </p>
            <div>
              <h1 className="text-[clamp(34px,3.9vw,58px)] leading-[1.02] font-semibold tracking-[-0.035em] text-balance">
                {played > 0 && record.league_position != null ? (
                  <>
                    {landing.headline(record.league_position, record.points)}
                    {xgKpi?.rank != null && (
                      <>
                        <br />
                        <span className="text-team">{landing.xgLine(xgKpi.rank)}</span>
                      </>
                    )}
                  </>
                ) : (
                  copy.titleEmpty(team.short_name, seasonMeta.label)
                )}
              </h1>
              <p className="mt-4 max-w-[62ch] text-[17px] leading-[1.5] text-slate">
                {played > 0
                  ? landing.record(
                      played,
                      seasonMeta.label,
                      record.won,
                      record.drawn,
                      record.lost,
                      record.goals_for,
                      record.goals_against,
                    )
                  : copy.dekEmpty(team.short_name, seasonMeta.label)}
              </p>
              {seasons.length > 1 && (
                <nav aria-label={copy.seasonsNav} className="mt-5 flex flex-wrap gap-2">
                  {seasons.map((s) =>
                    s.slug === season ? (
                      <span
                        key={s.slug}
                        aria-current="page"
                        className="rounded-full bg-team px-4 py-2 text-[14px] font-semibold text-white"
                      >
                        {s.label}
                      </span>
                    ) : (
                      <Link
                        key={s.slug}
                        href={routes.season(equipo, s.slug)}
                        className="rounded-full bg-panel-deep px-4 py-2 text-[14px] font-semibold transition-colors hover:bg-chalk"
                      >
                        {s.label}
                      </Link>
                    ),
                  )}
                </nav>
              )}
            </div>
          </header>

          {/* --- The five figures, raised ---------------------------------- */}
          <section aria-labelledby="cifras" className="mt-8 lg:mt-10">
            <h2 id="cifras" className="sr-only">
              {copy.figuresHeading}
            </h2>
            <KpiStrip kpis={overview.kpis} />
          </section>

          {/* --- The reading, its own card at full width -------------------- */}
          {reading && (
            <section
              aria-labelledby="lectura"
              className="mt-6 grid gap-6 rounded-panel bg-panel p-6 sm:p-8 lg:mt-8 lg:grid-cols-[minmax(220px,1fr)_3fr] lg:gap-10 lg:p-10"
            >
              <div>
                {/* A drawn quotation mark: the reading is the author's voice,
                    the one block on the page that is not data. */}
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
                  // Authored paragraphs have no id of their own; the index is
                  // the identity here.
                  <p key={index} className="mb-4 break-inside-avoid text-[17px] leading-[1.6] last:mb-0">
                    {paragraph}
                  </p>
                ))}
              </div>
            </section>
          )}

          {/* --- Table beside the spot panel -------------------------------- */}
          <div className="mt-6 grid gap-6 lg:mt-8 lg:grid-cols-[minmax(0,1fr)_minmax(0,1.15fr)] lg:gap-8">
            <section aria-labelledby="tabla" className="rounded-panel bg-panel p-6 sm:p-8">
              <h2 id="tabla" className="text-[22px] font-semibold tracking-[-0.02em]">
                {copy.tableHeading}
              </h2>
              <p className="mt-1 text-[14px] text-slate">
                {table.league_complete
                  ? copy.tableCaption(table.competition_name)
                  : copy.tableCaptionRunning(table.competition_name)}
              </p>
              <div className="mt-5">
                <LeagueTable rows={table.table} highlight={team.team_id} season={season} />
              </div>
              <p className="mt-4 text-[13px] text-slate">{copy.zoneLegend}</p>
            </section>

            <section aria-labelledby="partidos" className="flex flex-col gap-4 rounded-panel bg-spot p-4 sm:p-6">
              <div className="flex flex-wrap items-start justify-between gap-x-6 gap-y-3 px-2 pt-2">
                <div>
                  <h2 id="partidos" className="text-[26px] leading-tight font-semibold tracking-[-0.02em]">
                    {copy.spotHeading}
                  </h2>
                  <p className="mt-2 max-w-[46ch] text-[15px] text-spot-text">{copy.spotDek}</p>
                </div>
                {played > 0 && (
                  <Link
                    href={routes.matches(equipo, season)}
                    className="rounded-full bg-team px-5 py-3 text-[15px] font-semibold whitespace-nowrap text-white transition-colors hover:bg-team-deep"
                  >
                    {copy.matchesLinkN(num(played))} <Arrow className="ml-1" />
                  </Link>
                )}
              </div>

              <div className="rounded-row bg-white p-4 sm:p-5">
                <h3 className="text-[18px] font-semibold tracking-[-0.01em]">
                  {copy.formHeadingN(Math.min(played, 10))}
                </h3>
                <div className="mt-3">
                  <FormStrip matches={overview.matches} team={equipo} />
                </div>
                {played > 0 && <p className="mt-3 text-[13px] text-slate">{copy.formLegend}</p>}
              </div>

              <div className="flex-1 rounded-row bg-white p-4 sm:p-5">
                <h3 className="text-[18px] font-semibold tracking-[-0.01em]">{copy.rollingHeading}</h3>
                <div className="mt-3">
                  <SeasonTrend rows={trendRows} window={window} />
                </div>
              </div>
            </section>
          </div>

          {/* --- Profile ---------------------------------------------------- */}
          <section aria-labelledby="perfil" className="mt-10 lg:mt-14">
            <h2 id="perfil" className="text-[22px] font-semibold tracking-[-0.02em]">
              {copy.profileHeading}
            </h2>
            <p className="mt-1 max-w-[70ch] text-[15px] text-slate">{copy.profileCaption}</p>
            <div className="mt-5">
              <TeamProfile metrics={table.profile.metrics} values={profileValues} />
            </div>
          </section>

          {/* --- Leaders ---------------------------------------------------- */}
          <section aria-labelledby="destacados" className="mt-10 lg:mt-14">
            <h2 id="destacados" className="text-[22px] font-semibold tracking-[-0.02em]">
              {copy.leadersHeading}
            </h2>
            <p className="mt-1 text-[15px] text-slate">{copy.leadersCaption}</p>
            <div className="mt-5">
              <PlayerLeaders leaders={overview.leaders} team={equipo} season={season} linkable={linkable} />
            </div>
          </section>

          <Exits rows={exits} current="season" />

          <BoardFooter source={manifest.source} generatedAt={overview.generated_at} photoCredit={false} />
        </div>
      </main>
    </div>
  );
}
