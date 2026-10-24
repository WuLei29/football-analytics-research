/**
 * /{equipo}/partidos/{season} — the match list. Pizarra world.
 *
 * Renders, top to bottom:
 *   1. the floating pill nav (Partidos marked) and the board, no photo;
 *   2. the page's <h1> (how many matches; the record in team blue), the goals
 *      and xG line, the season pills, and the data's status locked top-right;
 *   3. the light-blue spot panel: the latest match, its score, result and xG
 *      pair beside a map of every shot, and the way into it;
 *   4. every match as a whole-row link, newest first (`MatchIndex`);
 *   5. the numbered exits and the footer.
 * Data:  `manifest.json`, `overview.json` -> `matches[]` (md/WEB_DATA.md §6)
 *        and each match file's `shots[]` (§7.1). All read at build time; the
 *        page ships no JavaScript of its own.
 *
 * Structure agreed with the author on 24 Oct 2026 (`/impeccable shape`): the
 * latest match on the spot (the reader nearly always wants the last one
 * played), a shot pitch on every row (the depth behind a scoreline, PRODUCT.md
 * principle 1), one flat list without filters. The fields and encodings are
 * the handoff row's and the form strip's; only the interface changed
 * (md/REDESIGN_HANDOFF.md).
 */

import Link from "next/link";

import { MatchIndex, type MatchIndexRow } from "@/components/matches/MatchIndex";
import { MatchShots } from "@/components/matches/MatchShots";
import { Arrow } from "@/components/shell/Arrow";
import { BoardFooter } from "@/components/shell/BoardFooter";
import { Exits, type ExitRow } from "@/components/shell/Exits";
import { PillNav } from "@/components/shell/PillNav";
import { getManifest, getMatch, getOverview, getSeason, getSquad, getTeam } from "@/lib/data/load";
import { dec, longDate, signed } from "@/lib/format";
import { matchList as copy, landing, season as seasonCopy, site } from "@/lib/labels";
import { routes } from "@/lib/routes";

const DISC = {
  W: "bg-good text-white",
  D: "bg-fair-fill text-ink",
  L: "bg-bad text-white",
} as const;

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
    title: `Partidos ${meta.label} · ${team.short_name}`,
    description: site.tagline,
  };
}

export default async function MatchListPage({ params }: PageProps) {
  const { equipo, season } = await params;

  const manifest = await getManifest();
  const team = await getTeam(equipo);
  const seasonMeta = await getSeason(season);
  const overview = await getOverview(equipo, season);
  const squad = await getSquad(equipo, season);

  // Every match file, for its shots. Read at build time; only the drawn
  // circles reach the page.
  const files = await Promise.all(overview.matches.map((m) => getMatch(equipo, season, m.match_id)));
  const rows: MatchIndexRow[] = overview.matches.map((m, i) => {
    const file = files[i];
    const side = file?.match.team_side ?? (m.is_home ? "home" : "away");
    return { match: m, shots: (file?.shots ?? []).filter((s) => s.side === side) };
  });

  const played = overview.matches.length;
  const latest = overview.matches.at(-1) ?? null;
  const latestFile = files.at(-1) ?? null;
  const teamSide = latestFile?.match.team_side ?? "home";
  const rivalSide = teamSide === "home" ? "away" : "home";

  const { record } = overview;
  const xgFor = overview.matches.reduce((t, m) => t + m.xg_for, 0);
  const xgAgainst = overview.matches.reduce((t, m) => t + m.xg_against, 0);

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
      href: routes.matches(equipo, season),
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

  // "Espanyol 1–3 Elche", in home–away order, as a scoreboard reads.
  const scoreline =
    latestFile &&
    `${latestFile.match.home.short_name} ${latestFile.match.home.goals}–${latestFile.match.away.goals} ${latestFile.match.away.short_name}`;
  const latestShots = latestFile?.shots ?? [];
  const latestXgd = latest ? latest.xg_for - latest.xg_against : 0;

  return (
    <div className="pizarra">
      <PillNav team={equipo} season={season} seasonLabel={short(season)} active="matches" />

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
                {played > 0 ? (
                  <>
                    {copy.title(played, seasonMeta.label)}
                    <br />
                    <span className="text-team">{copy.recordLine(record.won, record.drawn, record.lost)}</span>
                  </>
                ) : (
                  copy.titleEmpty(seasonMeta.label)
                )}
              </h1>
              <p className="mt-4 max-w-[62ch] text-[17px] leading-[1.5] text-slate">
                {played > 0
                  ? copy.lead(record.goals_for, record.goals_against, dec(xgFor, 1), dec(xgAgainst, 1))
                  : copy.leadEmpty}
              </p>
              {seasons.length > 1 && (
                <nav aria-label={seasonCopy.seasonsNav} className="mt-5 flex flex-wrap gap-2">
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
                        href={routes.matches(equipo, s.slug)}
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

          {/* --- The latest match, on the spot ------------------------------ */}
          {latest && latestFile && (
            <section
              aria-labelledby="ultimo"
              className="mt-8 grid gap-8 rounded-panel bg-spot p-6 sm:p-8 lg:mt-10 lg:grid-cols-[1fr_2.2fr] lg:p-10"
            >
              <div className="flex flex-col">
                <h2 id="ultimo" className="text-[26px] leading-tight font-semibold tracking-[-0.02em]">
                  {copy.latestHeading}
                </h2>
                <p className="mt-2 text-[15px] text-spot-text">
                  {copy.latestMeta(latest.matchday, longDate(latest.date), latest.is_home)}
                </p>
                <p className="mt-6 flex items-center gap-3">
                  <span
                    className={`flex size-8 shrink-0 items-center justify-center rounded-full text-[14px] font-semibold ${DISC[latest.result]}`}
                    aria-hidden="true"
                  >
                    {seasonCopy.resultLetter[latest.result]}
                  </span>
                  <span className="text-[32px] leading-[1.05] font-light tracking-[-0.03em] text-balance">{scoreline}</span>
                  <span className="sr-only">{seasonCopy.resultWord[latest.result]}</span>
                </p>
                <p className="mt-6 text-[14px] text-spot-text">{copy.xgPair}</p>
                <p className="mt-1 flex items-baseline gap-3">
                  <span className="text-[32px] leading-none font-light tracking-[-0.03em] tabular-nums">
                    <span className="text-team">{dec(latest.xg_for, 2)}</span>
                    <span className="text-spot-text">–</span>
                    <span className="text-rival">{dec(latest.xg_against, 2)}</span>
                  </span>
                  <span className={`text-[17px] font-medium tabular-nums ${latestXgd >= 0 ? "text-good" : "text-bad"}`}>
                    {signed(latestXgd, 2)}
                  </span>
                </p>
                <ul className="mt-6 space-y-1.5 text-[14px] text-spot-text">
                  <li>
                    <svg viewBox="0 0 12 12" width="12" height="12" aria-hidden="true" className="mr-2 inline-block align-[-1px]"><circle cx="6" cy="6" r="4.5" fill="var(--color-team)" stroke="var(--color-team)" strokeWidth="1.2" /></svg>
                    {copy.legendTeam(latestFile.match[teamSide].abbr)}
                  </li>
                  <li>
                    <svg viewBox="0 0 12 12" width="12" height="12" aria-hidden="true" className="mr-2 inline-block align-[-1px]"><circle cx="6" cy="6" r="4.5" fill="none" stroke="var(--color-rival)" strokeWidth="1.2" /></svg>
                    {copy.legendRival(latestFile.match[rivalSide].abbr)}
                  </li>
                  <li>{copy.legendSize}</li>
                </ul>
                <Link
                  href={routes.match(equipo, latest.match_id)}
                  className="mt-8 hidden self-start rounded-full bg-team px-6 py-3.5 text-[15px] font-semibold text-white transition-colors hover:bg-team-deep lg:inline-block"
                >
                  {landing.openMatch} <Arrow className="ml-1" />
                </Link>
              </div>
              <div className="lg:self-center">
                <MatchShots
                  shots={latestShots}
                  teamSide={teamSide}
                  lineColor="var(--color-spot-line)"
                  title={copy.latestPitchLabel(
                    latestShots.filter((s) => s.side === teamSide).length,
                    latestShots.filter((s) => s.side !== teamSide).length,
                  )}
                />
                {/* The same link after the pitch on a phone, where the columns
                    stack and the left one's button would come first. */}
                <Link
                  href={routes.match(equipo, latest.match_id)}
                  className="mt-6 inline-block rounded-full bg-team px-6 py-3.5 text-[15px] font-semibold text-white transition-colors hover:bg-team-deep lg:hidden"
                >
                  {landing.openMatch} <Arrow className="ml-1" />
                </Link>
              </div>
            </section>
          )}

          {/* --- Every match ------------------------------------------------ */}
          <section aria-labelledby="todos" className="mt-10 lg:mt-14">
            <h2 id="todos" className="text-[22px] font-semibold tracking-[-0.02em]">
              {copy.listHeading}
            </h2>
            {played > 0 ? (
              <>
                <p className="mt-1 max-w-[70ch] text-[15px] text-slate">{copy.listDek}</p>
                <div className="mt-5">
                  <MatchIndex rows={rows} team={equipo} />
                </div>
              </>
            ) : (
              <p className="mt-1 text-[15px] text-slate">{copy.empty}</p>
            )}
          </section>

          <Exits rows={exits} current="match" />

          <BoardFooter source={manifest.source} generatedAt={overview.generated_at} photoCredit={false} />
        </div>
      </main>
    </div>
  );
}
