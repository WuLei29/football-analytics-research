/**
 * page.tsx — the landing page, `/`. The first page of the Pizarra world.
 *
 * Renders, top to bottom:
 *   1. the floating pill nav, over the stadium photo;
 *   2. the board, rising over the photo's lower edge, with the data's status
 *      locked in its top-right corner;
 *   3. left, the season highlight (the page's <h1>, figures, the xG stroke,
 *      the two actions, the byline); right, the depth index 01–05;
 *   4. the light-blue spot panel: the latest match, tiro a tiro, where a shot draws its
 *      possession (the signature interaction);
 *   5. who is behind it, then provenance and credits.
 * Data:  `manifest.json`, the season's `overview.json`, its `squad.json`, and
 *        the latest match's file (md/WEB_DATA.md §2.1). Everything is read at
 *        build time; the only JavaScript shipped is `ChainPitch`.
 *
 * Direction contract: `.impeccable/surfaces/web-app-page-tsx.md` (24 Oct
 * 2026). Visual system: DESIGN.md. The old landing (coverage cards and a
 * build-status list) became the one status line in the board's corner.
 *
 * Which season: the manifest's default, unless the club has not played in it
 * yet (the weeks before matchday 1), in which case the newest season with
 * matches, with a line saying so. A landing that opens on "0 puntos" for a
 * month a year proves nothing.
 */

import Link from "next/link";

import { Arrow } from "@/components/shell/Arrow";
import { BoardFooter } from "@/components/shell/BoardFooter";
import { ChainPitch, type Chain, type ChainShot } from "@/components/landing/ChainPitch";
import { DepthIndex, type IndexRow } from "@/components/landing/DepthIndex";
import { PillNav } from "@/components/shell/PillNav";
import { SeasonHighlight } from "@/components/landing/SeasonHighlight";
import { ChainThumb, FigureThumb, ShotsThumb } from "@/components/landing/Thumbs";
import { XgStroke } from "@/components/landing/XgStroke";
import { brand } from "@/lib/brand";
import { getManifest, getMatch, getOverview, getSquad } from "@/lib/data/load";
import { longDate } from "@/lib/format";
import { landing, leaderLabels } from "@/lib/labels";
import { routes } from "@/lib/routes";

export default async function HomePage() {
  const manifest = await getManifest();
  const team = manifest.teams.find((t) => t.slug === manifest.default.team)!;

  // The default season, or the newest one the club has actually played in.
  const played = (slug: string) => (team.seasons.find((s) => s.slug === slug)?.matches_played ?? 0) > 0;
  const season =
    (played(manifest.default.season) && manifest.seasons.find((s) => s.slug === manifest.default.season)) ||
    manifest.seasons.find((s) => played(s.slug)) ||
    manifest.seasons[0];
  const fellBack = season.slug !== manifest.default.season;

  const [overview, squad] = await Promise.all([
    getOverview(team.slug, season.slug),
    getSquad(team.slug, season.slug),
  ]);

  // The latest match and the possessions its shots point at.
  const latest = overview.matches.at(-1) ?? null;
  const matchFile = latest ? await getMatch(team.slug, season.slug, latest.match_id) : null;
  const teamSide = matchFile?.match.team_side ?? "home";
  const rivalSide = teamSide === "home" ? "away" : "home";

  const shots: ChainShot[] = (matchFile?.shots ?? []).map((s) => ({
    shot_id: s.shot_id,
    side: s.side,
    name: s.name,
    minute: s.minute,
    x: s.x,
    y: s.y,
    xg: s.xg,
    outcome: s.outcome,
    s: s.s ?? null,
  }));
  const chains: Record<number, Chain> = {};
  for (const shot of shots) {
    const seq = shot.s != null ? matchFile?.sequences[shot.s] : undefined;
    if (shot.side === teamSide && shot.s != null && seq && seq.points.length > 1) {
      chains[shot.s] = {
        points: seq.points,
        events: seq.events,
        passes: seq.passes,
        duration_s: seq.duration_s,
        xt: seq.xt,
      };
    }
  }
  const ourShots = shots.filter((s) => s.side === teamSide);
  const topChain = [...ourShots]
    .filter((s) => s.s != null && chains[s.s])
    .sort((a, b) => b.xg - a.xg)[0];

  // "Elche 3–1 ESP", in home–away order, as a scoreboard reads.
  const latestTitle =
    matchFile &&
    `${matchFile.match.home.short_name} ${matchFile.match.home.goals}–${matchFile.match.away.goals} ${matchFile.match.away.short_name}`;

  const scorer = overview.leaders.find((l) => l.key === "goals")?.rows[0] ?? null;
  const matchesAllSeasons = team.seasons.reduce((n, s) => n + s.matches_played, 0);
  const seasonLabelShort = season.slug.replace("-", "/");

  const rows: IndexRow[] = [
    {
      key: "season",
      title: landing.chapters.season.title,
      dek: landing.chapters.season.dek(team.seasons.length),
      href: routes.season(team.slug, season.slug),
      thumb: (
        <XgStroke
          size="thumb"
          className="w-[76px] sm:w-[112px]"
          values={overview.matches.map((m) => m.xg_for - m.xg_against)}
          title={landing.thumbSeason}
        />
      ),
    },
    {
      key: "match",
      title: landing.chapters.match.title,
      dek: landing.chapters.match.dek(matchesAllSeasons),
      href: routes.matches(team.slug, season.slug),
      thumb: <ShotsThumb shots={ourShots} title={landing.thumbMatch} />,
    },
    {
      key: "sequence",
      title: landing.chapters.sequence.title,
      dek: landing.chapters.sequence.dek,
      href: latest ? routes.match(team.slug, latest.match_id) : null,
      thumb: topChain ? <ChainThumb points={chains[topChain.s!].points} title={landing.thumbSequence} /> : null,
    },
    {
      key: "player",
      title: landing.chapters.player.title,
      dek: landing.chapters.player.dek(squad?.players.length ?? 0),
      href: squad ? routes.squad(team.slug, season.slug) : null,
      thumb: scorer ? (
        <FigureThumb value={String(scorer.value)} unit={leaderLabels.goals.toLowerCase()} name={scorer.name} />
      ) : null,
    },
    {
      key: "method",
      title: landing.chapters.method.title,
      dek: landing.chapters.method.dek,
      href: null,
      thumb: null,
    },
  ];

  return (
    <div className="pizarra">
      <PillNav team={team.slug} season={season.slug} seasonLabel={seasonLabelShort} />

      {/* The stadium. Decorative to a screen reader: the page says what it
          is in words; the credit is in the footer, as CC BY requires. */}
      <div className="relative h-[clamp(240px,42vh,480px)] overflow-hidden bg-team-deep">
        <img
          src={brand.stadiumPhoto.src}
          srcSet={brand.stadiumPhoto.srcSet}
          sizes="100vw"
          width={brand.stadiumPhoto.width}
          height={brand.stadiumPhoto.height}
          alt=""
          fetchPriority="high"
          className="h-full w-full object-cover object-[50%_46%]"
        />
        {/* A wash from the top, so the pill and the bright roof do not fight. */}
        <div className="absolute inset-0 bg-[linear-gradient(180deg,rgb(10_20_40/0.38),rgb(10_20_40/0)_42%)]" />
      </div>

      <main className="relative z-10 mx-3 -mt-14 rounded-t-board bg-board px-4 pt-5 sm:mx-6 sm:px-8 lg:mx-10 lg:-mt-28 lg:px-10 lg:pt-7">
        <div className="mx-auto max-w-[1360px]">
          <p className="mb-4 text-[14px] text-slate lg:text-right">
            {landing.status(season.last_matchday_loaded, longDate(season.through_match_date ?? overview.generated_at.slice(0, 10)))}
          </p>

          <div className="grid gap-6 lg:grid-cols-[1.55fr_1fr] lg:gap-8">
            <SeasonHighlight
              overview={overview}
              seasonHref={routes.season(team.slug, season.slug)}
              latestHref={latest ? routes.match(team.slug, latest.match_id) : null}
              latestTitle={latestTitle}
              note={fellBack ? landing.previousSeasonNote(season.label) : null}
            />
            <DepthIndex rows={rows} />
          </div>

          {matchFile && latest && (
            <section
              aria-labelledby="ultimo"
              className="mt-6 grid gap-8 rounded-panel bg-spot p-6 text-ink sm:p-8 lg:mt-8 lg:grid-cols-[1fr_2.2fr] lg:p-10"
            >
              <div className="flex flex-col">
                <h2 id="ultimo" className="text-[26px] leading-tight font-semibold tracking-[-0.02em]">
                  {landing.latestHeading}
                </h2>
                <p className="mt-2 text-[15px] text-spot-text">
                  {landing.latestMeta(latest.matchday, longDate(latest.date))}
                </p>
                <p className="mt-5 text-[32px] leading-none font-light tracking-[-0.03em]">{latestTitle}</p>
                <p className="mt-6 max-w-[38ch] text-[15px] leading-[1.5] text-spot-text">{landing.latestHint}</p>
                <ul className="mt-6 space-y-1.5 text-[14px] text-spot-text">
                  <li>
                    <svg viewBox="0 0 12 12" width="12" height="12" aria-hidden="true" className="mr-2 inline-block align-[-1px]"><circle cx="6" cy="6" r="4.5" fill="var(--color-team)" stroke="var(--color-team)" strokeWidth="1.2" /></svg>
                    {landing.legendTeam(matchFile.match[teamSide].abbr)}
                  </li>
                  <li>
                    <svg viewBox="0 0 12 12" width="12" height="12" aria-hidden="true" className="mr-2 inline-block align-[-1px]"><circle cx="6" cy="6" r="4.5" fill="none" stroke="var(--color-rival)" strokeWidth="1.2" /></svg>
                    {landing.legendRival(matchFile.match[rivalSide].abbr)}
                  </li>
                  <li>{landing.legendSize}</li>
                  <li>
                    <svg viewBox="0 0 12 12" width="12" height="12" aria-hidden="true" className="mr-2 inline-block align-[-1px]"><circle cx="6" cy="6" r="4.5" fill="none" stroke="var(--color-team)" strokeOpacity="0.4" strokeWidth="1.2" /></svg>
                    {landing.legendUnlinked}
                  </li>
                </ul>
                <Link
                  href={routes.match(team.slug, latest.match_id)}
                  className="mt-8 hidden self-start rounded-full bg-team px-6 py-3.5 text-[15px] font-semibold text-white transition-colors hover:bg-team-deep lg:inline-block"
                >
                  {landing.openMatch} <Arrow className="ml-1" />
                </Link>
              </div>
              <div>
                <ChainPitch
                  shots={shots}
                  chains={chains}
                  teamSide={teamSide}
                  abbr={{ team: matchFile.match[teamSide].abbr, rival: matchFile.match[rivalSide].abbr }}
                />
                {/* The same link, after the pitch on a phone, where the
                    columns stack and the left one's button would come first. */}
                <Link
                  href={routes.match(team.slug, latest.match_id)}
                  className="mt-6 inline-block rounded-full bg-team px-6 py-3.5 text-[15px] font-semibold text-white transition-colors hover:bg-team-deep lg:hidden"
                >
                  {landing.openMatch} <Arrow className="ml-1" />
                </Link>
              </div>
            </section>
          )}

          <section
            aria-labelledby="autor"
            className="mt-6 grid gap-6 rounded-panel bg-panel p-6 sm:p-8 lg:mt-8 lg:grid-cols-[1fr_2.2fr] lg:p-10"
          >
            <h2 id="autor" className="text-[22px] font-semibold tracking-[-0.02em]">
              {landing.authorHeading}
            </h2>
            <div>
              <p className="text-[clamp(28px,3vw,40px)] leading-[1.05] font-light tracking-[-0.03em]">
                {brand.author.name}
              </p>
              <p className="mt-4 max-w-[62ch] text-[17px] leading-[1.55] text-slate">{landing.authorBody}</p>
              <p className="mt-6 flex flex-wrap gap-3">
                <a
                  href={brand.author.linkedin}
                  target="_blank"
                  rel="me noopener"
                  className="rounded-full bg-team px-5 py-3 text-[15px] font-semibold text-white transition-colors hover:bg-team-deep"
                >
                  {landing.linkedin}
                </a>
                <a
                  href={brand.author.github}
                  target="_blank"
                  rel="me noopener"
                  className="rounded-full bg-panel-deep px-5 py-3 text-[15px] font-semibold transition-colors hover:bg-chalk"
                >
                  {landing.github}
                </a>
                <a
                  href={brand.author.repo}
                  target="_blank"
                  rel="noopener"
                  className="rounded-full bg-panel-deep px-5 py-3 text-[15px] font-semibold transition-colors hover:bg-chalk"
                >
                  {landing.repoLink}
                </a>
              </p>
            </div>
          </section>

          <BoardFooter source={manifest.source} generatedAt={manifest.generated_at} />
        </div>
      </main>
    </div>
  );
}
