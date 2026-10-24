/**
 * /{equipo}/partidos/{season} — the plain match list.
 *
 * Renders: one row per match of the season — matchday, date, venue chip,
 * opponent, score badge, xG both ways — each row a link to that match page.
 * Data:  `overview.json` -> `matches[]` (md/WEB_DATA.md §6). No new export.
 *
 * Not a designed screen (WEB_PLAN.md §4). It exists because the form strip on
 * screen 01 reaches the last ten matches and the season has thirty-eight, and
 * a match page nobody can navigate to is a page nobody reads. It borrows the
 * form-strip row style and stays deliberately plain.
 *
 * Server component; no interactivity beyond the links.
 */

import Link from "next/link";

import { Footer } from "@/components/shell/Footer";
import { Masthead } from "@/components/shell/Masthead";
import { PageHeader } from "@/components/shell/PageHeader";
import { getManifest, getOverview, getSeason, getTeam } from "@/lib/data/load";
import { dec, shortDate, signed } from "@/lib/format";
import { matchList as copy, site } from "@/lib/labels";
import { routes } from "@/lib/routes";
import type { OverviewMatch } from "@/lib/data/types";

const RESULT_COLOR: Record<OverviewMatch["result"], string> = {
  W: "var(--color-pos)",
  D: "var(--color-faint)",
  L: "var(--color-neg)",
};

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
  const overview = await getOverview(equipo, season);

  return (
    <>
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
        title={copy.title}
        dek={copy.dek}
      />

      <section className="mt-5">
        {overview.matches.length === 0 ? (
          <p className="dek">{copy.empty}</p>
        ) : (
          <ul className="flex flex-col gap-1.5">
            {/* Newest first: the match a reader wants is almost always the last
                one played, and this page is entered from the form strip. */}
            {[...overview.matches].reverse().map((m) => (
              <li key={m.match_id}>
                <Link
                  href={routes.match(equipo, m.match_id)}
                  className="grid grid-cols-[28px_58px_20px_minmax(0,1fr)_54px_minmax(0,90px)] items-center gap-3 rounded-chip border border-line bg-card px-3 py-2.5 hover:border-blue"
                >
                  <span className="font-mono text-[10px] text-faint">
                    {m.matchday}
                  </span>

                  <span className="font-mono text-[10px] text-mid">
                    {shortDate(m.date)}
                  </span>

                  <span
                    className="rounded-[4px] px-1 py-0.5 text-center font-mono text-[8px] text-on-dark"
                    style={{
                      background: m.is_home
                        ? "var(--color-blue)"
                        : "var(--color-faint)",
                    }}
                    title={m.is_home ? copy.home : copy.away}
                  >
                    {m.is_home ? copy.homeShort : copy.awayShort}
                  </span>

                  <span className="truncate font-sans text-[13px] font-medium">
                    {m.opponent_short_name}
                  </span>

                  <span className="flex items-center gap-1.5">
                    <span
                      className="flex size-[18px] items-center justify-center rounded-[5px] font-mono text-[9px] text-on-dark"
                      style={{ background: RESULT_COLOR[m.result] }}
                    >
                      {m.result}
                    </span>
                    <span className="font-mono text-[11px]">
                      {m.goals_for}–{m.goals_against}
                    </span>
                  </span>

                  {/* xG both ways, then the net figure that says whether the
                      scoreline flattered anyone. */}
                  <span className="text-right font-mono text-[10px] text-mid">
                    {dec(m.xg_for, 2)}–{dec(m.xg_against, 2)}
                    <span
                      className="ml-2"
                      style={{
                        color:
                          m.xg_for - m.xg_against >= 0
                            ? "var(--color-pos)"
                            : "var(--color-neg)",
                      }}
                    >
                      {signed(m.xg_for - m.xg_against, 1)}
                    </span>
                  </span>
                </Link>
              </li>
            ))}
          </ul>
        )}
      </section>

      <Footer source={manifest.source} generatedAt={overview.generated_at} />
    </>
  );
}
