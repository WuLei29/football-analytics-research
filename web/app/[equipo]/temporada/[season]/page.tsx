/**
 * /{equipo}/temporada/{season} — screen 01, the season overview.
 *
 * Renders: the masthead, then the seven blocks of the design in order — page
 * header, KPI strip, league table beside the form strip and the two rolling
 * charts, team profile, player leaders, and the reading of the season.
 * Data:  `overview.json` and `league/{season}/table.json`
 *        (md/WEB_DATA.md §2.1). Both are read from disk at BUILD time; by the
 *        time a browser sees this page the numbers are already in the HTML
 *        (WEB_PLAN.md §3.5).
 *
 * Server component. The only JavaScript this page ships is the season chips
 * in the masthead; every figure on it is static SVG.
 *
 * The fixtures block of the handoff is deliberately absent: no fixture data
 * exists in silver, so the footer row is the reading of the season at full
 * width (WEB_PLAN.md §9.4 decision B).
 */

import Link from "next/link";

import { FormStrip } from "@/components/season/FormStrip";
import { KpiStrip } from "@/components/season/KpiStrip";
import { LeagueTable } from "@/components/season/LeagueTable";
import { PlayerLeaders } from "@/components/season/PlayerLeaders";
import { TeamProfile } from "@/components/season/TeamProfile";
import { Footer } from "@/components/shell/Footer";
import { Masthead } from "@/components/shell/Masthead";
import { Narrative } from "@/components/shell/Narrative";
import { PageHeader } from "@/components/shell/PageHeader";
import { SectionHead } from "@/components/shell/SectionHead";
import { RollingArea } from "@/components/viz/RollingArea";
import { seasonReading } from "@/content/narratives";
import { getLeagueTable, getManifest, getOverview, getSeason, getTeam } from "@/lib/data/load";
import { num } from "@/lib/format";
import { season as copy, site, viz } from "@/lib/labels";
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

  // The published club's own row of the profile block. The file carries all 20
  // clubs so that a comparison later needs no new file (§5).
  const profileValues =
    table.profile.values.find((v) => v.team_id === team.team_id)?.metrics ?? {};

  const reading = seasonReading(equipo, season);
  const played = overview.season.matches_played;
  const window = overview.rolling.window;

  return (
    <>
      <Masthead
        eyebrow={site.eyebrow}
        title={site.homeTitle}
        dek={site.homeDek}
        seasons={manifest.seasons}
        team={equipo}
        season={season}
        active="season"
      />

      <PageHeader
        eyebrow={copy.eyebrow}
        title={copy.title}
        dek={
          played > 0
            ? copy.dek(team.short_name, seasonMeta.label, num(played))
            : copy.dekEmpty(team.short_name, seasonMeta.label)
        }
      />

      {/* --- Block 2: the five KPI cards ---------------------------------- */}
      <section className="mt-5">
        <KpiStrip kpis={overview.kpis} />
      </section>

      {/* --- Block 3: table beside form and the two trends ---------------- */}
      <section className="mt-4 grid gap-4 lg:grid-cols-[minmax(0,1.05fr)_minmax(0,1fr)]">
        <div className="card">
          <SectionHead
            title={copy.tableHeading}
            caption={
              table.league_complete
                ? copy.tableCaption(table.competition_name)
                : copy.tableCaptionRunning(table.competition_name)
            }
            rule="none"
          />
          <LeagueTable
            rows={table.table}
            highlight={team.team_id}
            season={season}
          />
        </div>

        <div className="flex flex-col gap-4">
          <div className="card">
            <SectionHead
              title={copy.formHeading}
              caption={copy.formCaption}
              rule="none"
              as="h3"
              right={
                <Link
                  href={routes.matches(equipo, season)}
                  className="font-mono text-[9px] tracking-[0.14em] uppercase text-blue hover:underline"
                >
                  {copy.matchesLinkN(num(played))}
                </Link>
              }
            />
            <FormStrip matches={overview.matches} team={equipo} />
          </div>

          {/* The two rolling charts fill the height of the table column, as
              in the design: the card is the column's flex-1, and inside it the
              two charts are spread over whatever height the table leaves,
              with the viewBox tall enough (250 against the demo's 182) that
              beside a twenty-row table the gap left over is small. They are
              empty until the club has played `window` matches, which is the
              right state for the first month of a season rather than one noisy
              point. */}
          <div className="card flex flex-1 flex-col">
            <SectionHead
              title={copy.rollingHeading}
              caption={copy.rollingCaption(window)}
              rule="none"
              as="h3"
            />
            {overview.rolling.points.length > 0 ? (
              <div className="mt-2 flex flex-1 flex-col justify-around gap-3">
                <RollingArea
                  points={overview.rolling.points.map((p) => ({
                    matchday: p.matchday,
                    value: p.xg_difference,
                  }))}
                  height={250}
                  axisLabel={viz.axis.rollingXgd}
                  xCaption={viz.axis.matchday}
                />
                <RollingArea
                  points={overview.rolling.points.map((p) => ({
                    matchday: p.matchday,
                    value: p.xt_difference,
                  }))}
                  height={250}
                  axisLabel={viz.axis.rollingXt}
                  xCaption={viz.axis.matchday}
                />
              </div>
            ) : (
              <p className="dek mt-3">{copy.rollingEmpty(window)}</p>
            )}
          </div>
        </div>
      </section>

      {/* --- Block 5: the four profile cards ------------------------------ */}
      <section className="mt-8">
        <SectionHead title={copy.profileHeading} caption={copy.profileCaption} />
        <TeamProfile metrics={table.profile.metrics} values={profileValues} />
      </section>

      {/* --- Block 6: the four leader boxes ------------------------------- */}
      <section className="mt-8">
        <SectionHead title={copy.leadersHeading} caption={copy.leadersCaption} />
        <PlayerLeaders leaders={overview.leaders} team={equipo} />
      </section>

      {/* --- Block 7: the reading, at full width -------------------------- */}
      {reading && (
        <section className="mt-8">
          <Narrative eyebrow={copy.readingEyebrow} narrative={reading} />
        </section>
      )}

      <Footer source={manifest.source} generatedAt={overview.generated_at} />
    </>
  );
}
