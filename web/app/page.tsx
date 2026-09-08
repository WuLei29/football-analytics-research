/**
 * page.tsx — the landing page, `/`.
 *
 * Renders: the masthead, a card per published season showing how much data is
 * loaded, and the list of sections with their build status.
 * Data:  `manifest.json` only (md/WEB_DATA.md §2.1).
 *
 * This page is the whole data path in miniature, and that is its main job
 * while the rest of the site is being built: JSON written by `src/export/` ->
 * `readData()` at build time -> typed object -> server component -> static
 * HTML. If this page renders correct numbers, the chain works.
 *
 * From Phase 5a, WEB_PLAN.md §4 has `/` redirect to the newest season overview
 * of the default team. Until that page exists there is nowhere to send anyone,
 * so `/` stands on its own.
 */

import { Footer } from "@/components/shell/Footer";
import { Masthead } from "@/components/shell/Masthead";
import { getManifest } from "@/lib/data/load";
import { longDate } from "@/lib/format";
import { home, site } from "@/lib/labels";
import { NAV_ITEMS } from "@/lib/routes";

export default async function HomePage() {
  const manifest = await getManifest();
  const team = manifest.teams.find((t) => t.slug === manifest.default.team)!;

  return (
    <>
      <Masthead
        eyebrow={site.eyebrow}
        title={site.homeTitle}
        dek={site.homeDek}
        seasons={manifest.seasons}
        team={team.slug}
      />

      {/* --- Coverage: one card per published season ----------------------- */}
      <section className="mt-8">
        <SectionHead title={home.coverageHeading} dek={home.coverageDek} />

        <div className="mt-4 grid gap-4 sm:grid-cols-2">
          {manifest.seasons.map((season) => {
            // The club's own coverage, which is not the league's: a side can
            // finish while another still owes a postponed match (§3.5).
            const teamSeason = team.seasons.find((s) => s.slug === season.slug);

            return (
              <article key={season.slug} className="card">
                <div className="flex items-baseline justify-between gap-3 border-b border-ink pb-3">
                  <h3 className="font-display text-h2card font-bold">
                    {season.label}
                  </h3>
                  <span
                    className={[
                      "rounded-pill px-2 py-0.5 font-mono text-[8.5px] tracking-[0.14em] uppercase",
                      season.league_complete
                        ? "bg-pos text-on-dark"
                        : "bg-track text-mid",
                    ].join(" ")}
                  >
                    {season.league_complete
                      ? home.leagueComplete
                      : home.leagueRunning}
                  </span>
                </div>

                <dl className="mt-4 flex flex-col gap-3">
                  <Row
                    label={season.competition_name}
                    value={`${season.num_teams} equipos`}
                  />
                  <Row
                    label="Jornadas completas"
                    value={`${season.matchdays_complete} / ${season.matchdays_scheduled}`}
                  />
                  <Row
                    label={`${team.short_name} · partidos`}
                    value={
                      teamSeason
                        ? home.matchesPlayed(teamSeason.matches_played)
                        : "—"
                    }
                  />
                  <Row
                    label="Último dato"
                    value={
                      season.through_match_date
                        ? longDate(season.through_match_date)
                        : "—"
                    }
                  />
                </dl>
              </article>
            );
          })}
        </div>
      </section>

      {/* --- Sections and what is built ------------------------------------ */}
      <section className="mt-10">
        <SectionHead title={home.pagesHeading} dek={home.pagesDek} />

        <ul className="mt-4 grid gap-2 sm:grid-cols-2 lg:grid-cols-3">
          {NAV_ITEMS.map((item) => (
            <li
              key={item.key}
              className="flex items-center justify-between gap-3 rounded-chip bg-card border border-line px-3.5 py-3"
            >
              <span className="font-sans text-[13px] font-medium">
                {item.label}
              </span>
              <span
                className={[
                  "font-mono text-[8.5px] tracking-[0.14em] uppercase",
                  item.ready ? "text-pos" : "text-faint",
                ].join(" ")}
              >
                {item.ready ? home.ready : home.pending}
              </span>
            </li>
          ))}
        </ul>
      </section>

      <Footer source={manifest.source} generatedAt={manifest.generated_at} />
    </>
  );
}

/* --------------------------------------------------------------------------
 * Two shapes this page repeats. They stay local until a second page needs
 * them (WEB_PLAN.md §3.2: duplicate twice, extract on the third).
 * ------------------------------------------------------------------------ */

function SectionHead({ title, dek }: { title: string; dek: string }) {
  return (
    <div className="flex flex-wrap items-baseline justify-between gap-x-6 gap-y-1 border-b border-line pb-2">
      <h2 className="font-display text-h2card font-bold">{title}</h2>
      <p className="dek max-w-[52ch]">{dek}</p>
    </div>
  );
}

function Row({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex items-baseline justify-between gap-4">
      <dt className="font-sans text-[12px] font-medium text-mid">{label}</dt>
      <dd className="font-mono text-[12px] font-medium text-ink">{value}</dd>
    </div>
  );
}
