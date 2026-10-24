/**
 * SeasonHighlight — the landing's lead panel: where the club stands, and the
 * one comparison worth a second look.
 *
 * Renders: the page's only <h1> in two lines (table position and points; then
 * the xG-difference rank, in team blue), one factual line on the record, three
 * figures with their league rank, the match-by-match xG stroke, the two
 * actions ("Ver la temporada", the latest match) and the author's byline.
 * Props: `overview`, the links, and an optional `note` when the page fell back
 * to the previous season.
 * Data:  `overview.json` -> `record`, `kpis` (`points`, `xg_difference`,
 *        `possession_pct`) and `matches` (md/WEB_DATA.md §6).
 *
 * The headline is two facts side by side, built from the data. It never says
 * what they mean: that is a judgement, and judgements are written by hand in
 * `content/narratives.ts`. No eyebrow above the heading (the craft floor bans
 * it); the season and match count live in the line under it.
 */

import Link from "next/link";

import { brand } from "@/lib/brand";
import type { Overview } from "@/lib/data/types";
import { dec, orEmpty, signed } from "@/lib/format";
import { landing } from "@/lib/labels";

import { Arrow } from "@/components/shell/Arrow";
import { XgStroke } from "./XgStroke";

export function SeasonHighlight({
  overview,
  seasonHref,
  latestHref,
  latestTitle,
  note,
}: {
  overview: Overview;
  seasonHref: string;
  latestHref: string | null;
  latestTitle: string | null;
  note: string | null;
}) {
  const kpi = (key: string) => overview.kpis.find((k) => k.key === key) ?? null;
  const points = kpi("points");
  const xg = kpi("xg_difference");
  const possession = kpi("possession_pct");
  const { record, season } = overview;

  const figures = [
    { value: String(record.points), unit: null, label: landing.figurePoints, k: points },
    { value: orEmpty(xg?.value, (v) => signed(v, 1)), unit: null, label: landing.figureXg, k: xg },
    {
      value: orEmpty(possession?.value, (v) => dec(v, 1)),
      unit: "%",
      label: landing.figurePossession,
      k: possession,
    },
  ];
  const xgSeries = overview.matches.map((m) => m.xg_for - m.xg_against);

  return (
    <section className="flex flex-col rounded-panel bg-panel p-6 sm:p-8 lg:p-9">
      <h1 className="text-[clamp(34px,3.9vw,58px)] leading-[1.02] font-semibold tracking-[-0.035em] text-balance">
        {record.league_position != null && landing.headline(record.league_position, record.points)}
        {xg?.rank != null && (
          <>
            <br />
            <span className="text-team">{landing.xgLine(xg.rank)}</span>
          </>
        )}
      </h1>

      <p className="mt-4 max-w-[56ch] text-[17px] leading-[1.5] text-slate">
        {note && <span className="block font-medium text-ink">{note}</span>}
        {landing.record(
          season.matches_played,
          season.label,
          record.won,
          record.drawn,
          record.lost,
          record.goals_for,
          record.goals_against,
        )}
      </p>

      <div className="mt-7 flex flex-wrap items-end gap-x-8 gap-y-6">
        <dl className="grid w-full grid-cols-3 gap-x-4 sm:flex sm:w-auto sm:flex-wrap sm:gap-x-8 sm:gap-y-5">
          {figures.map((f) => (
            <div key={f.label} className="flex flex-col-reverse">
              <dt className="mt-2 text-[13px] leading-snug text-slate sm:text-[14px]">
                {f.label}
                {f.k?.rank != null && (
                  <>
                    {" · "}
                    <span className="whitespace-nowrap">{landing.rankOf(f.k.rank, f.k.peer_n)}</span>
                  </>
                )}
              </dt>
              <dd className="text-[clamp(34px,3.6vw,56px)] leading-[0.9] font-light tracking-[-0.04em]">
                {f.value}
                {f.unit && <span className="ml-1 text-[0.5em] tracking-normal">{f.unit}</span>}
              </dd>
            </div>
          ))}
        </dl>
        <figure className="m-0">
          <XgStroke values={xgSeries} title={landing.trendTitle(xgSeries.length)} />
          <figcaption className="mt-2 text-[14px] text-slate">{landing.trendCaption}</figcaption>
        </figure>
      </div>

      <div className="mt-8 flex flex-wrap gap-3">
        <Link
          href={seasonHref}
          className="rounded-full bg-team px-6 py-3.5 text-[15px] font-semibold text-white transition-colors hover:bg-team-deep"
        >
          {landing.ctaSeason} <Arrow className="ml-1" />
        </Link>
        {latestHref && latestTitle && (
          <Link
            href={latestHref}
            className="rounded-full bg-panel-deep px-6 py-3.5 text-[15px] font-semibold transition-colors hover:bg-chalk"
          >
            {landing.ctaLatest(latestTitle)}
          </Link>
        )}
      </div>

      <p className="mt-auto pt-7 text-[14px] text-slate">
        {landing.byline} <span className="font-semibold text-ink">{brand.author.name}</span>
        {" · "}
        <a href={brand.author.linkedin} className="font-semibold text-team hover:underline" rel="me noopener" target="_blank">
          {landing.linkedin}
        </a>
        {" · "}
        <a href={brand.author.github} className="font-semibold text-team hover:underline" rel="me noopener" target="_blank">
          {landing.github}
        </a>
      </p>
    </section>
  );
}
