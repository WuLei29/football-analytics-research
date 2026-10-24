/**
 * PillNav — the Pizarra world's navigation: one floating pill, fixed to the
 * top of the viewport, over the photo first and over the board as you scroll.
 *
 * Renders: the site name (a link home), the built sections from `NAV_ITEMS`,
 * and the season as a filled pill linking to its overview. On a phone only
 * the name and the season pill stay; the landing's index carries the rest.
 * Props: `team`, `season` (slug), `seasonLabel` ("2026/27"), and `active`,
 *        the `NAV_ITEMS` key of the page it sits on (marked `aria-current`,
 *        team ink on the light-blue spot instead of slate).
 * Data:  none beyond the manifest values the page passes.
 *
 * Raised and frosted (revised 24 Oct 2026, at the author's request): an
 * opaque board-coloured pill vanished over the white board once the photo
 * scrolled away. Now it is white at 78% with a strong backdrop blur, a hairline
 * ring and the `float` shadow, so it reads as a separate object over the
 * photo, the board and the panels alike. It is one of only two raised things
 * in the world (the other: the season figures). A small team disc with a
 * drawn chalk play marks the site name. Sections that are not built (`ready: false`) are left out
 * rather than shown inert, unlike the handoff masthead: a pill has no room to
 * explain "pronto".
 */

import Link from "next/link";

import { landing, site } from "@/lib/labels";
import { NAV_ITEMS, routes } from "@/lib/routes";

export function PillNav({
  team,
  season,
  seasonLabel,
  active,
}: {
  team: string;
  season: string;
  seasonLabel: string;
  active?: string;
}) {
  return (
    <nav
      aria-label={landing.navLabel}
      className="fixed top-3 left-1/2 z-50 flex max-w-[calc(100vw-24px)] -translate-x-1/2 items-center gap-1 rounded-full bg-white/78 py-1.5 pr-1.5 pl-1.5 text-[14px] font-medium shadow-float ring-1 ring-ink/[0.07] backdrop-blur-xl backdrop-saturate-150 sm:top-5"
    >
      <Link
        href={routes.home()}
        className="mr-2 flex items-center gap-2.5 rounded-full py-0.5 pr-2 pl-0.5 font-semibold tracking-[-0.01em] whitespace-nowrap"
      >
        {/* The mark: a team disc with a chalk play drawn on it (a run that
            curves to a dot), the same hand as the data strokes. */}
        <svg viewBox="0 0 32 32" width="30" height="30" aria-hidden="true" className="shrink-0">
          <circle cx="16" cy="16" r="16" fill="var(--color-team)" />
          <path
            d="M9 22c2.5-7 7.5-10 14-11"
            fill="none"
            stroke="white"
            strokeWidth="2.2"
            strokeLinecap="round"
            strokeDasharray="0.1 4.2"
          />
          <circle cx="23" cy="11" r="2.6" fill="white" />
        </svg>
        {site.name}
      </Link>
      {NAV_ITEMS.filter((item) => item.ready).map((item) => (
        <Link
          key={item.key}
          href={item.href(team, season)}
          aria-current={item.key === active ? "page" : undefined}
          className={`hidden rounded-full px-3 py-2 transition-colors md:block ${
            item.key === active ? "bg-spot text-team" : "text-slate hover:bg-panel hover:text-ink"
          }`}
        >
          {item.label}
        </Link>
      ))}
      <Link
        href={routes.season(team, season)}
        className="ml-2 rounded-full bg-team px-4 py-2 font-semibold whitespace-nowrap text-white transition-colors hover:bg-team-deep"
      >
        {seasonLabel}
      </Link>
    </nav>
  );
}
