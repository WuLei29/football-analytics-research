"use client";

/**
 * MatchPicker — the match dropdown in the header of screen 02.
 *
 * Renders: a calendar glyph and a native `<select>` listing every match of
 * the season, newest first, the current one selected. Choosing another
 * navigates to that match page.
 * Props: `team` (slug), `matches` (`overview.matches`) and `current` (the
 * match id of the page).
 * Data:  `overview.json` -> `matches[]` (md/WEB_DATA.md §6) — the same rows
 *        the form strip and the match list read, so the three cannot drift.
 *
 * Why a native select and not a menu of our own: it is keyboard-navigable,
 * it opens as a proper picker on a phone, and it needs no state — the page
 * already knows which match it is. The only JavaScript here is the
 * navigation on change, which is why this file is the client boundary and
 * the page header around it stays static HTML.
 *
 * Why it exists: the match list is the only other way from one match to the
 * next, and reading a season match by match meant a round trip through it
 * every time (review of 11 Sep 2026).
 */

import { useRouter } from "next/navigation";

import { shortDate } from "@/lib/format";
import { matchList, matchPicker as copy } from "@/lib/labels";
import { routes } from "@/lib/routes";
import type { OverviewMatch } from "@/lib/data/types";

export function MatchPicker({
  team,
  matches,
  current,
}: {
  team: string;
  matches: OverviewMatch[];
  current: number;
}) {
  const router = useRouter();

  // Newest first, as on the match list: the match a reader wants is almost
  // always a recent one. `overview.matches` arrives by matchday ascending,
  // which is date order except for a postponed fixture — hence the sort.
  const ordered = [...matches].sort((a, b) => (a.date < b.date ? 1 : -1));

  return (
    <label className="relative flex items-center gap-2">
      <span className="sr-only">{copy.label}</span>

      {/* A calendar glyph drawn inline. The design ships no icon set, so
          this is the one icon the site has, and it stays an outline in the
          text colour rather than a coloured mark. */}
      <svg
        aria-hidden
        viewBox="0 0 16 16"
        width="14"
        height="14"
        className="pointer-events-none absolute left-2.5 text-mid"
        fill="none"
        stroke="currentColor"
        strokeWidth="1.3"
        strokeLinecap="round"
      >
        <rect x="1.5" y="3" width="13" height="11.5" rx="1.5" />
        <path d="M1.5 6.5h13M5 1.5v3M11 1.5v3" />
      </svg>

      <select
        value={current}
        onChange={(e) => router.push(routes.match(team, Number(e.target.value)))}
        className="cursor-pointer appearance-none rounded-chip border border-line bg-card py-1.5 pr-7 pl-8 font-mono text-[10px] tracking-[0.12em] uppercase text-ink hover:border-blue focus:border-blue focus:outline-none"
      >
        {ordered.map((m) => (
          <option key={m.match_id} value={m.match_id}>
            {copy.option(
              m.matchday,
              shortDate(m.date),
              m.is_home ? matchList.homeShort : matchList.awayShort,
              m.opponent_short_name,
              m.goals_for,
              m.goals_against,
            )}
          </option>
        ))}
      </select>

      {/* The chevron, since `appearance-none` removes the native one. */}
      <svg
        aria-hidden
        viewBox="0 0 10 6"
        width="10"
        height="6"
        className="pointer-events-none absolute right-2.5 text-mid"
        fill="none"
        stroke="currentColor"
        strokeWidth="1.3"
        strokeLinecap="round"
      >
        <path d="M1 1l4 4 4-4" />
      </svg>
    </label>
  );
}
