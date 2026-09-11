"use client";

/**
 * SeasonSwitcher — the season chips in the hero, and the coverage line under
 * them ("Datos hasta la jornada 3 · 3 jornadas completas de 38").
 *
 * Renders: a row of buttons, one per published season, plus one line of meta.
 * Props: `seasons` — `manifest.seasons`, newest first.
 * Data:  `manifest.json` -> `SeasonRef[]` (md/WEB_DATA.md §4), which the export
 *        derives from `silver.competition_seasons` and `silver.matches`.
 *
 * This is the site's first client component, and it is here as much to mark
 * the boundary as to do a job. Everything above it — the page, the masthead,
 * the data read — runs once at build time and ships as HTML. This file ships
 * as JavaScript because it has to answer a click. That is the whole rule
 * (WEB_PLAN.md §3.3, point 3).
 *
 * Two modes, and the difference is the whole point of Phase 5a:
 *
 *   - **`team` given** (every page from screen 01 onwards): the chips are
 *     links to `/{equipo}/temporada/{slug}` and the selection comes from the
 *     URL. This component then keeps no state at all, which is the right
 *     answer — the season is a fact about the page, not about a widget.
 *   - **no `team`** (the landing page, which is not season-scoped): the chips
 *     are buttons and the selection is local, changing only the coverage line
 *     underneath them.
 */

import Link from "next/link";
import { useState } from "react";

import { home } from "@/lib/labels";
import { longDate } from "@/lib/format";
import { routes } from "@/lib/routes";
import type { SeasonRef } from "@/lib/data/types";

export function SeasonSwitcher({
  seasons,
  team,
  current,
}: {
  seasons: SeasonRef[];
  /** Team slug. When given, the chips navigate instead of setting state. */
  team?: string;
  /** The season slug of the current page, when the page is season-scoped. */
  current?: string;
}) {
  const [selected, setSelected] = useState(current ?? seasons[0]?.slug);
  const season =
    seasons.find((s) => s.slug === (current ?? selected)) ?? seasons[0];

  if (!season) return null;

  /** One class list for both modes, so a link and a button cannot drift. */
  const chipClass = (active: boolean) =>
    [
      "rounded-pill border px-3 py-1.5 font-mono text-[10px] tracking-[0.14em] uppercase",
      active
        ? "border-transparent bg-on-dark text-ink"
        : "border-white/25 text-on-dark/75 hover:border-white/60 hover:text-on-dark",
    ].join(" ");

  if (team) {
    return (
      <div className="flex flex-col gap-3">
        <div className="flex flex-wrap gap-2">
          {seasons.map((s) => (
            <Link
              key={s.slug}
              href={routes.season(team, s.slug)}
              aria-current={s.slug === season.slug ? "page" : undefined}
              className={chipClass(s.slug === season.slug)}
            >
              {/* "2025-26" -> "2025/26"; the slug already has the short form. */}
              {s.slug.replace("-", "/")}
            </Link>
          ))}
        </div>
        <Coverage season={season} />
      </div>
    );
  }

  return (
    <div className="flex flex-col gap-3">
      <div className="flex flex-wrap gap-2">
        {seasons.map((s) => {
          const active = s.slug === season.slug;
          return (
            <button
              key={s.slug}
              type="button"
              onClick={() => setSelected(s.slug)}
              aria-pressed={active}
              className={chipClass(active)}
            >
              {/* "2025-26" -> "2025/26"; the slug already has the short form. */}
              {s.slug.replace("-", "/")}
            </button>
          );
        })}
      </div>

      <Coverage season={season} />
    </div>
  );
}

/** How much of the season is loaded: the one line under the chips. */
function Coverage({ season }: { season: SeasonRef }) {
  return (
    <p className="font-mono text-[10px] tracking-[0.12em] uppercase text-on-dark/70">
      {home.matchdaysComplete(
        season.matchdays_complete,
        season.matchdays_scheduled,
      )}
      {season.through_match_date ? (
        <>
          {" · "}
          {home.throughDate(longDate(season.through_match_date))}
        </>
      ) : null}
    </p>
  );
}
