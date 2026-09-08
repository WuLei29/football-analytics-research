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
 * Today the selection only changes the coverage line. From Phase 5a it will
 * also navigate to `/{equipo}/temporada/{slug}`, at which point the selected
 * season comes from the URL and this component keeps no state of its own.
 */

import { useState } from "react";

import { home } from "@/lib/labels";
import { longDate } from "@/lib/format";
import type { SeasonRef } from "@/lib/data/types";

export function SeasonSwitcher({ seasons }: { seasons: SeasonRef[] }) {
  const [selected, setSelected] = useState(seasons[0]?.slug);
  const season = seasons.find((s) => s.slug === selected) ?? seasons[0];

  if (!season) return null;

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
              className={[
                "rounded-pill border px-3 py-1.5 font-mono text-[10px] tracking-[0.14em] uppercase",
                active
                  ? "border-transparent bg-on-dark text-ink"
                  : "border-white/25 text-on-dark/75 hover:border-white/60 hover:text-on-dark",
              ].join(" ")}
            >
              {/* "2025-26" -> "2025/26"; the slug already has the short form. */}
              {s.slug.replace("-", "/")}
            </button>
          );
        })}
      </div>

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
    </div>
  );
}
