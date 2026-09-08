/**
 * Masthead — the global shell: the dark hero block and the nav under it.
 *
 * Renders: the 238px hero (radius 22, navy ground, optional photo behind a
 * left-to-right scrim), the crest slot, an eyebrow, the H1 with its accent
 * half, a dek, the season chips, and the section nav.
 * Props: `eyebrow`, `title` ({ plain, accent }), `dek`, `seasons`, and the
 * `team`/`season` slugs the nav builds its links from.
 * Data:  `manifest.json` (md/WEB_DATA.md §4) — the seasons and the team slug.
 *
 * Design source: handoff README, "Global Shell". Every value below (238px,
 * radius 22, #0A2340, the four-stop gradient, the 54x54 crest) is copied from
 * it. The `masthead` nav variant is the one that ships; the `rail` and
 * `topbar` variants in the prototype are exploration and are not built.
 *
 * This is a server component: it runs at build time and ships as HTML. The one
 * interactive part, the season chips, is `SeasonSwitcher` ("use client").
 */

import Link from "next/link";

import { brand } from "@/lib/brand";
import { home, site } from "@/lib/labels";
import { NAV_ITEMS } from "@/lib/routes";
import type { SeasonRef } from "@/lib/data/types";

import { SeasonSwitcher } from "./SeasonSwitcher";

/** The scrim the design paints over the hero photo, left to right. */
const SCRIM =
  "linear-gradient(100deg, rgba(7,26,48,.94) 0%, rgba(7,26,48,.86) 34%," +
  " rgba(7,26,48,.46) 70%, rgba(7,26,48,.30) 100%)";

interface MastheadProps {
  eyebrow: string;
  title: { plain: string; accent: string };
  dek: string;
  seasons: SeasonRef[];
  team: string;
  /** The season slug the nav links point at; defaults to the newest. */
  season?: string;
  /** Which nav item is the current page, if any. */
  active?: string;
}

export function Masthead({
  eyebrow,
  title,
  dek,
  seasons,
  team,
  season,
  active,
}: MastheadProps) {
  const navSeason = season ?? seasons[0]?.slug ?? "";

  return (
    <header>
      {/* The hero is the flex container itself, not a wrapper inside it:
          `justify-between` has to work against the 238px min-height, and a
          percentage height on a child of an auto-height box resolves to auto. */}
      <div
        className="relative flex min-h-[238px] flex-col justify-between gap-8 overflow-hidden rounded-hero bg-navy p-8"
        style={
          brand.heroPhoto
            ? {
                backgroundImage: `${SCRIM}, url(${brand.heroPhoto})`,
                backgroundSize: "cover",
                backgroundPosition: "center",
              }
            : { backgroundImage: SCRIM }
        }
      >
        <div className="flex flex-wrap items-start justify-between gap-6">
          <div className="flex items-start gap-4">
            {/* Crest slot. The club's crest is not shipped (lib/brand.ts);
                a monogram tile occupies the same 54x54 box. */}
            <div
              aria-hidden
              className="flex size-[54px] shrink-0 items-center justify-center rounded-[12px] bg-blue font-mono text-[11px] tracking-[0.08em] text-on-dark"
            >
              {brand.monogram}
            </div>

            <div className="flex flex-col gap-2">
              <p className="eyebrow text-blue2">{eyebrow}</p>
              <h1 className="max-w-[15ch] font-display text-h1 font-bold text-on-dark">
                {title.plain}
                <span className="text-blue2">{title.accent}</span>
              </h1>
            </div>
          </div>

          <p className="dek max-w-[400px] text-on-dark/80">{dek}</p>
        </div>

        <SeasonSwitcher seasons={seasons} />
      </div>

      {/* Section nav. Items whose page does not exist yet are inert text with
          a "pronto" chip rather than dead links — a static export has no
          useful 404. Flip `ready` in lib/routes.ts when a page lands. */}
      <nav className="mt-5 flex flex-wrap items-center gap-x-6 gap-y-3 border-b border-ink pb-3">
        {NAV_ITEMS.map((item) => {
          const isActive = item.key === active;

          if (!item.ready) {
            return (
              <span
                key={item.key}
                className="flex items-center gap-2 font-mono text-[10px] tracking-[0.16em] uppercase text-faint"
              >
                {item.label}
                <span className="rounded-pill bg-track px-1.5 py-0.5 text-[8px] tracking-[0.1em] text-mid">
                  {home.pending}
                </span>
              </span>
            );
          }

          return (
            <Link
              key={item.key}
              href={item.href(team, navSeason)}
              aria-current={isActive ? "page" : undefined}
              className={[
                "font-mono text-[10px] tracking-[0.16em] uppercase",
                isActive ? "text-blue" : "text-mid hover:text-blue",
              ].join(" ")}
            >
              {item.label}
            </Link>
          );
        })}

        <span className="ml-auto font-mono text-[9px] tracking-[0.16em] uppercase text-faint">
          {site.name}
        </span>
      </nav>
    </header>
  );
}
