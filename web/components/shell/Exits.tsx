/**
 * Exits — the numbered exits at the foot of every Pizarra page except the
 * landing: the same five levels as the landing's depth index (Temporada →
 * Partido → Secuencia → Jugador → Método), with the page you are on marked.
 *
 * Renders: a heading and five tiles (stacked on a phone, five across at lg).
 * Each tile is a light team-ink number, an 18px title and one slate line. A
 * built level is a whole-tile link that steps one tone darker on hover and
 * focus, turns its title blue and slides in the arrow; the current level is
 * on panel-deep with "Estás aquí" and `aria-current`; an unbuilt one is inert
 * with "Pronto".
 * Props: `rows` — the five levels, built by the page from real data (counts,
 *        the season's latest match); `current` — the key of this page's level.
 * Data:  none of its own.
 *
 * The numbers are allowed (the craft floor refuses decorative section
 * numbers) because the order carries information: it is the depth ladder
 * of md/REDESIGN_HANDOFF.md, "Rules of the world". No thumbnails here, unlike
 * the landing: at the end of a page they would be a second, smaller copy of
 * charts the reader has just seen.
 */

import Link from "next/link";

import { landing } from "@/lib/labels";

import { Arrow } from "./Arrow";

export interface ExitRow {
  key: string;
  title: string;
  dek: string;
  href: string | null;
}

export function Exits({ rows, current }: { rows: ExitRow[]; current: string }) {
  return (
    <nav aria-labelledby="salidas" className="mt-10 lg:mt-14">
      <h2 id="salidas" className="text-[22px] font-semibold tracking-[-0.02em]">
        {landing.indexHeading}
      </h2>
      <p className="mt-1 text-[15px] text-slate">{landing.indexDek}</p>

      <ol className="mt-5 grid gap-2 lg:grid-cols-5">
        {rows.map((row, i) => {
          const here = row.key === current;
          const body = (
            <>
              <span className="text-[26px] leading-none font-light text-team tabular-nums">
                {String(i + 1).padStart(2, "0")}
              </span>
              <span className="min-w-0">
                <span className="flex items-center gap-2 text-[18px] font-semibold tracking-[-0.01em] transition-colors group-hover:text-team group-focus-visible:text-team">
                  {row.title}
                  {row.href && !here && (
                    <Arrow className="-translate-x-1 text-[0.9em] opacity-0 transition-[opacity,translate] duration-200 group-hover:translate-x-0 group-hover:opacity-100 group-focus-visible:translate-x-0 group-focus-visible:opacity-100" />
                  )}
                </span>
                <span className="block text-[14px] text-slate">
                  {here ? landing.exitsCurrent : row.href ? row.dek : landing.soon}
                </span>
              </span>
            </>
          );
          const cls =
            "grid h-full grid-cols-[36px_1fr] items-center gap-3 rounded-row px-4 py-4 sm:grid-cols-[44px_1fr] sm:px-5 lg:grid-cols-1 lg:items-start lg:gap-4 lg:py-5";
          return (
            <li key={row.key}>
              {here ? (
                <div aria-current="page" className={`${cls} bg-panel-deep`}>
                  {body}
                </div>
              ) : row.href ? (
                <Link
                  href={row.href}
                  className={`group ${cls} bg-panel transition-colors hover:bg-panel-deep focus-visible:bg-panel-deep`}
                >
                  {body}
                </Link>
              ) : (
                <div className={`${cls} bg-panel`}>{body}</div>
              )}
            </li>
          );
        })}
      </ol>
    </nav>
  );
}
