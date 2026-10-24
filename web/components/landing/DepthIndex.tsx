/**
 * DepthIndex — the landing's right column: the site as five numbered levels,
 * each one a step deeper than the last.
 *
 * Renders: a heading, one line saying what the order means, and five rows —
 * Temporada, Partido, Secuencia, Jugador, Método — each a whole-row link with
 * a number, a title, one line, and a small real thumbnail. A row whose page is
 * not built renders inert with "Pronto".
 * Props: `rows`, built by the page from real data (counts, the latest match's
 * shots and chain, the season's top scorer).
 * Data:  none of its own.
 *
 * The numbers are allowed here (the craft floor refuses decorative section
 * numbers) because the order carries information: it is the depth ladder
 * season → match → possession → player, the "escalera" of the landing brief.
 * Hover and keyboard focus step the row one tone darker, turn the title team
 * blue and slide in an arrow — the board's own material, no shadow, no border.
 */

import Link from "next/link";
import type { ReactNode } from "react";

import { landing } from "@/lib/labels";

import { Arrow } from "@/components/shell/Arrow";

export interface IndexRow {
  key: string;
  title: string;
  dek: string;
  href: string | null;
  thumb: ReactNode;
}

export function DepthIndex({ rows }: { rows: IndexRow[] }) {
  return (
    <section aria-labelledby="indice" className="flex flex-col">
      <h2 id="indice" className="px-1 text-[22px] font-semibold tracking-[-0.02em]">
        {landing.indexHeading}
      </h2>
      <p className="mt-1 px-1 text-[15px] text-slate">{landing.indexDek}</p>

      <ol className="mt-5 flex flex-col gap-2">
        {rows.map((row, i) => {
          const body = (
            <>
              <span className="text-[26px] leading-none font-light text-team tabular-nums">
                {String(i + 1).padStart(2, "0")}
              </span>
              <span className="min-w-0">
                <span className="flex items-center gap-2 text-[18px] font-semibold tracking-[-0.01em] transition-colors group-hover:text-team group-focus-visible:text-team">
                  {row.title}
                  {row.href && (
                    <Arrow className="-translate-x-1 text-[0.9em] opacity-0 transition-[opacity,translate] duration-200 group-hover:translate-x-0 group-hover:opacity-100 group-focus-visible:translate-x-0 group-focus-visible:opacity-100" />
                  )}
                </span>
                <span className="block text-[14px] text-slate">{row.dek}</span>
              </span>
              {row.href ? (
                <span className="flex justify-end">{row.thumb}</span>
              ) : (
                <span className="text-[14px] text-slate">{landing.soon}</span>
              )}
            </>
          );
          const cls =
            "grid grid-cols-[36px_1fr_auto] items-center gap-3 rounded-row bg-panel px-4 py-4 sm:grid-cols-[44px_1fr_auto] sm:gap-4 sm:px-5";
          return (
            <li key={row.key}>
              {row.href ? (
                <Link
                  href={row.href}
                  className={`group ${cls} transition-colors hover:bg-panel-deep focus-visible:bg-panel-deep`}
                >
                  {body}
                </Link>
              ) : (
                <div className={cls}>{body}</div>
              )}
            </li>
          );
        })}
      </ol>
    </section>
  );
}
