/**
 * ShotLegend — the key of a shot map, as real marks with one or two words
 * each, set in the corner under the pitch.
 *
 * Renders: a right-aligned row of glyphs: goal (solid), on target (half),
 * off target or blocked (hollow) in team blue; the rival's outline in rival
 * red with its abbreviation; three growing circles for "size = xG"; and,
 * when `unlinked` is set, the dimmed mark of a shot with no drawable chain.
 * Props: `rival` (the opponent's abbreviation), `unlinked` (landing only).
 * Data:  none; it mirrors the encoding of `MatchShots` and `ChainPitch`
 *        (same fills, same 0.45 half fill, same 0.4 dimming).
 *
 * Replaces the sentence legend ("relleno = gol, medio = a puerta, hueco =
 * fuera") the two spot panels carried in their left column (1 Nov 2026):
 * the author found the marks read faster than the words, and a key belongs
 * next to the figure it explains. Used by the landing and the match list.
 */

import type { ReactNode } from "react";

import { shotLegend as L } from "@/lib/labels";

const dot = (fillOpacity: number, color = "var(--color-team)", opacity = 1) => (
  <svg viewBox="0 0 12 12" width="12" height="12" aria-hidden="true" className="mr-1.5 inline-block shrink-0" opacity={opacity}>
    <circle cx="6" cy="6" r="4.5" fill={color} fillOpacity={fillOpacity} stroke={color} strokeWidth="1.2" />
  </svg>
);

const sizes = (
  <svg viewBox="0 0 30 12" width="30" height="12" aria-hidden="true" className="mr-1.5 inline-block shrink-0">
    {[
      { cx: 3, r: 2 },
      { cx: 11, r: 3.5 },
      { cx: 23, r: 5.2 },
    ].map((c) => (
      <circle key={c.cx} cx={c.cx} cy="6" r={c.r} fill="none" stroke="var(--color-spot-text)" strokeWidth="1" />
    ))}
  </svg>
);

export function ShotLegend({ rival, unlinked = false }: { rival: string; unlinked?: boolean }) {
  const items: ReactNode[] = [
    <>{dot(1)}{L.goal}</>,
    <>{dot(0.45)}{L.onTarget}</>,
    <>{dot(0)}{L.offTarget}</>,
    <>{dot(0, "var(--color-rival)")}{rival}</>,
    <>{sizes}{L.size}</>,
    ...(unlinked ? [<>{dot(0.45, "var(--color-team)", 0.4)}{L.unlinked}</>] : []),
  ];

  return (
    <ul aria-label={L.label} className="mt-3 flex flex-wrap justify-end gap-x-4 gap-y-1 text-[13px] text-spot-text">
      {items.map((item, i) => (
        <li key={i} className="flex items-center whitespace-nowrap">
          {item}
        </li>
      ))}
    </ul>
  );
}
