/**
 * Club colours on the page: the opponent's colour as the match page's
 * `--color-rival` (WEB_PLAN.md §10.1, DESIGN.md "Club identity").
 *
 * The colours come from the export (`config.CLUB_COLOURS`, WEB_DATA.md §3.8);
 * nothing here names a club. What this file adds is the one rule a club
 * colour cannot be trusted to meet on its own: TEXT in it must read 4.5:1 on
 * the panel and the spot (the latest-match panels). Valencia's orange is
 * 2.7:1 on panel, Athletic's red 3.4:1. Marks (strokes, fills, rings) keep
 * the club's own colour; text gets the same hue
 * mixed toward ink until it passes, as `--color-rival-ink`.
 */

import type { CSSProperties } from "react";

/** The darkest light ground opponent text sits on: spot (globals.css),
    a shade below panel, so text that passes here passes on panel and white. */
const GROUND = "#dfe7f4";
const INK = "#17181c";
const MIN_TEXT_CONTRAST = 4.5;

function rgb(hex: string): [number, number, number] {
  const h = hex.replace("#", "");
  return [0, 2, 4].map((i) => parseInt(h.slice(i, i + 2), 16)) as [number, number, number];
}

function luminance([r, g, b]: [number, number, number]): number {
  const lin = (c: number) => {
    const s = c / 255;
    return s <= 0.03928 ? s / 12.92 : ((s + 0.055) / 1.055) ** 2.4;
  };
  return 0.2126 * lin(r) + 0.7152 * lin(g) + 0.0722 * lin(b);
}

function contrast(a: [number, number, number], b: [number, number, number]): number {
  const [hi, lo] = [luminance(a), luminance(b)].sort((x, y) => y - x);
  return (hi + 0.05) / (lo + 0.05);
}

function hex(c: [number, number, number]): string {
  return `#${c.map((v) => Math.round(v).toString(16).padStart(2, "0")).join("")}`;
}

/** The colour itself when it reads 4.5:1 on spot, else the least ink-mixed
    shade of it that does. */
export function textSafe(color: string): string {
  const base = rgb(color);
  const ground = rgb(GROUND);
  const ink = rgb(INK);
  for (let t = 0; t <= 1; t += 0.05) {
    const mixed = base.map((v, i) => v + (ink[i] - v) * t) as [number, number, number];
    if (contrast(mixed, ground) >= MIN_TEXT_CONTRAST) return hex(mixed);
  }
  return INK;
}

/** The style a match page puts on its root: the opponent becomes the rival. */
export function rivalStyle(color: string): CSSProperties {
  return {
    "--color-rival": color,
    "--color-rival-ink": textSafe(color),
  } as CSSProperties;
}
