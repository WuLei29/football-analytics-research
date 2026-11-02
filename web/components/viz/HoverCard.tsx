/**
 * HoverCard — the small floating card beside a hovered mark on a chart.
 *
 * Renders: a white card with the float shadow, a bold title, and one or more
 * lines under it, positioned over the chart at the mark. It sits above the
 * mark, or below when the mark is near the top; it anchors left or right
 * instead of centred when the mark is near an edge, so it never leaves the
 * chart. Not interactive (`pointer-events: none`) and hidden from assistive
 * technology: every chart that shows one also has an `aria-live` readout or
 * labelled marks saying the same thing.
 * Props: `x`, `y` — the mark's position as a fraction (0–1) of the chart's
 *        box; `title`; `lines`; optional `accent` (a colour token for the
 *        small dot before the title: who the mark belongs to).
 * Data:  none; the chart passes the strings.
 *
 * Brought back on 1 Nov 2026 at the author's request, beside the readouts
 * (not instead of them). The handoff's hover cards were black; Pizarra has no
 * black fills, so this one is white and raised, like the pill nav.
 *
 * The parent must be `position: relative` and the same box as the chart.
 */

import type { ReactNode } from "react";

export function HoverCard({
  x,
  y,
  title,
  lines,
  accent,
}: {
  x: number;
  y: number;
  title: ReactNode;
  lines: ReactNode[];
  accent?: string;
}) {
  const below = y < 0.3;
  const align = x < 0.18 ? "left" : x > 0.82 ? "right" : "center";
  const tx = align === "left" ? "-12px" : align === "right" ? "calc(-100% + 12px)" : "-50%";
  const ty = below ? "14px" : "calc(-100% - 14px)";

  return (
    <div
      aria-hidden="true"
      className="pointer-events-none absolute z-20 w-max max-w-[260px] rounded-[12px] bg-white px-3.5 py-2.5 text-left shadow-float ring-1 ring-ink/[0.07]"
      style={{ left: `${x * 100}%`, top: `${y * 100}%`, transform: `translate(${tx}, ${ty})` }}
    >
      <p className="flex items-center gap-1.5 text-[14px] leading-snug font-semibold text-ink">
        {accent && <span className="inline-block size-2 shrink-0 rounded-full" style={{ background: accent }} />}
        {title}
      </p>
      {lines.map((line, i) => (
        <p key={i} className="text-[13px] leading-snug text-slate tabular-nums">
          {line}
        </p>
      ))}
    </div>
  );
}
