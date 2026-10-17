/**
 * HoverCard — the dark hover card of a pitch mark, as a reusable piece.
 *
 * Renders: an HTML card pinned above a pitch point (below it when there is no
 * room above): a bold title line, up to a few mono lines, and an optional
 * accent line — the player page uses it for "clic: ver la jugada".
 * Props: `x`, `y` (pitch metres, the event's own frame), `orientation`,
 *        `figureHeight` (the measured px height of the positioned wrapper,
 *        0 when unknown), `title`, `lines`, `hint`.
 * Data:  whatever the caller describes; this draws words, not data.
 *
 * The same card `ShotMap` and `DefensiveActions` draw inline, extracted on
 * the player page's event map (16 Oct 2026), where every mode needs one. It
 * must sit in a `position: relative` wrapper whose box is the pitch SVG's,
 * because it is placed by `projectFraction` — a fraction of the SVG's box.
 * The flip rule is `ShotMap`'s: real pixels of headroom, not a fraction,
 * so it holds on a phone and a desktop alike.
 */

import { projectFraction, type Orientation } from "@/lib/viz/pitch";

/** Upper bound of the card's height plus its gap, in px: title + 3 lines. */
const MAX_HEIGHT = 84;

export function HoverCard({
  x,
  y,
  orientation = "horizontal",
  figureHeight,
  title,
  lines,
  hint,
}: {
  x: number;
  y: number;
  orientation?: Orientation;
  figureHeight: number;
  title: string;
  lines: string[];
  hint?: string | null;
}) {
  const at = projectFraction(orientation, x, y);
  const above = figureHeight > 0 ? at.y * figureHeight > MAX_HEIGHT : at.y > 0.34;
  // Near either touchline-end the centred card would run off the figure.
  const shift = at.x < 0.15 ? "-12%" : at.x > 0.85 ? "-88%" : "-50%";

  return (
    <div
      style={{
        position: "absolute",
        left: `${at.x * 100}%`,
        top: `${at.y * 100}%`,
        transform: above ? `translate(${shift},-118%)` : `translate(${shift},18%)`,
        background: "var(--color-ink)",
        borderRadius: 10,
        padding: "6px 9px",
        pointerEvents: "none",
        whiteSpace: "nowrap",
        zIndex: 4,
      }}
    >
      <div
        style={{
          fontFamily: "var(--font-sans)",
          fontSize: 12,
          fontWeight: 600,
          color: "var(--color-on-dark)",
        }}
      >
        {title}
      </div>
      <div
        style={{
          fontFamily: "var(--font-mono)",
          fontSize: 9.5,
          color: "rgba(246,239,230,.72)",
          lineHeight: 1.6,
        }}
      >
        {lines.map((line, i) => (
          <div key={i}>{line}</div>
        ))}
        {hint && <div style={{ color: "var(--color-gold)" }}>{hint}</div>}
      </div>
    </div>
  );
}
