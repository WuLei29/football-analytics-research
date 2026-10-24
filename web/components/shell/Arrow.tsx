/**
 * Arrow — the one arrow of the Pizarra world: a round-capped stroke, drawn,
 * not a Unicode "→" (the craft floor refuses glyphs as icons).
 *
 * Renders: an inline 1em SVG in `currentColor`, hidden from screen readers;
 * the link it sits in already says where it goes.
 * Props: `className` for position and transitions.
 * Data:  none.
 *
 * Stroke 2 at a 16 px box, round caps and joins: the same hand as the data
 * strokes on the pitches, so an action and a possession look drawn alike.
 */

export function Arrow({ className = "" }: { className?: string }) {
  return (
    <svg
      viewBox="0 0 16 16"
      width="1em"
      height="1em"
      aria-hidden="true"
      className={`inline-block shrink-0 align-[-0.125em] ${className}`}
      fill="none"
      stroke="currentColor"
      strokeWidth={2}
      strokeLinecap="round"
      strokeLinejoin="round"
    >
      <path d="M2.5 8h11M9 3.5 13.5 8 9 12.5" />
    </svg>
  );
}
