/**
 * VizDefs — the SVG `<defs>` shared by every chart on a page.
 *
 * Renders: a zero-sized SVG holding the arrowhead markers, and nothing visible.
 * Props: none.
 * Data:  none.
 *
 * Markers are referenced by id (`url(#viz-arrow-team)`), and an id has to be
 * unique in the document — so they are declared once per page rather than once
 * per chart. Render this component near the top of any page that draws a
 * `SequenceDetail` (the match and player pages, /demo).
 *
 * `markerUnits` defaults to `strokeWidth`, so an arrowhead scales with the line
 * that carries it, without a second marker size. Two inks since the Pizarra
 * move (24 Oct 2026): team for the shot, slate for the passes; the handoff's
 * gold head went with the gold.
 *
 * The head is 4 stroke-widths long rather than the prototype's 5: with thirty
 * progression arrows on one pitch the heads were the loudest thing in the
 * figure, and they read as direction, not as data.
 */

export function VizDefs() {
  return (
    <svg width={0} height={0} style={{ position: "absolute" }} aria-hidden="true">
      <defs>
        <Arrowhead id="viz-arrow-team" fill="var(--color-team)" />
        <Arrowhead id="viz-arrow-slate" fill="var(--color-slate)" />
      </defs>
    </svg>
  );
}

function Arrowhead({ id, fill }: { id: string; fill: string }) {
  return (
    <marker
      id={id}
      viewBox="0 0 10 10"
      refX={9}
      refY={5}
      markerWidth={4}
      markerHeight={4}
      orient="auto-start-reverse"
    >
      <path d="M0,1 L9,5 L0,9 z" fill={fill} />
    </marker>
  );
}
