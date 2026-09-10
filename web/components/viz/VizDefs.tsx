/**
 * VizDefs — the SVG `<defs>` shared by every chart on a page.
 *
 * Renders: a zero-sized SVG holding the arrowhead markers, and nothing visible.
 * Props: none.
 * Data:  none.
 *
 * Markers are referenced by id (`url(#viz-arrow-blue)`), and an id has to be
 * unique in the document — so they are declared once per page rather than once
 * per chart. Render this component near the top of any page that uses
 * `ProgressionArrows`; the design prototype does exactly the same thing.
 *
 * `markerUnits` defaults to `strokeWidth`, so an arrowhead scales with the line
 * that carries it: the gold carry arrows come out larger than the blue pass
 * arrows without a second marker size, which is the design's intent.
 *
 * The head is 4 stroke-widths long rather than the prototype's 5: with thirty
 * progression arrows on one pitch the heads were the loudest thing in the
 * figure, and they read as direction, not as data.
 */

export function VizDefs() {
  return (
    <svg width={0} height={0} style={{ position: "absolute" }} aria-hidden="true">
      <defs>
        <Arrowhead id="viz-arrow-blue" fill="var(--color-blue)" />
        <Arrowhead id="viz-arrow-gold" fill="var(--color-gold)" />
        <Arrowhead id="viz-arrow-faint" fill="var(--color-faint)" />
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
