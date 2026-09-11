/**
 * Legend — the mark legend the design puts under a figure, and the LOW→HIGH
 * ramp it puts under a heat layer.
 *
 * Renders: real marks in HTML (a dot, a ring, a bar, a dashed rule, a curve, a
 * diamond) beside their label, either as one wrapping row (`items`) or as one
 * named row per group (`rows`).
 * Props: `items` or `rows`; every label is a string the caller took from
 * `lib/labels.ts`, so this file holds no Spanish.
 * Data:  none.
 *
 * Why HTML rather than SVG `<text>`: the handoff's rule for anything a reader
 * consumes (WEB_PLAN.md §3.2). Inside a pitch `viewBox` a 9px label is 9 user
 * units, which is 2 metres of pitch and scales with the figure; in HTML it is
 * 9 real pixels whatever the pitch is rendered at.
 *
 * `rows` exists for the shot map, where the same three marks appear once per
 * team: six marks in one wrapping row put the line break wherever the
 * container happens to end, which is never the team boundary.
 *
 * Written for `/demo` in Phase 4 and lifted out of it when screen 02 became
 * its third caller (WEB_PLAN.md §3.2: duplicate twice, extract on the third).
 */

export interface LegendItem {
  /** A filled dot. */
  swatch?: string;
  /** A hollow dot with this outline. */
  outline?: string;
  /** A 14x2 rule — a pass, an arrow, a series. */
  bar?: string;
  /** A dashed rule — the carry mark. */
  dotted?: string;
  /** A curved rule — the cross mark; the colour is its origin dot. */
  curved?: string;
  /** Rotates the swatch 45° — the defensive-action mark. */
  diamond?: boolean;
  text: string;
}

export function Legend({
  items,
  rows,
}: {
  items?: LegendItem[];
  /** One line per group, each with a name in the gutter. */
  rows?: { label: string; items: LegendItem[] }[];
}) {
  if (rows) {
    return (
      <div className="label mt-2 flex flex-col gap-[5px]">
        {rows.map((row) => (
          <div key={row.label} className="flex flex-wrap items-center gap-x-3 gap-y-[5px]">
            {/* Fixed gutter so the first mark of every row starts at the same
                x — the alignment is what makes the block read as a table. */}
            <span style={{ minWidth: 68, color: "var(--color-ink)", fontWeight: 600 }}>
              {row.label}
            </span>
            <LegendMarks items={row.items} />
          </div>
        ))}
      </div>
    );
  }

  return (
    <div className="label mt-2 flex flex-wrap gap-3">
      <LegendMarks items={items ?? []} />
    </div>
  );
}

/** The marks themselves, shared by both arrangements. */
function LegendMarks({ items }: { items: LegendItem[] }) {
  return (
    <>
      {items.map((item) => (
        <span key={item.text} className="flex items-center gap-[5px]">
          {item.dotted ? (
            <span
              style={{
                display: "inline-block",
                width: 14,
                height: 0,
                borderTop: `2px dotted ${item.dotted}`,
              }}
            />
          ) : item.curved ? (
            <svg width={14} height={9} viewBox="0 0 14 9" aria-hidden="true">
              <path d="M1,8 Q7,0 13,5" fill="none" stroke="var(--color-mid)" strokeWidth={1.2} />
              <circle cx={1} cy={8} r={1.6} fill={item.curved} />
            </svg>
          ) : (
            <span
              style={{
                display: "inline-block",
                width: item.bar ? 14 : 9,
                height: item.bar ? 2 : 9,
                borderRadius: item.bar || item.diamond ? 0 : "50%",
                transform: item.diamond ? "rotate(45deg)" : undefined,
                background: item.swatch ?? item.bar ?? "transparent",
                border: item.outline ? `1px solid ${item.outline}` : undefined,
              }}
            />
          )}
          {item.text}
        </span>
      ))}
    </>
  );
}

/**
 * The LOW → HIGH gradient under a heat layer. The ramp is the same blue at the
 * same two opacities `heatOpacity` maps a cell to, so the legend and the cells
 * are one scale rather than two that look alike.
 */
export function Ramp({ low, high }: { low: string; high: string }) {
  return (
    <div className="mt-2 flex items-center gap-[6px]">
      <span className="label">{low}</span>
      <div
        style={{
          flex: 1,
          height: 8,
          background: "linear-gradient(90deg,rgba(11,76,158,.06),rgba(11,76,158,1))",
        }}
      />
      <span className="label">{high}</span>
    </div>
  );
}
