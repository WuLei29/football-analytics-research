"use client";

/**
 * MetricToggle — the xT / VAEP switch, with the definition of each on hover.
 *
 * Renders: a mono label ("ORDENAR POR") and two segmented buttons. Hovering
 * or focusing a button opens a card under it with the metric's full name, the
 * one-line answer and the paragraph from `metricDefinitions` in
 * `lib/labels.ts` — the only place those definitions live, so every selector
 * on the site explains the two metrics with the same words.
 * Props: `value`, `onChange`.
 * Data:  none.
 *
 * The card is CSS-only (`group-hover` / `group-focus-within`), so a keyboard
 * reader tabbing onto a button gets the same text as a mouse does, and there
 * is no state to keep for something that is purely a reading aid. It is
 * anchored to the RIGHT edge of its button: the toggle sits at the right end
 * of a section head, and a left-anchored 300 px card would run off the page.
 */

import { match, metricDefinitions, type SequenceMetric } from "@/lib/labels";

const METRICS: SequenceMetric[] = ["xt", "vaep"];

export function MetricToggle({
  value,
  onChange,
}: {
  value: SequenceMetric;
  onChange: (metric: SequenceMetric) => void;
}) {
  return (
    <div className="flex items-center gap-2">
      <span className="label">{match.sequencesSortLabel}</span>

      <div
        role="radiogroup"
        className="flex overflow-visible rounded-[6px] border border-line"
      >
        {METRICS.map((metric) => {
          const def = metricDefinitions[metric];
          const active = metric === value;

          return (
            <div key={metric} className="group relative">
              <button
                type="button"
                role="radio"
                aria-checked={active}
                onClick={() => onChange(metric)}
                className="font-mono"
                style={{
                  fontSize: 10,
                  letterSpacing: "0.08em",
                  padding: "4px 10px",
                  cursor: "pointer",
                  color: active ? "var(--color-paper)" : "var(--color-mid)",
                  background: active ? "var(--color-blue)" : "transparent",
                }}
              >
                {def.abbr}
              </button>

              {/* The definition. `hidden` until the button is hovered or
                  focused; `z-20` so it paints over the card below it. */}
              <div
                role="tooltip"
                className="card absolute right-0 top-full z-20 mt-1 hidden w-[300px] text-left group-hover:block group-focus-within:block"
                style={{ padding: "10px 12px" }}
              >
                <p className="font-display text-[13px] font-bold">
                  {def.abbr} · {def.name}
                </p>
                <p
                  className="font-sans"
                  style={{ fontSize: 12.5, marginTop: 4, color: "var(--color-ink)" }}
                >
                  {def.short}
                </p>
                <p
                  className="font-sans"
                  style={{
                    fontSize: 12,
                    lineHeight: 1.45,
                    marginTop: 6,
                    color: "var(--color-mid)",
                  }}
                >
                  {def.long}
                </p>
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}
