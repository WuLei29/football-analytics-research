/**
 * SequenceLegend — the marks key under a `SequenceBrowser` pitch.
 *
 * Renders: the action vocabulary of `SequenceDetail` (start ring, regain,
 * finisher, pass, carry, clearance, cross, take-on, shot) and the italic
 * hover instruction. With `focus`, one more item first: the gold node of
 * the player the page is about.
 * Props: `focus` (that player's surname), optional.
 * Data:  none.
 *
 * Extracted from the match page on 16 Oct 2026 when the player page started
 * drawing its chains with the same browser: one key for one mark grammar.
 */

import { viz } from "@/lib/labels";

import { Legend, type LegendItem } from "./Legend";

export function SequenceLegend({ focus }: { focus?: string }) {
  const items: LegendItem[] = [
    { halo: "var(--color-blue)", text: viz.legend.sequenceStart },
    { swatch: "var(--color-blue)", diamond: true, text: viz.legend.sequenceRegain },
    { swatch: "var(--color-blue)", text: viz.legend.finisher },
    { bar: "var(--color-mid)", text: viz.legend.pass },
    { dotted: "var(--color-mid)", text: viz.legend.carry },
    { dashed: "var(--color-mid)", text: viz.legend.clearance },
    { curved: "var(--color-gold)", text: viz.legend.cross },
    { swatch: "var(--color-blue)", text: viz.legend.takeOn },
    { bar: "var(--color-blue)", text: viz.legend.shot },
  ];
  if (focus) items.unshift({ swatch: "var(--color-gold)", text: viz.legend.focusPlayer(focus) });

  return (
    <div className="mt-2">
      <Legend items={items} />
      {/* An instruction, not a mark: italic, sentence case. */}
      <p
        className="label mt-1"
        style={{ letterSpacing: 0, textTransform: "none", fontStyle: "italic" }}
      >
        {viz.legend.sequenceHover}
      </p>
    </div>
  );
}
