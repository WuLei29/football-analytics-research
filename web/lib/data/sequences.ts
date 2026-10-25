/**
 * sequences.ts — a match file's sequence as the `SequenceBrowser` reads it.
 *
 * `SequenceEntry` lives here since 24 Oct 2026, when the handoff's
 * `SequenceBrowser` that declared it was retired for
 * `components/sequences/SequenceParts`.
 *
 * One mapping for both pages that draw chains (the match page, and the
 * player page, which draws the same chains from the same match files), so a
 * play is titled and summarised identically wherever it appears. Extracted
 * from the match page on 16 Oct 2026.
 */

import type { SequenceAction } from "@/components/viz/SequenceDetail";

import type { MatchSequence } from "./types";

/**
 * One entry of `sequences[]`: the `gold.sequences` header plus its actions.
 *
 * `primary_phase` and `outcome` are the raw column VALUES, not Spanish: the
 * closed enums of GOLD_LAYER §4.1.6 and §4.1.3. They are translated here at
 * render time through `lib/labels.ts`, so the export never ships prose and an
 * enum value added in gold shows up as `⟨new_value⟩` rather than silently.
 */
export interface SequenceEntry {
  sequence_id: string;
  /** `gold.sequences.primary_phase`, e.g. `"fast_attacking"`. */
  primary_phase: string;
  /**
   * The row's title — the export's priority ladder over the phase flags
   * (WEB_DATA §7.6): `counter_attack`, `high_transition`, `corner`,
   * `throw_in`, `free_kick`, `direct_long`, `positional`, `buildup`, `fast`.
   * Optional so files exported before 21 Sep 2026 fall back to the phase.
   */
  kind?: string | null;
  /** `gold.sequences.outcome`, e.g. `"shot_saved"`. */
  outcome: string;
  /**
   * How the chain began and, on a `turnover`, how it lost the ball — the
   * export's `start_kind` and `end_action` (WEB_DATA §7.3). Optional so the
   * demo fixtures need neither.
   */
  start_kind?: string | null;
  end_action?: string | null;
  start_minute: number;
  start_second: number;
  duration_s: number;
  event_count: number;
  xt: number;
  vaep: number;
  actions: SequenceAction[];
  /**
   * Which match the chain is from — "J9 · OVI (V)" — when the list spans
   * several (the player page). Shown before the clock; absent on a match
   * page, where every row is the same match.
   */
  context?: string;
}


export function sequenceEntry(s: Omit<MatchSequence, "rank">, context?: string): SequenceEntry {
  return {
    sequence_id: s.sequence_id,
    primary_phase: s.primary_phase ?? "chaotic",
    outcome: s.outcome ?? "turnover",
    start_kind: s.start_kind,
    end_action: s.end_action,
    start_minute: s.minute,
    start_second: s.second,
    duration_s: s.duration_s,
    event_count: s.events,
    xt: s.xt ?? 0,
    vaep: s.vaep ?? 0,
    actions: s.actions,
    context,
  };
}
