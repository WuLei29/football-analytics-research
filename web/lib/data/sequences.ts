/**
 * sequences.ts — a match file's sequence as the `SequenceBrowser` reads it.
 *
 * One mapping for both pages that draw chains (the match page, and the
 * player page, which draws the same chains from the same match files), so a
 * play is titled and summarised identically wherever it appears. Extracted
 * from the match page on 16 Oct 2026.
 */

import type { SequenceEntry } from "@/components/viz/SequenceBrowser";

import type { MatchSequence } from "./types";

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
