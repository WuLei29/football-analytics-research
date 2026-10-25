"use client";

/**
 * SequenceLink — the one link between the match page's maps and its sequence
 * block (22 Oct 2026, the player page's map -> sequence click brought here).
 *
 * Renders: nothing of its own; its children, inside a context.
 * Props: `ids`, the file's `sequences[].sequence_id` in file order — what a
 *        map row's `s` indexes — and `children`.
 * Data:  `matches/{match_id}.json` -> `shots[].s`, `progression[].s`,
 *        `defence.actions[].s` (index into `sequences[]`, null when the chain
 *        is not drawable).
 *
 * WHY A CONTEXT. The maps and the sequence block are siblings on a server
 * page, several sections apart; the page cannot hand a callback from one to
 * the other. A provider wrapping the page body holds the opened chain, and
 * the server-rendered sections between them pass straight through it.
 *
 * Since 24 Oct 2026 (Pizarra) the context also hands the maps `ids`, so a
 * map can ring the mark whose chain is the one open in the sequence block.
 *
 * The opened chain carries a `nonce`, so a second click on the same mark,
 * after the reader has picked another row, opens it again.
 */

import { createContext, useCallback, useContext, useMemo, useState, type ReactNode } from "react";

export interface OpenedSequence {
  sequence_id: string;
  nonce: number;
}

interface SequenceLinkValue {
  /** `sequences[].sequence_id` in file order: what a map row's `s` indexes. */
  ids: string[];
  opened: OpenedSequence | null;
  /** Open the chain a map row points at (`s`). Null or unknown does nothing. */
  open: (s: number | null | undefined) => void;
  clear: () => void;
}

const SequenceLinkContext = createContext<SequenceLinkValue>({
  ids: [],
  opened: null,
  open: () => {},
  clear: () => {},
});

export function SequenceLinkProvider({ ids, children }: { ids: string[]; children: ReactNode }) {
  const [opened, setOpened] = useState<OpenedSequence | null>(null);

  const open = useCallback(
    (s: number | null | undefined) => {
      const sequence_id = s == null ? undefined : ids[s];
      if (sequence_id === undefined) return;
      setOpened((prev) => ({ sequence_id, nonce: (prev?.nonce ?? 0) + 1 }));
    },
    [ids],
  );
  const clear = useCallback(() => setOpened(null), []);

  const value = useMemo(() => ({ ids, opened, open, clear }), [ids, opened, open, clear]);
  return <SequenceLinkContext.Provider value={value}>{children}</SequenceLinkContext.Provider>;
}

export function useSequenceLink(): SequenceLinkValue {
  return useContext(SequenceLinkContext);
}
