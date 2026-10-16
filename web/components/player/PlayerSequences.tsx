"use client";

/**
 * PlayerSequences — screen 03 block 5: the possession chains the player was
 * part of, ranked by xT or VAEP, over the matches the filter selects.
 *
 * Renders: the match page's sequence block (`MatchSequences`), with the
 * player's nodes in gold — a section head with the `MetricToggle`, a card
 * holding a four-figure aggregate strip and the `SequenceBrowser` (one play
 * at a time, action by action, beside the list of the top ten), and the
 * shared `SequenceLegend`.
 * Props: `file` (the player file), `idx` (the selected match indices, null
 *        for the season), `team` and `season` slugs, `demo`.
 * Data:  `players/{player_id}.json` -> `sequences.index` ranks the chains;
 *        each chain is drawn from `matches/{match_id}.json` -> `sequences[]`
 *        (WEB_DATA §7.3), the same entry the match page draws. The season
 *        top lists ship inside the player file (`sequences.entries`), so the
 *        default view fetches nothing.
 *
 * Deviation from the site rule "no fetching" (.claude/rules/web.md), on
 * purpose (16 Oct 2026): a filtered top ten can contain any of ~500 chains,
 * and shipping them all with their actions would add ~1 MB to every player
 * file. Instead the page fetches the match files the selected top ten comes
 * from — one file for one match, at most ten for a group — and caches them
 * for the visit. `lib/data/load.ts` anticipated this path for screen 04.
 *
 * Replaced on 16 Oct 2026: the trace overlay (`SequenceTraces`, player
 * variant) and its pill list. The review asked for the match page's look.
 */

import { useEffect, useMemo, useState } from "react";

import { SectionHead } from "@/components/shell/SectionHead";
import { dec, EMPTY, num } from "@/lib/format";
import { metricDefinitions, player as copy, type SequenceMetric } from "@/lib/labels";
import { rankSequences } from "@/lib/data/playerFilter";
import { sequenceEntry } from "@/lib/data/sequences";
import type { MatchFile, PlayerFile, PlayerSequenceEntry } from "@/lib/data/types";

import { MetricToggle } from "../viz/MetricToggle";
import { SequenceBrowser } from "../viz/SequenceBrowser";
import { SequenceLegend } from "../viz/SequenceLegend";

type Chains = Map<string, Omit<PlayerSequenceEntry, "match_id">>;

/** One promise per match file for the whole visit, shared by every player. */
const matchFiles = new Map<string, Promise<Chains>>();

function loadMatch(team: string, season: string, matchId: number): Promise<Chains> {
  const url = `/data/teams/${team}/${season}/matches/${matchId}.json`;
  let pending = matchFiles.get(url);
  if (!pending) {
    pending = fetch(url)
      .then((r) => {
        if (!r.ok) throw new Error(`${url}: ${r.status}`);
        return r.json() as Promise<MatchFile>;
      })
      .then((f) => new Map(f.sequences.map((s) => [s.sequence_id, s])));
    // A failed fetch is not cached: the next selection retries it.
    pending.catch(() => matchFiles.delete(url));
    matchFiles.set(url, pending);
  }
  return pending;
}

export function PlayerSequences({
  file,
  idx,
  team,
  season,
  demo = false,
}: {
  file: PlayerFile;
  idx: Set<number> | null;
  team: string;
  season: string;
  demo?: boolean;
}) {
  const [metric, setMetric] = useState<SequenceMetric>("xt");
  const { sequences, matches, player } = file;

  const ranked = useMemo(
    () => rankSequences(sequences.index, idx, metric, sequences.top),
    [sequences, idx, metric],
  );

  // Chains already in hand: the season top lists, then whatever was fetched.
  const [fetched, setFetched] = useState<Chains>(() => new Map());
  const [failed, setFailed] = useState(false);
  const known = useMemo(() => {
    const all: Chains = new Map(fetched);
    for (const e of sequences.entries) all.set(e.sequence_id, e);
    return all;
  }, [sequences.entries, fetched]);

  const missing = useMemo(
    () => [...new Set(ranked.top.filter((r) => !known.has(r[1])).map((r) => matches[r[0]].match_id))],
    [ranked, known, matches],
  );
  const missingKey = missing.join(".");

  useEffect(() => {
    if (!missingKey || demo) return;
    let live = true;
    setFailed(false);
    Promise.all(missingKey.split(".").map((id) => loadMatch(team, season, Number(id))))
      .then((maps) => {
        if (!live) return;
        setFetched((prev) => {
          const next = new Map(prev);
          for (const m of maps) for (const [id, s] of m) next.set(id, s);
          return next;
        });
      })
      .catch(() => live && setFailed(true));
    return () => {
      live = false;
    };
  }, [missingKey, team, season, demo]);

  const list = ranked.top.flatMap(([m, id]) => {
    const chain = known.get(id);
    const match = matches[m];
    return chain
      ? [sequenceEntry(chain, copy.sequenceContext(match.matchday, match.opponent_abbr, match.is_home))]
      : [];
  });

  const agg = ranked.aggregates;
  const abbr = metricDefinitions[metric].abbr;
  const title = copy.sequencesHeading(player.name.last);
  // The browser keeps its selection by index; a new list is a new browser.
  const listKey = `${metric}:${ranked.top.map((r) => r[1]).join(",")}`;

  let body;
  if (ranked.top.length === 0) {
    body = <p className="label">{copy.sequencesEmpty}</p>;
  } else if (missing.length > 0) {
    body = <p className="label">{failed ? copy.sequencesError : copy.sequencesLoading}</p>;
  } else {
    body = (
      <SequenceBrowser
        key={listKey}
        sequences={list}
        title={title}
        legend={<SequenceLegend focus={player.name.last} />}
        focusPlayerId={player.player_id}
      />
    );
  }

  return (
    <section className="mt-8">
      <SectionHead
        title={title}
        right={
          <div className="flex flex-wrap items-baseline gap-x-4 gap-y-1">
            <span className="font-mono text-[9px] tracking-[0.04em] text-faint">
              {copy.sequencesCaption(ranked.top.length, num(agg.sequences), abbr)}
            </span>
            <MetricToggle value={metric} onChange={setMetric} />
          </div>
        }
      />
      <div className="card mt-4">
        <div className="mb-4 grid grid-cols-2 gap-3 border-b border-line pb-3 sm:grid-cols-4">
          <Figure label={copy.aggregates.sequences} value={num(agg.sequences)} />
          <Figure label={copy.aggregates.xt} value={agg.xt === null ? EMPTY : dec(agg.xt, 2)} accent />
          <Figure label={copy.aggregates.shots} value={num(agg.endsInShot)} />
          <Figure
            label={copy.aggregates.passes}
            value={agg.passesPerSequence === null ? EMPTY : dec(agg.passesPerSequence, 1)}
          />
        </div>
        {body}
      </div>
    </section>
  );
}

function Figure({ label: text, value, accent = false }: { label: string; value: string; accent?: boolean }) {
  return (
    <div>
      <div className="font-mono text-[8.5px] tracking-[0.14em] uppercase text-mid">{text}</div>
      <div
        className="font-display font-bold"
        style={{ fontSize: 26, lineHeight: 1.05, color: accent ? "var(--color-blue)" : "var(--color-ink)" }}
      >
        {value}
      </div>
    </div>
  );
}
