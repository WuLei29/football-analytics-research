"use client";

/**
 * PlayerSequences — screen 03 block 5: the possession chains the player was
 * part of, ranked by xT or VAEP, over the matches the filter selects.
 *
 * Renders: the page's light-blue spot panel (Pizarra, 24 Oct 2026): the
 * heading with the xT / VAEP switch, a white strip of four aggregate
 * figures, then the match page's sequence halves (`SequenceParts`): the
 * selected chain large on the left, with his nodes in the adapter's light
 * blue, and the top ten on the right.
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
 *
 * `pinned` (16 Oct 2026, the second review): a chain opened from the event
 * map. It opens first — in its place when it is already in the top ten,
 * otherwise put before them — under a bar naming its match and offering the
 * way back. Its match file is fetched like any filtered chain's.
 */

import { useEffect, useMemo, useState, type ReactNode, type Ref } from "react";

import { dec, EMPTY, num } from "@/lib/format";
import { metricDefinitions, player as copy, type SequenceMetric } from "@/lib/labels";
import { rankSequences } from "@/lib/data/playerFilter";
import { sequenceEntry } from "@/lib/data/sequences";
import type { MatchFile, PlayerFile, PlayerSequenceEntry } from "@/lib/data/types";

import { ChainCard, MetricSwitch, SequenceList } from "../sequences/SequenceParts";
import type { SequenceEntry } from "../viz/SequenceBrowser";

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
  ref,
  file,
  idx,
  team,
  season,
  pinned = null,
  onUnpin,
  demo = false,
}: {
  ref?: Ref<HTMLElement>;
  file: PlayerFile;
  idx: Set<number> | null;
  team: string;
  season: string;
  /** Index into `sequences.index` of the chain opened from the map. */
  pinned?: number | null;
  onUnpin?: () => void;
  demo?: boolean;
}) {
  const [metric, setMetric] = useState<SequenceMetric>("xt");
  const { sequences, matches, player } = file;

  const ranked = useMemo(
    () => rankSequences(sequences.index, idx, metric, sequences.top),
    [sequences, idx, metric],
  );

  // What the browser lists: the top ten, with the pinned chain first when
  // it is not one of them.
  const pinRow = pinned === null ? null : (sequences.index[pinned] ?? null);
  const rows = useMemo(
    () => (pinRow && !ranked.top.some((r) => r[1] === pinRow[1]) ? [pinRow, ...ranked.top] : ranked.top),
    [pinRow, ranked],
  );
  const initialIndex = pinRow ? rows.findIndex((r) => r[1] === pinRow[1]) : 0;

  // Chains already in hand: the season top lists, then whatever was fetched.
  const [fetched, setFetched] = useState<Chains>(() => new Map());
  const [failed, setFailed] = useState(false);
  const known = useMemo(() => {
    const all: Chains = new Map(fetched);
    for (const e of sequences.entries) all.set(e.sequence_id, e);
    return all;
  }, [sequences.entries, fetched]);

  const missing = useMemo(
    () => [...new Set(rows.filter((r) => !known.has(r[1])).map((r) => matches[r[0]].match_id))],
    [rows, known, matches],
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

  const list = rows.flatMap(([m, id]) => {
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
  const listKey = `${metric}:${pinRow?.[1] ?? ""}:${rows.map((r) => r[1]).join(",")}`;

  const pinMatch = pinRow ? matches[pinRow[0]] : null;
  const pinBar = pinMatch && (
    <div className="mb-4 flex flex-wrap items-center justify-between gap-2 rounded-[12px] bg-spot px-3 py-2">
      <span className="text-[14px] text-spot-text">
        {copy.pinned.note(copy.sequenceContext(pinMatch.matchday, pinMatch.opponent_abbr, pinMatch.is_home))}
      </span>
      {onUnpin && (
        <button type="button" onClick={onUnpin} className="rounded-full text-[14px] font-semibold text-team underline-offset-2 hover:underline">
          {copy.pinned.back}
        </button>
      )}
    </div>
  );

  let body;
  if (rows.length === 0) {
    body = <p className="rounded-row bg-white p-5 text-[15px] text-slate">{copy.sequencesEmpty}</p>;
  } else if (missing.length > 0) {
    body = (
      <p aria-live="polite" className="rounded-row bg-white p-5 text-[15px] text-slate">
        {failed ? copy.sequencesError : copy.sequencesLoading}
      </p>
    );
  } else {
    body = (
      <Browser
        key={listKey}
        list={list}
        initial={pinRow?.[1] ?? list[0]?.sequence_id ?? null}
        metric={metric}
        focusPlayerId={player.player_id}
        focusName={player.name.last}
        pinBar={pinBar}
        pinnedId={pinRow?.[1] ?? null}
      />
    );
  }

  return (
    <section ref={ref} aria-labelledby="jugadas-titulo" className="mt-6 flex scroll-mt-24 flex-col gap-4 rounded-panel bg-spot p-4 sm:p-6 lg:mt-8">
      <div className="flex flex-wrap items-end justify-between gap-x-6 gap-y-3 px-2 pt-2">
        <div>
          <h2 id="jugadas-titulo" className="text-[26px] leading-tight font-semibold tracking-[-0.02em]">
            {title}
          </h2>
          <p className="mt-2 max-w-[70ch] text-[15px] text-spot-text">
            {copy.sequencesCaption(ranked.top.length, num(agg.sequences), abbr)}
          </p>
        </div>
        <MetricSwitch value={metric} onChange={setMetric} />
      </div>

      <dl className="grid grid-cols-2 gap-3 rounded-row bg-white p-4 sm:grid-cols-4 sm:p-5">
        <Figure label={copy.aggregates.sequences} value={num(agg.sequences)} />
        <Figure label={copy.aggregates.xt} value={agg.xt === null ? EMPTY : dec(agg.xt, 2)} accent />
        <Figure label={copy.aggregates.shots} value={num(agg.endsInShot)} />
        <Figure
          label={copy.aggregates.passes}
          value={agg.passesPerSequence === null ? EMPTY : dec(agg.passesPerSequence, 1)}
        />
      </dl>

      {body}
    </section>
  );
}

/** The chain and the top ten; keyed by its list, so a new list starts fresh. */
function Browser({
  list,
  initial,
  metric,
  focusPlayerId,
  focusName,
  pinBar,
  pinnedId,
}: {
  list: SequenceEntry[];
  initial: string | null;
  metric: SequenceMetric;
  focusPlayerId: number;
  focusName: string;
  pinBar: ReactNode;
  pinnedId: string | null;
}) {
  const [selected, setSelected] = useState<string | null>(initial);
  const chain = list.find((s) => s.sequence_id === selected) ?? list[0] ?? null;
  if (!chain) return null;
  return (
    <div className="grid gap-4 lg:grid-cols-[minmax(0,2fr)_minmax(0,1fr)]">
      <ChainCard
        chain={chain}
        focusPlayerId={focusPlayerId}
        focusName={focusName}
        top={chain.sequence_id === pinnedId ? pinBar : null}
      />
      <SequenceList
        rows={list.map((seq) => ({ key: seq.sequence_id, seq }))}
        selectedKey={chain.sequence_id}
        onSelect={setSelected}
        metric={metric}
      />
    </div>
  );
}

function Figure({ label: text, value, accent = false }: { label: string; value: string; accent?: boolean }) {
  return (
    <div>
      <dt className="text-[13px] text-slate">{text}</dt>
      <dd className={`mt-1 text-[32px] leading-none font-light tracking-[-0.03em] tabular-nums ${accent ? "text-team" : ""}`}>
        {value}
      </dd>
    </div>
  );
}
