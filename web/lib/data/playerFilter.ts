/**
 * playerFilter.ts — screen 03 narrowed to a match or a group of matches.
 *
 * The reader picks matches in the header (`MatchFilter`); the profile card,
 * the event map and the sequences follow, the percentile cards do not (they
 * rank a season against the league). Everything here is pure: it takes the
 * player file and a selection and returns what the components draw.
 *
 * The arithmetic mirrors the export. A KPI is rebuilt from the per-match
 * parts by its `formula` — the same sums `src/export/player.py:kpi_value`
 * checks against gold's season value on every run — so "Temporada completa"
 * and "every match ticked" can never show two different numbers.
 *
 * The selection lives in the URL (`?partidos=`, `lib/vista.ts`) so a view is
 * shareable and survives a move to the next player. Its forms:
 *   (absent)        the whole season
 *   ultimos5        the player's last five matches
 *   local|visitante his home or away matches
 *   1543.1553       these match ids (any that he did not play are ignored)
 */

import type {
  DefenceRow,
  PlayerBio,
  PlayerFile,
  PlayerKpi,
  PlayerMatch,
  PlayerMaps,
  PassRow,
  CarryRow,
  ReceptionRow,
  SequenceIndexRow,
  TouchRow,
} from "./types";
import { PASS_FLAG } from "./types";

export type MatchPreset = "season" | "last5" | "home" | "away";

export type MatchSelection =
  | { kind: MatchPreset }
  | { kind: "custom"; ids: number[] };

export const SEASON: MatchSelection = { kind: "season" };
const LAST_N = 5;

const PRESET_PARAM: Record<Exclude<MatchPreset, "season">, string> = {
  last5: "ultimos5",
  home: "local",
  away: "visitante",
};

export function parseSelection(param: string | null): MatchSelection {
  if (!param) return SEASON;
  for (const [kind, value] of Object.entries(PRESET_PARAM)) {
    if (param === value) return { kind: kind as MatchPreset };
  }
  const ids = param
    .split(".")
    .map(Number)
    .filter((n) => Number.isInteger(n) && n > 0);
  return ids.length ? { kind: "custom", ids: [...new Set(ids)].sort((a, b) => a - b) } : SEASON;
}

export function serializeSelection(sel: MatchSelection): string | null {
  if (sel.kind === "season") return null;
  if (sel.kind === "custom") return sel.ids.length ? sel.ids.join(".") : null;
  return PRESET_PARAM[sel.kind];
}

/** Indices into `matches` the selection keeps, or null for the whole season. */
export function selectedIndices(sel: MatchSelection, matches: PlayerMatch[]): Set<number> | null {
  switch (sel.kind) {
    case "season":
      return null;
    case "last5":
      return new Set(matches.map((_, i) => i).slice(-LAST_N));
    case "home":
    case "away": {
      const home = sel.kind === "home";
      return new Set(matches.flatMap((m, i) => (m.is_home === home ? [i] : [])));
    }
    case "custom": {
      const ids = new Set(sel.ids);
      return new Set(matches.flatMap((m, i) => (ids.has(m.match_id) ? [i] : [])));
    }
  }
}

const keep = (idx: Set<number> | null, m: number) => idx === null || idx.has(m);

/* -- Profile card ---------------------------------------------------------- */

/** The bio grid over the selection: identity fields stay, counts are summed. */
export function filteredBio(bio: PlayerBio, matches: PlayerMatch[], idx: Set<number> | null): PlayerBio {
  if (idx === null) return bio;
  const sel = matches.filter((_, i) => idx.has(i));
  const sum = (f: (m: PlayerMatch) => number) => sel.reduce((a, m) => a + f(m), 0);
  return {
    ...bio,
    matches: sel.length,
    starts: sum((m) => (m.started ? 1 : 0)),
    minutes: sum((m) => m.minutes),
    subbed_off: sum((m) => (m.subbed_off ? 1 : 0)),
    yellow_cards: sum((m) => m.yellow_cards),
    red_cards: sum((m) => m.red_cards),
    penalties_taken: sum((m) => m.penalties_taken),
    penalties_scored: sum((m) => m.penalties_scored),
  };
}

function round(value: number, decimals: number): number {
  const f = 10 ** decimals;
  return Math.round(value * f) / f;
}

/** One KPI over the selected matches, display-scaled like the export ships it. */
export function kpiOver(kpi: PlayerKpi, sel: PlayerMatch[]): number | null {
  const total = (terms: Record<string, number>) =>
    sel.reduce(
      (acc, m) =>
        acc + Object.entries(terms).reduce((a, [col, coef]) => a + coef * (m.kpi_parts[col] ?? 0), 0),
      0,
    );
  const num = total(kpi.formula.num);
  let value: number | null;
  if (kpi.formula.per90) {
    const minutes = sel.reduce((a, m) => a + m.minutes, 0);
    value = minutes ? (num * 90) / minutes : null;
  } else if (kpi.formula.den) {
    const den = total(kpi.formula.den);
    value = den ? num / den : null;
  } else {
    value = num;
  }
  if (value === null) return null;
  return round(kpi.scale === "pct" ? value * 100 : value, kpi.decimals);
}

/**
 * The four KPIs over the selection. The rank-in-squad sub line is a season
 * fact (it ranks against teammates' seasons), so a filtered KPI drops it;
 * a total keeps its per-90 over the selected minutes.
 */
export function filteredKpis(kpis: PlayerKpi[], matches: PlayerMatch[], idx: Set<number> | null): PlayerKpi[] {
  if (idx === null) return kpis;
  const sel = matches.filter((_, i) => idx.has(i));
  const minutes = sel.reduce((a, m) => a + m.minutes, 0);
  return kpis.map((kpi) => {
    const value = kpiOver(kpi, sel);
    return {
      ...kpi,
      value,
      per_90: kpi.total && value !== null && minutes > 0 ? round((value * 90) / minutes, 2) : null,
      squad_rank: null,
      squad_n: null,
    };
  });
}

/* -- Event map -------------------------------------------------------------- */

/** A defensive action as `DefensiveActions` takes it. */
export interface DefenceMark {
  type: string;
  x: number;
  y: number;
  outcome: "won" | "lost";
  minute: number | null;
  /** Index into `matches`. */
  m: number;
  /** Index into `sequences.index`; null when the event is in no drawable chain. */
  s: number | null;
}

/** What the event map draws for the selection, caps applied. */
export interface MapView {
  /** The `caps.passes` most recent passes. */
  passes: PassRow[];
  /** Progressive passes, best `caps.progressive` by xT. */
  progressive: PassRow[];
  keyPasses: PassRow[];
  /** Progressive carries, best `caps.progressive` by xT. */
  carries: CarryRow[];
  touchZones: { zone_id: number; value: number }[];
  receptionZones: { zone_id: number; value: number }[];
  /** Every reception, one by one (the events display). */
  receptions: ReceptionRow[];
  /** Every touch, one by one (the events display). */
  touches: TouchRow[];
  shots: PlayerMaps["shots"];
  defence: DefenceMark[];
  /** Totals behind each capped list ("150 DE 1,284 PASES"). */
  counts: {
    passes: number;
    progressive: number;
    keyPasses: number;
    carries: number;
    touches: number;
    receptions: number;
  };
}

/** Best `n` by xT (index 6), ties kept in time order. */
function topByXt<T extends PassRow | CarryRow>(rows: T[], n: number): T[] {
  return rows
    .map((row, i) => ({ row, i }))
    .sort((a, b) => (b.row[6] ?? -Infinity) - (a.row[6] ?? -Infinity) || a.i - b.i)
    .slice(0, n)
    .map((e) => e.row);
}

function zoneSums(rows: [number, number, number][], idx: Set<number> | null) {
  const by = new Map<number, number>();
  for (const [m, zone, count] of rows) {
    if (keep(idx, m)) by.set(zone, (by.get(zone) ?? 0) + count);
  }
  return [...by].map(([zone_id, value]) => ({ zone_id, value }));
}

export function mapView(file: PlayerFile, idx: Set<number> | null): MapView {
  const { maps, matches } = file;
  const ids = idx === null ? null : new Set([...idx].map((i) => matches[i].match_id));

  const passes = maps.passes.filter((r) => keep(idx, r[5]));
  const progressiveAll = passes.filter((r) => (r[4] & PASS_FLAG.progressive) !== 0);
  const keyPasses = passes.filter((r) => (r[4] & (PASS_FLAG.keyPass | PASS_FLAG.assist)) !== 0);
  const carries = maps.carries.filter((r) => keep(idx, r[5]));
  const touchZones = zoneSums(maps.touch_zones, idx);
  const receptionZones = zoneSums(maps.reception_zones, idx);

  return {
    passes: passes.slice(-maps.caps.passes),
    progressive: topByXt(progressiveAll, maps.caps.progressive),
    keyPasses,
    carries: topByXt(carries, maps.caps.progressive),
    touchZones,
    receptionZones,
    receptions: maps.receptions.filter((r) => keep(idx, r[3])),
    touches: maps.touches.filter((r) => keep(idx, r[3])),
    shots: ids === null ? maps.shots : maps.shots.filter((s) => ids.has(s.match_id)),
    defence: maps.defence
      .filter((r: DefenceRow) => keep(idx, r[5]))
      .map(([type, x, y, won, minute, m, s]) => ({ type, x, y, outcome: won ? "won" : "lost", minute, m, s })),
    counts: {
      passes: passes.length,
      progressive: progressiveAll.length,
      keyPasses: keyPasses.length,
      carries: carries.length,
      touches: touchZones.reduce((a, z) => a + z.value, 0),
      receptions: receptionZones.reduce((a, z) => a + z.value, 0),
    },
  };
}

/* -- Sequences --------------------------------------------------------------- */

export interface RankedSequences {
  /** The top `n` index rows by the metric, best first. */
  top: SequenceIndexRow[];
  aggregates: {
    sequences: number;
    xt: number | null;
    endsInShot: number;
    passesPerSequence: number | null;
  };
}

export function rankSequences(
  index: SequenceIndexRow[],
  idx: Set<number> | null,
  metric: "xt" | "vaep",
  n: number,
): RankedSequences {
  const rows = index.filter((r) => keep(idx, r[0]));
  const col = metric === "xt" ? 2 : 3;
  // The export's tiebreak is start_event_id; the index arrives in
  // (match date, start_event_id) order, so a stable sort keeps it.
  const top = rows
    .map((row, i) => ({ row, i }))
    .sort((a, b) => ((b.row[col] as number | null) ?? -Infinity) - ((a.row[col] as number | null) ?? -Infinity) || a.i - b.i)
    .slice(0, n)
    .map((e) => e.row);

  const withPasses = rows.filter((r) => r[5] !== null);
  return {
    top,
    aggregates: {
      sequences: rows.length,
      xt: rows.length ? rows.reduce((a, r) => a + (r[2] ?? 0), 0) : null,
      endsInShot: rows.reduce((a, r) => a + r[4], 0),
      passesPerSequence: withPasses.length
        ? withPasses.reduce((a, r) => a + (r[5] ?? 0), 0) / withPasses.length
        : null,
    },
  };
}
