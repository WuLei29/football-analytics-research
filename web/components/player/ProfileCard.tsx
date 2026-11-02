/**
 * ProfileCard — the left column of screen 03: photo, bio grid, four KPIs.
 *
 * Renders: README-PLAYER §3a, in the Pizarra world: a panel whose photo
 * block FLEXES to the height of the event-map panel beside it (so the two
 * columns bottom-align), with a white name plate and a team shirt chip over
 * it; a 12-item bio grid; the role's four headline KPIs as white cards.
 * Props: `player`, `kpis`, `photo` (a URL under /players, or null), and
 *        `selected` — how many matches the filter keeps, null for the season.
 * Data:  `players/{player_id}.json` -> `player`, `kpis` (README-PLAYER "Data
 *        contract"): `silver.players` bio fields and `gold.player_season_stats`
 *        counts; the KPI set per role is `PLAYER_KPIS` in src/export/config.py.
 *        Under a match filter the counts and KPIs arrive rebuilt from the
 *        per-match rows (`lib/data/playerFilter.ts`); age, height, foot and
 *        position stay season facts.
 *
 * No photo source exists in the data (open decision 6). Photos are
 * hand-supplied, licensed assets; without one the block shows the shirt
 * number as a large light chalk numeral on the panel-deep ground. The plate
 * and chip stay opaque (white, team) so the type passes contrast over ANY
 * photo; Pizarra has no black fills, so the handoff's dark plate is white.
 *
 * KPI verdicts (2 Nov 2026, author's review): each KPI figure is good green
 * or bad red against the league peers of the player's role, the percentile
 * cards' tiers (top 30% good, bottom 45% bad, ink between), from the cut-offs
 * the export ships in `kpi.band` (a total is judged per 90). Because the cut-
 * offs are values, not a percentile, a match-filtered figure is judged the
 * same way. The first KPI is no longer team blue: blue says who, and the
 * figure now says how good. When the player leads the squad in a KPI (rank 1
 * among at least SQUAD_MIN ranked teammates) its sub line becomes a good pill.
 */

import { dec, EMPTY, num, numericDate } from "@/lib/format";
import {
  footLabel,
  label,
  player as copy,
  playerKpiLabels,
  positionLabels,
} from "@/lib/labels";
import type { PlayerBio, PlayerKpi } from "@/lib/data/types";

/** A squad lead below this many ranked teammates is not news (a goalkeeper
    is "first" of one or two at his own rates). */
const SQUAD_MIN = 3;

const VERDICT_CLASS = { good: "text-good", fair: "", bad: "text-bad" } as const;

/** Good / middling / bad against the role's league peers, or null when the
    player has no band (under the minutes gate) or no value. */
function kpiVerdict(kpi: PlayerKpi): keyof typeof VERDICT_CLASS | null {
  const band = kpi.band;
  const on = band?.on === "per_90" ? kpi.per_90 : kpi.value;
  if (!band || on === null) return null;
  if (band.good_from !== null && on >= band.good_from) return "good";
  if (band.bad_below !== null && on < band.bad_below) return "bad";
  return "fair";
}

/** Rank 1 in the squad, among enough teammates to mean something. */
function leadsSquad(kpi: PlayerKpi): boolean {
  return kpi.squad_rank === 1 && kpi.squad_n !== null && kpi.squad_n >= SQUAD_MIN;
}

export function ProfileCard({
  player,
  kpis,
  photo = null,
  selected = null,
}: {
  player: PlayerBio;
  kpis: PlayerKpi[];
  photo?: string | null;
  selected?: number | null;
}) {
  const b = copy.bio;
  const bio: [string, string][] = [
    [b.age, player.age === null ? EMPTY : String(player.age)],
    [b.born, player.date_of_birth ? numericDate(player.date_of_birth) : EMPTY],
    [b.height, player.height_cm === null ? EMPTY : copy.heightCm(num(player.height_cm))],
    [b.foot, player.preferred_foot ? footLabel(player.preferred_foot) : EMPTY],
    [b.nationality, player.nationality ?? EMPTY],
    [b.position, player.position ? label(positionLabels, player.position) : EMPTY],
    [b.matches, num(player.matches)],
    [b.starts, num(player.starts)],
    [b.minutes, num(player.minutes)],
    [b.subbedOff, num(player.subbed_off)],
    [b.cards, copy.cards(num(player.yellow_cards), num(player.red_cards))],
    [
      b.penalties,
      player.penalties_taken > 0
        ? copy.penalties(num(player.penalties_scored), num(player.penalties_taken))
        : EMPTY,
    ],
  ];

  return (
    <article className="flex h-full flex-col gap-4 rounded-panel bg-panel p-3 sm:p-4">
      {/* 1 · Photo, flexing to the height of the map panel. */}
      <div className="relative min-h-[220px] flex-1 overflow-hidden rounded-row bg-panel-deep">
        {photo ? (
          // eslint-disable-next-line @next/next/no-img-element -- static export, no optimiser
          <img src={photo} alt="" className="absolute inset-0 size-full object-cover" />
        ) : (
          <div
            aria-hidden
            className="absolute inset-0 flex items-center justify-center text-[150px] leading-none font-light tracking-[-0.04em] text-chalk"
          >
            {player.shirt_number ?? ""}
          </div>
        )}
        <div className="pointer-events-none absolute bottom-3 left-3 rounded-[14px] bg-white px-3 py-2">
          {player.name.first && <div className="text-[13px] text-slate">{player.name.first}</div>}
          <div className="text-[22px] leading-tight font-semibold tracking-[-0.02em]">{player.name.last}</div>
        </div>
        {player.shirt_number !== null && (
          <div className="pointer-events-none absolute top-3 right-3 flex size-10 items-center justify-center rounded-full bg-team text-[17px] font-semibold text-white">
            {player.shirt_number}
          </div>
        )}
      </div>

      {/* 2 · Bio grid: two columns of six. */}
      <dl className="grid grid-cols-2 gap-x-5 gap-y-1.5 px-2">
        {bio.map(([key, value]) => (
          <div key={key} className="flex items-baseline justify-between gap-2">
            <dt className="text-[13px] text-slate">{key}</dt>
            <dd className="min-w-0 text-right text-[14px] font-medium break-words">{value}</dd>
          </div>
        ))}
      </dl>

      {/* 3 · Four KPIs, white cards on the panel; the figure in its
          verdict colour, a squad lead as a good pill. */}
      <dl className="grid grid-cols-2 gap-2">
        {kpis.map((kpi) => {
          const verdict = kpiVerdict(kpi);
          const lead = leadsSquad(kpi);
          return (
            <div key={kpi.key} className="flex flex-col rounded-row bg-white px-4 py-3.5">
              <dt className="text-[13px] leading-snug font-medium">{label(playerKpiLabels, kpi.key)}</dt>
              <dd className={`mt-2 text-[36px] leading-[0.95] font-light tracking-[-0.04em] tabular-nums ${verdict ? VERDICT_CLASS[verdict] : ""}`}>
                {kpi.value === null ? EMPTY : dec(kpi.value, kpi.decimals)}
                {kpi.value !== null && kpi.scale === "pct" && <span className="ml-0.5 text-[0.5em] tracking-normal">%</span>}
                {verdict && kpi.band && (
                  <span className="sr-only">. {copy.kpiVerdict[verdict](kpi.band.peer_group)}</span>
                )}
              </dd>
              <dd className="mt-auto flex flex-col items-start gap-1 pt-2 text-[13px] leading-snug text-slate">
                {lead && (
                  <span className="rounded-full bg-good/12 px-2 py-0.5 font-semibold whitespace-nowrap text-good">
                    {copy.kpiSquadLead}
                    <span className="sr-only"> ({copy.kpiSquadRank(1, kpi.squad_n ?? 0)})</span>
                  </span>
                )}
                {!(lead && !kpi.total) && <span>{kpiSub(kpi, selected)}</span>}
              </dd>
            </div>
          );
        })}
      </dl>
    </article>
  );
}

/**
 * Per-90 under a total; the rank inside the squad under a season rate. A
 * filtered rate has no squad rank (teammates are ranked on their seasons),
 * so it says what it covers instead.
 */
function kpiSub(kpi: PlayerKpi, selected: number | null): string {
  if (kpi.per_90 !== null) return copy.kpiPer90(dec(kpi.per_90, 2));
  if (selected !== null) return copy.kpiSelection(selected);
  if (kpi.squad_rank !== null && kpi.squad_n !== null) {
    return copy.kpiSquadRank(kpi.squad_rank, kpi.squad_n);
  }
  return copy.kpiUnranked;
}
