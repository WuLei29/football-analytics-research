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

      {/* 3 · Four KPIs, white cards on the panel. */}
      <dl className="grid grid-cols-2 gap-2">
        {kpis.map((kpi, i) => (
          <div key={kpi.key} className="flex flex-col rounded-row bg-white px-4 py-3.5">
            <dt className="text-[13px] leading-snug font-medium">{label(playerKpiLabels, kpi.key)}</dt>
            <dd className={`mt-2 text-[36px] leading-[0.95] font-light tracking-[-0.04em] tabular-nums ${i === 0 ? "text-team" : ""}`}>
              {kpi.value === null ? EMPTY : dec(kpi.value, kpi.decimals)}
              {kpi.value !== null && kpi.scale === "pct" && <span className="ml-0.5 text-[0.5em] tracking-normal">%</span>}
            </dd>
            <dd className="mt-auto pt-2 text-[13px] leading-snug text-slate">{kpiSub(kpi, selected)}</dd>
          </div>
        ))}
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
