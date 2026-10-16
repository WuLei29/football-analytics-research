/**
 * ProfileCard — the left column of screen 03: photo, bio grid, four KPIs.
 *
 * Renders: README-PLAYER §3a. A photo block that FLEXES to the height of the
 * event-map card beside it (so the two columns bottom-align), with a name
 * plate and a shirt chip over it; a 12-item bio grid; a 2 x 2 hairline grid
 * of the role's four headline KPIs.
 * Props: `player`, `kpis`, `photo` (a URL under /players, or null).
 * Data:  `players/{player_id}.json` -> `player`, `kpis` (README-PLAYER "Data
 *        contract"): `silver.players` bio fields and `gold.player_season_stats`
 *        counts; the KPI set per role is `PLAYER_KPIS` in src/export/config.py.
 *
 * No photo source exists in the data (open decision 6). Photos are
 * hand-supplied, licensed assets; without one the block shows the shirt
 * number as a large `--line` numeral on the photo ground, the brief's
 * proposed fallback. The plate and chip keep their dark surfaces either way:
 * they exist so the type passes contrast over ANY photo.
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

/** The handoff's #EFE3D4, which is the `--color-track` token. */
const PHOTO_GROUND = "var(--color-track)";

export function ProfileCard({
  player,
  kpis,
  photo = null,
}: {
  player: PlayerBio;
  kpis: PlayerKpi[];
  photo?: string | null;
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
    <article
      className="flex h-full flex-col overflow-hidden rounded-card border border-line bg-card"
      style={{ padding: 2 }}
    >
      {/* 1 · Photo, flexing to the height of the pitch card. */}
      <div
        className="relative flex-1 overflow-hidden"
        style={{
          minHeight: 206,
          margin: "-2px -2px 0",
          borderRadius: "14px 14px 0 0",
          background: PHOTO_GROUND,
        }}
      >
        {photo ? (
          // eslint-disable-next-line @next/next/no-img-element -- static export, no optimiser
          <img src={photo} alt="" className="absolute inset-0 size-full object-cover" />
        ) : (
          <div
            aria-hidden
            className="absolute inset-0 flex items-center justify-center font-display font-bold text-line"
            style={{ fontSize: 150, lineHeight: 1, letterSpacing: "-0.04em" }}
          >
            {player.shirt_number ?? ""}
          </div>
        )}
        <div className="pointer-events-none absolute" style={{ left: 14, bottom: 11 }}>
          <div
            className="inline-block"
            style={{ background: "rgba(23,24,28,.82)", borderRadius: 9, padding: "5px 10px 6px" }}
          >
            {player.name.first && (
              <div
                className="font-mono uppercase"
                style={{ fontSize: 9, letterSpacing: "0.18em", color: "rgba(255,255,255,.8)" }}
              >
                {player.name.first}
              </div>
            )}
            <div
              className="font-display font-bold text-white"
              style={{ fontSize: 24, lineHeight: 1.05, letterSpacing: "-0.03em" }}
            >
              {player.name.last}
            </div>
          </div>
        </div>
        {player.shirt_number !== null && (
          <div
            className="pointer-events-none absolute bg-ink font-display font-bold text-white"
            style={{ right: 12, top: 12, borderRadius: 9, padding: "4px 9px 3px", fontSize: 20 }}
          >
            {player.shirt_number}
          </div>
        )}
      </div>

      {/* 2 · Bio grid: two columns of six. */}
      <dl
        className="grid grid-cols-2 border-b border-line"
        style={{ columnGap: 14, padding: "12px 16px" }}
      >
        {bio.map(([key, value]) => (
          <div key={key} className="flex items-baseline justify-between gap-2" style={{ padding: "3px 0" }}>
            <dt className="font-mono text-[8.5px] tracking-[0.12em] uppercase text-faint">{key}</dt>
            <dd className="truncate text-right font-sans text-[12px] font-semibold text-ink">{value}</dd>
          </div>
        ))}
      </dl>

      {/* 3 · Four KPIs, a collapsed hairline grid. */}
      <div className="grid grid-cols-2">
        {kpis.map((kpi, i) => (
          <div key={kpi.key} className="border border-line" style={{ padding: "14px 16px", margin: -0.5 }}>
            <div className="font-mono text-[9px] tracking-[0.14em] uppercase text-mid">
              {label(playerKpiLabels, kpi.key)}
            </div>
            <div
              className="mt-1.5 font-display font-bold"
              style={{
                fontSize: 42,
                lineHeight: 0.86,
                color: i === 0 ? "var(--color-blue)" : "var(--color-ink)",
              }}
            >
              {kpi.value === null ? EMPTY : `${dec(kpi.value, kpi.decimals)}${kpi.scale === "pct" ? "%" : ""}`}
            </div>
            <div className="font-mono text-[9px] text-faint" style={{ marginTop: 3 }}>
              {kpiSub(kpi)}
            </div>
          </div>
        ))}
      </div>
    </article>
  );
}

/** Per-90 under a season total; the rank inside the squad under a rate. */
function kpiSub(kpi: PlayerKpi): string {
  if (kpi.per_90 !== null) return copy.kpiPer90(dec(kpi.per_90, 2));
  if (kpi.squad_rank !== null && kpi.squad_n !== null) {
    return copy.kpiSquadRank(kpi.squad_rank, kpi.squad_n);
  }
  return copy.kpiUnranked;
}
