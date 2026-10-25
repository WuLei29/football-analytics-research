"use client";

/**
 * PlayerPicker — the dropdown that moves screen 03 from one player to the
 * next. Replaced the strip of pills on 16 Oct 2026 (25 pills made two rows
 * of noise above the profile).
 *
 * Renders: a trigger with the current player's shirt and name; the panel
 * lists the squad grouped by role (Porteros, Centrales, …), one row per
 * player — a shirt with his number, the surname, his minutes — each a link
 * to that player's page. The current row is marked and not a link.
 * Props: `players` (squad order), `currentId`, `team`, `season`, and `demo`
 *        on /demo, where the rows link nowhere.
 * Data:  `squad.json` -> `players` (WEB_DATA §8): GK -> CB -> RB -> LB ->
 *        MID -> AM -> W -> ST, then minutes, ordered by the export.
 *
 * Every link carries `?vista=&partidos=` (`lib/vista.ts`): a reader comparing
 * two players keeps the same map mode over the same matches.
 */

import Link from "next/link";

import { num } from "@/lib/format";
import { label, peerGroupLabels, player as copy } from "@/lib/labels";
import { routes } from "@/lib/routes";
import { useCarriedQuery } from "@/lib/vista";
import type { SquadPlayer } from "@/lib/data/types";

import { Dropdown } from "../shell/Dropdown";

/**
 * A shirt with the number on it. Club blue with the number in the card
 * colour — the site has no kit artwork, and the shape alone says "dorsal".
 */
export function Shirt({ number, size = 24 }: { number: number | null; size?: number }) {
  return (
    <svg aria-hidden viewBox="0 0 32 30" width={size} height={(size * 30) / 32} className="shrink-0">
      <path
        d="M11 1.5 4.2 4.6 1 11.2l5 2.3 1.4-2.4V28.5h17.2V11.1l1.4 2.4 5-2.3-3.2-6.6L21 1.5c-.8 2.4-2.6 3.8-5 3.8s-4.2-1.4-5-3.8Z"
        fill="var(--color-team)"
      />
      <text
        x={16}
        y={21.2}
        textAnchor="middle"
        fontFamily="var(--font-board)"
        fontSize={number !== null && number >= 10 ? 10.5 : 11.5}
        fontWeight={600}
        fill="white"
      >
        {number ?? ""}
      </text>
    </svg>
  );
}

export function PlayerPicker({
  players,
  currentId,
  team,
  season,
  demo = false,
}: {
  players: SquadPlayer[];
  currentId: number;
  team: string;
  season: string;
  demo?: boolean;
}) {
  const query = useCarriedQuery();
  const current = players.find((p) => p.player_id === currentId);

  // Consecutive runs of one role, in the export's order.
  const groups: { role: string; players: SquadPlayer[] }[] = [];
  for (const p of players) {
    const role = p.role ?? p.position_group ?? "";
    const last = groups[groups.length - 1];
    if (last && last.role === role) last.players.push(p);
    else groups.push({ role, players: [p] });
  }

  return (
    <Dropdown
      label={copy.pickerLabel}
      width={320}
      trigger={
        <span className="flex items-center gap-2">
          <Shirt number={current?.shirt_number ?? null} size={20} />
          <span className="truncate">{current?.name.full ?? ""}</span>
        </span>
      }
    >
      {(close) => (
        <nav aria-label={copy.tabsLabel} className="max-h-[420px] overflow-y-auto pb-2">
          {groups.map((g) => (
            <div key={g.role}>
              <p className="px-4 pt-3 pb-1 text-[13px] text-slate">{label(peerGroupLabels, g.role)}</p>
              <ul>
                {g.players.map((p) => {
                  const isCurrent = p.player_id === currentId;
                  const row = (
                    <>
                      <Shirt number={p.shirt_number} />
                      <span className="min-w-0 flex-1 truncate text-[15px] font-medium text-ink">{p.name.full}</span>
                      <span className="text-[13px] text-slate tabular-nums">{num(p.minutes)}&apos;</span>
                    </>
                  );
                  const className = "mx-1.5 flex items-center gap-2.5 rounded-[12px] px-2.5 py-1.5";
                  return (
                    <li key={p.player_id}>
                      {demo || isCurrent ? (
                        <span
                          className={`${className} ${isCurrent ? "bg-spot" : ""}`}
                          aria-current={isCurrent ? "page" : undefined}
                        >
                          {row}
                        </span>
                      ) : (
                        <Link
                          href={routes.player(team, season, p.player_id, query)}
                          onClick={close}
                          className={`${className} transition-colors hover:bg-panel`}
                        >
                          {row}
                        </Link>
                      )}
                    </li>
                  );
                })}
              </ul>
            </div>
          ))}
        </nav>
      )}
    </Dropdown>
  );
}
