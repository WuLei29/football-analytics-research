"use client";

/**
 * MatchFilter — which matches the profile card, the event map and the
 * sequences of screen 03 cover. The percentile cards ignore it: they rank a
 * season against the league.
 *
 * Renders: a trigger with the calendar glyph of `MatchPicker` and the
 * current choice ("Temporada completa", "Últimos 5", "J5 · L LEV 3–0",
 * "4 partidos"). The panel holds four presets as chips (season, last five,
 * home, away), then a checklist of every match the player played, newest
 * first — "J5 · 16 ago · L Levante 3–0" and his minutes — to build any
 * group, and a line saying what the filter does and does not change.
 * Props: `matches` (the player file's, date order), `selection` (parsed
 *        `?partidos=`), `idx` (what it resolves to).
 * Data:  `players/{player_id}.json` -> `matches[]`.
 *
 * Ticking a box while a preset is on starts from the preset's matches, so
 * "Últimos 5" then unticking one gives the other four. The choice is written
 * to `?partidos=` (`lib/vista.ts`), which every other block reads.
 */

import { shortDate, num } from "@/lib/format";
import { matchList, matchPicker, player as copy } from "@/lib/labels";
import { serializeSelection, type MatchPreset, type MatchSelection } from "@/lib/data/playerFilter";
import { setPartidos } from "@/lib/vista";
import type { PlayerMatch } from "@/lib/data/types";

import { Dropdown } from "../shell/Dropdown";
import { Chip } from "../viz/DefensiveActions";

const PRESETS: MatchPreset[] = ["season", "last5", "home", "away"];

function presetLabel(p: MatchPreset): string {
  const f = copy.filter;
  return { season: f.season, last5: f.last5, home: f.home, away: f.away }[p];
}

const venue = (m: PlayerMatch) => (m.is_home ? matchList.homeShort : matchList.awayShort);

export function MatchFilter({
  matches,
  selection,
  idx,
}: {
  matches: PlayerMatch[];
  selection: MatchSelection;
  idx: Set<number> | null;
}) {
  const choose = (sel: MatchSelection) => setPartidos(serializeSelection(sel));

  const toggle = (i: number) => {
    const ids = new Set(
      [...(idx ?? matches.keys())].map((j) => matches[j].match_id),
    );
    const id = matches[i].match_id;
    if (ids.has(id)) {
      // The last match stays: an empty selection would draw nothing at all.
      if (ids.size === 1) return;
      ids.delete(id);
    } else ids.add(id);
    // Every match ticked is the season, said the short way.
    if (ids.size === matches.length) choose({ kind: "season" });
    else choose({ kind: "custom", ids: [...ids].sort((a, b) => a - b) });
  };

  let current: string;
  if (selection.kind !== "custom") current = presetLabel(selection.kind);
  else if (idx && idx.size === 1) {
    const m = matches[[...idx][0]];
    current = copy.filter.one(m.matchday, venue(m), m.opponent_abbr, m.goals_for, m.goals_against);
  } else current = copy.filter.count(idx?.size ?? 0);

  return (
    <Dropdown
      label={copy.filter.label}
      align="right"
      width={340}
      trigger={
        <span className="flex items-center gap-2">
          <svg
            aria-hidden
            viewBox="0 0 16 16"
            width="14"
            height="14"
            className="shrink-0 text-mid"
            fill="none"
            stroke="currentColor"
            strokeWidth="1.3"
            strokeLinecap="round"
          >
            <rect x="1.5" y="3" width="13" height="11.5" rx="1.5" />
            <path d="M1.5 6.5h13M5 1.5v3M11 1.5v3" />
          </svg>
          <span>{current}</span>
        </span>
      }
    >
      <div className="p-3">
        <div role="group" aria-label={copy.filter.label} className="flex flex-wrap gap-[4px]">
          {PRESETS.map((p) => (
            <Chip
              key={p}
              label={presetLabel(p)}
              pressed={selection.kind === p}
              onClick={() => choose({ kind: p })}
            />
          ))}
        </div>

        <p className="label mt-3 mb-1">{copy.filter.pick}</p>
        <ul
          className="max-h-[300px] overflow-y-auto"
          style={{ borderTop: "1px solid var(--color-line)" }}
        >
          {[...matches.keys()].reverse().map((i) => {
            const m = matches[i];
            const on = idx === null || idx.has(i);
            return (
              <li key={m.match_id} style={{ borderBottom: "1px solid var(--color-line)" }}>
                <label className="flex cursor-pointer items-center gap-2.5 px-1 py-[6px] hover:bg-[rgba(11,76,158,.04)]">
                  <input
                    type="checkbox"
                    checked={on}
                    onChange={() => toggle(i)}
                    className="size-3.5 shrink-0 cursor-pointer accent-[var(--color-blue)]"
                  />
                  <span className="min-w-0 flex-1 truncate font-mono text-[10px] tracking-[0.04em] text-ink">
                    {matchPicker.option(
                      m.matchday,
                      shortDate(m.date),
                      venue(m),
                      m.opponent_short_name,
                      m.goals_for,
                      m.goals_against,
                    )}
                  </span>
                  <span className="font-mono text-[9.5px] text-faint">{num(m.minutes)}&apos;</span>
                </label>
              </li>
            );
          })}
        </ul>

        <p className="label mt-2" style={{ letterSpacing: 0, textTransform: "none" }}>
          {copy.filter.scope}
        </p>
      </div>
    </Dropdown>
  );
}
