"use client";

/**
 * PlayerBody — the part of screen 03 the match filter reaches.
 *
 * Renders: the toolbar (player picker left, match filter right), the profile
 * card beside the event map, the percentile cards the page hands in, and the
 * sequence block.
 * Props: the player file, the squad (for the picker), the team and season
 *        slugs, the photo URL, `percentiles` (server-rendered, season-wide,
 *        drawn between the map and the sequences), `demo`.
 * Data:  `players/{player_id}.json`, narrowed by `?partidos=` through
 *        `lib/data/playerFilter.ts`.
 *
 * The smallest client boundary that holds the filter's three readers. The
 * selection is in the URL rather than in state here, so the picker's links
 * can carry it to the next player.
 */

import { useMemo, type ReactNode } from "react";

import {
  filteredBio,
  filteredKpis,
  mapView,
  parseSelection,
  selectedIndices,
} from "@/lib/data/playerFilter";
import { usePartidos } from "@/lib/vista";
import type { PlayerFile, SquadPlayer } from "@/lib/data/types";

import { EventMap } from "./EventMap";
import { MatchFilter } from "./MatchFilter";
import { PlayerPicker } from "./PlayerPicker";
import { PlayerSequences } from "./PlayerSequences";
import { ProfileCard } from "./ProfileCard";

export function PlayerBody({
  file,
  squad,
  team,
  season,
  photo,
  percentiles,
  demo = false,
}: {
  file: PlayerFile;
  squad: SquadPlayer[];
  team: string;
  season: string;
  photo: string | null;
  percentiles: ReactNode;
  demo?: boolean;
}) {
  const param = usePartidos();
  const selection = useMemo(() => parseSelection(param), [param]);
  const idx = useMemo(() => selectedIndices(selection, file.matches), [selection, file.matches]);
  const view = useMemo(() => mapView(file, idx), [file, idx]);
  const p = file.player;

  return (
    <>
      <div className="mt-4 mb-4 flex flex-wrap items-center justify-between gap-2">
        <PlayerPicker players={squad} currentId={p.player_id} team={team} season={season} demo={demo} />
        <MatchFilter matches={file.matches} selection={selection} idx={idx} />
      </div>

      {/* 340px card beside the pitch; the card's photo flexes to the pitch
          card's height so the two columns bottom-align. Stacks below lg. */}
      <section className="grid items-stretch gap-4 lg:grid-cols-[minmax(0,340px)_minmax(0,1fr)]">
        <ProfileCard
          player={filteredBio(p, file.matches, idx)}
          kpis={filteredKpis(file.kpis, file.matches, idx)}
          photo={photo}
          selected={idx === null ? null : idx.size}
        />
        <EventMap view={view} playerName={p.name.full} teamAbbr={file.team.abbr} />
      </section>

      {percentiles}

      <PlayerSequences file={file} idx={idx} team={team} season={season} demo={demo} />
    </>
  );
}
