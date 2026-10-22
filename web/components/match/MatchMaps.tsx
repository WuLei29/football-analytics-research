"use client";

/**
 * MatchMaps — the match page's three event maps, linked to its sequence block.
 *
 * Renders: `LinkedShotMap` (`ShotMap`), `LinkedProgression` (the progression
 * arrows, now with a hover card per arrow) and `LinkedDefence`
 * (`DefensiveActions`). A click on a mark whose chain is drawable opens that
 * chain in the sequence block (`SequenceLink`), as on the player page.
 * Props: what the wrapped component takes, rows straight from the file.
 * Data:  `matches/{match_id}.json` -> `shots`, `progression`,
 *        `defence.actions`, each row's `s` (md/WEB_DATA.md §7.1).
 *
 * WHY WRAPPERS. The page is a server component and cannot hand a click
 * handler across the client boundary; these read the link from context and
 * hand it on. The drawing stays in the shared viz components.
 *
 * Only the published team's events link: the file ships its chains only, so
 * an opponent's shot has `s` null and its hover card offers no click.
 *
 * `LinkedProgression` deviates from the handoff's block 5, which has no hover:
 * a click needs a card saying what it will open, and the card is the player
 * page's arrow card (`ArrowLayer` hit lines, one delegated listener).
 */

import { useRef, useState, type MouseEvent } from "react";

import type { MatchProgression, MatchShot } from "@/lib/data/types";
import { dec } from "@/lib/format";
import { match as copy, viz } from "@/lib/labels";

import { ArrowLayer, type ArrowMark } from "@/components/viz/ActionArrows";
import { DefensiveActions, type DefensiveAction } from "@/components/viz/DefensiveActions";
import { HoverCard } from "@/components/viz/HoverCard";
import { Pitch } from "@/components/viz/Pitch";
import { PROGRESSION_STYLE } from "@/components/viz/ProgressionArrows";
import { ShotMap } from "@/components/viz/ShotMap";

import { useSequenceLink } from "./SequenceLink";

export function LinkedShotMap({
  shots,
  teamSide,
  abbr,
}: {
  shots: MatchShot[];
  teamSide: "home" | "away";
  abbr: { home: string; away: string };
}) {
  const { open } = useSequenceLink();
  return (
    <ShotMap
      shots={shots.map((s) => ({ ...s, linked: s.s != null }))}
      teamSide={teamSide}
      abbr={abbr}
      onSelect={(shot) => open(shots.find((s) => s.shot_id === shot.shot_id)?.s)}
    />
  );
}

export function LinkedDefence({
  actions,
  lineX,
  lineLabel,
  title,
}: {
  actions: (DefensiveAction & { s?: number | null })[];
  lineX?: number;
  lineLabel?: string;
  title: string;
}) {
  const { open } = useSequenceLink();
  return (
    <DefensiveActions
      actions={actions.map((a) => ({ ...a, linked: a.s != null }))}
      lineX={lineX}
      lineLabel={lineLabel}
      title={title}
      onSelect={(i) => open(actions[i]?.s)}
    />
  );
}

export function LinkedProgression({
  actions,
  title,
}: {
  actions: MatchProgression[];
  title: string;
}) {
  const { open } = useSequenceLink();
  const [hovered, setHovered] = useState<number | null>(null);
  const figureRef = useRef<HTMLDivElement>(null);

  const marks: ArrowMark[] = actions.map((a) => ({
    x: a.x,
    y: a.y,
    end_x: a.end_x,
    end_y: a.end_y,
    completed: a.completed,
    ...PROGRESSION_STYLE[a.kind],
  }));

  function index(e: MouseEvent): number | null {
    const i = (e.target as Element).getAttribute("data-i");
    return i === null ? null : Number(i);
  }

  const hot = hovered === null ? null : (actions[hovered] ?? null);
  const H = copy.progressionHover;

  return (
    <div
      ref={figureRef}
      style={{ position: "relative" }}
      onMouseOver={(e) => setHovered(index(e))}
      onMouseLeave={() => setHovered(null)}
      onClick={(e) => {
        const i = index(e);
        if (i !== null) open(actions[i]?.s);
      }}
    >
      <Pitch title={title}>
        <ArrowLayer actions={marks} hoverable highlight={hovered} />
      </Pitch>
      {hot && (
        <HoverCard
          x={hot.x}
          y={hot.y}
          figureHeight={figureRef.current?.clientHeight ?? 0}
          title={hot.player ? `${H[hot.kind]} · ${hot.player}` : H[hot.kind]}
          lines={[
            [
              hot.minute == null ? null : `${hot.minute}'`,
              // A failed action earns 0 xT by construction (CLAUDE.md).
              hot.completed ? (hot.xt === null ? null : H.xt(dec(hot.xt, 3))) : H.failed,
            ]
              .filter(Boolean)
              .join(" · "),
          ].filter(Boolean)}
          hint={hot.s != null ? viz.openSequence : null}
        />
      )}
    </div>
  );
}
