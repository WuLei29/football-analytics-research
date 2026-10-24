/**
 * MatchShots — every shot of one match on a pitch, static. The match list's
 * spot panel draws the latest match with it.
 *
 * Renders: a horizontal pitch with Espanyol's shots in `--color-team`
 * (shooting right) and the rival's in `--color-rival` (shooting left), area
 * proportional to xG; Espanyol's solid for goals, half fill on target, hollow
 * off target; the rival's outline only, solid for goals.
 * Props: `shots` (straight from the match file), `teamSide`, `lineColor`
 *        (spot-line on the spot panel, chalk on a panel), `title` for a
 *        screen reader.
 * Data:  `matches/{match_id}.json` -> `shots[]` (md/WEB_DATA.md §7.1). The
 *        rival's shots already arrive mirrored into the left half.
 *
 * The encoding is `ShotMap`'s and `ChainPitch`'s (same radius law, same three
 * fills), so the three maps read alike. Unlike `ChainPitch` nothing here is a
 * control: it composes on `<Pitch>`, which is one labelled image. The marks
 * that can be explored one by one live on screen 02, one click away.
 */

import { project } from "@/lib/viz/pitch";
import type { MatchShot } from "@/lib/data/types";

import { Pitch } from "@/components/viz/Pitch";

const SIZE = { base: 0.75, k: 3.3 };
const ON_TARGET = new Set(["goal", "saved", "woodwork"]);
const radius = (xg: number) => SIZE.base + Math.sqrt(Math.max(0, xg)) * SIZE.k;

export function MatchShots({
  shots,
  teamSide,
  lineColor,
  title,
}: {
  shots: Pick<MatchShot, "shot_id" | "side" | "x" | "y" | "xg" | "outcome">[];
  teamSide: "home" | "away";
  lineColor: string;
  title: string;
}) {
  // Big chances underneath, small ones on top, so none is hidden.
  const byXg = [...shots].sort((a, b) => b.xg - a.xg);

  return (
    <Pitch title={title} lineColor={lineColor}>
      {byXg.map((s) => {
        const p = project("horizontal", s.x, s.y);
        const goal = s.outcome === "goal";
        if (s.side !== teamSide) {
          return (
            <circle
              key={s.shot_id}
              cx={p.x}
              cy={p.y}
              r={radius(s.xg)}
              fill={goal ? "var(--color-rival)" : "none"}
              fillOpacity={goal ? 0.9 : 0}
              stroke="var(--color-rival)"
              strokeWidth={0.3}
              strokeOpacity={0.75}
            />
          );
        }
        return (
          <circle
            key={s.shot_id}
            cx={p.x}
            cy={p.y}
            r={radius(s.xg)}
            fill="var(--color-team)"
            fillOpacity={goal ? 1 : ON_TARGET.has(s.outcome) ? 0.45 : 0}
            stroke="var(--color-team)"
            strokeWidth={0.35}
          />
        );
      })}
    </Pitch>
  );
}
