/**
 * Thumbs — the small real drawings in the landing's index rows.
 *
 * Renders: `ShotsThumb` (the latest match's Espanyol shots on a chalk pitch),
 * `ChainThumb` (one possession of that match as a stroke) and `FigureThumb`
 * (one number with its unit, for the player row).
 * Props: the rows straight from the match file; the figure and its words.
 * Data:  `matches/{match_id}.json` -> `shots[]`, `sequences[].points`
 *        (md/WEB_DATA.md §7); `overview.json` -> `leaders` for the figure.
 *
 * Both pitches compose on `<Pitch>`, as every chart on the site does, with
 * the markings in `--color-chalk` so they sit quietly on a panel. At 112 px a
 * shot keeps only its position and whether it was a goal; size by xG would be
 * noise at this scale.
 */

import { project } from "@/lib/viz/pitch";

import { Pitch } from "@/components/viz/Pitch";

const W = "w-[76px] sm:w-[112px]";

export function ShotsThumb({
  shots,
  title,
}: {
  shots: { x: number; y: number; outcome: string }[];
  title: string;
}) {
  return (
    <div className={W}>
      <Pitch title={title} lineColor="var(--color-chalk)">
        {shots.map((s, i) => {
          const p = project("horizontal", s.x, s.y);
          const goal = s.outcome === "goal";
          return (
            <circle
              key={i}
              cx={p.x}
              cy={p.y}
              r={goal ? 3 : 2.2}
              fill={goal ? "var(--color-team)" : "none"}
              stroke="var(--color-team)"
              strokeWidth={0.9}
            />
          );
        })}
      </Pitch>
    </div>
  );
}

export function ChainThumb({ points, title }: { points: [number, number][]; title: string }) {
  const d = points
    .map(([x, y], i) => {
      const p = project("horizontal", x, y);
      return `${i === 0 ? "M" : "L"}${p.x.toFixed(1)} ${p.y.toFixed(1)}`;
    })
    .join(" ");
  const end = points.length ? project("horizontal", ...points[points.length - 1]) : null;
  return (
    <div className={W}>
      <Pitch title={title} lineColor="var(--color-chalk)">
        <path d={d} fill="none" stroke="var(--color-team)" strokeWidth={2} strokeLinecap="round" strokeLinejoin="round" />
        {end && <circle cx={end.x} cy={end.y} r={3.2} fill="var(--color-team)" />}
      </Pitch>
    </div>
  );
}

export function FigureThumb({ value, unit, name }: { value: string; unit: string; name: string }) {
  return (
    <span className="flex w-[76px] flex-col items-end text-right sm:w-[112px]">
      <span className="text-[26px] leading-none font-light tracking-[-0.03em] sm:text-[30px]">{value}</span>
      <span className="mt-1 text-[12px] leading-tight text-slate">
        {unit} · {name}
      </span>
    </span>
  );
}
