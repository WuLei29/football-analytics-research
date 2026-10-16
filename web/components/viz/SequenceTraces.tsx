/**
 * SequenceTraces — possession sequences as chains across the pitch.
 *
 * Renders: one polyline per sequence, a dot at the start, and a ring at the
 * end — large when the chain ended in a shot, small otherwise. When one
 * sequence is selected it is drawn in blue and every other chain fades back.
 * Props: `sequences`, `selectedId`, `onSelect` (optional — pass it and the
 * chains become clickable), `orientation`, `title`.
 * Data:  `matches/{match_id}.json` -> `sequences[]` and
 *        `teams/{team}/{season}/sequences.json` -> `sequences[]`
 *        (md/WEB_DATA.md §7 and §10), both from `gold.sequences`. `points` is
 *        the sequence's events ordered by `(json_index, event_id)`, simplified
 *        to start, end and up to six waypoints.
 *
 * Selection is the interaction of screen 02 block 6 and screen 04: the pitch
 * and the sequence list beside it are two views of one selection, which the
 * page owns. This component is deliberately not a client component — it takes
 * the selected id as a prop, so a page can hold that state and a page that has
 * no selection can render it as a server component with no JavaScript at all.
 *
 * Unselected chains keep the `--faint` grey of the design rather than a per-
 * chain colour: inside a pitch figure only the thing being read gets the
 * accent (espanyol-viz-design §1).
 *
 * `variant="player"` is screen 03's encoding (README-PLAYER §5): the stroke
 * width grows with the chain's `value` (its xT or VAEP, per the page's
 * toggle), the end ring is 1.8 on a shot, and the selected chain gets a
 * `--gold` dot on `touch`, the player's own first touch, which the export
 * forced into the polyline so the dot always sits on the line.
 */

import { project, type Orientation } from "@/lib/viz/pitch";

import { Pitch } from "./Pitch";

/** One entry of `sequences[]`, reduced to what a trace needs. */
export interface SequenceTrace {
  sequence_id: string;
  /** `[[x, y], ...]` in metres, already simplified by the export. */
  points: [number, number][];
  ends_in_shot: boolean;
  /** `variant="player"` only: the value the width encodes (xT or VAEP). */
  value?: number | null;
  /** `variant="player"` only: the player's touch, drawn on the selected chain. */
  touch?: [number, number] | null;
}

interface SequenceTracesProps {
  sequences: SequenceTrace[];
  /** The chain drawn in blue. `null` draws every chain at equal weight. */
  selectedId?: string | null;
  /** Pass to make the chains clickable; the page owns the selection. */
  onSelect?: (sequenceId: string) => void;
  orientation?: Orientation;
  /** "match" (screen 02, the default) or "player" (screen 03). */
  variant?: "match" | "player";
  title: string;
  className?: string;
}

/**
 * Screen 03: width = base + value * gain, the value floored at 0 (a VAEP can
 * be negative, and a negative width is no width at all).
 */
function playerStyle(selected: boolean, value: number | null | undefined) {
  const v = Math.max(0, value ?? 0);
  return selected
    ? { width: 1.1 + v * 3.4, opacity: 1, color: "var(--color-blue)" }
    : { width: 0.35 + v * 1.2, opacity: 0.3, color: "var(--color-faint)" };
}

/** Handoff, screen 02 block 6: selected 1.5 / 0.95, others 0.5 / 0.2. */
const SELECTED = { width: 1.5, opacity: 0.95, color: "var(--color-blue)" };
const MUTED = { width: 0.5, opacity: 0.2, color: "var(--color-faint)" };
/** With nothing selected, every chain reads at the same middling weight. */
const NEUTRAL = { width: 0.9, opacity: 0.6, color: "var(--color-blue)" };

export function SequenceTraces({
  sequences,
  selectedId = null,
  onSelect,
  orientation = "horizontal",
  variant = "match",
  title,
  className,
}: SequenceTracesProps) {
  const isPlayer = variant === "player";
  // The selected chain is drawn last, so it is never under a grey one.
  const ordered = isPlayer
    ? [
        ...sequences.filter((s) => s.sequence_id !== selectedId),
        ...sequences.filter((s) => s.sequence_id === selectedId),
      ]
    : sequences;

  return (
    <Pitch orientation={orientation} title={title} className={className}>
      {ordered.map((sequence) => {
        const selected = sequence.sequence_id === selectedId;
        const style = isPlayer
          ? playerStyle(selected, sequence.value)
          : selectedId === null
            ? NEUTRAL
            : selected
              ? SELECTED
              : MUTED;
        const touch =
          isPlayer && selected && sequence.touch
            ? project(orientation, sequence.touch[0], sequence.touch[1])
            : null;

        const points = sequence.points.map(([x, y]) => project(orientation, x, y));
        const start = points[0];
        const end = points[points.length - 1];
        if (!start || !end) return null;

        return (
          <g
            key={sequence.sequence_id}
            onClick={onSelect ? () => onSelect(sequence.sequence_id) : undefined}
            style={onSelect ? { cursor: "pointer" } : undefined}
          >
            <polyline
              points={points.map((p) => `${p.x.toFixed(1)},${p.y.toFixed(1)}`).join(" ")}
              fill="none"
              stroke={style.color}
              strokeWidth={style.width}
              strokeOpacity={style.opacity}
              strokeLinejoin="round"
            />
            <circle cx={start.x} cy={start.y} r={0.9} fill={style.color} fillOpacity={style.opacity} />
            <circle
              cx={end.x}
              cy={end.y}
              r={sequence.ends_in_shot ? (isPlayer ? 1.8 : 1.7) : isPlayer ? 0.9 : 0.8}
              fill="none"
              stroke={style.color}
              strokeWidth={0.4}
              strokeOpacity={style.opacity}
            />
            {touch && <circle cx={touch.x} cy={touch.y} r={1.5} fill="var(--color-gold)" />}
          </g>
        );
      })}
    </Pitch>
  );
}
