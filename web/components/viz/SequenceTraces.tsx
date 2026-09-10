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
 */

import { project, type Orientation } from "@/lib/viz/pitch";

import { Pitch } from "./Pitch";

/** One entry of `sequences[]`, reduced to what a trace needs. */
export interface SequenceTrace {
  sequence_id: string;
  /** `[[x, y], ...]` in metres, already simplified by the export. */
  points: [number, number][];
  ends_in_shot: boolean;
}

interface SequenceTracesProps {
  sequences: SequenceTrace[];
  /** The chain drawn in blue. `null` draws every chain at equal weight. */
  selectedId?: string | null;
  /** Pass to make the chains clickable; the page owns the selection. */
  onSelect?: (sequenceId: string) => void;
  orientation?: Orientation;
  title: string;
  className?: string;
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
  title,
  className,
}: SequenceTracesProps) {
  return (
    <Pitch orientation={orientation} title={title} className={className}>
      {sequences.map((sequence) => {
        const style =
          selectedId === null
            ? NEUTRAL
            : sequence.sequence_id === selectedId
              ? SELECTED
              : MUTED;

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
              r={sequence.ends_in_shot ? 1.7 : 0.8}
              fill="none"
              stroke={style.color}
              strokeWidth={0.4}
              strokeOpacity={style.opacity}
            />
          </g>
        );
      })}
    </Pitch>
  );
}
