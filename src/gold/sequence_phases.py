"""Phase pass — gold.sequence_phase_segments + the phase columns on gold.sequences.

Spec: md/GOLD_LAYER.md §4.1.6, §6 Step 3b, the `phases-of-play` skill.

Runs as::

    python -m src.gold.build_gold --step sequence_phases
    python -m src.gold.sequence_phases              # standalone, same work

Division of labour, following the events-pipeline convention (pure transform /
SQL / orchestration kept apart):

* ``phases.py``  -- the zone x tempo state machine. No SQL.
* this module    -- reads events, writes segments, then derives everything else
                    in SQL from columns that already exist.

Only the segmentation genuinely needs a row-by-row walk. The four overlay flags
(counter_attack, high_transition, direct_long, direct_fk_pk) are pure functions
of columns build_sequences.sql already computed -- start_trigger, start_x,
field_progression, directness, duration_seconds, pass_count -- so they are a
single UPDATE rather than more Python. phase_count, primary_phase and
phase_path likewise aggregate straight out of the segments table.

**This step is a hard dependency of the team build, not an enrichment.**
Running build_team_match_stats.sql without it leaves counter_attack_sequences
and high_transition_sequences at zero across the league -- a plausible-looking
number that is simply wrong (§5).
"""

from __future__ import annotations

import argparse
import logging
import sys
from itertools import groupby

from psycopg2.extras import execute_values

from src.gold.phases import PhaseEvent, segment_sequence

log = logging.getLogger("sequence_phases")

# Matches per fetch. Keeps the working set to ~50k event rows rather than
# pulling all 884k into memory at once.
MATCH_BATCH = 25

# The eleven has_* flags, in DDL order.
ZONE_FLAGS = [
    "buildup",
    "fast_buildup",
    "midblock",
    "fast_midblock",
    "attacking",
    "fast_attacking",
    "set_piece",
]

_FETCH_EVENTS = """
    SELECT s.sequence_id,
           s.sequence_type,
           e.event_id, e.event_type,
           e.x, e.y, e.end_x, e.end_y,
           e.minute, e.second, e.xt,
           (e.raw_data->'qualifier' @> '[{"qualifierId": 2}]') AS is_cross
    FROM gold.sequences s
    JOIN silver.events e
      ON e.sequence_id = s.sequence_id
     AND e.team_id     = s.team_id
    WHERE s.match_id = ANY(%(match_ids)s)
    ORDER BY s.sequence_id, e.json_index, e.event_id
"""

_INSERT_SEGMENTS = """
    INSERT INTO gold.sequence_phase_segments (
        sequence_id, phase_order, phase_type,
        start_event_id, end_event_id, event_count, pass_carry_count,
        start_x, end_x, start_third, end_third, duration_seconds, xt
    ) VALUES %s
"""

# Derived from the segments themselves. primary_phase is the segment with the
# most events, ties broken by the LATER phase_order (§4.1.6).
_UPDATE_FROM_SEGMENTS = """
    UPDATE gold.sequences s SET
        has_buildup        = f.has_buildup,
        has_fast_buildup   = f.has_fast_buildup,
        has_midblock       = f.has_midblock,
        has_fast_midblock  = f.has_fast_midblock,
        has_attacking      = f.has_attacking,
        has_fast_attacking = f.has_fast_attacking,
        has_set_piece      = f.has_set_piece,
        phase_count        = f.phase_count,
        primary_phase      = f.primary_phase,
        phase_path         = f.phase_path
    FROM (
        SELECT sequence_id,
               bool_or(phase_type = 'buildup')        AS has_buildup,
               bool_or(phase_type = 'fast_buildup')   AS has_fast_buildup,
               bool_or(phase_type = 'midblock')       AS has_midblock,
               bool_or(phase_type = 'fast_midblock')  AS has_fast_midblock,
               bool_or(phase_type = 'attacking')      AS has_attacking,
               bool_or(phase_type = 'fast_attacking') AS has_fast_attacking,
               bool_or(phase_type = 'set_piece')      AS has_set_piece,
               count(*)::smallint                     AS phase_count,
               (array_agg(phase_type ORDER BY event_count DESC, phase_order DESC))[1]
                                                      AS primary_phase,
               left(string_agg(phase_type, '>' ORDER BY phase_order), 120)
                                                      AS phase_path
        FROM gold.sequence_phase_segments
        WHERE sequence_id = ANY(%(sequence_ids)s)
        GROUP BY sequence_id
    ) f
    WHERE f.sequence_id = s.sequence_id
"""

# The origin and structural overlays (skill §2.2, §2.3, §5.4). Every input is
# already a column on gold.sequences, except the direct-long event test.
#
#   counter_attack   ball won in OWN half + >= 75 pct directionality
#                    + >= 16.5 m forward + (<= 15 s or <= 5 passes)
#   high_transition  the same from a recovery in the OPPONENT half,
#                    with the looser (<= 20 s or <= 8 passes) tempo test
#   direct_long      one ball >= 32 m forward within 30 deg of the goal axis
#   direct_fk_pk     a goal scored directly from a free kick or penalty
_UPDATE_OVERLAY_FLAGS = """
    UPDATE gold.sequences s SET
        has_counter_attack = (
            s.start_trigger IN ('tackle','interception','recovery','block')
            AND s.start_x < 52.5
            AND s.field_progression >= 16.5
            AND COALESCE(s.directness, 0) >= 0.75
            AND (s.duration_seconds <= 15 OR s.pass_count <= 5)
        ),
        has_high_transition = (
            s.start_trigger IN ('tackle','interception','recovery','block')
            AND s.start_x >= 52.5
            AND s.field_progression >= 16.5
            AND COALESCE(s.directness, 0) >= 0.75
            AND (s.duration_seconds <= 20 OR s.pass_count <= 8)
        ),
        has_direct_long = EXISTS (
            SELECT 1 FROM silver.events e
            WHERE e.sequence_id = s.sequence_id
              AND e.team_id     = s.team_id
              AND e.event_type IN ('Pass','Carry')
              AND e.end_x - e.x >= 32
              AND abs(e.end_y - e.y) <= tan(radians(30)) * (e.end_x - e.x)
        ),
        has_direct_fk_pk = (
            s.sequence_type IN ('free_kick','penalty')
            AND s.goal_count > 0
            AND s.event_count <= 2
        )
    WHERE s.match_id = ANY(%(match_ids)s)
"""


def _scope_match_ids(cur, match_ids: list[int] | None) -> list[int]:
    if match_ids:
        return list(match_ids)
    cur.execute("SELECT DISTINCT match_id FROM gold.sequences ORDER BY 1")
    return [r[0] for r in cur.fetchall()]


def _reset_scope(cur, match_ids: list[int]) -> None:
    """Make the pass idempotent.

    The segments are removed and every phase column is put back to its default
    before anything is written, so a re-run over the same matches cannot leave
    a flag set by a previous definition.
    """
    cur.execute(
        """
        DELETE FROM gold.sequence_phase_segments
        WHERE sequence_id IN (
            SELECT sequence_id FROM gold.sequences WHERE match_id = ANY(%(match_ids)s)
        )
        """,
        {"match_ids": match_ids},
    )
    cur.execute(
        """
        UPDATE gold.sequences SET
            has_buildup = FALSE, has_fast_buildup = FALSE,
            has_midblock = FALSE, has_fast_midblock = FALSE,
            has_attacking = FALSE, has_fast_attacking = FALSE,
            has_set_piece = FALSE, has_counter_attack = FALSE,
            has_high_transition = FALSE, has_direct_long = FALSE,
            has_direct_fk_pk = FALSE,
            phase_count = NULL, primary_phase = NULL, phase_path = NULL
        WHERE match_id = ANY(%(match_ids)s)
        """,
        {"match_ids": match_ids},
    )


def run_phase_pass(conn, match_ids: list[int] | None = None) -> tuple[int, int]:
    """Classify phases for the given matches (all of them when None).

    Returns ``(sequences_classified, segments_written)``.
    """
    total_sequences = 0
    total_segments = 0

    with conn.cursor() as cur:
        scope = _scope_match_ids(cur, match_ids)
        if not scope:
            log.warning("gold.sequences is empty for the requested scope")
            return (0, 0)

        _reset_scope(cur, scope)

        for start in range(0, len(scope), MATCH_BATCH):
            batch = scope[start:start + MATCH_BATCH]
            cur.execute(_FETCH_EVENTS, {"match_ids": batch})
            rows = cur.fetchall()

            segment_rows: list[tuple] = []
            batch_sequence_ids: list[str] = []

            for sequence_id, group in groupby(rows, key=lambda r: r[0]):
                group = list(group)
                sequence_type = group[0][1]
                events = [
                    PhaseEvent(
                        event_id=r[2],
                        event_type=r[3],
                        x=r[4],
                        y=r[5],
                        end_x=r[6],
                        end_y=r[7],
                        minute=r[8],
                        second=r[9],
                        xt=r[10],
                        is_cross=bool(r[11]),
                    )
                    for r in group
                ]

                segments = segment_sequence(events, sequence_type)
                batch_sequence_ids.append(sequence_id)
                total_sequences += 1
                for seg in segments:
                    segment_rows.append((sequence_id, *seg[:]))

            if segment_rows:
                execute_values(cur, _INSERT_SEGMENTS, segment_rows, page_size=1000)
                total_segments += len(segment_rows)

            if batch_sequence_ids:
                cur.execute(
                    _UPDATE_FROM_SEGMENTS, {"sequence_ids": batch_sequence_ids}
                )

            log.info(
                "  matches %d-%d of %d  |  %s sequences  %s segments",
                start + 1,
                min(start + MATCH_BATCH, len(scope)),
                len(scope),
                f"{total_sequences:,}",
                f"{total_segments:,}",
            )

        cur.execute(_UPDATE_OVERLAY_FLAGS, {"match_ids": scope})

    conn.commit()
    return (total_sequences, total_segments)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Classify phases of play over gold.sequences (GOLD_LAYER §4.1.6)."
    )
    parser.add_argument("--match-ids", type=int, nargs="+")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-7s %(message)s",
        datefmt="%H:%M:%S",
    )

    from src.gold.build_gold import connect

    conn = connect()
    try:
        n_seq, n_seg = run_phase_pass(conn, match_ids=args.match_ids)
    finally:
        conn.close()
    log.info("%s sequences classified, %s segments written", f"{n_seq:,}", f"{n_seg:,}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
