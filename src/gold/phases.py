"""Zone x tempo phase segmentation — pure transform, no SQL.

Spec: the `phases-of-play` skill (§2 definitions, §3 edge cases, §5 the state
machine), md/GOLD_LAYER.md §4.1.6.

This module owns the one part of the sequence layer that cannot be a GROUP BY:
a sequence is walked event by event, and a phase boundary depends on what
happens *after* the event that might have caused it. Everything else about
phases -- the four origin/structural flags, phase_count, primary_phase,
phase_path -- is derived in SQL by sequence_phases.py from columns that already
exist, so only this walk lives in Python.

Two decisions worth knowing about, both recorded here because the skill leaves
them slightly open:

1. **A zone change is confirmed by TWO consecutive events in the new zone.**
   Skill §3.1 prose says "at least 3 events", but its own implementation hint
   and pseudocode (§5.2) confirm on the next event -- "If the next event is
   still in the new zone, confirm the transition" -- and set
   ``events_in_zone = 2``. The pseudocode is unambiguous, so it wins. The
   threshold is a single constant (:data:`ZONE_CONFIRM_EVENTS`) if it ever
   needs revisiting.

2. **Segments carry zone x tempo, set_piece and chaotic only.** counter_attack,
   high_transition, direct_long and direct_fk_pk are sequence-level flags, per
   skill §5.4 ("applied as additional boolean flags, not as segment types") and
   §2.3 (direct long play "can occur within any other phase"). The CHECK
   constraint on gold.sequence_phase_segments permits all twelve values, so
   promoting one to a segment type later needs no migration.
"""

from __future__ import annotations

import math
from typing import NamedTuple, Sequence

# ── tunable parameters, all from the skill ──────────────────────────────────
TEMPO_THRESHOLD = 3          # >= 3 passes/carries in a zone == established (§2.1)
ZONE_CONFIRM_EVENTS = 2      # consecutive events needed to confirm a zone change
SET_PIECE_WINDOW_SEC = 20    # the set-piece phase window (§2.3)
OWN_HALF_X = 52.5            # halfway line, metres

# Zone 1 = defensive third, 2 = middle, 3 = final. Boundaries match
# gold.third() and §4.5.0 exactly.
ZONE_BOUNDARIES = (35.0, 70.0)

# zone -> (established phase, fast phase)
ZONE_PHASE = {
    1: ("buildup", "fast_buildup"),
    2: ("midblock", "fast_midblock"),
    3: ("attacking", "fast_attacking"),
}

ON_BALL_PROGRESSION_TYPES = ("Pass", "Carry")


class PhaseEvent(NamedTuple):
    """One possessing-team event, in json_index order."""

    event_id: int
    event_type: str
    x: float | None
    y: float | None
    end_x: float | None
    end_y: float | None
    minute: int
    second: int
    xt: float | None
    is_cross: bool


class PhaseSegment(NamedTuple):
    """One contiguous phase within a sequence."""

    phase_order: int
    phase_type: str
    start_event_id: int
    end_event_id: int
    event_count: int
    pass_carry_count: int
    start_x: float | None
    end_x: float | None
    start_third: int | None
    end_third: int | None
    duration_seconds: float
    xt: float | None


def classify_zone(x: float | None) -> int | None:
    """Third 1-3 from an x coordinate. Mirrors gold.third()."""
    if x is None:
        return None
    if x < ZONE_BOUNDARIES[0]:
        return 1
    if x < ZONE_BOUNDARIES[1]:
        return 2
    return 3


def _clock(event: PhaseEvent) -> int:
    """Seconds since kick-off of the period.

    Derived from minute*60 + second, NEVER from `timestamp`, which is NULL on
    every synthesised carry (GOLD_LAYER §4.1.0 #1).
    """
    return event.minute * 60 + event.second


def _tempo_phase(zone: int, pass_carry_count: int) -> str:
    established, fast = ZONE_PHASE[zone]
    return established if pass_carry_count >= TEMPO_THRESHOLD else fast


def _build_segment(
    events: Sequence[PhaseEvent],
    lo: int,
    hi: int,
    phase_order: int,
    phase_type: str | None,
) -> PhaseSegment:
    """Materialise events[lo:hi] as one segment.

    `phase_type` of None means "classify by zone and tempo"; a string forces
    the label (used for the set-piece window).
    """
    chunk = events[lo:hi]
    pass_carry = sum(1 for e in chunk if e.event_type in ON_BALL_PROGRESSION_TYPES)

    if phase_type is None:
        zone = classify_zone(chunk[0].x)
        phase_type = "chaotic" if zone is None else _tempo_phase(zone, pass_carry)

    xt_values = [e.xt for e in chunk if e.xt is not None]

    return PhaseSegment(
        phase_order=phase_order,
        phase_type=phase_type,
        start_event_id=chunk[0].event_id,
        end_event_id=chunk[-1].event_id,
        event_count=len(chunk),
        pass_carry_count=pass_carry,
        start_x=chunk[0].x,
        end_x=chunk[-1].x,
        start_third=classify_zone(chunk[0].x),
        end_third=classify_zone(chunk[-1].x),
        duration_seconds=float(max(_clock(chunk[-1]) - _clock(chunk[0]), 0)),
        xt=sum(xt_values) if xt_values else None,
    )


def qualifies_as_set_piece(
    sequence_type: str,
    events: Sequence[PhaseEvent],
) -> bool:
    """Whether the sequence opens with a set-piece phase (skill §2.3).

    Exclusions are the point of this function: a quick free kick in the team's
    own half, or one with no cross attempt, is *not* a set piece -- it is the
    start of build-up / mid-block / attacking play in whatever zone it happens
    in. Treating every free kick as a set piece is what makes set-piece counts
    look absurdly high.
    """
    if not events:
        return False

    first = events[0]

    if sequence_type == "corner":
        return True

    if sequence_type == "free_kick":
        # Own-half quick free kicks are excluded; so are free kicks with no
        # attempt to cross or otherwise deliver into the area.
        if first.x is None or first.x < OWN_HALF_X:
            return False
        return bool(first.is_cross) or _ends_in_box(first)

    if sequence_type == "throw_in":
        # Only a LONG throw directed into the penalty area counts.
        return _ends_in_box(first)

    return False


def _ends_in_box(event: PhaseEvent) -> bool:
    if event.end_x is None or event.end_y is None:
        return False
    return event.end_x >= 88.5 and 13.84 <= event.end_y <= 54.16


def is_direct_long(event: PhaseEvent) -> bool:
    """A single ball played >= 32 m forward within 30 degrees of the goal axis.

    Skill §2.3 / §6 #10. Kept here rather than in SQL only because the angle
    test reads far more clearly in Python; sequence_phases.py mirrors it.
    """
    if None in (event.x, event.y, event.end_x, event.end_y):
        return False
    dx = event.end_x - event.x
    if dx < 32.0:
        return False
    dy = abs(event.end_y - event.y)
    return dy <= math.tan(math.radians(30.0)) * dx


def segment_sequence(
    events: Sequence[PhaseEvent],
    sequence_type: str,
) -> list[PhaseSegment]:
    """Split one sequence's possessing-team events into phase segments.

    Returns segments in order, `phase_order` starting at 1. An empty input
    returns an empty list; a single-event sequence returns one `chaotic`
    segment (skill §3.2), which is the expected label for the ~43,000
    one-event clearances and recoveries Fix B began recording.
    """
    if not events:
        return []

    segments: list[PhaseSegment] = []
    offset = 0

    # ── the set-piece window, when it applies (skill §2.3, §3.3) ────────────
    if qualifies_as_set_piece(sequence_type, events):
        restart = _clock(events[0])
        offset = len(events)
        for i, event in enumerate(events):
            if _clock(event) - restart > SET_PIECE_WINDOW_SEC:
                offset = i
                break
        if offset > 0:
            segments.append(_build_segment(events, 0, offset, 1, "set_piece"))
        if offset >= len(events):
            return segments

    remaining = events[offset:]

    if len(remaining) == 1:
        segments.append(
            _build_segment(remaining, 0, 1, len(segments) + 1, "chaotic")
        )
        return segments

    # ── the zone x tempo walk (skill §5.2) ─────────────────────────────────
    #
    # `pending_zone` is the buffer that makes §3.1 work: a single event in a
    # new zone followed by a return does NOT open a phase. Without it, one
    # pass played into midfield and immediately back to the centre backs
    # fragments a patient build-up into three phases.
    current_zone = classify_zone(remaining[0].x)
    segment_start = 0
    pending_zone: int | None = None
    pending_index: int | None = None
    confirm_run = 0

    for i, event in enumerate(remaining):
        zone = classify_zone(event.x)

        if zone is None or zone == current_zone:
            # Back in the current zone: whatever was pending was a blip.
            pending_zone = None
            pending_index = None
            confirm_run = 0
            continue

        if zone == pending_zone:
            confirm_run += 1
            if confirm_run >= ZONE_CONFIRM_EVENTS - 1:
                # Confirmed. The segment ends just before the event that first
                # entered the new zone, not before this one.
                segments.append(
                    _build_segment(
                        remaining, segment_start, pending_index,
                        len(segments) + 1, None,
                    )
                )
                segment_start = pending_index
                current_zone = zone
                pending_zone = None
                pending_index = None
                confirm_run = 0
        else:
            pending_zone = zone
            pending_index = i
            confirm_run = 0

    segments.append(
        _build_segment(
            remaining, segment_start, len(remaining), len(segments) + 1, None
        )
    )
    return segments
