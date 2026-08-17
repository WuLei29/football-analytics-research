"""
sequences.py — Possession Sequence Pipeline (Step 1)
═════════════════════════════════════════════════════
Populates sequence_id, sequence_start, sequence_end, sequence_event_number
in silver.events.

Placement: src/silver/events/sequences.py

Pipeline:
  1. SELECT events for target match_ids from silver.events
  2. Pre-process: filter excluded rows, materialise qualifier flags
  3. Run classify_possession_sequences() — state-machine classifier
  4. UPDATE silver.events with computed sequence columns

Idempotency:
  - Resets all sequence columns for target match_ids before processing
  - Safe to re-run after classifier fixes or new data

Usage:
  python -m src.silver.events.sequences [--match-ids 123 456] [--limit 10] [--batch-size 50]
"""

import json
import logging
from typing import Any, Dict, List, Optional, Set, Tuple

import numpy as np
import pandas as pd
import psycopg2
import psycopg2.extras

log = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────────
# Constants
# ──────────────────────────────────────────────────────────────────────

# Meta events: hard sequence boundaries. Never assigned to a sequence.
# A sequence ends just before them, a new one starts just after.
META_EVENTS = frozenset({
    'Start', 'End', 'Player on', 'Player off', 'Player retired',
})

# Excluded events: filtered out entirely before classification.
# They never affect sequence logic — treated as if they don't exist.
EXCLUDED_EVENTS = frozenset({
    'Formation change',
    'Team set up',
    'Card',
    'Coach Setup',
    'Collection End',
    'Condition change',
    'Contentious referee decision',
    'End delay',
    'Injury Time Announcement',
    'Official change',
    'Start delay',
    'Attempted Tackle',
})

# Events that are excluded from the classifier but should be backfilled
# into their surrounding sequence afterwards (sequence_event_number = 0
# to mark them as interpolated, not part of the possession chain).
BACKFILL_EVENTS = frozenset({
    'Attempted Tackle',
})

# Excluded periods: pre-match (16), penalty shootout / other non-live (14)
EXCLUDED_PERIODS = frozenset({14, 16})

# Qualifier IDs for set-piece passes (always start a sequence)
#   Q5  = Free kick taken
#   Q6  = Corner taken
#   Q107 = Throw-in
SET_PIECE_QUALIFIER_IDS = frozenset({5, 6, 107})

# Qualifier IDs for dead-ball goals (only these goals can start a sequence)
#   Q9 = Penalty
#   Q5 = Free kick taken
DEAD_BALL_GOAL_QUALIFIER_IDS = frozenset({9, 5})

# Shot event types (for gold-layer aggregation reference)
SHOT_EVENT_TYPES = frozenset({
    'Goal', 'Attempt Saved', 'Miss', 'Post',
})

# ── Fix A / Fix B support sets (see md/GOLD_SEQUENCES.md §6.3) ────────
#
# Both fixes are ON by default as of 16 Aug 2026, after verification on 100
# matches against two gates: the classifier is deterministic under input
# shuffling, and no event that belonged to a sequence loses its coverage.
# Pass --no-fix-a / --no-fix-b to reproduce the pre-fix behaviour for an A/B
# comparison; do not run a production backfill that way, or the database ends
# up with two classification generations mixed together.
#
# Neither fix ever removes sequence coverage. Fix A changes coverage by
# exactly zero events — it only re-partitions events between sequences.

# Fix A — events that do NOT open a new sequence even when they belong to a
# team other than the one that owns the running sequence.  These are
# deflections, contested duels, and mirror records (the same physical action
# logged once per team).  Opening a sequence on any of them would break
# behaviour that is currently correct — in particular a `Ball touch` with
# outcome 'success' is Opta's "ball simply hit the player unintentionally",
# i.e. a deflection that does not concede possession.
PASSIVE_EVENTS = frozenset({
    'Ball touch', 'Error', 'Challenge', 'Aerial', 'Foul', 'Corner Awarded',
    'Save', 'Offside provoked', 'Attempted Tackle', 'Penalty faced',
})

# Fix B — events that may open a sequence when no sequence is running.
# Deliberately conservative: possession-bearing actions only.  Every member
# either already opens a sequence under some narrower condition today, or is
# an unambiguous on-ball action.  Mirror/passive events are excluded so the
# gap closer cannot manufacture a sequence out of a bookkeeping duplicate.
ON_BALL_START_EVENTS = frozenset({
    'Pass', 'Carry', 'Take On', 'Clearance', 'Ball recovery', 'Tackle',
    'Interception', 'Blocked Pass', 'Keeper pick-up', 'Claim',
    'Goal', 'Miss', 'Post', 'Attempt Saved',
})

# Of those, the contested ones only carry possession when they SUCCEED.
# A `Tackle` with outcome 'failure' means the tackler did *not* win the ball,
# so opening a sequence for their team there would invent a possession that
# never happened — 1,342 of them in a 100-match sample before this gate was
# added.  `Pass` and `Take On` are deliberately absent: a misplaced pass and a
# failed dribble are both actions by a player who *had* the ball, which is the
# whole point of the gap closer.
FIX_B_REQUIRES_SUCCESS = frozenset({
    'Tackle', 'Interception', 'Blocked Pass', 'Ball recovery',
    'Keeper pick-up', 'Claim',
})

# Columns required from silver.events for the classifier
REQUIRED_COLUMNS = [
    'event_id', 'match_id', 'period', 'minute', 'second',
    'event_type', 'outcome', 'source_team_id', 'h_a',
    'team_id', 'type_id', 'raw_data',
]


# ──────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────

def _extract_qualifier_ids(raw_data) -> Set[int]:
    """Extract the set of qualifierIds from a raw_data JSONB payload."""
    if raw_data is None or (isinstance(raw_data, float) and np.isnan(raw_data)):
        return set()
    if isinstance(raw_data, str):
        try:
            raw_data = json.loads(raw_data)
        except (json.JSONDecodeError, TypeError):
            return set()
    if not isinstance(raw_data, dict):
        return set()
    qualifiers = raw_data.get('qualifier', [])
    ids = set()
    for q in qualifiers:
        if isinstance(q, dict):
            qid = q.get('qualifierId')
            if qid is None:
                qid = q.get('id')
            if qid is not None:
                ids.add(int(qid))
    return ids


def _has_value_assist(row) -> bool:
    """Check whether the event carries a non-null value_assist."""
    val = row.get('value_assist')
    if val is None:
        return False
    if isinstance(val, float) and np.isnan(val):
        return False
    return True


# ──────────────────────────────────────────────────────────────────────
# Pre-processing
# ──────────────────────────────────────────────────────────────────────

def preprocess_for_sequences(df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare a silver.events DataFrame for the possession-sequence classifier.

    Steps:
      1. Filter out excluded periods (14, 16) and excluded event types.
      2. Sort chronologically within each match.
      3. Materialise boolean flag columns from raw_data qualifiers:
         - is_set_piece_pass:  Pass with Q5, Q6, or Q107
         - is_dead_ball_goal:  Goal with Q9 or Q5
      4. Ensure value_assist column exists (default NaN if absent).

    Returns a new DataFrame — the original is never modified.
    """
    # ── Filter ────────────────────────────────────────────────────────
    mask = (
        ~df['period'].isin(EXCLUDED_PERIODS)
        & ~df['event_type'].isin(EXCLUDED_EVENTS)
    )
    out = df.loc[mask].copy()

    # ── Sort chronologically ──────────────────────────────────────────
    # json_index captures the original JSONP array position.
    # Carries get fractional values (e.g. 42.5 between events 42 and 43),
    # so sorting by json_index preserves correct chronological order
    # for both real and synthesised events.
    #
    # event_id is a REQUIRED tiebreaker, not a nicety: 11,766 events share a
    # json_index with another event in the same match (6,026 Carry, 5,445
    # Challenge). Without it the order of those pairs is whatever the database
    # happened to return, and since _is_sequence_start reads prev_row's team,
    # a Carry/Challenge pair resolving the other way flips the decision. The
    # classifier was non-deterministic across runs before this was added.
    sort_cols = ['match_id', 'json_index']
    if 'event_id' in out.columns:
        sort_cols.append('event_id')
    else:
        log.warning(
            "Column 'event_id' not found — sequence classification will be "
            "non-deterministic for the %d events that share a json_index.",
            int(out.duplicated(subset=['match_id', 'json_index'], keep=False).sum()),
        )
    out = out.sort_values(sort_cols, na_position='last').reset_index(drop=True)

    # ── Materialise qualifier flags ───────────────────────────────────
    qualifier_ids_series = out['raw_data'].apply(_extract_qualifier_ids)

    out['is_set_piece_pass'] = (
        (out['event_type'] == 'Pass')
        & qualifier_ids_series.apply(
            lambda qids: bool(qids & SET_PIECE_QUALIFIER_IDS)
        )
    )

    out['is_dead_ball_goal'] = (
        (out['event_type'] == 'Goal')
        & qualifier_ids_series.apply(
            lambda qids: bool(qids & DEAD_BALL_GOAL_QUALIFIER_IDS)
        )
    )

    # ── Ensure value_assist exists ────────────────────────────────────
    if 'value_assist' not in out.columns:
        out['value_assist'] = np.nan
        log.warning(
            "Column 'value_assist' not found in DataFrame — "
            "assist-based sequence guards will be inactive."
        )

    return out


# ──────────────────────────────────────────────────────────────────────
# Start / End logic
# ──────────────────────────────────────────────────────────────────────

def _is_sequence_start(
    row: dict,
    prev_row: Optional[dict],
    prev_prev_row: Optional[dict] = None,
    in_sequence: bool = False,
    current_seq_team: Optional[Any] = None,
    fix_a: bool = True,
    fix_b: bool = True,
) -> bool:
    """
    Determine whether the current event opens a new possession sequence.

    Conditions are evaluated in priority order.  The first match wins.

    Parameters
    ----------
    current_seq_team : source_team_id owning the running sequence, or None.
        Only read by Fix A.
    fix_a, fix_b : bool
        Opt-in classifier fixes, both default OFF.  With both off this
        function returns exactly what it returned before they existed —
        every new branch is guarded and appended after the original ones.
    """
    evt = row['event_type']
    outcome = row['outcome']

    # ── Corner Awarded can be *part of* a sequence but never starts one
    if evt == 'Corner Awarded':
        return False

    # ── Sandwich continuation guard ────────────────────────────────────
    # If we're inside an active sequence and the previous event was a
    # Ball touch or Error from the OTHER team that didn't end the sequence
    # (sandwich exemption applied), this event is a continuation, not a
    # new start.  Without this guard, the team-change checks below would
    # incorrectly open a new sequence.
    #
    # Only suppress when the Ball touch/Error was a genuine sandwich:
    # prev_prev_row must be the same team as the current row (Team A →
    # Team B touch → Team A pattern).
    if (in_sequence
            and prev_row is not None
            and prev_row['event_type'] in ('Ball touch', 'Error')
            and prev_row['source_team_id'] != row['source_team_id']
            and prev_prev_row is not None
            and prev_prev_row['source_team_id'] == row['source_team_id']):
        return False

    # ── Fix 3: Set-piece pass always starts ────────────────────────────
    if row.get('is_set_piece_pass', False):
        return True

    # ── Fix 5: Dead-ball goal starts (penalty / direct free kick) ──────
    if evt == 'Goal' and row.get('is_dead_ball_goal', False):
        return True

    # ── Meta-event boundary ────────────────────────────────────────────
    # Any non-meta event after a meta event opens a new sequence.
    if (prev_row is not None
            and prev_row['event_type'] in META_EVENTS
            and evt not in META_EVENTS):
        return True

    # ── Successful Pass — sub-conditions ───────────────────────────────
    if evt == 'Pass' and outcome == 'success':
        # Fix 2: assist pass must not break the current sequence
        if _has_value_assist(row):
            return False

        if prev_row is None:
            return True
        if prev_row['event_type'] == 'Start':
            return True
        if prev_row['event_type'] == 'Corner Awarded':
            return True
        if prev_row['event_type'] == 'Offside provoked':
            return True
        # Team change, but NOT via a Challenge (contested duel)
        if (prev_row['source_team_id'] != row['source_team_id']
                and prev_row['event_type'] != 'Challenge'):
            return True

    # ── Successful Tackle ──────────────────────────────────────────────
    if evt == 'Tackle' and outcome == 'success':
        return True

    # ── Successful Blocked Pass ────────────────────────────────────────
    if evt == 'Blocked Pass' and outcome == 'success':
        return True

    # ── Successful Ball recovery — Fix 4: also after Ball touch ────────
    if evt == 'Ball recovery' and outcome == 'success':
        if prev_row is not None:
            if prev_row['source_team_id'] != row['source_team_id']:
                return True
            if prev_row['event_type'] == 'Ball touch':
                return True

    # ── Successful Interception ────────────────────────────────────────
    if evt == 'Interception' and outcome == 'success':
        return True

    # ── Successful Claim (GK) ─────────────────────────────────────────
    if evt == 'Claim' and outcome == 'success':
        return True

    # ── Successful Keeper pick-up ──────────────────────────────────────
    if evt == 'Keeper pick-up' and outcome == 'success':
        return True

    # ── Attempt Saved from opposition (not after Aerial/Challenge) ─────
    if evt == 'Attempt Saved':
        if (prev_row is not None
                and prev_row['source_team_id'] != row['source_team_id']
                and prev_row['event_type'] not in ('Aerial', 'Challenge')):
            return True

    # ── Clearance after opposition action ──────────────────────────────
    if evt == 'Clearance':
        if prev_row is not None and prev_row['source_team_id'] != row['source_team_id']:
            return True

    # ══ Everything below is opt-in and additive ════════════════════════
    # Reaching here means the original rules all declined.  Neither branch
    # can therefore suppress a start that would otherwise have happened.

    # ── Fix A: possession changed relative to the SEQUENCE OWNER ───────
    # The original rules compare each event to its immediate predecessor.
    # Once an opponent event has been absorbed (a deflection, a duel), every
    # event after it looks "same team as previous" and the real possession
    # change is never seen — so an entire opposition attack can end up
    # stamped with the wrong team.  Compare to the sequence owner instead.
    #
    # PASSIVE_EVENTS are exempt: they are the opponent-side events that
    # legitimately live inside someone else's sequence.
    if (fix_a
            and in_sequence
            and current_seq_team is not None
            and row['source_team_id'] != current_seq_team
            and evt not in PASSIVE_EVENTS):
        return True

    # ── Fix B: gap closer ──────────────────────────────────────────────
    # No sequence is running and a possession-bearing action occurs.  Today
    # play can continue for a dozen events with nothing recorded because the
    # start rules require either a team change or a success outcome.
    if fix_b and not in_sequence and evt in ON_BALL_START_EVENTS:
        if evt in FIX_B_REQUIRES_SUCCESS and outcome != 'success':
            return False
        return True

    return False


def _is_sequence_end(
    row: dict,
    next_row: Optional[dict],
    prev_row: Optional[dict],
    next_next_row: Optional[dict],
) -> bool:
    """
    Determine whether the current event closes the active possession sequence.

    Conditions are evaluated in priority order.
    """
    evt = row['event_type']
    outcome = row['outcome']

    # ── Fix 8: Corner Awarded always ends the sequence it belongs to ───
    if evt == 'Corner Awarded':
        return True

    # ── Fix 8 (suppress): if next event IS Corner Awarded, do NOT end
    #    here — let the sequence extend to include Corner Awarded.
    #    Exception: Goal always ends regardless. ────────────────────────
    if (next_row is not None
            and next_row['event_type'] == 'Corner Awarded'
            and evt != 'Goal'):
        return False

    # ── Unsuccessful Pass ──────────────────────────────────────────────
    if evt == 'Pass' and outcome == 'failure':
        # Fix 2: assist pass must not end the sequence
        if _has_value_assist(row):
            return False
        # Fix 6: Ball touch / Error sandwich — deflection, possession kept
        if next_row is not None and next_row['event_type'] in ('Ball touch', 'Error'):
            if (next_next_row is not None
                    and next_next_row['source_team_id'] == row['source_team_id']):
                return False
        return True

    # ── Successful Dispossessed ────────────────────────────────────────
    if evt == 'Dispossessed' and outcome == 'success':
        return True

    # ── Unsuccessful Ball touch / Error — Fix 7: sandwich exemption ────
    if evt in ('Ball touch', 'Error') and outcome == 'failure':
        if (prev_row is not None and next_row is not None
                and prev_row['source_team_id'] == next_row['source_team_id']
                and prev_row['source_team_id'] != row['source_team_id']):
            return False  # deflection — sequence continues for other team
        return True

    # ── Offside Pass (any outcome) ─────────────────────────────────────
    if evt == 'Offside Pass':
        return True

    # ── Successful Foul ────────────────────────────────────────────────
    if evt == 'Foul' and outcome == 'success':
        return True

    # ── Goal ───────────────────────────────────────────────────────────
    if evt == 'Goal':
        return True

    # ── Miss ───────────────────────────────────────────────────────────
    if evt == 'Miss':
        return True

    # ── Post — only if opposition gets the ball next ───────────────────
    if evt == 'Post':
        if next_row is not None and next_row['source_team_id'] != row['source_team_id']:
            return True

    # ── Attempt Saved — ends when next event is different team or absent
    if evt == 'Attempt Saved':
        if next_row is None or next_row['source_team_id'] != row['source_team_id']:
            return True

    # ── Unsuccessful Take On (except when fouled immediately after) ────
    if evt == 'Take On' and outcome == 'failure':
        if next_row is not None and next_row['event_type'] == 'Foul':
            return False
        return True

    # ── Next event is a meta/structural event → close now ──────────────
    if next_row is not None and next_row['event_type'] in META_EVENTS:
        return True

    return False


# ──────────────────────────────────────────────────────────────────────
# Core classifier
# ──────────────────────────────────────────────────────────────────────

def classify_possession_sequences(
    df: pd.DataFrame,
    match_id_column: str = 'match_id',
    fix_a: bool = True,
    fix_b: bool = True,
) -> pd.DataFrame:
    """
    Segment a pre-processed event stream into discrete possession sequences.

    The function processes rows in order via a state machine.  It expects
    the DataFrame to have been run through ``preprocess_for_sequences``
    first (filtered, sorted, flags materialised).

    Parameters
    ----------
    df : pd.DataFrame
        Pre-processed events.  Must contain at minimum:
        event_type, outcome, source_team_id, h_a, match_id, period,
        is_set_piece_pass, is_dead_ball_goal, value_assist.
    match_id_column : str
        Column holding the match identifier.
    fix_a, fix_b : bool
        Opt-in classifier fixes (md/GOLD_SEQUENCES.md §6.3), both default
        OFF.  With both off the output is bit-identical to the shipped
        classifier.  Both are strictly additive — they can only open extra
        sequences, never suppress an existing one.

    Returns
    -------
    pd.DataFrame
        Copy of *df* with four new columns:
        sequence_id, sequence_start, sequence_end, sequence_event_number.
    """
    if match_id_column not in df.columns:
        raise ValueError(
            f"Column '{match_id_column}' not found.  "
            f"Available: {list(df.columns)}"
        )

    df_result = df.copy()
    df_result = df_result.reset_index(drop=True)
    n = len(df_result)

    # Convert to list-of-dicts for O(1) row access (much faster than iloc)
    rows: List[dict] = df_result.to_dict('records')

    # Pre-allocate output arrays
    seq_ids:        List[Optional[str]]  = [None]  * n
    seq_starts:     List[bool]           = [False] * n
    seq_ends:       List[bool]           = [False] * n
    seq_event_nums: List[int]            = [0]     * n

    # State
    sequence_counters: Dict[Any, Dict[str, int]] = {}
    current_seq_id:    Optional[str] = None
    current_match_id:  Any           = None
    current_event_num: int           = 0
    in_sequence:       bool          = False
    # source_team_id owning the running sequence — read by Fix A only.
    current_seq_team:  Any           = None

    for idx in range(n):
        row = rows[idx]
        match_id = row[match_id_column]

        # ── Match boundary → reset state ──────────────────────────────
        if current_match_id != match_id:
            # Close dangling sequence from previous match
            if in_sequence:
                _mark_prev_end(rows, seq_ends, idx, match_id_column, current_match_id)
            current_match_id = match_id
            if match_id not in sequence_counters:
                sequence_counters[match_id] = {'home': 0, 'away': 0}
            in_sequence       = False
            current_seq_id    = None
            current_event_num = 0
            current_seq_team  = None

        # ── Meta events: close active sequence, skip stamping ─────────
        if row['event_type'] in META_EVENTS:
            if in_sequence:
                _mark_prev_end(rows, seq_ends, idx, match_id_column, match_id)
                in_sequence       = False
                current_seq_id    = None
                current_event_num = 0
                current_seq_team  = None
            continue  # meta events are never stamped with a sequence

        # ── Lookup neighbours (same match only) ───────────────────────
        prev_row = (
            rows[idx - 1]
            if idx > 0 and rows[idx - 1][match_id_column] == match_id
            else None
        )
        next_row = (
            rows[idx + 1]
            if idx < n - 1 and rows[idx + 1][match_id_column] == match_id
            else None
        )
        next_next_row = (
            rows[idx + 2]
            if idx < n - 2 and rows[idx + 2][match_id_column] == match_id
            else None
        )
        # prev_prev_row: needed for sandwich guard validation
        prev_prev_row = (
            rows[idx - 2]
            if idx > 1 and rows[idx - 2][match_id_column] == match_id
            else None
        )

        # ── Check sequence start ──────────────────────────────────────
        if _is_sequence_start(
            row, prev_row,
            prev_prev_row=prev_prev_row,
            in_sequence=in_sequence,
            current_seq_team=current_seq_team,
            fix_a=fix_a,
            fix_b=fix_b,
        ):
            # Close existing sequence before opening a new one
            if in_sequence and current_seq_id is not None:
                _mark_prev_end(rows, seq_ends, idx, match_id_column, match_id)

            team = row['h_a']
            sequence_counters[match_id][team] += 1
            current_seq_id = (
                f"{match_id}_{team}_{sequence_counters[match_id][team]:03d}"
            )
            current_event_num = 1
            in_sequence       = True
            current_seq_team  = row['source_team_id']
            seq_starts[idx]   = True

        elif in_sequence:
            current_event_num += 1

        # ── Stamp active sequence onto current row ────────────────────
        if in_sequence and current_seq_id is not None:
            seq_ids[idx]        = current_seq_id
            seq_event_nums[idx] = current_event_num

        # ── Check sequence end ────────────────────────────────────────
        if in_sequence and _is_sequence_end(row, next_row, prev_row, next_next_row):
            seq_ends[idx]     = True
            in_sequence       = False
            current_seq_id    = None
            current_event_num = 0
            current_seq_team  = None

    # Write results into the DataFrame
    df_result['sequence_id']           = seq_ids
    df_result['sequence_start']        = seq_starts
    df_result['sequence_end']          = seq_ends
    df_result['sequence_event_number'] = seq_event_nums

    return df_result


def _mark_prev_end(
    rows: List[dict],
    seq_ends: List[bool],
    current_idx: int,
    match_id_column: str,
    match_id: Any,
) -> None:
    """Walk backwards and mark the most recent non-meta event as sequence end."""
    for back in range(current_idx - 1, -1, -1):
        if rows[back][match_id_column] != match_id:
            break
        if rows[back]['event_type'] not in META_EVENTS:
            seq_ends[back] = True
            return


# ──────────────────────────────────────────────────────────────────────
# Step 1 orchestrator — populate silver.events
# ──────────────────────────────────────────────────────────────────────

# Columns we SELECT from silver.events to feed the classifier.
# event_id is included so we can UPDATE back.
_SELECT_COLS = """
    event_id, match_id, period, minute, second,
    json_index, provider_event_id, source_event_id,
    event_type, outcome, type_id,
    team_id, source_team_id, player_id,
    h_a, x, y, end_x, end_y, xt,
    raw_data
"""

# Check if value_assist column exists in silver.events
_CHECK_VALUE_ASSIST = """
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = 'silver'
      AND table_name   = 'events'
      AND column_name  = 'value_assist'
"""


def _get_unprocessed_match_ids(conn, limit: Optional[int] = None) -> List[int]:
    """
    Find match_ids in silver.events that have no sequence data yet.

    A match is considered unprocessed if NONE of its non-excluded events
    have a non-NULL sequence_id.
    """
    sql = """
        SELECT DISTINCT e.match_id
        FROM silver.events e
        WHERE e.event_type NOT IN ('Formation change', 'Team set up')
          AND e.period NOT IN (14, 16)
          AND NOT EXISTS (
              SELECT 1 FROM silver.events e2
              WHERE e2.match_id = e.match_id
                AND e2.sequence_id IS NOT NULL
          )
        ORDER BY e.match_id
    """
    if limit:
        sql += f" LIMIT {int(limit)}"

    with conn.cursor() as cur:
        cur.execute(sql)
        return [row[0] for row in cur.fetchall()]


def _reset_sequence_columns(conn, match_ids: List[int]) -> int:
    """
    Clear all sequence columns for the given match_ids.
    Returns the number of rows reset.
    """
    sql = """
        UPDATE silver.events
        SET sequence_id           = NULL,
            sequence_start        = FALSE,
            sequence_end          = FALSE,
            sequence_event_number = 0
        WHERE match_id = ANY(%s)
    """
    with conn.cursor() as cur:
        cur.execute(sql, (match_ids,))
        return cur.rowcount


def _batch_update_sequences(
    conn,
    updates: List[Tuple],
    page_size: int = 500,
) -> int:
    """
    Batch-update silver.events with classified sequence data.

    Each tuple in *updates*: (sequence_id, sequence_start, sequence_end,
    sequence_event_number, event_id).
    """
    sql = """
        UPDATE silver.events
        SET sequence_id           = data.seq_id,
            sequence_start        = data.seq_start,
            sequence_end          = data.seq_end,
            sequence_event_number = data.seq_num
        FROM (VALUES %s) AS data(seq_id, seq_start, seq_end, seq_num, eid)
        WHERE silver.events.event_id = data.eid
    """
    total = 0
    with conn.cursor() as cur:
        for start in range(0, len(updates), page_size):
            batch = updates[start : start + page_size]
            psycopg2.extras.execute_values(
                cur, sql, batch,
                template="(%s, %s::boolean, %s::boolean, %s::int, %s::bigint)",
                page_size=page_size,
            )
            total += len(batch)
    return total


def _backfill_excluded_events(conn, match_ids: List[int]) -> int:
    """
    Backfill excluded-but-relevant events into their surrounding sequence.

    Events in BACKFILL_EVENTS (e.g. Attempted Tackle) are excluded from
    the classifier so they don't interfere with possession logic, but they
    are real on-pitch actions that belong to the sequence they sit inside.

    For each such event, we look at the nearest classified event before and
    after it (by json_index within the same match).  If both neighbours
    share the same sequence_id, the event inherits that sequence_id with
    sequence_event_number = 0 to mark it as interpolated.

    Returns the number of rows backfilled.
    """
    if not BACKFILL_EVENTS:
        return 0

    event_types = tuple(BACKFILL_EVENTS)

    sql = """
        UPDATE silver.events e
        SET sequence_id           = surrounding.prev_seq_id,
            sequence_event_number = 0
        FROM (
            SELECT
                e2.event_id,
                (SELECT e3.sequence_id FROM silver.events e3
                 WHERE e3.match_id = e2.match_id
                   AND e3.json_index < e2.json_index
                   AND e3.sequence_id IS NOT NULL
                 ORDER BY e3.json_index DESC LIMIT 1
                ) AS prev_seq_id,
                (SELECT e3.sequence_id FROM silver.events e3
                 WHERE e3.match_id = e2.match_id
                   AND e3.json_index > e2.json_index
                   AND e3.sequence_id IS NOT NULL
                 ORDER BY e3.json_index ASC LIMIT 1
                ) AS next_seq_id
            FROM silver.events e2
            WHERE e2.event_type IN %s
              AND e2.match_id = ANY(%s)
              AND e2.sequence_id IS NULL
        ) surrounding
        WHERE e.event_id = surrounding.event_id
          AND surrounding.prev_seq_id IS NOT NULL
          AND surrounding.prev_seq_id = surrounding.next_seq_id
    """
    with conn.cursor() as cur:
        cur.execute(sql, (event_types, match_ids))
        return cur.rowcount


def populate_silver_sequences(
    conn,
    match_ids: Optional[List[int]] = None,
    limit: Optional[int] = None,
    batch_size: int = 50,
    fix_a: bool = True,
    fix_b: bool = True,
) -> Dict[str, int]:
    """
    Step 1 — classify possession sequences and write results to silver.events.

    Parameters
    ----------
    conn : psycopg2 connection
        Open connection to the football analytics database.
    match_ids : list[int] | None
        Specific match_ids to process.  If None, discovers unprocessed
        matches automatically.
    limit : int | None
        Cap the number of matches to process in one run (useful for
        incremental loads).
    batch_size : int
        Number of matches to load into memory at a time.
    fix_a, fix_b : bool
        Opt-in classifier fixes, both default OFF (md/GOLD_SEQUENCES.md §6.3).

    Returns
    -------
    dict with keys: matches_processed, events_updated, matches_skipped.
    """
    # ── Discover target matches ───────────────────────────────────────
    if match_ids is None:
        match_ids = _get_unprocessed_match_ids(conn, limit=limit)
        log.info("Discovered %d unprocessed match(es)", len(match_ids))
    else:
        log.info("Processing %d explicitly requested match(es)", len(match_ids))

    if not match_ids:
        log.info("Nothing to process.")
        return {'matches_processed': 0, 'events_updated': 0, 'matches_skipped': 0}

    # ── Check if value_assist column exists ───────────────────────────
    with conn.cursor() as cur:
        cur.execute(_CHECK_VALUE_ASSIST)
        has_value_assist = cur.fetchone() is not None
    select_cols = _SELECT_COLS
    if has_value_assist:
        select_cols += ", value_assist"

    stats = {'matches_processed': 0, 'events_updated': 0, 'events_backfilled': 0, 'matches_skipped': 0}

    # ── Process in batches ────────────────────────────────────────────
    for batch_start in range(0, len(match_ids), batch_size):
        batch_ids = match_ids[batch_start : batch_start + batch_size]

        # Reset existing sequence data (idempotency)
        reset_count = _reset_sequence_columns(conn, batch_ids)
        if reset_count:
            log.debug("Reset %d event rows for %d matches", reset_count, len(batch_ids))

        # Load events
        query = f"""
            SELECT {select_cols}
            FROM silver.events
            WHERE match_id = ANY(%s)
            ORDER BY match_id, json_index, event_id
        """
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, (batch_ids,))
            df = pd.DataFrame(cur.fetchall())

        if df.empty:
            stats['matches_skipped'] += len(batch_ids)
            log.warning("No events found for match_ids %s", batch_ids)
            continue

        # Pre-process
        df_clean = preprocess_for_sequences(df)

        if df_clean.empty:
            stats['matches_skipped'] += len(batch_ids)
            continue

        # Classify
        df_classified = classify_possession_sequences(
            df_clean, fix_a=fix_a, fix_b=fix_b,
        )

        # Build update tuples — only rows that belong to a sequence
        mask = df_classified['sequence_id'].notna()
        df_to_update = df_classified.loc[mask]

        if df_to_update.empty:
            log.warning("No sequences found for batch starting at %d", batch_start)
            stats['matches_skipped'] += len(batch_ids)
            continue

        updates = list(zip(
            df_to_update['sequence_id'],
            df_to_update['sequence_start'],
            df_to_update['sequence_end'],
            df_to_update['sequence_event_number'],
            df_to_update['event_id'],
        ))

        updated = _batch_update_sequences(conn, updates)
        conn.commit()

        # Backfill excluded-but-relevant events (e.g. Attempted Tackle)
        backfilled = _backfill_excluded_events(conn, batch_ids)
        conn.commit()
        if backfilled:
            log.debug("Backfilled %d excluded events into sequences", backfilled)

        n_matches = df_to_update['match_id'].nunique()
        stats['matches_processed']  += n_matches
        stats['events_updated']     += updated
        stats['events_backfilled']  += backfilled
        log.info(
            "Batch done: %d matches, %d events updated, %d backfilled",
            n_matches, updated, backfilled,
        )

    log.info(
        "Step 1 complete: %(matches_processed)d matches processed, "
        "%(events_updated)d events updated, "
        "%(events_backfilled)d events backfilled, "
        "%(matches_skipped)d matches skipped",
        stats,
    )
    return stats


# ──────────────────────────────────────────────────────────────────────
# CLI entry point
# ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    import os
    import sys

    from dotenv import load_dotenv

    load_dotenv()

    parser = argparse.ArgumentParser(
        description="Step 1 — Classify possession sequences in silver.events",
    )
    parser.add_argument(
        "--match-ids", nargs="+", type=int, default=None,
        help="Specific match IDs to process (default: auto-discover unprocessed)",
    )
    parser.add_argument(
        "--limit", type=int, default=None,
        help="Max number of matches to process (only used with auto-discovery)",
    )
    parser.add_argument(
        "--batch-size", type=int, default=50,
        help="Matches per batch (default: 50)",
    )
    parser.add_argument(
        "--no-fix-a", dest="fix_a", action="store_false", default=True,
        help="Disable Fix A (possession change detected against the sequence "
             "owner rather than the previous event). A/B comparison only — "
             "see md/GOLD_SEQUENCES.md 6.3.",
    )
    parser.add_argument(
        "--no-fix-b", dest="fix_b", action="store_false", default=True,
        help="Disable Fix B (gap closer: open a sequence on any on-ball action "
             "when none is running). A/B comparison only.",
    )
    parser.add_argument(
        "--log-level", default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity (default: INFO)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%H:%M:%S",
    )

    dsn = os.getenv("FOOTBALL_DB_DSN")
    if not dsn:
        log.error("FOOTBALL_DB_DSN is not set in your .env file.")
        sys.exit(1)

    conn = psycopg2.connect(dsn)
    try:
        stats = populate_silver_sequences(
            conn,
            match_ids=args.match_ids,
            limit=args.limit,
            batch_size=args.batch_size,
            fix_a=args.fix_a,
            fix_b=args.fix_b,
        )
        print(f"\nDone: {stats}")
    except Exception:
        log.exception("Pipeline failed")
        conn.rollback()
        sys.exit(1)
    finally:
        conn.close()