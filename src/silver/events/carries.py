"""
carries.py
──────────
Carry detection and insertion.

Takes a pandas DataFrame of events and returns a new DataFrame with
synthesised carry rows inserted at the correct positions.

IMPORTANT: The DataFrame is sorted by json_index (the 0-based position
in the raw JSONP array) at the start of calculate_carries(). This is
the only reliable source of chronological order — neither the provider's
"id" (UUID) nor "eventId" follow sequential order.

No DB access. No parsing. Pure transformation.
"""

import pandas as pd
from typing import Any, Dict, Optional

# The four shot outcomes the parser emits (Opta typeIds 13-16).
SHOT_EVENTS = frozenset({"Miss", "Post", "Attempt Saved", "Goal"})

# Where a ball-carrier's run can end: the next on-ball action of the same
# team.  A take-on (any outcome) counts — the player ran with the ball up to
# the point where the duel started.
CARRY_DESTINATIONS = SHOT_EVENTS | frozenset({"Pass", "Take On", "Dispossessed", "Foul"})

# Opponent mirror records that sit between two actions of the possessing team
# without the ball changing hands.  Skipped when pairing consecutive events.
TRANSPARENT_EVENTS = frozenset({"Challenge"})


# ── Carry row factory ──────────────────────────────────────────────────────────

def _carry_row(
    event1: pd.Series,
    event2: pd.Series,
    start_x: float,
    start_y: float,
    end_x: float,
    end_y: float,
    source_player_id: Any,
    player_name: Optional[str],
    jersey_number: Optional[int],
    source_team_id: Any,
    period: Any,
    match_id: int,
    team_name: Optional[str],
    opposition_team_name: Optional[str],
    h_a: Optional[str],
) -> Dict:
    
    # Midpoint on the full clock.  Averaging minute and second separately put
    # a carry between 55:57 and 56:00 at 55:29 (fixed 19 Sep 2026).
    mid_seconds = (
        (event1["minute"] * 60 + event1["second"])
        + (event2["minute"] * 60 + event2["second"])
    ) / 2
    avg_minute, avg_second = divmod(mid_seconds, 60)

    e1_idx = event1.get("json_index")
    e2_idx = event2.get("json_index")
    carry_json_index = (
        (e1_idx + e2_idx) / 2
        if e1_idx is not None and e2_idx is not None
        else None
    )


    return {
        "match_id":             match_id,
        "source_event_id":      None,   # synthesised — no provider UUID
        "provider_event_id":    None,           # synthesised — no provider eventId
        "json_index":           carry_json_index, # fractional, sits between neighbors
        "type_id":              -1, # ← marks synthesised carries
        "minute":               int(avg_minute),
        "second":               avg_second,
        "source_team_id":       source_team_id,
        "team_name":            team_name,
        "opposition_team_name": opposition_team_name,
        "h_a":                  h_a,
        "x":                    start_x,
        "y":                    start_y,
        "end_x":                end_x,
        "end_y":                end_y,
        "period":               period,
        "event_type":           "Carry",
        "source_player_id":     source_player_id,
        "player_name":          player_name,
        "jersey_number":        jersey_number,
        "outcome":              "success",
        "team_id":              None,
        "player_id":            None,
        "raw_data":             None,
    }


# Shortest ball movement that counts as a carry, in metres (coordinates are
# already converted by the parser when this module runs).  Below it the gap
# between one event's end and the next one's start is coordinate rounding,
# not a player moving with the ball: 54,104 of the 255,795 carries loaded
# before 19 Sep 2026 were under a metre and drew as blobs on the sequence view.
MIN_CARRY_DISTANCE_M = 1.0


def _coords_mismatch(e1: pd.Series, e2: pd.Series) -> bool:
    """True when the ball moved at least MIN_CARRY_DISTANCE_M between e1's end and e2's start."""
    try:
        dx = e1["end_x"] - e2["x"]
        dy = e1["end_y"] - e2["y"]
        return (dx * dx + dy * dy) ** 0.5 >= MIN_CARRY_DISTANCE_M
    except (TypeError, KeyError):
        return False


def _all_coords_valid(*values) -> bool:
    return all(v is not None and pd.notna(v) for v in values)


# ── Main function ──────────────────────────────────────────────────────────────

def calculate_carries(df: pd.DataFrame) -> pd.DataFrame:
    """
    Insert synthesised carry events between consecutive actions where the
    ball moved without a recorded event.

    Args:
        df: Events DataFrame. Must have columns: event_type, outcome,
            source_team_id, source_player_id, player_name, jersey_number,
            x, y, end_x, end_y, period, minute, second, match_id,
            provider_event_id, team_name, opposition_team_name, h_a.

    Returns:
        New DataFrame with carry rows inserted at the correct positions,
        sorted chronologically by (period, minute, second, provider_event_id).
    """
    # ── Sort chronologically ──────────────────────────────────────────────
    # Provider eventIds are assigned at recording time, so late-added events
    # (off-ball actions, VAR reviews) get higher IDs despite occurring earlier
    # in the match. Sorting by json_index solves the ordering edge-case.
    df = df.sort_values(
        by=["json_index"],
        na_position="last",
    ).reset_index(drop=True)

    carries: list[tuple[int, dict]] = []  # (insert_before_index, row_dict)
    n = len(df)

    for i in range(n - 1):
        cur = df.iloc[i]
        cur_type = cur["event_type"]

        # A Challenge is the opponent's mirror of a take-on ("failed to win the
        # ball as the opponent dribbled past"): it is never a carry origin, and
        # it is skipped when looking for the destination, so a carry can run
        # Pass -> [Challenge] -> Take On or Take On -> [Challenge] -> Pass.
        # Until 21 Sep 2026 this was handled by two special-case blocks that
        # only fired when the Challenge was adjacent to a SUCCESSFUL take-on;
        # every other take-on (failed, or with the Challenge not adjacent)
        # got no carry on either side.
        if cur_type in TRANSPARENT_EVENTS:
            continue
        j = i + 1
        while j < n and df.iloc[j]["event_type"] in TRANSPARENT_EVENTS:
            j += 1
        if j >= n:
            break
        nxt = df.iloc[j]

        # Never synthesise a carry across a period boundary
        if cur["period"] != nxt["period"]:
            continue
        nxt_type = nxt["event_type"]
        same_team = cur["source_team_id"] == nxt["source_team_id"]
        mismatch  = _coords_mismatch(cur, nxt)

        match_id = int(cur["match_id"])

        def make_carry(e1, e2, sx, sy, ex, ey, pid, pname, jnum, tid, period):
            return _carry_row(
                e1, e2, sx, sy, ex, ey, pid, pname, jnum, tid, period,
                match_id,
                e1.get("team_name"),
                e1.get("opposition_team_name"),
                e1.get("h_a"),
            )

        # ── Never create carry ────────────────────────────────────────────────
        # Event names are the parser's (Opta descriptions: 'Ball recovery',
        # 'Keeper pick-up', 'Attempt Saved' ...).  Until 19 Sep 2026 this block
        # and the rules below compared against 'BallRecovery', 'KeeperPickup',
        # 'Shot', 'MissedShot', 'SavedShot' — names this pipeline never emits —
        # so no carry was ever synthesised after a recovery or a keeper pick-up,
        # nor before a shot.
        if nxt_type in ("Ball touch", "Ball recovery", "Aerial", "Corner Awarded"):
            # One exception: a dribbler who knocks the ball past the defender
            # and runs onto it is logged as Take On -> Ball recovery by the
            # same player.  That is a run, and was already a carry before the
            # 21 Sep 2026 rewrite (17 of 27 carries the rewrite would have
            # dropped in a 12-match sample).
            if not (cur_type == "Take On" and cur.get("outcome") == "success"
                    and nxt_type == "Ball recovery"
                    and cur["source_player_id"] == nxt["source_player_id"]):
                continue
        if cur_type in ("Foul", "Card"):
            continue
        if cur_type == "Miss" and nxt_type == "Ball touch":
            continue

        create_carry = False

        # ── Tackle ────────────────────────────────────────────────────────────
        if cur_type == "Tackle":
            if nxt_type == "Ball recovery" and cur["source_player_id"] != nxt["source_player_id"]:
                continue
            if nxt_type in ("Pass", "Take On") and cur["source_player_id"] == nxt["source_player_id"] and same_team and mismatch:
                create_carry = True

        # ── Pass ──────────────────────────────────────────────────────────────
        if cur_type == "Pass" and same_team and mismatch and cur.get("outcome") == "success":
            if nxt_type in CARRY_DESTINATIONS:
                create_carry = True

        # ── Ball recoveries / keeper / interceptions ──────────────────────────
        if cur_type in ("Ball recovery", "Keeper pick-up", "Interception", "Claim"):
            if same_team and mismatch and (nxt_type in ("Pass", "Take On") or nxt_type in SHOT_EVENTS):
                create_carry = True

        # ── Clearance ─────────────────────────────────────────────────────────
        if cur_type == "Clearance" and nxt_type in ("Pass", "Take On") and same_team and mismatch:
            create_carry = True

        # ── Take On ───────────────────────────────────────────────────────────
        # The dribbler keeps the ball after beating the opponent, so a
        # successful take-on is a carry origin like a completed pass.  A failed
        # one is not: the ball is lost (or at best contested) at its location.
        if cur_type == "Take On" and cur.get("outcome") == "success" and same_team and mismatch:
            if nxt_type in CARRY_DESTINATIONS or nxt_type == "Ball recovery":
                create_carry = True

        # ── Standard carry ────────────────────────────────────────────────────
        if create_carry and _all_coords_valid(cur["end_x"], cur["end_y"], nxt["x"], nxt["y"]):
            carries.append((j, make_carry(
                cur, nxt, cur["end_x"], cur["end_y"], nxt["x"], nxt["y"],
                nxt["source_player_id"], nxt.get("player_name"),
                nxt.get("jersey_number"), nxt["source_team_id"], nxt["period"],
            )))

    # Insert in reverse order so earlier indices stay valid
    result = df.copy()
    for idx, row in sorted(carries, key=lambda t: t[0], reverse=True):
        result = pd.concat([
            result.iloc[:idx],
            pd.DataFrame([row]),
            result.iloc[idx:],
        ]).reset_index(drop=True)

    return result