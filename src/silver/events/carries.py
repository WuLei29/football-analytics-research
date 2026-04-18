"""
carries.py
──────────
Carry detection and insertion.

Takes a pandas DataFrame of events and returns a new DataFrame with
synthesised carry rows inserted at the correct positions.

IMPORTANT: The DataFrame is sorted by (period, minute, second,
provider_event_id) at the start of calculate_carries() to guarantee
chronological order. Provider eventIds are assigned at recording time,
so late-added events (e.g. off-the-ball actions, VAR corrections) get
higher IDs despite occurring earlier — sorting by eventId alone produces
incorrect consecutive pairs and phantom carries.

No DB access. No parsing. Pure transformation.
"""

import pandas as pd
from typing import Any, Dict, Optional


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
    
    avg_minute = (event1["minute"] + event2["minute"]) / 2
    avg_second = (event1["second"] + event2["second"]) / 2
    if avg_second >= 60:
        avg_minute += 1
        avg_second -= 60

    e1_pid = event1.get("provider_event_id")
    e2_pid = event2.get("provider_event_id")
    carry_sort_key = (
        (e1_pid + e2_pid) / 2
        if e1_pid is not None and e2_pid is not None
        else None
    )


    return {
        "match_id":             match_id,
        "source_event_id":      None,   # synthesised — no provider UUID
        "provider_event_id":    carry_sort_key, # ← fractional, for in-memory sort only
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


def _coords_mismatch(e1: pd.Series, e2: pd.Series) -> bool:
    """True when the end coords of e1 don't match the start coords of e2."""
    try:
        return (
            abs(e1["end_x"] - e2["x"]) > 0.01
            or abs(e1["end_y"] - e2["y"]) > 0.01
        )
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
    # in the match. Sorting by (period, minute, second) with provider_event_id
    # as tiebreaker guarantees correct consecutive pairs for carry detection.
    df = df.sort_values(
        by=["period", "minute", "second", "provider_event_id"],
        na_position="last",
    ).reset_index(drop=True)

    carries: list[tuple[int, dict]] = []  # (insert_before_index, row_dict)

    for i in range(len(df) - 1):
        cur  = df.iloc[i]
        nxt  = df.iloc[i + 1]

        # Never synthesise a carry across a period boundary
        if cur["period"] != nxt["period"]:
            continue
        cur_type = cur["event_type"]
        nxt_type = nxt["event_type"]
        same_team = cur["source_team_id"] == nxt["source_team_id"]
        mismatch  = _coords_mismatch(cur, nxt)

        match_id = int(cur["match_id"])

        def make_carry(e1, e2, sx, sy, ex, ey, pid, pname, jnum, tid, period):
            ti = e1["source_team_id"]
            return _carry_row(
                e1, e2, sx, sy, ex, ey, pid, pname, jnum, tid, period,
                match_id,
                e1.get("team_name"),
                e1.get("opposition_team_name"),
                e1.get("h_a"),
            )

        # ── Never create carry ────────────────────────────────────────────────
        if nxt_type in ("BallTouch", "BallRecovery", "Aerial", "CornerAwarded"):
            continue
        if cur_type in ("Foul", "Card"):
            continue
        if cur_type == "MissedShot" and nxt_type == "BallTouch":
            continue

        create_carry = False

        # ── Tackle ────────────────────────────────────────────────────────────
        if cur_type == "Tackle":
            if nxt_type == "BallRecovery" and cur["source_player_id"] != nxt["source_player_id"]:
                continue
            if nxt_type == "Pass" and cur["source_player_id"] == nxt["source_player_id"] and same_team and mismatch:
                create_carry = True

        # ── Pass ──────────────────────────────────────────────────────────────
        if cur_type == "Pass" and same_team and mismatch and cur.get("outcome") == "success":
            if nxt_type in ("Pass", "Shot", "MissedShot", "SavedShot", "Dispossessed", "Foul"):
                create_carry = True

        # ── Ball recoveries / keeper / interceptions ──────────────────────────
        if cur_type in ("BallRecovery", "KeeperPickup", "Interception", "Claim"):
            if same_team and mismatch and nxt_type in ("Pass", "Shot", "MissedShot", "SavedShot"):
                create_carry = True

        # ── Clearance ─────────────────────────────────────────────────────────
        if cur_type == "Clearance" and nxt_type == "Pass" and same_team and mismatch:
            create_carry = True

        # ── Challenge unsuccessful → TakeOn successful (opposing teams) ───────
        if cur_type == "Challenge" and cur.get("outcome") == "failure":
            if nxt_type == "Take On" and nxt.get("outcome") == "success" and not same_team:
                if i > 0:
                    prev = df.iloc[i - 1]
                    if (prev["source_team_id"] == nxt["source_team_id"]
                            and _all_coords_valid(prev["end_x"], prev["end_y"], nxt["x"], nxt["y"])
                            and _coords_mismatch(prev, nxt)):
                        carries.append((i + 1, make_carry(
                            prev, nxt, prev["end_x"], prev["end_y"], nxt["x"], nxt["y"],
                            nxt["source_player_id"], nxt.get("player_name"),
                            nxt.get("jersey_number"), nxt["source_team_id"], nxt["period"],
                        )))
                if i + 2 < len(df):
                    nxt2 = df.iloc[i + 2]
                    if (nxt2["source_team_id"] == nxt["source_team_id"]
                            and _all_coords_valid(nxt["end_x"], nxt["end_y"], nxt2["x"], nxt2["y"])
                            and _coords_mismatch(nxt, nxt2)):
                        carries.append((i + 2, make_carry(
                            nxt, nxt2, nxt["end_x"], nxt["end_y"], nxt2["x"], nxt2["y"],
                            nxt["source_player_id"], nxt.get("player_name"),
                            nxt.get("jersey_number"), nxt["source_team_id"], nxt["period"],
                        )))
                continue

        # ── TakeOn successful → Challenge unsuccessful (opposing teams) ───────
        if cur_type == "Take On" and cur.get("outcome") == "success":
            if nxt_type == "Challenge" and nxt.get("outcome") == "failure" and not same_team:
                if i > 0:
                    prev = df.iloc[i - 1]
                    if (prev["source_team_id"] == cur["source_team_id"]
                            and _all_coords_valid(prev["end_x"], prev["end_y"], cur["x"], cur["y"])
                            and _coords_mismatch(prev, cur)):
                        carries.append((i, make_carry(
                            prev, cur, prev["end_x"], prev["end_y"], cur["x"], cur["y"],
                            cur["source_player_id"], cur.get("player_name"),
                            cur.get("jersey_number"), cur["source_team_id"], cur["period"],
                        )))
                if i + 2 < len(df):
                    nxt2 = df.iloc[i + 2]
                    if (nxt2["source_team_id"] == cur["source_team_id"]
                            and _all_coords_valid(cur["end_x"], cur["end_y"], nxt2["x"], nxt2["y"])
                            and _coords_mismatch(cur, nxt2)):
                        carries.append((i + 2, make_carry(
                            cur, nxt2, cur["end_x"], cur["end_y"], nxt2["x"], nxt2["y"],
                            cur["source_player_id"], cur.get("player_name"),
                            cur.get("jersey_number"), cur["source_team_id"], cur["period"],
                        )))
                continue

        # ── Standard carry ────────────────────────────────────────────────────
        if create_carry and _all_coords_valid(cur["end_x"], cur["end_y"], nxt["x"], nxt["y"]):
            carries.append((i + 1, make_carry(
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