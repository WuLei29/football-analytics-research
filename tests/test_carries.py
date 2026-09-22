"""
Unit tests for src/silver/events/carries.py — the take-on cases in
particular, since those were re-worked on 21 Sep 2026 (Challenge made
transparent, Take On added to the general rules).
"""

import pandas as pd
import pytest

from src.silver.events.carries import calculate_carries

HOME, AWAY = 10, 20
P1, P2, P3 = 101, 102, 201


def ev(idx, etype, team, player, x, y, end_x=None, end_y=None,
       outcome="success", period=1, minute=10, second=0):
    return {
        "match_id": 1, "json_index": idx, "event_type": etype,
        "outcome": outcome, "source_team_id": team, "source_player_id": player,
        "player_name": f"p{player}", "jersey_number": player % 100,
        "x": x, "y": y,
        "end_x": x if end_x is None else end_x,
        "end_y": y if end_y is None else end_y,
        "period": period, "minute": minute, "second": second + idx,
        "provider_event_id": idx, "team_name": "H" if team == HOME else "A",
        "opposition_team_name": "A" if team == HOME else "H", "h_a": "h",
    }


def run(rows):
    out = calculate_carries(pd.DataFrame(rows))
    return out, out[out["event_type"] == "Carry"].reset_index(drop=True)


def carry_keys(carries):
    """(player, start, end) triples — what a carry *is*, independent of order."""
    return {
        (int(c.source_player_id), (c.x, c.y), (c.end_x, c.end_y))
        for c in carries.itertuples()
    }


# ── Regression: the two cases the old special blocks covered ──────────────────

def test_challenge_then_take_on_gets_carries_both_sides():
    rows = [
        ev(0, "Pass", HOME, P1, 30, 30, end_x=50, end_y=30),
        ev(1, "Challenge", AWAY, P3, 45, 40, outcome="failure"),
        ev(2, "Take On", HOME, P2, 55, 32),
        ev(3, "Pass", HOME, P2, 62, 35, end_x=80, end_y=40),
    ]
    out, carries = run(rows)
    assert carry_keys(carries) == {
        (P2, (50, 30), (55, 32)),   # approach: pass end -> take-on
        (P2, (55, 32), (62, 35)),   # exit: take-on -> next pass
    }
    # Order on the pitch: Pass, Challenge, Carry, Take On, Carry, Pass
    assert list(out["event_type"]) == ["Pass", "Challenge", "Carry", "Take On", "Carry", "Pass"]


def test_take_on_then_challenge_gets_carries_both_sides():
    rows = [
        ev(0, "Pass", HOME, P1, 30, 30, end_x=50, end_y=30),
        ev(1, "Take On", HOME, P2, 55, 32),
        ev(2, "Challenge", AWAY, P3, 45, 40, outcome="failure"),
        ev(3, "Pass", HOME, P2, 62, 35, end_x=80, end_y=40),
    ]
    out, carries = run(rows)
    assert carry_keys(carries) == {
        (P2, (50, 30), (55, 32)),
        (P2, (55, 32), (62, 35)),
    }
    assert list(out["event_type"]) == ["Pass", "Carry", "Take On", "Challenge", "Carry", "Pass"]


def test_no_duplicate_carries_around_a_paired_take_on():
    rows = [
        ev(0, "Pass", HOME, P1, 30, 30, end_x=50, end_y=30),
        ev(1, "Challenge", AWAY, P3, 45, 40, outcome="failure"),
        ev(2, "Take On", HOME, P2, 55, 32),
        ev(3, "Pass", HOME, P2, 62, 35, end_x=80, end_y=40),
    ]
    _, carries = run(rows)
    assert len(carries) == 2


# ── New: take-ons the old code ignored ────────────────────────────────────────

def test_failed_take_on_gets_approach_carry_only():
    """Pass -> Take On (failure) -> Tackle by the opponent."""
    rows = [
        ev(0, "Pass", HOME, P1, 30, 30, end_x=50, end_y=30),
        ev(1, "Take On", HOME, P2, 56, 33, outcome="failure"),
        ev(2, "Tackle", AWAY, P3, 44, 35),
        ev(3, "Pass", AWAY, P3, 44, 35, end_x=20, end_y=20),
    ]
    _, carries = run(rows)
    assert carry_keys(carries) == {(P2, (50, 30), (56, 33))}


def test_unpaired_successful_take_on_gets_carries_both_sides():
    rows = [
        ev(0, "Ball recovery", HOME, P2, 40, 30),
        ev(1, "Take On", HOME, P2, 48, 31),
        ev(2, "Goal", HOME, P2, 95, 34),
    ]
    _, carries = run(rows)
    assert carry_keys(carries) == {
        (P2, (40, 30), (48, 31)),
        (P2, (48, 31), (95, 34)),
    }


def test_take_on_with_non_adjacent_challenge_still_gets_carries():
    """A Ball touch (opponent deflection) between the Take On and its Challenge."""
    rows = [
        ev(0, "Pass", HOME, P1, 30, 30, end_x=50, end_y=30),
        ev(1, "Take On", HOME, P2, 55, 32),
        ev(2, "Ball touch", AWAY, P3, 45, 40),
        ev(3, "Challenge", AWAY, P3, 45, 40, outcome="failure"),
        ev(4, "Pass", HOME, P2, 62, 35, end_x=80, end_y=40),
    ]
    _, carries = run(rows)
    # Approach carry yes; exit carry no — the Ball touch is a real interruption.
    assert carry_keys(carries) == {(P2, (50, 30), (55, 32))}


# ── Guards that must keep holding ─────────────────────────────────────────────

def test_challenge_is_never_a_carry_origin():
    rows = [
        ev(0, "Challenge", AWAY, P3, 45, 40, outcome="failure"),
        ev(1, "Pass", AWAY, P3, 60, 40, end_x=70, end_y=40),
    ]
    _, carries = run(rows)
    assert carries.empty


def test_no_carry_across_period_even_through_a_challenge():
    rows = [
        ev(0, "Pass", HOME, P1, 30, 30, end_x=50, end_y=30, period=1),
        ev(1, "Challenge", AWAY, P3, 45, 40, outcome="failure", period=2),
        ev(2, "Take On", HOME, P2, 55, 32, period=2),
    ]
    _, carries = run(rows)
    assert carries.empty


def test_sub_metre_gap_is_not_a_carry():
    rows = [
        ev(0, "Pass", HOME, P1, 30, 30, end_x=50.0, end_y=30.0),
        ev(1, "Take On", HOME, P2, 50.5, 30.4),
    ]
    _, carries = run(rows)
    assert carries.empty


def test_plain_pass_to_pass_carry_unchanged():
    rows = [
        ev(0, "Pass", HOME, P1, 30, 30, end_x=50, end_y=30),
        ev(1, "Pass", HOME, P2, 58, 34, end_x=70, end_y=40),
    ]
    out, carries = run(rows)
    assert carry_keys(carries) == {(P2, (50, 30), (58, 34))}
    c = carries.iloc[0]
    assert c.type_id == -1 and c.outcome == "success"
    assert c.json_index == 0.5
    assert list(out["event_type"]) == ["Pass", "Carry", "Pass"]


def test_failed_pass_creates_no_carry():
    rows = [
        ev(0, "Pass", HOME, P1, 30, 30, end_x=50, end_y=30, outcome="failure"),
        ev(1, "Take On", HOME, P2, 58, 34),
    ]
    _, carries = run(rows)
    assert carries.empty


def test_take_on_then_own_ball_recovery_is_a_carry():
    """The dribbler knocks it past the defender and runs onto it."""
    rows = [
        ev(0, "Take On", HOME, P2, 55, 32),
        ev(1, "Challenge", AWAY, P3, 45, 40, outcome="failure"),
        ev(2, "Ball recovery", HOME, P2, 63, 30),
    ]
    _, carries = run(rows)
    assert carry_keys(carries) == {(P2, (55, 32), (63, 30))}


def test_take_on_then_teammate_ball_recovery_is_not_a_carry():
    rows = [
        ev(0, "Take On", HOME, P2, 55, 32),
        ev(1, "Ball recovery", HOME, P1, 63, 30),
    ]
    _, carries = run(rows)
    assert carries.empty


def test_failed_take_on_then_own_ball_recovery_is_not_a_carry():
    rows = [
        ev(0, "Take On", HOME, P2, 55, 32, outcome="failure"),
        ev(1, "Ball recovery", HOME, P2, 63, 30),
    ]
    _, carries = run(rows)
    assert carries.empty
