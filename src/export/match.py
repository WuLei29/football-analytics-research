"""
match.py
--------
Writes `teams/{team}/{season}/matches/{match_id}.json`: screen 02, and the
chain the sequence lab shows when a reader selects one.

Spec: md/WEB_DATA.md §7, with the shaping rules in §7.1, the fourteen totals in
§7.2 and the action-level sequence block in §7.3.

Reads: silver.matches, silver.events, silver.players, silver.match_lineups,
gold.team_match_stats, gold.player_match_stats, gold.sequences.
Writes: teams/{team}/{season}/matches/{match_id}.json.

Two conventions carry most of the risk in this file, both from §3:

  - **The event frame is per attacking team.** Every team attacks towards
    x = 105, home and away alike, so a single-team figure ships as stored and
    the one both-team figure -- the shot map -- has the OPPONENT mirrored here
    (`x' = 105 - x`, `y' = 68 - y`). The site never flips anything.
  - **Order events by (json_index, event_id).** json_index alone is not unique
    (GOLD_SEQUENCES.md §6, Fix 0); a tie reorders a possession chain.
"""

from __future__ import annotations

import logging
import math
from datetime import datetime
from typing import Any

from . import db
from .config import (
    DEFENSIVE_ACTIONS,
    MATCH_PLAYER_BOXES,
    MATCH_PLAYER_ROWS,
    MATCH_SEQUENCES,
    MATCH_TOTALS,
    NETWORK_MIN_COMBINATIONS,
    PROGRESSION_ARROWS,
    SEQUENCE_MAX_WAYPOINTS,
    SEQUENCE_MIN_EVENTS,
    TeamConfig,
)
from .io import Writer, as_int, r, r_coord, r_rate, r_value, r_xg
from .seasons import SeasonMeta

log = logging.getLogger("export")

PITCH_X = 105.0
PITCH_Y = 68.0

# The four Opta event types that are a shot at goal. Everything about a shot in
# this file -- the shot map, the `shot` action kind -- reads this set.
SHOT_TYPES = {"Goal", "Attempt Saved", "Miss", "Post"}


# ---------------------------------------------------------------------------
# match (§7)
# ---------------------------------------------------------------------------

def _match_block(match: dict[str, Any], stats: dict[int, dict[str, Any]],
                 season: SeasonMeta, team_cfg: TeamConfig) -> dict[str, Any]:
    home_id = int(match["home_team_id"])
    away_id = int(match["away_team_id"])

    def side(team_id: int, short_name: str, abbr: str, goals: Any, goals_ht: Any):
        return {
            "team_id": team_id,
            "short_name": short_name,
            "abbr": abbr,
            "goals": as_int(goals),
            "goals_ht": as_int(goals_ht),
            "xg": r_xg(stats[team_id]["xg_for"]),
        }

    return {
        "match_id": int(match["match_id"]),
        "matchday": as_int(match["matchday"]),
        "date": match["match_date"],
        "competition_name": season.competition_name,
        "season": season.slug,
        "venue": match["venue"],
        "match_length_min": as_int(match["match_length_min"]),
        # Which of the two sides the site has pages for, so no page has to
        # work it out from the team id.
        "team_side": "home" if team_cfg.team_id == home_id else "away",
        "home": side(home_id, match["home_short_name"], match["home_abbr"],
                     match["home_score"], match["home_score_ht"]),
        "away": side(away_id, match["away_short_name"], match["away_abbr"],
                     match["away_score"], match["away_score_ht"]),
    }


# ---------------------------------------------------------------------------
# momentum (§7.1)
# ---------------------------------------------------------------------------

def _momentum(rows: list[dict[str, Any]], markers: list[dict[str, Any]],
              home_id: int, away_id: int) -> dict[str, Any]:
    """One bin per (period, minute) in playing order, zero-filled, both sides
    positive.

    The site draws the away side below the axis and applies the 5-minute
    rolling window itself (handoff, screen 02 block 2), so the export ships
    the raw per-minute sum: a file that had already been smoothed could not be
    re-smoothed with a different window.

    Bins are keyed by period AND minute because the match clock restarts at
    45 for the second half: a first half that ran to 47' and a second half
    that starts at 45' both have minutes 45-47. The bins are laid out as the
    match was played — every first-half minute, then every second-half
    minute — and the site places them by position, so 47' of the first half
    is drawn before 45' of the second. `match_length_min` is deliberately not
    the axis: it is the SUM of the two halves (97 on a 47 + 50 match), and no
    single minute number on the clock ever reaches it.
    """
    by_key: dict[tuple[int, int], dict[str, float]] = {}
    for row in rows:
        key = (int(row["period"]), int(row["minute"]))
        bin_ = by_key.setdefault(key, {"home": 0.0, "away": 0.0})
        team = "home" if int(row["team_id"]) == home_id else "away"
        bin_[team] += float(row["xt"] or 0.0)

    # Each half runs at least its regulation length, and as far as its last
    # event if stoppage time carried the clock past it. The End event of the
    # half (typeId 30) is the authority when present; it is the last event of
    # the half, so an xT-less final minute still gets its bin.
    half_end = {1: 45, 2: 90}
    for (period, minute) in by_key:
        half_end[period] = max(half_end[period], minute)
    for row in markers:
        if row["type_id"] == 30 and int(row["period"]) in half_end:
            half_end[int(row["period"])] = max(half_end[int(row["period"])],
                                               int(row["minute"]))

    bins: list[dict[str, Any]] = []
    for period, first in ((1, 1), (2, 45)):
        for minute in range(first, half_end[period] + 1):
            values = by_key.get((period, minute), {})
            bins.append({
                "period": period,
                "minute": minute,
                "home": r_value(values.get("home", 0.0)),
                "away": r_value(values.get("away", 0.0)),
            })

    return {"bins": bins, "markers": _markers(markers, home_id, away_id)}


def _markers(rows: list[dict[str, Any]], home_id: int, away_id: int
             ) -> list[dict[str, Any]]:
    """Goals, cards, substitutions and half time, as vertical rules.

    `label_key` is a key, not a sentence: the Spanish lives in the site's
    labels file (§3.7).
    """
    out: list[dict[str, Any]] = []
    for row in rows:
        type_id = row["type_id"]
        if row["event_type"] == "Goal":
            kind = "goal"
        elif type_id == 17:
            kind = "card"
        elif type_id == 19:
            kind = "sub"
        elif type_id == 30:
            # Only the end of the first half is a marker a reader wants; the
            # end of the second is the right-hand edge of the chart.
            if int(row["period"]) != 1:
                continue
            kind = "period"
        else:
            continue

        team_id = as_int(row["team_id"])
        side = None
        if team_id == home_id:
            side = "home"
        elif team_id == away_id:
            side = "away"
        # Half time belongs to neither side, so it is drawn without one even
        # though the event carries a team.
        if kind == "period":
            side = None

        out.append({
            "period": as_int(row["period"]),
            "minute": as_int(row["minute"]),
            "type": kind,
            "side": side,
            "label_key": kind,
            "player_id": as_int(row["player_id"]) if kind != "period" else None,
            "name": row["player_name"] if kind != "period" else None,
        })

    # Markers outside the two halves (period 14 is the post-match End event)
    # have no bin to sit on.
    out = [m for m in out if m["period"] in (1, 2)]

    # One half-time rule, even if both period-1 End events are present (there
    # is one per team).
    seen_period = False
    deduped: list[dict[str, Any]] = []
    for marker in out:
        if marker["type"] == "period":
            if seen_period:
                continue
            seen_period = True
        deduped.append(marker)
    return deduped


# ---------------------------------------------------------------------------
# network (§7.1)
# ---------------------------------------------------------------------------

def _network(nodes: list[dict[str, Any]], edges: list[dict[str, Any]],
             side: str) -> dict[str, Any]:
    """The published team's starting XI, their mean positions and combinations.

    Deviation from §7.1 worth knowing: the nodes are the STARTING XI, not
    every player with a touch. The contract says "mean (x, y) of that player's
    on-ball events while on the pitch" without saying which players; drawing
    every substitute too puts fourteen nodes on a pitch that has eleven
    positions, and a 20-minute substitute's mean position is not a position.
    Edges are still counted over everyone, then filtered to pairs of nodes, so
    a combination with a substitute is dropped rather than drawn as an edge
    into empty space.
    """
    ids = {int(n["player_id"]) for n in nodes}

    return {
        "side": side,
        "min_combinations": NETWORK_MIN_COMBINATIONS,
        "nodes": [
            {
                "player_id": int(n["player_id"]),
                "surname": n["surname"],
                "shirt_number": as_int(n["shirt_number"]),
                "x": r_coord(n["x"]),
                "y": r_coord(n["y"]),
                # gold.player_match_stats.touches is the authority; the count
                # of on-ball events is the fallback for a row gold has not
                # computed (it never happens on a loaded match, but a null
                # radius would silently drop the node).
                "touches": as_int(n["touches"] if n["touches"] is not None
                                  else n["on_ball_events"]),
            }
            for n in nodes
        ],
        "edges": [
            {
                "from": int(e["player_a"]),
                "to": int(e["player_b"]),
                "passes": int(e["passes"]),
            }
            for e in edges
            if int(e["player_a"]) in ids and int(e["player_b"]) in ids
        ],
    }


# ---------------------------------------------------------------------------
# totals (§7.2)
# ---------------------------------------------------------------------------

def _totals(stats: dict[int, dict[str, Any]], home_id: int, away_id: int
            ) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for total in MATCH_TOTALS:
        def value(team_id: int) -> float | None:
            raw = stats[team_id][total.column]
            if raw is None:
                return None
            # gold stores every share as a 0-1 fraction: possession_pct is
            # 0.531, and the row reads "53.1".
            scaled = float(raw) * 100 if total.scale == "pct" else float(raw)
            return r(scaled, total.decimals)

        out.append({
            "key": total.key,
            "home": value(home_id),
            "away": value(away_id),
        })
    return out


# ---------------------------------------------------------------------------
# shots (§7.1)
# ---------------------------------------------------------------------------

def _body_part(row: dict[str, Any]) -> str:
    if row["headed"]:
        return "head"
    if row["right_foot"]:
        return "right_foot"
    if row["left_foot"]:
        return "left_foot"
    return "other"


def _shot_outcome(row: dict[str, Any]) -> str:
    event_type = row["event_type"]
    if event_type == "Goal":
        return "goal"
    if event_type == "Post":
        return "woodwork"
    if event_type == "Miss":
        return "off_target"
    # 'Attempt Saved' is both a save and a block; Q82 is the only thing that
    # separates them, and the design's legend distinguishes the two.
    return "blocked" if row["blocked"] else "saved"


def _shots(rows: list[dict[str, Any]], team_id: int, home_id: int
           ) -> list[dict[str, Any]]:
    """Both teams, with the OPPONENT mirrored into the published team's frame.

    This is the only both-team figure on the site, and the only place a
    coordinate is transformed (§3.1). The goalmouth pair is deliberately not
    mirrored: that frame is the goal itself and has no attacking direction.
    """
    out: list[dict[str, Any]] = []
    for index, row in enumerate(rows, start=1):
        shooting_team = int(row["team_id"])
        mirror = shooting_team != team_id
        x = float(row["x"])
        y = float(row["y"])

        out.append({
            "shot_id": index,
            "side": "home" if shooting_team == home_id else "away",
            "player_id": as_int(row["player_id"]),
            "name": row["player_name"],
            "minute": as_int(row["minute"]),
            "x": r_coord(PITCH_X - x if mirror else x),
            "y": r_coord(PITCH_Y - y if mirror else y),
            "xg": r_value(row["xg"]),
            "body_part": _body_part(row),
            "outcome": _shot_outcome(row),
            "play_pattern": row["shot_play_pattern"],
            "first_time": row["first_time"],
            "big_chance": bool(row["big_chance"]),
            # Opta's own goalmouth frame: posts at 45.2 and 54.8, crossbar 38.
            "goal_mouth_y": r_rate(row["goal_mouth_y"]),
            "goal_mouth_z": r_rate(row["goal_mouth_z"]),
        })
    return out


# ---------------------------------------------------------------------------
# xt_grid, progression, defence (§7.1)
# ---------------------------------------------------------------------------

def _xt_grid(rows: list[dict[str, Any]], side: str) -> dict[str, Any]:
    return {
        "side": side,
        "cols": 12,
        "rows": 8,
        "cells": [
            {"cx": int(row["cx"]), "cy": int(row["cy"]), "xt": r_value(row["xt"])}
            for row in rows
        ],
    }


def _xt_zones(rows: list[dict[str, Any]], side: str) -> dict[str, Any]:
    """`sum(xt)` per zone of `gold.pitch_zones`, for the `ZoneHeatmap`."""
    return {
        "side": side,
        "cells": [
            {"zone_id": int(row["zone_id"]), "value": r_value(row["xt"])}
            for row in rows
        ],
    }


def _progression(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            # 'Carry' is the synthesised type_id = -1 of carries.py.
            "kind": "carry" if row["event_type"] == "Carry" else "pass",
            "x": r_coord(row["x"]),
            "y": r_coord(row["y"]),
            "end_x": r_coord(row["end_x"]),
            "end_y": r_coord(row["end_y"]),
            "xt": r_value(row["xt"]),
            "completed": row["outcome"] == "success",
        }
        for row in rows
    ]


def _defence(rows: list[dict[str, Any]], line_x: Any) -> dict[str, Any]:
    return {
        # In the attacking frame of §3.1, so a low value is a deep line and
        # the value is drawn on a pitch attacking right, unchanged.
        "line_x": r_coord(line_x),
        "actions": [
            {
                "type": row["event_type"].lower().replace(" ", "_"),
                "x": r_coord(row["x"]),
                "y": r_coord(row["y"]),
                # A Challenge only ever carries outcome='failure' -- it IS the
                # beaten player -- so it lands on the right side of this
                # without a special case (WEB_DATA.md §14 item 5).
                "outcome": "won" if row["outcome"] == "success" else "lost",
                "minute": as_int(row["minute"]),
                "player": row["player_name"],
            }
            for row in rows
        ],
    }


# ---------------------------------------------------------------------------
# sequences (§7.1, §7.3)
# ---------------------------------------------------------------------------

# Opta qualifier ids the sequence block reads (opta-events-reference §qualifiers).
Q_LONG_BALL, Q_CROSS, Q_HEAD_PASS, Q_THROUGH_BALL = 1, 2, 3, 4
Q_FREE_KICK, Q_CORNER, Q_THROW_IN, Q_GOAL_KICK, Q_KICK_OFF = 5, 6, 107, 124, 279

# Opta's goalmouth frame (Q102) is the pitch-width 0-100 scale with the posts
# at 45.2 and 54.8, so 9.6 units span the 7.32 m goal. Low goal_mouth_y is the
# RIGHT flank -- the same side as low `y` in metres (GOLD_LAYER §4.6.0).
GOAL_MOUTH_M_PER_UNIT = 7.32 / 9.6
GOAL_CENTRE_Y = PITCH_Y / 2

# How a sequence-ending action lost the ball, by our LAST action, keyed for
# `lib/labels.ts`. Only shipped when gold's `outcome` is the coarse `turnover`
# (63 % of all sequences); every other outcome already says what happened.
END_ACTION_BY_TYPE = {
    "Clearance": "clearance",
    "Take On": "take_on",
    "Dispossessed": "dispossessed",
    "Ball touch": "touch",
    "Carry": "carry",
    # A duel or pick-up won, then the ball lost before a controlled action.
    "Interception": "loose_regain",
    "Blocked Pass": "loose_regain",
    "Aerial": "loose_regain",
    "Tackle": "loose_regain",
    "Ball recovery": "loose_regain",
    "Challenge": "loose_regain",
}

# The set-piece restarts hiding inside gold's `start_trigger = 'pass'` (28 % of
# all sequences), in precedence order -- a corner is also a "free kick taken"
# in some feeds, so the more specific qualifier is tested first.
RESTART_QUALIFIERS = (
    (Q_CORNER, "corner"),
    (Q_THROW_IN, "throw_in"),
    (Q_GOAL_KICK, "goal_kick"),
    (Q_KICK_OFF, "kick_off"),
    (Q_FREE_KICK, "free_kick"),
)

# The restarts `phases.py` can open a set-piece phase from (its
# qualifies_as_set_piece reads the same `sequence_type`), so `has_set_piece`
# never fires on any other value; the guard is belt and braces.
SET_PIECE_KINDS = ("corner", "throw_in", "free_kick")


def action_kind(event_type: str, qualifier_ids: list[int]) -> str:
    """One of the seven kinds the detailed sequence view draws (§7.3)."""
    if event_type in SHOT_TYPES:
        return "shot"
    if event_type == "Carry":
        return "carry"
    if event_type == "Take On":
        return "take_on"
    if event_type in ("Pass", "Offside Pass"):
        return "cross" if Q_CROSS in qualifier_ids else "pass"
    if event_type == "Clearance":
        # The one non-pass action with a destination of its own (Q140/141). It
        # shipped as `other` until 19 Sep 2026, so a 32 m clearance drew as a
        # dot and the carry that picked it up started in mid-air.
        return "clearance"
    return "other"


def action_type(event_type: str) -> str:
    """`event_type` as a snake_case key the site can translate: `Ball recovery`
    -> `ball_recovery`. Every action ships one, so an `other` dot can be named
    in a tooltip rather than left as an unexplained point."""
    return event_type.lower().replace("-", " ").replace(" ", "_")


def simplify(points: list[list[float]], max_waypoints: int) -> list[list[float]]:
    """Start, end, and the `max_waypoints` inner points that carry the shape.

    The exposure rule of WEB_PLAN.md §6: a trace, not the chain's events. The
    full event list of a sequence ships separately in `actions`, which is
    scoped to one match and is what the detailed view reads.

    Ramer-Douglas-Peucker under a point budget (20 Sep 2026): the kept set
    starts as {start, end} and grows by the point furthest from the polyline
    through the points kept so far, until the budget is spent or nothing is
    further than `RDP_STOP_M` from it. Even sampling, which this replaced,
    spent its six points along a straight build-up and lost the switch of
    flank in a chain that turned; here a straight stretch costs nothing.
    """
    if len(points) <= max_waypoints + 2:
        return points
    kept = [0, len(points) - 1]
    while len(kept) < max_waypoints + 2:
        best_d, best_i, best_slot = RDP_STOP_M, -1, -1
        for slot in range(len(kept) - 1):
            a, b = points[kept[slot]], points[kept[slot + 1]]
            for i in range(kept[slot] + 1, kept[slot + 1]):
                d = _point_to_segment(points[i], a, b)
                if d > best_d:
                    best_d, best_i, best_slot = d, i, slot
        if best_i < 0:
            break
        kept.insert(best_slot + 1, best_i)
    return [points[i] for i in kept]


# Below this distance from the simplified line a point adds nothing the eye
# can see at the traces' scale (a full pitch in ~600 px is ~0.2 m per px).
RDP_STOP_M = 0.25


def _point_to_segment(p: list[float], a: list[float], b: list[float]) -> float:
    """Perpendicular distance from `p` to segment `ab`, clamped to its ends."""
    ax, ay = a
    dx, dy = b[0] - ax, b[1] - ay
    length2 = dx * dx + dy * dy
    if length2 == 0:
        return ((p[0] - ax) ** 2 + (p[1] - ay) ** 2) ** 0.5
    t = max(0.0, min(1.0, ((p[0] - ax) * dx + (p[1] - ay) * dy) / length2))
    cx, cy = ax + t * dx, ay + t * dy
    return ((p[0] - cx) ** 2 + (p[1] - cy) ** 2) ** 0.5


def _sequence_points(events: list[dict[str, Any]]) -> list[list[float]]:
    points = [[r_coord(e["x"]), r_coord(e["y"])] for e in events]
    # End the polyline where the ball ended rather than at the last touch, so
    # a chain that finishes with a shot reaches the goal.
    tail = _action_end(events[-1])
    if tail is not None and tail != points[-1]:
        points.append(tail)
    return points


def _action_end(event: dict[str, Any]) -> list[float] | None:
    """Where the ball went: `end_x/end_y` for a pass, cross, carry or
    clearance; the goal line at `goal_mouth_y` for a shot (20 Sep 2026 --
    every shot used to be drawn to the goal centre, although Opta records
    where it crossed the line, on target or not); nothing for a point event."""
    kind = action_kind(event["event_type"], event["qualifier_ids"])
    if kind == "shot":
        gm = event["goal_mouth_y"]
        y = GOAL_CENTRE_Y if gm is None else GOAL_CENTRE_Y + (float(gm) - 50.0) * GOAL_MOUTH_M_PER_UNIT
        return [PITCH_X, r_coord(max(0.0, min(PITCH_Y, y)))]
    if kind in ("pass", "cross", "carry", "clearance"):
        if event["end_x"] is None or event["end_y"] is None:
            return None
        return [r_coord(event["end_x"]), r_coord(event["end_y"])]
    return None


def start_kind(start_trigger: str | None, first: dict[str, Any]) -> str | None:
    """`gold.sequences.start_trigger`, with a `pass` start refined into the
    restart it really is (corner, throw-in, goal kick, kick-off, free kick)
    from the first action's qualifiers. Open-play passes stay `pass`."""
    if start_trigger == "pass" and first["event_type"] == "Pass":
        ids = first["qualifier_ids"]
        for qualifier, kind in RESTART_QUALIFIERS:
            if qualifier in ids:
                return kind
    return start_trigger


# `has_direct_long` (phases.py) fires on one qualifying ball ANYWHERE in the
# sequence -- 7,285 of 19,017 flagged chains had it at event 6+, averaging
# 12.2 events and 7.1 passes, which titled a settled possession "Juego
# directo" for a single mid-chain switch of play. As a *title* that
# overreaches, so `kind` recomputes the same test (dx/angle mirrored from
# sequence_phases.py) but only over the team's own first three events -- the
# classifier's own established-possession threshold (TEMPO_THRESHOLD),
# so a long ball only earns the title if it happens before the chain could
# count as settled. 7,097 of those 19,017 keep it (measured 22 Sep 2026).
DIRECT_LONG_MIN_DX_M = 32.0
DIRECT_LONG_MAX_ANGLE_DEG = 30.0
DIRECT_LONG_EVENT_WINDOW = 3


def _is_direct_long_event(event: dict[str, Any]) -> bool:
    """One ball >= 32 m forward within 30 deg of the goal axis."""
    if event["event_type"] not in ("Pass", "Carry"):
        return False
    x, end_x = event["x"], event["end_x"]
    if x is None or end_x is None:
        return False
    dx = end_x - x
    if dx < DIRECT_LONG_MIN_DX_M:
        return False
    y, end_y = event["y"], event["end_y"]
    dy = abs(end_y - y) if y is not None and end_y is not None else 0.0
    return dy <= dx * math.tan(math.radians(DIRECT_LONG_MAX_ANGLE_DEG))


def is_early_direct_long(chain: list[dict[str, Any]]) -> bool:
    """Whether the chain opened with a direct long ball (§7.6, 22 Sep 2026).

    `chain` is already the possessing team's own events, ordered by
    (json_index, event_id) -- the same events gold's has_direct_long scans,
    just windowed to the first three.
    """
    return any(_is_direct_long_event(e) for e in chain[:DIRECT_LONG_EVENT_WINDOW])


def sequence_kind(row: dict[str, Any], chain: list[dict[str, Any]]) -> str:
    """The one word for the chain in a list row (§7.6, 21/22 Sep 2026).

    A priority ladder over the gold phase flags, most defining first. How the
    ball was won outranks everything (a counter-attack is a counter-attack
    whatever it did next); a set piece is named by the restart it started
    from; then a long ball in its opening events; then where the possession
    settled -- the middle or final third beats the own third, so a chain that
    built up and then established itself upfield is `positional`, and
    `buildup` is one that never got past its own third. `fast` is what is
    left: no established segment at all.
    """
    if row["has_counter_attack"]:
        return "counter_attack"
    if row["has_high_transition"]:
        return "high_transition"
    if row["has_set_piece"] and row["sequence_type"] in SET_PIECE_KINDS:
        return row["sequence_type"]
    if is_early_direct_long(chain):
        return "direct_long"
    if row["has_midblock"] or row["has_attacking"]:
        return "positional"
    if row["has_buildup"]:
        return "buildup"
    return "fast"


def end_action(outcome: str | None, last: dict[str, Any]) -> str | None:
    """How a `turnover` lost the ball, from our last action (§7.3). A failed
    pass is split by its sub-type; null on every other outcome."""
    if outcome != "turnover":
        return None
    event_type = last["event_type"]
    if event_type in ("Pass", "Offside Pass"):
        ids = last["qualifier_ids"]
        if Q_CROSS in ids:
            return "pass_cross"
        if Q_THROUGH_BALL in ids:
            return "pass_through"
        if Q_LONG_BALL in ids:
            return "pass_long"
        if Q_HEAD_PASS in ids:
            return "pass_head"
        return "pass"
    return END_ACTION_BY_TYPE.get(event_type, "other")


def _actions(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for event in events:
        kind = action_kind(event["event_type"], event["qualifier_ids"])
        # A point event (take-on, recovery, block) has no destination. A shot's
        # is the goal line at its goalmouth y, converted to metres.
        end = _action_end(event)

        result = event["spadl_result_id"]
        if result is not None:
            outcome = "success" if int(result) == 1 else "fail"
        else:
            outcome = "success" if event["outcome"] == "success" else "fail"

        action = {
            "kind": kind,
            "type": action_type(event["event_type"]),
            "minute": as_int(event["minute"]),
            "second": as_int(event["second"]),
            "x": r_coord(event["x"]),
            "y": r_coord(event["y"]),
            "end_x": end[0] if end else None,
            "end_y": end[1] if end else None,
            "player_id": as_int(event["player_id"]),
            "surname": event["surname"],
            "shirt_number": as_int(event["shirt_number"]),
            "outcome": outcome,
        }
        if kind == "shot":
            action["xg"] = r_value(event["xg"])
        out.append(action)
    return out


def _sequences(rows: list[dict[str, Any]], events: list[dict[str, Any]]
               ) -> list[dict[str, Any]]:
    """Every sequence with at least SEQUENCE_MIN_EVENTS events, best xT first.

    `rank` is 1..12 on the twelve the design draws and null on the rest: the
    site takes the first twelve as they come and never re-sorts, so the pitch
    and the list beside it cannot disagree.
    """
    by_sequence: dict[str, list[dict[str, Any]]] = {}
    for event in events:
        by_sequence.setdefault(event["sequence_id"], []).append(event)

    out: list[dict[str, Any]] = []
    for index, row in enumerate(rows):
        sequence_id = row["sequence_id"]
        chain = by_sequence.get(sequence_id, [])
        if not chain:
            # A sequence whose events all lack coordinates cannot be drawn;
            # shipping it would put an empty row in the list beside the pitch.
            log.warning("sequence %s has no drawable events; skipped", sequence_id)
            continue

        out.append({
            "sequence_id": sequence_id,
            "rank": index + 1 if index < MATCH_SEQUENCES else None,
            "period": as_int(row["period"]),
            "minute": as_int(row["start_minute"]),
            "second": as_int(row["start_second"]),
            "duration_s": r_rate(row["duration_seconds"]),
            "events": as_int(row["event_count"]),
            "passes": as_int(row["pass_count"]),
            "xt": r_value(row["xt"]),
            "xg": r_value(row["xg"]),
            "vaep": r_value(row["vaep"]),
            "start_zone": as_int(row["start_zone"]),
            "start_trigger": row["start_trigger"],
            "start_kind": start_kind(row["start_trigger"], chain[0]),
            "outcome": row["outcome"],
            "end_action": end_action(row["outcome"], chain[-1]),
            "primary_phase": row["primary_phase"],
            "kind": sequence_kind(row, chain),
            "final_third_entry": bool(row["final_third_entry"]),
            "penalty_box_entry": bool(row["penalty_box_entry"]),
            "ends_in_shot": int(row["shot_count"] or 0) > 0,
            "points": simplify(_sequence_points(chain), SEQUENCE_MAX_WAYPOINTS),
            "actions": _actions(chain),
        })
    return out


# ---------------------------------------------------------------------------
# players (§7.1)
# ---------------------------------------------------------------------------

def _players(rows: list[dict[str, Any]], home_id: int) -> list[dict[str, Any]]:
    """Four boxes of six rows, both teams, ranked here rather than in SQL.

    One query feeds all four boxes: ranking four columns in Postgres would be
    four round trips for twenty-two rows.
    """
    out: list[dict[str, Any]] = []
    for key, columns in MATCH_PLAYER_BOXES:
        def total(row: dict[str, Any]) -> float:
            return sum(float(row[c] or 0.0) for c in columns)

        ranked = sorted(
            (row for row in rows if any(row[c] is not None for c in columns)),
            # Ties break on minutes then name, so a refresh that changes
            # nothing does not reshuffle the box.
            key=lambda row: (-total(row), -(row["minutes_played"] or 0), row["name"]),
        )[:MATCH_PLAYER_ROWS]

        box_rows: list[dict[str, Any]] = []
        for row in ranked:
            entry = {
                "player_id": int(row["player_id"]),
                "name": row["name"],
                "side": "home" if int(row["team_id"]) == home_id else "away",
                "abbr": row["abbr"],
                "value": r_value(total(row)) if len(columns) > 1
                         or columns[0] in ("xg", "xa") else as_int(total(row)),
            }
            # The xG + xA box shows the sum but has to name its parts, or a
            # reader cannot tell a finisher from a creator.
            if len(columns) > 1:
                entry["components"] = {c: r_value(row[c]) for c in columns}
            box_rows.append(entry)

        out.append({"key": key, "rows": box_rows})
    return out


# ---------------------------------------------------------------------------

def build(conn, writer: Writer, team_cfg: TeamConfig, season: SeasonMeta,
          match_id: int, generated_at: datetime) -> None:
    match = db.fetch_match(conn, match_id)
    home_id = int(match["home_team_id"])
    away_id = int(match["away_team_id"])
    team_id = team_cfg.team_id

    if team_id not in (home_id, away_id):
        raise LookupError(
            f"match {match_id} is {home_id} v {away_id}; team {team_id} did not "
            "play in it"
        )

    stats = {int(row["team_id"]): row for row in db.fetch_match_team_stats(conn, match_id)}
    side = "home" if team_id == home_id else "away"

    sequence_rows = db.fetch_sequences(conn, match_id, team_id, SEQUENCE_MIN_EVENTS)
    sequence_events = db.fetch_sequence_events(
        conn, match_id, team_id, [row["sequence_id"] for row in sequence_rows]
    )

    payload = {
        "match": _match_block(match, stats, season, team_cfg),
        "momentum": _momentum(
            db.fetch_momentum(conn, match_id),
            db.fetch_match_markers(conn, match_id),
            home_id, away_id,
        ),
        "network": _network(
            db.fetch_network_nodes(conn, match_id, team_id),
            db.fetch_network_edges(conn, match_id, team_id, NETWORK_MIN_COMBINATIONS),
            side,
        ),
        "totals": _totals(stats, home_id, away_id),
        "shots": _shots(db.fetch_shots(conn, match_id), team_id, home_id),
        "xt_grid": _xt_grid(db.fetch_xt_grid(conn, match_id, team_id), side),
        "xt_zones": _xt_zones(db.fetch_xt_zones(conn, match_id, team_id), side),
        "progression": _progression(
            db.fetch_progression(conn, match_id, team_id, PROGRESSION_ARROWS)
        ),
        "defence": _defence(
            db.fetch_defensive_actions(conn, match_id, team_id, DEFENSIVE_ACTIONS),
            stats[team_id]["defensive_line_height"],
        ),
        "sequences": _sequences(sequence_rows, sequence_events),
        "players": _players(db.fetch_match_players(conn, match_id), home_id),
    }

    writer.write(
        f"teams/{team_cfg.slug}/{season.slug}/matches/{match_id}.json",
        payload,
        generated_at,
    )
