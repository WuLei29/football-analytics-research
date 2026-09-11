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
              home_id: int, away_id: int, length_min: int,
              ) -> dict[str, Any]:
    """One bin per minute, zero-filled, both sides positive.

    The site draws the away side below the axis and applies the 5-minute
    rolling window itself (handoff, screen 02 block 2), so the export ships
    the raw per-minute sum: a file that had already been smoothed could not be
    re-smoothed with a different window.
    """
    by_minute: dict[int, dict[str, float]] = {}
    for row in rows:
        minute = int(row["minute"])
        bin_ = by_minute.setdefault(minute, {"home": 0.0, "away": 0.0})
        key = "home" if int(row["team_id"]) == home_id else "away"
        bin_[key] += float(row["xt"] or 0.0)

    # Stoppage time can carry the clock past match_length_min in the event
    # stream; the axis must cover whatever exists.
    last = max([length_min or 0, *by_minute.keys()], default=0)

    return {
        "bins": [
            {
                "minute": minute,
                "home": r_value(by_minute.get(minute, {}).get("home", 0.0)),
                "away": r_value(by_minute.get(minute, {}).get("away", 0.0)),
            }
            for minute in range(1, last + 1)
        ],
        "markers": _markers(markers, home_id, away_id),
    }


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
            "minute": as_int(row["minute"]),
            "type": kind,
            "side": side,
            "label_key": kind,
            "player_id": as_int(row["player_id"]) if kind != "period" else None,
            "name": row["player_name"] if kind != "period" else None,
        })

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

def action_kind(event_type: str, is_cross: Any) -> str:
    """One of the six kinds the detailed sequence view draws (§7.3)."""
    if event_type in SHOT_TYPES:
        return "shot"
    if event_type == "Carry":
        return "carry"
    if event_type == "Take On":
        return "take_on"
    if event_type in ("Pass", "Offside Pass"):
        return "cross" if is_cross else "pass"
    return "other"


def simplify(points: list[list[float]], max_waypoints: int) -> list[list[float]]:
    """Start, end, and up to `max_waypoints` evenly sampled points between.

    The exposure rule of WEB_PLAN.md §6: a trace, not the chain's events. The
    full event list of a sequence ships separately in `actions`, which is
    scoped to one match and is what the detailed view reads.
    """
    if len(points) <= max_waypoints + 2:
        return points
    inner = points[1:-1]
    step = len(inner) / max_waypoints
    sampled = [inner[min(int(i * step), len(inner) - 1)] for i in range(max_waypoints)]
    return [points[0], *sampled, points[-1]]


def _sequence_points(events: list[dict[str, Any]]) -> list[list[float]]:
    points = [[r_coord(e["x"]), r_coord(e["y"])] for e in events]
    # End the polyline where the ball ended rather than at the last touch, so
    # a chain that finishes with a shot reaches the goal.
    last = events[-1]
    if last["end_x"] is not None and last["end_y"] is not None:
        tail = [r_coord(last["end_x"]), r_coord(last["end_y"])]
        if tail != points[-1]:
            points.append(tail)
    return points


def _actions(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for event in events:
        kind = action_kind(event["event_type"], event["is_cross"])
        # A point event has no destination: a take-on happens where it happens,
        # and a shot's end is the goalmouth, which is a different frame.
        has_end = kind in ("pass", "cross", "carry")

        result = event["spadl_result_id"]
        if result is not None:
            outcome = "success" if int(result) == 1 else "fail"
        else:
            outcome = "success" if event["outcome"] == "success" else "fail"

        out.append({
            "kind": kind,
            "x": r_coord(event["x"]),
            "y": r_coord(event["y"]),
            "end_x": r_coord(event["end_x"]) if has_end else None,
            "end_y": r_coord(event["end_y"]) if has_end else None,
            "player_id": as_int(event["player_id"]),
            "surname": event["surname"],
            "shirt_number": as_int(event["shirt_number"]),
            "outcome": outcome,
        })
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
            "outcome": row["outcome"],
            "primary_phase": row["primary_phase"],
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
            home_id, away_id, int(match["match_length_min"] or 0),
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
