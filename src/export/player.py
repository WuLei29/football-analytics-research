"""
player.py
---------
Writes `teams/{team}/{season}/players/{player_id}.json`: screen 03, one file
per player in `squad.json`.

Spec: md/WEB_DATA.md §9, README-PLAYER "Data contract" (which replaces §9 for
screen 03).

Reads: silver.players, silver.events, silver.matches, gold.player_season_stats,
gold.pass_receptions, gold.sequences, gold.sequence_players.
Writes: teams/{team}/{season}/players/{player_id}.json.

Nothing is aggregated on the site and no gold table is created for this page:
every block below is a slice of an existing gold or silver table.

Frame: every coordinate is the player's own event, in his team's attacking
frame (§3.1), so the site draws it as shipped -- a right back's marks sit at
low y, which the site puts at the bottom of a horizontal pitch.

Deviation from README-PLAYER's contract, on purpose: the passes ship as THREE
lists (`passes`, `progressive_passes`, `key_passes`) rather than one list with
flag bits filtered on the site. The three modes have different caps -- the
150 most recent, the 60 best by xT, all of them -- and one capped list cannot
serve all three without the progressive mode showing only whatever progressive
passes happened to be recent.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

from . import db
from .config import (
    PLAYER_KPIS,
    PLAYER_PASS_MAP_MAX,
    PLAYER_PROGRESSIVE_MAX,
    PLAYER_TOP_SEQUENCES,
    RECEPTION_MAX_GAP_M,
    SEQUENCE_MAX_WAYPOINTS,
    SEQUENCE_MIN_EVENTS,
    PlayerKpi,
    TeamConfig,
    min_minutes_for,
)
from .io import Writer, as_int, r, r_coord, r_rate, r_value
from .match import (
    _body_part,
    _sequence_points,
    _shot_outcome,
    sequence_kind,
    simplify,
)
from .seasons import SeasonMeta
from .squad import display_name

# Bits of the compact pass row [x, y, end_x, end_y, flags].
FLAG_COMPLETED, FLAG_PROGRESSIVE, FLAG_KEY_PASS, FLAG_ASSIST = 1, 2, 4, 8

# README-PLAYER §3b: these two are always a duel lost (the player went in and
# missed, or was dribbled past); a Tackle is won or lost by its outcome; every
# other defensive type is a ball won.
DEFENCE_ALWAYS_LOST = {"Attempted Tackle", "Challenge"}


def age_on(born: date | None, on: date | None) -> int | None:
    """Whole years. Measured on the last match date the season has, so the
    file does not change on the day a birthday passes between two runs."""
    if born is None or on is None:
        return None
    return on.year - born.year - ((on.month, on.day) < (born.month, born.day))


# ---------------------------------------------------------------------------
# player (header + bio grid)
# ---------------------------------------------------------------------------

def _player_block(row: dict[str, Any], season: SeasonMeta) -> dict[str, Any]:
    pct = row["primary_position_pct"]
    return {
        "player_id": int(row["player_id"]),
        "name": display_name(row),
        "shirt_number": as_int(row["shirt_number"]),
        "nationality": row["nationality"],
        "preferred_foot": row["preferred_foot"],
        "height_cm": as_int(row["height_cm"]),
        "date_of_birth": row["date_of_birth"],
        "age": age_on(row["date_of_birth"], season.through_match_date),
        "position": row["primary_position"],
        "position_group": row["primary_position_group"],
        "role": row["role_group"] or row["primary_position_group"],
        "is_utility": bool(row["is_utility"]),
        # gold stores the share as 0-1; the chip reads "62 %".
        "primary_position_pct": None if pct is None else r(float(pct) * 100, 0),
        "matches": as_int(row["matches_played"]),
        "starts": as_int(row["matches_started"]),
        "minutes": as_int(row["minutes_played"]),
        "subbed_off": as_int(row["matches_subbed_off"]),
        "yellow_cards": as_int(row["yellow_cards"]),
        "red_cards": as_int(row["red_cards"]),
        "penalties_taken": as_int(row["penalties_taken"]),
        "penalties_scored": as_int(row["penalties_scored"]),
    }


# ---------------------------------------------------------------------------
# kpis (§3a)
# ---------------------------------------------------------------------------

def _scaled(kpi: PlayerKpi, raw: Any) -> float | None:
    if raw is None:
        return None
    return r(float(raw) * 100 if kpi.scale == "pct" else float(raw), kpi.decimals)


def squad_rank(rows: list[dict[str, Any]], player_id: int, key: str,
               min_minutes: float) -> tuple[int | None, int]:
    """1-based rank inside the squad among the players past the minutes gate,
    ties sharing a rank. (None, n) when the player is under the gate: a rate
    over 40 minutes ranked first in the squad is the noise the gate exists
    to keep out."""
    peers = [row for row in rows
             if row[key] is not None and row["minutes_played"] >= min_minutes]
    mine = next((row for row in peers if int(row["player_id"]) == player_id), None)
    if mine is None:
        return None, len(peers)
    better = sum(1 for row in peers if float(row[key]) > float(mine[key]))
    return better + 1, len(peers)


def kpi_keys() -> list[str]:
    return sorted({k.key for kpis in PLAYER_KPIS.values() for k in kpis})


def _kpis(role: str, player_id: int, club_rows: list[dict[str, Any]],
          min_minutes: float) -> list[dict[str, Any]]:
    mine = next(row for row in club_rows if int(row["player_id"]) == player_id)
    minutes = int(mine["minutes_played"])
    out: list[dict[str, Any]] = []
    for kpi in PLAYER_KPIS[role]:
        raw = mine[kpi.key]
        entry: dict[str, Any] = {
            "key": kpi.key,
            "value": _scaled(kpi, raw),
            "scale": kpi.scale,
            "decimals": kpi.decimals,
            "per_90": None,
            "squad_rank": None,
            "squad_n": None,
        }
        if kpi.total:
            # The sub line under a season total is its per-90.
            if raw is not None and minutes > 0:
                entry["per_90"] = r(float(raw) * 90.0 / minutes, 2)
        else:
            rank, n = squad_rank(club_rows, player_id, kpi.key, min_minutes)
            entry["squad_rank"], entry["squad_n"] = rank, n
        out.append(entry)
    return out


# ---------------------------------------------------------------------------
# maps (§3b)
# ---------------------------------------------------------------------------

def pass_row(row: dict[str, Any]) -> list[float | int]:
    flags = ((FLAG_COMPLETED if row["completed"] else 0)
             | (FLAG_PROGRESSIVE if row["progressive"] else 0)
             | (FLAG_KEY_PASS if row["key_pass"] else 0)
             | (FLAG_ASSIST if row["assist"] else 0))
    return [r_coord(row["x"]), r_coord(row["y"]),
            r_coord(row["end_x"]), r_coord(row["end_y"]), flags]


def _zones(rows: list[dict[str, Any]], key: str) -> list[dict[str, Any]]:
    total = sum(int(row[key]) for row in rows)
    return [
        {
            "zone_id": int(row["zone_id"]),
            "count": int(row[key]),
            "share": r_rate(100.0 * int(row[key]) / total) if total else None,
        }
        for row in rows
        if row["zone_id"] is not None
    ]


def _shots(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """The match file's Shot shape (§7.1) without `side` and `name`: every
    shot is the player's own, in his team's frame. The goalmouth pair is
    Opta's goal frame and is never mirrored."""
    return [
        {
            "shot_id": index,
            "match_id": int(row["match_id"]),
            "matchday": as_int(row["matchday"]),
            "minute": as_int(row["minute"]),
            "x": r_coord(row["x"]),
            "y": r_coord(row["y"]),
            "xg": r_value(row["xg"]),
            "body_part": _body_part(row),
            "outcome": _shot_outcome(row),
            "play_pattern": row["shot_play_pattern"],
            "first_time": row["first_time"],
            "big_chance": bool(row["big_chance"]),
            "goal_mouth_y": r_rate(row["goal_mouth_y"]),
            "goal_mouth_z": r_rate(row["goal_mouth_z"]),
        }
        for index, row in enumerate(rows, start=1)
    ]


def defence_outcome(event_type: str, outcome: str | None) -> str:
    if event_type in DEFENCE_ALWAYS_LOST:
        return "lost"
    if event_type == "Tackle":
        return "won" if outcome == "success" else "lost"
    return "won"


def _defence(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            # The page-02 type keys, so the chips and tooltips reuse its labels.
            "type": row["event_type"].lower().replace(" ", "_"),
            "x": r_coord(row["x"]),
            "y": r_coord(row["y"]),
            "outcome": defence_outcome(row["event_type"], row["outcome"]),
            "minute": as_int(row["minute"]),
            "matchday": as_int(row["matchday"]),
        }
        for row in rows
    ]


def _maps(conn, cs_id: int, team_id: int, player_id: int) -> dict[str, Any]:
    counts = db.fetch_player_pass_counts(conn, cs_id, team_id, player_id) or {}
    touch_rows = db.fetch_player_touch_zones(conn, cs_id, team_id, player_id)
    reception_rows = db.fetch_player_reception_zones(conn, cs_id, team_id, player_id)
    return {
        # Totals behind every capped list, for the "150 DE 1,284" note.
        "counts": {
            "passes": int(counts.get("passes") or 0),
            "progressive_passes": int(counts.get("progressive_passes") or 0),
            "key_passes": int(counts.get("key_passes") or 0),
            "assists": int(counts.get("assists") or 0),
            "progressive_carries": db.fetch_player_carry_count(conn, cs_id, team_id, player_id),
            "touches": sum(int(row["touches"]) for row in touch_rows),
            "receptions": sum(int(row["receptions"]) for row in reception_rows),
        },
        "passes": [pass_row(row) for row in db.fetch_player_recent_passes(
            conn, cs_id, team_id, player_id, PLAYER_PASS_MAP_MAX)],
        "progressive_passes": [pass_row(row) for row in db.fetch_player_progressive_passes(
            conn, cs_id, team_id, player_id, PLAYER_PROGRESSIVE_MAX)],
        "key_passes": [pass_row(row) for row in db.fetch_player_key_passes(
            conn, cs_id, team_id, player_id)],
        "carries": [
            [r_coord(row["x"]), r_coord(row["y"]), r_coord(row["end_x"]),
             r_coord(row["end_y"]), 1 if row["completed"] else 0]
            for row in db.fetch_player_carries(conn, cs_id, team_id, player_id,
                                               PLAYER_PROGRESSIVE_MAX)
        ],
        "touch_zones": _zones(touch_rows, "touches"),
        "reception_zones": _zones(reception_rows, "receptions"),
        "progressive_receptions": [
            [r_coord(row["rx_x"]), r_coord(row["rx_y"]), 1 if row["rx_in_box"] else 0]
            for row in db.fetch_player_progressive_receptions(
                conn, cs_id, team_id, player_id, RECEPTION_MAX_GAP_M)
        ],
        "shots": _shots(db.fetch_player_shots(conn, cs_id, team_id, player_id)),
        "defence": _defence(db.fetch_player_defence(conn, cs_id, team_id, player_id)),
    }


# ---------------------------------------------------------------------------
# sequences (§5)
# ---------------------------------------------------------------------------

def sequence_entry(row: dict[str, Any], chain: list[dict[str, Any]],
                   player_id: int) -> dict[str, Any]:
    """One row of the top-10 list, with its trace.

    The player's FIRST touch of the chain is forced into the simplified
    polyline as a waypoint, so the gold dot the site draws on it always sits
    on the line (README-PLAYER §5). `player_touch_index` is its index in
    `points`, or null if none of his events in the chain has a coordinate.
    """
    points = _sequence_points(chain)
    touch = next((i for i, e in enumerate(chain) if e["player_id"] == player_id), None)
    simplified = simplify(points, SEQUENCE_MAX_WAYPOINTS,
                          keep=(touch,) if touch is not None else ())
    touch_index = None
    if touch is not None:
        # simplify returns the same list objects, so identity finds the point
        # even when two waypoints share a coordinate.
        touch_index = next(j for j, p in enumerate(simplified) if p is points[touch])

    return {
        # Stable across gold rebuilds, unlike sequence_id (CLAUDE.md traps).
        "id": int(row["start_event_id"]),
        "match_id": int(row["match_id"]),
        "matchday": as_int(row["matchday"]),
        "opponent_abbr": row["opponent_abbr"],
        "is_home": bool(row["is_home"]),
        "period": as_int(row["period"]),
        "minute": as_int(row["start_minute"]),
        "second": as_int(row["start_second"]),
        # The match page's row title, by the same function (§7.6).
        "kind": sequence_kind(row, chain),
        "outcome": row["outcome"],
        "events": as_int(row["event_count"]),
        "passes": as_int(row["pass_count"]),
        "player_touches": as_int(row["player_touches"]),
        "xt": r_value(row["xt"]),
        "vaep": r_value(row["vaep"]),
        "ends_in_shot": int(row["shot_count"] or 0) > 0,
        "points": simplified,
        "player_touch_index": touch_index,
    }


def _sequences(conn, cs_id: int, team_id: int, player_id: int) -> dict[str, Any]:
    agg = db.fetch_player_sequence_aggregates(
        conn, cs_id, team_id, player_id, SEQUENCE_MIN_EVENTS) or {}
    tops = {
        metric: db.fetch_player_top_sequences(
            conn, cs_id, team_id, player_id, SEQUENCE_MIN_EVENTS, metric,
            PLAYER_TOP_SEQUENCES)
        for metric in ("xt", "vaep")
    }
    ids = sorted({row["sequence_id"] for rows in tops.values() for row in rows})
    by_sequence: dict[str, list[dict[str, Any]]] = {}
    for event in db.fetch_player_sequence_events(conn, team_id, ids):
        by_sequence.setdefault(event["sequence_id"], []).append(event)

    def entries(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [sequence_entry(row, by_sequence[row["sequence_id"]], player_id)
                for row in rows if by_sequence.get(row["sequence_id"])]

    n = int(agg.get("sequences") or 0)
    return {
        # Over EVERY chain with the player and 3+ events, not the top 10.
        "aggregates": {
            "sequences": n,
            "xt": r_value(agg.get("xt")) if n else None,
            "ends_in_shot": int(agg.get("ends_in_shot") or 0),
            "passes_per_sequence": r_rate(agg.get("passes_per_sequence")) if n else None,
        },
        "min_events": SEQUENCE_MIN_EVENTS,
        "top_xt": entries(tops["xt"]),
        "top_vaep": entries(tops["vaep"]),
    }


# ---------------------------------------------------------------------------

def build(conn, writer: Writer, team_cfg: TeamConfig, season: SeasonMeta,
          squad_rows: list[dict[str, Any]], kpi_rows: list[dict[str, Any]],
          percentiles: dict[tuple[int, int], dict[str, Any]],
          team: dict[str, Any], through_matchday: int | None,
          generated_at: datetime) -> None:
    """Write one file per row of `squad_rows` (the rows squad.json shipped)."""
    cs_id = season.competition_season_id
    team_id = team_cfg.team_id
    team_matches = max((int(row["team_matches"]) for row in squad_rows), default=0)
    gate = min_minutes_for(team_matches)

    for row in squad_rows:
        player_id = int(row["player_id"])
        role = row["role_group"] or row["primary_position_group"]
        if role not in PLAYER_KPIS:
            raise KeyError(f"player {player_id}: no KPI set for role {role!r}")

        payload = {
            "team": {
                "slug": team_cfg.slug,
                "team_id": team_id,
                "short_name": team["short_name"],
                "abbr": team["abbreviation"],
            },
            "season": {
                "slug": season.slug,
                "label": season.label,
                "competition_name": season.competition_name,
                # "HASTA J7" in the header: the club's last matchday played.
                "through_matchday": through_matchday,
            },
            "player": _player_block(row, season),
            "kpis": _kpis(role, player_id, kpi_rows, gate),
            "maps": _maps(conn, cs_id, team_id, player_id),
            "percentiles": percentiles.get((player_id, team_id)),
            "sequences": _sequences(conn, cs_id, team_id, player_id),
        }
        writer.write(
            f"teams/{team_cfg.slug}/{season.slug}/players/{player_id}.json",
            payload, generated_at,
        )
