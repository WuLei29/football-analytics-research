"""
overview.py
-----------
Writes `teams/{team}/{season}/overview.json`: screen 01 minus the league table
and the profile cards (those live in league/{season}/table.json), plus the full
per-match list the plain match-list page reads.

Spec: md/WEB_DATA.md §6.

Reads: gold.team_season_stats, gold.team_match_stats, gold.player_season_stats,
silver.teams. Writes: teams/{team}/{season}/overview.json.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from . import db
from .config import KPIS, LEADER_METRICS, LEADER_ROWS, Kpi, TeamConfig
from .io import Writer, as_int, r, r_rate, r_value, r_xg
from .seasons import SeasonMeta

ROLLING_WINDOW = 5


# ---------------------------------------------------------------------------
# KPI strip (§6)
# ---------------------------------------------------------------------------

def rank_of(values: list[float], value: float, higher_is_better: bool) -> int:
    """1-based rank among the clubs of the season, ties sharing a rank.

    PPDA is the metric that makes this a parameter rather than a constant:
    fewer opponent passes per defensive action means more pressing, so it
    ranks ascending. Hard-coding "higher is better" would silently invert its
    chip on screen 01.
    """
    if higher_is_better:
        better = sum(1 for x in values if x > value)
    else:
        better = sum(1 for x in values if x < value)
    return better + 1


def _secondary(kpi: Kpi, season_row: dict[str, Any]) -> dict[str, Any] | None:
    """The italic note under a KPI value, as a key plus a number."""
    if kpi.secondary is None:
        return None

    played = season_row["matches_played"] or 0
    if kpi.secondary == "points_per_match":
        value = season_row["points_per_match"]
    elif kpi.secondary == "xg_difference_per_match":
        value = (float(season_row["xg_difference"]) / played) if played else None
    elif kpi.secondary == "set_piece_goal_share":
        goals = season_row["goals_for"] or 0
        set_piece = season_row["set_piece_goals_for"] or 0
        value = (100.0 * set_piece / goals) if goals else None
    else:
        raise KeyError(f"no rule for KPI secondary {kpi.secondary!r}")

    return {"key": kpi.secondary, "value": r_rate(value)}


def _kpis(conn, cs_id: int, team_id: int, season_row: dict[str, Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for kpi in KPIS:
        mine = season_row[kpi.key]
        # gold stores possession as a 0-1 fraction; the card says "%".
        value = None if mine is None else (
            float(mine) * 100 if kpi.scale == "pct" else float(mine)
        )

        if kpi.rank_from:
            # Points reads the standings directly rather than ranking itself,
            # so the chip agrees with the table below it on clubs level on
            # points but separated by goal difference.
            rank = as_int(season_row[kpi.rank_from])
            peer_n = len(db.fetch_kpi_column(conn, cs_id, kpi.rank_from))
        else:
            peers = db.fetch_kpi_column(conn, cs_id, kpi.key)
            values = [float(p["value"]) for p in peers if p["value"] is not None]
            rank = (None if mine is None
                    else rank_of(values, float(mine), kpi.higher_is_better))
            peer_n = len(values)

        out.append({
            "key": kpi.key,
            "value": r(value, kpi.decimals),
            "unit": kpi.unit,
            "rank": rank,
            "peer_n": peer_n,
            "secondary": _secondary(kpi, season_row),
        })
    return out


# ---------------------------------------------------------------------------
# Matches and the two rolling charts (§6)
# ---------------------------------------------------------------------------

def _match_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "match_id": as_int(row["match_id"]),
        "matchday": as_int(row["matchday"]),
        "date": row["match_date"],
        "is_home": bool(row["is_home"]),
        "opponent_team_id": as_int(row["opponent_team_id"]),
        "opponent_name": row["opponent_name"],
        "opponent_short_name": row["opponent_short_name"],
        "opponent_abbr": row["opponent_abbr"],
        "result": row["result"],
        "goals_for": as_int(row["goals_for"]),
        "goals_against": as_int(row["goals_against"]),
        "xg_for": r_xg(row["xg_for"]),
        "xg_against": r_xg(row["xg_against"]),
        "xt_for": r_value(row["xt_for"]),
        "xt_against": r_value(row["xt_against"]),
    }


def rolling(matches: list[dict[str, Any]], window: int = ROLLING_WINDOW
            ) -> list[dict[str, Any]]:
    """5-match trailing mean of xGD and xTD, one point per matchday.

    Empty until the club has played `window` matches, which is the correct
    empty state for the first month of a season rather than a chart that
    starts with a single noisy point.
    """
    points: list[dict[str, Any]] = []
    for i in range(window - 1, len(matches)):
        chunk = matches[i - window + 1: i + 1]
        xgd = sum(float(m["xg_for"] or 0) - float(m["xg_against"] or 0) for m in chunk)
        xtd = sum(float(m["xt_for"] or 0) - float(m["xt_against"] or 0) for m in chunk)
        points.append({
            "matchday": matches[i]["matchday"],
            "xg_difference": r_value(xgd / window),
            "xt_difference": r_value(xtd / window),
        })
    return points


# ---------------------------------------------------------------------------
# Player leaders (§6)
# ---------------------------------------------------------------------------

def _leaders(conn, cs_id: int, team_id: int) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for metric, unit in LEADER_METRICS:
        rows = db.fetch_leaders(conn, cs_id, team_id, metric, LEADER_ROWS)
        out.append({
            "key": metric,
            "unit": unit,
            "rows": [
                {
                    "player_id": as_int(row["player_id"]),
                    "name": row["player_name"],
                    "shirt_number": as_int(row["shirt_number"]),
                    "position_group": row["primary_position_group"],
                    # Counts stay integers, per-90 rates keep two decimals.
                    "value": r(row["value"], 0 if float(row["value"]).is_integer() else 2),
                }
                for row in rows
            ],
        })
    return out


# ---------------------------------------------------------------------------

def build(conn, writer: Writer, team_cfg: TeamConfig, season: SeasonMeta,
          generated_at: datetime) -> None:
    cs_id = season.competition_season_id
    team = db.fetch_team(conn, team_cfg.team_id)
    season_row = db.fetch_team_season(conn, cs_id, team_cfg.team_id)
    matches = [_match_row(m) for m in db.fetch_team_matches(conn, cs_id, team_cfg.team_id)]

    played = int(season_row["matches_played"])

    payload = {
        "team": {
            "slug": team_cfg.slug,
            "team_id": team_cfg.team_id,
            "short_name": team["short_name"],
            "abbr": team["abbreviation"],
        },
        "season": {
            "slug": season.slug,
            "label": season.label,
            "matchdays_scheduled": season.matchdays_scheduled,
            "matchdays_complete": season.matchdays_complete,
            "matches_played": played,
            "is_final": bool(season.matchdays_scheduled)
                        and played >= season.matchdays_scheduled,
        },
        "record": {
            "won": as_int(season_row["wins"]),
            "drawn": as_int(season_row["draws"]),
            "lost": as_int(season_row["losses"]),
            "goals_for": as_int(season_row["goals_for"]),
            "goals_against": as_int(season_row["goals_against"]),
            "points": as_int(season_row["points"]),
            "league_position": as_int(season_row["league_position"]),
            "points_per_match": r_rate(season_row["points_per_match"]),
            "form_last_5": season_row["form_last_5"],
        },
        "kpis": _kpis(conn, cs_id, team_cfg.team_id, season_row),
        # The only place per-match rows live. The form strip is its last 10 and
        # the match-list page is all of it; neither is duplicated here.
        "matches": matches,
        "rolling": {"window": ROLLING_WINDOW, "points": rolling(matches)},
        "leaders": _leaders(conn, cs_id, team_cfg.team_id),
    }
    writer.write(
        f"teams/{team_cfg.slug}/{season.slug}/overview.json", payload, generated_at
    )
