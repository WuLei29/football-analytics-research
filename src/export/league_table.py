"""
league_table.py
---------------
Writes `league/{season}/table.json`: the La Liga table (screen 01 block 3) and
the 24 team-profile metrics with their league percentiles (block 5), for all
20 clubs rather than only the published one, so a future comparison against
another side needs no new file.

Spec: md/WEB_DATA.md §5, §5.1. Per-match rule: WEB_PLAN.md §9.5.

Reads: gold.team_season_stats. Writes: league/{season}/table.json.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from . import db
from .config import EXPORT_TEAMS, PROFILE_METRICS, ProfileMetric
from .io import Writer, as_int, r, r_rate
from .seasons import SeasonMeta

log = logging.getLogger("export")

_SLUG_BY_TEAM_ID = {cfg.team_id: cfg.slug for cfg in EXPORT_TEAMS}


def profile_value(row: dict[str, Any], metric: ProfileMetric) -> float | None:
    """The value shown on the card: per match for counts, as stored for rates.

    A club with 3 matches played has to compare with one that has played 38 on
    the same footing, so every count is divided by matches_played (WEB_PLAN.md
    §9.5). Rates are already normalised -- dividing xg_per_shot by matches
    would produce a confident-looking number describing nothing.

    gold stores shares as 0-1 fractions (possession_pct 0.6799 is 68.0%), so
    `scale="pct"` metrics are multiplied by 100 here. Without that step a
    possession card reads "0.7" and every share rounds to one of a handful of
    values.
    """
    raw = row[metric.key]
    if raw is None:
        return None
    if metric.kind == "rate":
        value = float(raw) * 100 if metric.scale == "pct" else float(raw)
        return r(value, metric.decimals)
    played = row["matches_played"]
    if not played:
        return None
    return r(float(raw) / float(played), metric.decimals)


def percentile(peers: list[float], value: float, invert: bool) -> int:
    """Percentile of `value` among `peers`, always "higher is better" to read.

    Counts the clubs this one is at least as good as, so the best club scores
    100. `invert` flips the comparison for the one metric where fewer is
    better (fouls committed), exactly as the design's caption promises.
    """
    if invert:
        at_or_below = sum(1 for x in peers if x >= value)
    else:
        at_or_below = sum(1 for x in peers if x <= value)
    return round(100 * at_or_below / len(peers))


def _profile(rows: list[dict[str, Any]], season_slug: str) -> dict[str, Any]:
    # Per-match values first, then percentiles across whatever is non-null.
    values: dict[int, dict[str, float | None]] = {
        int(row["team_id"]): {m.key: profile_value(row, m) for m in PROFILE_METRICS}
        for row in rows
    }

    peers_by_metric = {
        m.key: [v[m.key] for v in values.values() if v[m.key] is not None]
        for m in PROFILE_METRICS
    }

    # A metric on which all 20 clubs score the same carries no information, and
    # percentiling it hands every club 100 -- a full green bar on a card that
    # means nothing. gold.team_season_stats.duels_won is exactly this: Opta's
    # `Challenge` event only ever carries outcome='failure' (it IS the beaten
    # player) and `50/50` never appears, so the gold expression can only count
    # zero. Ship the value, drop the percentile, and say so in the log.
    degenerate = {
        m.key for m in PROFILE_METRICS
        if len(set(peers_by_metric[m.key])) <= 1
    }
    for key in sorted(degenerate):
        log.warning(
            "season %s: team-profile metric %r has no variance across the "
            "%d clubs (every value is %s); shipping the value with a null "
            "percentile", season_slug, key, len(rows),
            peers_by_metric[key][0] if peers_by_metric[key] else "null",
        )

    out_values: list[dict[str, Any]] = []
    for team_id, metrics in values.items():
        entry: dict[str, Any] = {}
        for m in PROFILE_METRICS:
            value = metrics[m.key]
            peers = peers_by_metric[m.key]
            entry[m.key] = {
                "v": value,
                "p": None if value is None or not peers or m.key in degenerate
                     else percentile(peers, value, m.invert),
            }
        out_values.append({"team_id": team_id, "metrics": entry})

    return {
        "metrics": [
            {
                "key": m.key,
                "card": m.card,
                "kind": m.kind,
                "scale": m.scale,
                "decimals": m.decimals,
                "invert": m.invert,
            }
            for m in PROFILE_METRICS
        ],
        "values": out_values,
    }


def _table_row(row: dict[str, Any]) -> dict[str, Any]:
    team_id = int(row["team_id"])
    return {
        "position": as_int(row["league_position"]),
        "team_id": team_id,
        # Non-null only for clubs the site has pages for; the other 19 rows
        # are text, not links.
        "slug": _SLUG_BY_TEAM_ID.get(team_id),
        "name": row["team_name"],
        "short_name": row["team_short_name"],
        "abbr": row["team_abbreviation"],
        "played": as_int(row["matches_played"]),
        "won": as_int(row["wins"]),
        "drawn": as_int(row["draws"]),
        "lost": as_int(row["losses"]),
        "goals_for": as_int(row["goals_for"]),
        "goals_against": as_int(row["goals_against"]),
        "goal_difference": as_int(row["goal_difference"]),
        "xg_for": r_rate(row["xg_for"]),
        "xg_against": r_rate(row["xg_against"]),
        "xg_difference": r_rate(row["xg_difference"]),
        "points": as_int(row["points"]),
    }


def build(conn, writer: Writer, season: SeasonMeta, generated_at: datetime) -> None:
    rows = db.fetch_league_table(conn, season.competition_season_id)
    if len(rows) != season.num_teams:
        raise LookupError(
            f"season {season.slug}: gold.team_season_stats has {len(rows)} clubs, "
            f"expected {season.num_teams}"
        )

    payload = {
        "season": season.slug,
        "competition_name": season.competition_name,
        "matchdays_scheduled": season.matchdays_scheduled,
        "league_complete": season.league_complete,
        "table": [_table_row(row) for row in rows],
        "profile": _profile(rows, season.slug),
    }
    writer.write(f"league/{season.slug}/table.json", payload, generated_at)
