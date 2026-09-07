"""
manifest.py
-----------
Writes `manifest.json`: the only file the site loads before it knows anything.
Everything else is addressed from it, and `generateStaticParams` reads it to
decide which season pages to pre-render.

Spec: md/WEB_DATA.md §4.

Reads: silver.competition_seasons/competitions/seasons/matches/teams,
gold.team_season_stats. Writes: manifest.json.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from . import db
from .config import EXPORT_TEAMS
from .io import Writer
from .seasons import SeasonMeta


def _team_seasons(conn, team_id: int, seasons: list[SeasonMeta]) -> list[dict[str, Any]]:
    """Per-season coverage for one club, in manifest order (newest first).

    `is_final` is this club's own season being complete, which is what the
    masthead's "data through MD n" copy needs. It is not `league_complete`
    (§3.5): a club can finish while another still has a postponed match.
    """
    out: list[dict[str, Any]] = []
    for season in seasons:
        row = db.fetch_team_season(conn, season.competition_season_id, team_id)
        played = int(row["matches_played"])
        out.append({
            "slug": season.slug,
            "matches_played": played,
            "is_final": bool(season.matchdays_scheduled)
                        and played >= season.matchdays_scheduled,
        })
    return out


def build(conn, writer: Writer, seasons: list[SeasonMeta], generated_at: datetime) -> None:
    teams: list[dict[str, Any]] = []
    for cfg in EXPORT_TEAMS:
        team = db.fetch_team(conn, cfg.team_id)
        teams.append({
            "slug": cfg.slug,
            "team_id": cfg.team_id,
            "name": team["name"],
            "short_name": team["short_name"],
            "abbreviation": team["abbreviation"],
            "city": team["city"],
            "seasons": _team_seasons(conn, cfg.team_id, seasons),
        })

    payload = {
        # The methodology page carries the prose; this is the key it renders.
        "source": {"provider": "Opta", "metrics_note": "derived_by_author"},
        "default": {"team": teams[0]["slug"], "season": seasons[0].slug},
        "seasons": [s.to_json() for s in seasons],
        "teams": teams,
    }
    writer.write("manifest.json", payload, generated_at)
