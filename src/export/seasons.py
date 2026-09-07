"""
seasons.py
----------
Season identity and coverage: the slug the site puts in URLs, and the four
derived coverage facts that tell a page how much of the season exists.

Spec: md/WEB_DATA.md §1.1 (slugs), §3.5 (status is derived, never read).

No SQL here; it takes what db.py fetched. `silver.competition_seasons.status`
is deliberately ignored -- it stays 'active' after a season ends, so trusting
it would label a finished season as in progress forever.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from typing import Any

from . import db
from .config import EXPORT_SEASONS

_LABEL = re.compile(r"^(\d{4})/(\d{4})$")


def season_slug(label: str) -> str:
    """'2025/2026' -> '2025-26'."""
    m = _LABEL.match(label)
    if not m:
        raise ValueError(f"unexpected season label {label!r}; expected 'YYYY/YYYY'")
    start, end = m.groups()
    return f"{start}-{end[2:]}"


def matchdays_complete(counts: dict[int, int], matches_per_matchday: int) -> int:
    """The greatest m such that every matchday 1..m is fully loaded (§3.5).

    This is the "datos hasta la jornada n" number. It is not max(matchday):
    2026/27 has one early matchday-6 fixture loaded while only matchdays 1-3
    are complete, and the copy must say 3.
    """
    complete = 0
    while counts.get(complete + 1, 0) >= matches_per_matchday:
        complete += 1
    return complete


@dataclass(frozen=True)
class SeasonMeta:
    """Everything the export knows about one published season."""

    competition_season_id: int
    slug: str
    label: str
    competition_code: str
    competition_name: str
    tier_level: int
    num_teams: int
    matchdays_scheduled: int
    matchdays_complete: int
    last_matchday_loaded: int
    league_complete: bool
    first_match_date: date | None
    through_match_date: date | None

    def to_json(self) -> dict[str, Any]:
        return {
            "slug": self.slug,
            "label": self.label,
            "competition_season_id": self.competition_season_id,
            "competition_code": self.competition_code,
            "competition_name": self.competition_name,
            "tier_level": self.tier_level,
            "num_teams": self.num_teams,
            "matchdays_scheduled": self.matchdays_scheduled,
            "matchdays_complete": self.matchdays_complete,
            "last_matchday_loaded": self.last_matchday_loaded,
            "league_complete": self.league_complete,
            "first_match_date": self.first_match_date,
            "through_match_date": self.through_match_date,
        }


def load_season(conn, cs_id: int) -> SeasonMeta:
    row = db.fetch_season(conn, cs_id)
    agg = db.fetch_season_aggregate(conn, cs_id)
    counts = db.fetch_matchday_counts(conn, cs_id)

    # num_teams is nullable on silver.competition_seasons (a documented
    # new-season gotcha), so fall back to the clubs gold actually has.
    num_teams = int(row["num_teams"] or agg["clubs"])
    scheduled = int(row["total_matchdays"] or agg["matchdays_scheduled"] or 0)

    return SeasonMeta(
        competition_season_id=cs_id,
        slug=season_slug(row["season_label"]),
        label=row["season_label"],
        competition_code=row["competition_code"],
        competition_name=row["competition_name"],
        tier_level=int(row["tier_level"]),
        num_teams=num_teams,
        matchdays_scheduled=scheduled,
        matchdays_complete=matchdays_complete(counts, num_teams // 2),
        last_matchday_loaded=max(counts) if counts else 0,
        league_complete=bool(scheduled) and int(agg["min_played"]) >= scheduled,
        first_match_date=agg["first_match_date"],
        through_match_date=agg["through_match_date"],
    )


def load_seasons(conn, only: list[int] | None = None) -> list[SeasonMeta]:
    """The published seasons, in EXPORT_SEASONS order (newest first)."""
    wanted = [cs for cs in EXPORT_SEASONS if only is None or cs in only]
    return [load_season(conn, cs_id) for cs_id in wanted]
