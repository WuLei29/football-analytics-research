"""
squad.py
--------
Writes `teams/{team}/{season}/squad.json`: every player who has played for the
club this season, in tab-strip order. Screen 03 reads it for the player tab
strip and for `generateStaticParams` (one page per row); screen 01 reads it to
know which leader rows can link to a player page.

Spec: md/WEB_DATA.md §8, README-PLAYER §2.

Reads: gold.player_season_stats, silver.players.
Writes: teams/{team}/{season}/squad.json.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from . import db
from .config import SQUAD_ROLE_ORDER, TeamConfig, min_minutes_for
from .io import Writer, as_int
from .seasons import SeasonMeta


def display_name(row: dict[str, Any]) -> dict[str, str]:
    """The two lines of the name block (README-PLAYER §1).

    Opta's `known_name` wins when set ("Pedri", "Puado"), split on its last
    space so a two-word known name still has a surname line; otherwise the
    short given name and surname. `full` is what a tooltip or a page title
    prints.
    """
    known = (row.get("known_name") or "").strip()
    if known:
        first, _, last = known.rpartition(" ")
        return {"first": first, "last": last, "full": known}
    first = (row.get("short_first_name") or row.get("first_name") or "").strip()
    last = (row.get("short_last_name") or row.get("last_name") or "").strip()
    full = (row.get("match_name") or f"{first} {last}").strip()
    return {"first": first, "last": last or full, "full": full}


def _role(row: dict[str, Any]) -> str | None:
    return row["role_group"] or row["primary_position_group"]


def squad_order(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """GK -> CB -> RB -> LB -> MID -> AM -> W -> ST, then minutes, then id."""
    def key(row: dict[str, Any]):
        role = _role(row)
        rank = SQUAD_ROLE_ORDER.index(role) if role in SQUAD_ROLE_ORDER else len(SQUAD_ROLE_ORDER)
        return (rank, -int(row["minutes_played"]), int(row["player_id"]))
    return sorted(rows, key=key)


def squad_entry(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "player_id": int(row["player_id"]),
        "name": display_name(row),
        "shirt_number": as_int(row["shirt_number"]),
        "position": row["primary_position"],
        "position_group": row["primary_position_group"],
        "role": _role(row),
        "matches": as_int(row["matches_played"]),
        "starts": as_int(row["matches_started"]),
        "minutes": as_int(row["minutes_played"]),
    }


def build(conn, writer: Writer, team_cfg: TeamConfig, season: SeasonMeta,
          generated_at: datetime) -> list[dict[str, Any]]:
    """Write squad.json and return the rows, so the player step reuses them."""
    rows = squad_order(db.fetch_squad(conn, season.competition_season_id,
                                      team_cfg.team_id))
    team_matches = max((int(r["team_matches"]) for r in rows), default=0)
    payload = {
        "team": team_cfg.slug,
        "season": season.slug,
        # The percentile gate of §3.6, so the squad table can grey out the
        # players who are not ranked yet.
        "min_minutes": round(min_minutes_for(team_matches)),
        "players": [squad_entry(r) for r in rows],
    }
    writer.write(f"teams/{team_cfg.slug}/{season.slug}/squad.json", payload,
                 generated_at)
    return rows
