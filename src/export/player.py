"""
player.py
---------
Writes `teams/{team}/{season}/players/{player_id}.json`: screen 03, one file
per player in `squad.json`.

Spec: md/WEB_DATA.md §9, README-PLAYER "Data contract" (which replaces §9 for
screen 03).

Reads: silver.players, silver.events, silver.matches, gold.player_season_stats,
gold.player_match_stats, gold.pass_receptions, gold.sequences,
gold.sequence_players.
Writes: teams/{team}/{season}/players/{player_id}.json.

No gold table is created for this page: every block below is a slice of an
existing gold or silver table.

Frame: every coordinate is the player's own event, in his team's attacking
frame (§3.1), so the site draws it as shipped -- a right back's marks sit at
low y, which the site puts at the bottom of a horizontal pitch.

The match filter (16 Oct 2026). The page can narrow the profile card, the
event map and the sequences to one match or a group of matches; the
percentile cards stay season-wide. So the file is per match underneath:

  - `matches[]` -- every match the player played, in date order, with its bio
    counts and the gold.player_match_stats columns the headline KPIs are
    rebuilt from (`KPI_FORMULAS`). `build` checks that the formula over every
    match reproduces the season value, so the two cannot drift.
  - every map row carries `m`, its index into `matches[]`, and ships
    UNCAPPED: the caps (150 recent passes, 60 best progressive) are applied
    on the site after the filter (`caps`).
  - every map row also carries its minute and `s`, its chain's index into
    `sequences.index` (null when the event is in no chain the page can draw:
    an opponent's possession, or one under SEQUENCE_MIN_EVENTS). The page
    names the match and minute on hover, and a click opens the chain in the
    sequence block (16 Oct 2026).
  - `sequences.index` is every chain the player was in; the page ranks it for
    the selection and draws each chain from its match file, which carries
    the same actions (WEB_DATA §7.3). The season top lists ship in full in
    `sequences.entries`, so the default view needs no fetch.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

from . import db
from .config import (
    KPI_FORMULAS,
    PLAYER_KPIS,
    PLAYER_PASS_MAP_MAX,
    PLAYER_PROGRESSIVE_MAX,
    PLAYER_TOP_SEQUENCES,
    RECEPTION_MAX_GAP_M,
    SEQUENCE_MIN_EVENTS,
    PlayerKpi,
    TeamConfig,
    min_minutes_for,
)
from .io import Writer, as_int, r, r_coord, r_rate, r_value
from .match import _body_part, _sequences as match_sequences, _shot_outcome
from .seasons import SeasonMeta
from .squad import display_name

# Bits of the compact pass row [x, y, end_x, end_y, flags, m, xt, minute, s].
FLAG_COMPLETED, FLAG_PROGRESSIVE, FLAG_KEY_PASS, FLAG_ASSIST = 1, 2, 4, 8

# Bits of the reception row [x, y, flags, m, minute, s].
RX_PROGRESSIVE, RX_IN_BOX = 1, 2

# README-PLAYER §3b: these two are always a duel lost (the player went in and
# missed, or was dribbled past); a Tackle is won or lost by its outcome; every
# other defensive type is a ball won.
DEFENCE_ALWAYS_LOST = {"Attempted Tackle", "Challenge"}

# The bio counts that are summed over the selected matches, and the season
# column each must add up to.
BIO_SUMS = {
    "minutes": "minutes_played",
    "yellow_cards": "yellow_cards",
    "red_cards": "red_cards",
    "penalties_taken": "penalties_taken",
    "penalties_scored": "penalties_scored",
}


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
# matches (the filter's rows)
# ---------------------------------------------------------------------------

def kpi_columns(role: str) -> list[str]:
    """The gold.player_match_stats columns the role's KPIs are rebuilt from."""
    cols: set[str] = set()
    for kpi in PLAYER_KPIS[role]:
        formula = KPI_FORMULAS[kpi.key]
        cols.update(formula["num"])
        cols.update(formula.get("den") or {})
    return sorted(cols)


def _match_entry(row: dict[str, Any], columns: list[str]) -> dict[str, Any]:
    return {
        "match_id": int(row["match_id"]),
        "matchday": as_int(row["matchday"]),
        "date": row["match_date"],
        "is_home": bool(row["is_home"]),
        "opponent_abbr": row["opponent_abbr"],
        "opponent_short_name": row["opponent_short_name"],
        "goals_for": as_int(row["goals_for"]),
        "goals_against": as_int(row["goals_against"]),
        "started": bool(row["started"]),
        "subbed_off": bool(row["subbed_off"]),
        "minutes": int(row["minutes_played"]),
        "yellow_cards": int(row["yellow_cards"] or 0),
        "red_cards": int(row["red_cards"] or 0),
        "penalties_taken": int(row["penalties_taken"]),
        "penalties_scored": int(row["penalties_scored"]),
        # Unrounded beyond 4 decimals: the site sums these, and a season of
        # 2-decimal VAEP values would drift from the season figure.
        "kpi_parts": {c: round(float(row[c]), 4) for c in columns},
    }


def kpi_value(kpi: PlayerKpi, matches: list[dict[str, Any]]) -> float | None:
    """The KPI over `matches` by its KPI_FORMULAS entry, display-scaled. The
    site runs the same arithmetic (lib/data/playerFilter.ts)."""
    formula = KPI_FORMULAS[kpi.key]

    def total(terms: dict[str, float]) -> float:
        return sum(coef * m["kpi_parts"][col] for m in matches for col, coef in terms.items())

    num = total(formula["num"])
    if formula.get("per90"):
        minutes = sum(m["minutes"] for m in matches)
        value = num * 90.0 / minutes if minutes else None
    elif formula.get("den"):
        den = total(formula["den"])
        value = num / den if den else None
    else:
        value = num
    return _scaled(kpi, value)


def check_against_season(player_id: int, entries: list[dict[str, Any]],
                         kpis: list[dict[str, Any]], role: str,
                         season_row: dict[str, Any]) -> None:
    """Fail loudly when the per-match rows do not add up to the season: the
    page would show one number for "Temporada" and another for every match
    selected one by one."""
    problems: list[str] = []
    if len(entries) != int(season_row["matches_played"]):
        problems.append(f"matches {len(entries)} vs {season_row['matches_played']}")
    starts = sum(1 for m in entries if m["started"])
    if starts != int(season_row["matches_started"]):
        problems.append(f"starts {starts} vs {season_row['matches_started']}")
    for key, col in BIO_SUMS.items():
        got = sum(m[key] for m in entries)
        if got != int(season_row[col] or 0):
            problems.append(f"{key} {got} vs {season_row[col]}")
    for kpi, shipped in zip(PLAYER_KPIS[role], kpis):
        rebuilt = kpi_value(kpi, entries)
        season = shipped["value"]
        if (rebuilt is None) != (season is None) or (
                rebuilt is not None and abs(rebuilt - season) > 1.01 * 10 ** -kpi.decimals):
            problems.append(f"{kpi.key} {rebuilt} vs season {season}")
    if problems:
        raise ValueError(
            f"player {player_id}: per-match rows do not add up to the season: "
            + "; ".join(problems))


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
        formula = KPI_FORMULAS[kpi.key]
        entry: dict[str, Any] = {
            "key": kpi.key,
            "value": _scaled(kpi, raw),
            "scale": kpi.scale,
            "decimals": kpi.decimals,
            "per_90": None,
            "squad_rank": None,
            "squad_n": None,
            # How the page rebuilds the value for a selection of matches.
            "formula": {
                "num": formula["num"],
                "den": formula.get("den"),
                "per90": bool(formula.get("per90")),
            },
            "total": kpi.total,
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

def pass_row(row: dict[str, Any], m: int, s: int | None) -> list[float | int | None]:
    flags = ((FLAG_COMPLETED if row["completed"] else 0)
             | (FLAG_PROGRESSIVE if row["progressive"] else 0)
             | (FLAG_KEY_PASS if row["key_pass"] else 0)
             | (FLAG_ASSIST if row["assist"] else 0))
    return [r_coord(row["x"]), r_coord(row["y"]),
            r_coord(row["end_x"]), r_coord(row["end_y"]), flags, m,
            r_value(row["xt"]), as_int(row["minute"]), s]


def reception_row(row: dict[str, Any], m: int, s: int | None) -> list[float | int | None]:
    flags = ((RX_PROGRESSIVE if row["progressive"] else 0)
             | (RX_IN_BOX if row["rx_in_box"] else 0))
    return [r_coord(row["rx_x"]), r_coord(row["rx_y"]), flags, m,
            as_int(row["minute"]), s]


def touch_key(event_type: str) -> str:
    """`silver.events.event_type` as the site's snake_case key."""
    return event_type.lower().replace(" ", "_").replace("-", "_")


TOUCH_KEYS = [touch_key(t) for t in db.TOUCH_TYPES]


def _zone_rows(rows: list[dict[str, Any]], key: str,
               index: dict[int, int]) -> list[list[int]]:
    """[m, zone_id, count] per match and zone."""
    return [
        [index[int(row["match_id"])], int(row["zone_id"]), int(row[key])]
        for row in rows
        if row["zone_id"] is not None
    ]


def _shots(rows: list[dict[str, Any]], s) -> list[dict[str, Any]]:
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
            "s": s(row),
        }
        for index, row in enumerate(rows, start=1)
    ]


def defence_outcome(event_type: str, outcome: str | None) -> str:
    if event_type in DEFENCE_ALWAYS_LOST:
        return "lost"
    if event_type == "Tackle":
        return "won" if outcome == "success" else "lost"
    return "won"


def _defence(rows: list[dict[str, Any]], m, s) -> list[list[Any]]:
    """[type, x, y, won 0|1, minute, m, s] -- compact: a centre back has 500+.
    `type` is the page-02 key, so the chips and tooltips reuse its labels."""
    return [
        [
            row["event_type"].lower().replace(" ", "_"),
            r_coord(row["x"]),
            r_coord(row["y"]),
            1 if defence_outcome(row["event_type"], row["outcome"]) == "won" else 0,
            as_int(row["minute"]),
            m(row),
            s(row),
        ]
        for row in rows
    ]


def _maps(conn, cs_id: int, team_id: int, player_id: int,
          index: dict[int, int], chains: dict[str, int]) -> dict[str, Any]:
    def s(row: dict[str, Any]) -> int | None:
        # Only a chain the sequence block can draw: the player's own, 3+ events.
        return chains.get(row["sequence_id"])

    def m(row: dict[str, Any]) -> int:
        match_id = int(row["match_id"])
        if match_id not in index:
            raise ValueError(
                f"player {player_id}: an event of match {match_id}, which is "
                "not in his gold.player_match_stats rows")
        return index[match_id]

    return {
        # Applied on the site after the match filter.
        "caps": {"passes": PLAYER_PASS_MAP_MAX, "progressive": PLAYER_PROGRESSIVE_MAX},
        "passes": [pass_row(row, m(row), s(row)) for row in db.fetch_player_passes(
            conn, cs_id, team_id, player_id)],
        "carries": [
            [r_coord(row["x"]), r_coord(row["y"]), r_coord(row["end_x"]),
             r_coord(row["end_y"]), 1 if row["completed"] else 0, m(row),
             r_value(row["xt"]), as_int(row["minute"]), s(row)]
            for row in db.fetch_player_carries(conn, cs_id, team_id, player_id)
        ],
        "touch_zones": _zone_rows(
            db.fetch_player_touch_zones(conn, cs_id, team_id, player_id),
            "touches", index),
        # One row per touch, [x, y, t, m, minute, s]; `t` indexes `touch_types`.
        "touch_types": TOUCH_KEYS,
        "touches": [
            [r_coord(row["x"]), r_coord(row["y"]),
             TOUCH_KEYS.index(touch_key(row["event_type"])), m(row),
             as_int(row["minute"]), s(row)]
            for row in db.fetch_player_touches(conn, cs_id, team_id, player_id)
        ],
        "reception_zones": _zone_rows(
            db.fetch_player_reception_zones(conn, cs_id, team_id, player_id),
            "receptions", index),
        "receptions": [
            reception_row(row, m(row), s(row))
            for row in db.fetch_player_receptions(
                conn, cs_id, team_id, player_id, RECEPTION_MAX_GAP_M)
        ],
        "shots": _shots(db.fetch_player_shots(conn, cs_id, team_id, player_id), s),
        "defence": _defence(db.fetch_player_defence(conn, cs_id, team_id, player_id), m, s),
    }


# ---------------------------------------------------------------------------
# sequences (§5)
# ---------------------------------------------------------------------------

def top_ids(rows: list[dict[str, Any]], metric: str, limit: int) -> list[str]:
    """The `limit` best chains by `metric`, NULLs last, start_event_id the
    tiebreak (it does not renumber on a gold rebuild)."""
    ranked = sorted(
        rows,
        key=lambda row: (row[metric] is None, -float(row[metric] or 0),
                         int(row["start_event_id"])),
    )
    return [row["sequence_id"] for row in ranked[:limit]]


def _sequences(conn, team_id: int, rows: list[dict[str, Any]],
               index: dict[int, int]) -> dict[str, Any]:
    season_ids = sorted(set(top_ids(rows, "xt", PLAYER_TOP_SEQUENCES))
                        | set(top_ids(rows, "vaep", PLAYER_TOP_SEQUENCES)))
    entries = match_sequences(
        db.fetch_player_sequence_rows(conn, team_id, season_ids),
        db.fetch_player_sequence_events(conn, team_id, season_ids),
    )
    by_id = {
        row["sequence_id"]: int(row["match_id"])
        for row in rows
    }
    for entry in entries:
        # The match file's entry, word for word, minus the per-match rank.
        entry.pop("rank", None)
        entry["match_id"] = by_id[entry["sequence_id"]]

    return {
        "min_events": SEQUENCE_MIN_EVENTS,
        "top": PLAYER_TOP_SEQUENCES,
        # [m, sequence_id, xt, vaep, ends_in_shot 0|1, passes] per chain.
        "index": [
            [index[int(row["match_id"])], row["sequence_id"],
             r_value(row["xt"]), r_value(row["vaep"]),
             1 if int(row["shot_count"] or 0) > 0 else 0,
             as_int(row["pass_count"])]
            for row in rows
        ],
        "entries": entries,
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

        columns = kpi_columns(role)
        matches = [_match_entry(m, columns) for m in db.fetch_player_matches(
            conn, cs_id, team_id, player_id, columns)]
        index = {m["match_id"]: i for i, m in enumerate(matches)}
        kpis = _kpis(role, player_id, kpi_rows, gate)
        check_against_season(player_id, matches, kpis, role, row)
        # The chain index first: every map row points into it (`s`).
        chain_rows = db.fetch_player_sequence_index(conn, cs_id, team_id, player_id,
                                                    SEQUENCE_MIN_EVENTS)
        chains = {c["sequence_id"]: i for i, c in enumerate(chain_rows)}

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
            "matches": matches,
            "kpis": kpis,
            "maps": _maps(conn, cs_id, team_id, player_id, index, chains),
            "percentiles": percentiles.get((player_id, team_id)),
            "sequences": _sequences(conn, team_id, chain_rows, index),
        }
        writer.write(
            f"teams/{team_cfg.slug}/{season.slug}/players/{player_id}.json",
            payload, generated_at, compact=True,
        )
