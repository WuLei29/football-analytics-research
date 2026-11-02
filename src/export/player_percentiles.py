"""
player_percentiles.py
---------------------
The percentile block of screen 03: each player's spine (config.PLAYER_SPINE)
with a league percentile per metric, ranked among every La Liga player of the
same role who clears the site's minutes gate.

Spec: md/WEB_DATA.md §3.6 and §9, README-PLAYER §4.

Pure: `rank_season` takes rows and returns dicts, and touches no database.
`player_percentiles` is the one-call wrapper the player exporter uses.

It follows src/gold/build_player_percentiles.sql rule for rule -- role peer
sets, RB/LB -> FB and AM -> MID pooling under PEER_POOL_MIN, the pooled set
ranked on the union of its roles' metrics, the attempt floors, percent_rank,
unranked NULLs -- and differs in ONE thing, the eligibility gate:
25% of the club's minutes here, a flat 450 there. Passing
`min_minutes=lambda row: 450` reproduces gold exactly; that is the parity
check in GOLD_LAYER §9.5 G19.
"""

from __future__ import annotations

from typing import Any, Callable

from . import db
from .config import (PEER_POOL_MIN, PLAYER_KPIS, PLAYER_SPINE, PlayerKpi,
                     SpineMetric, min_minutes_for)

Row = dict[str, Any]


def _peer_group(role: str, pool_fb: bool, pool_am: bool) -> str:
    if role in ("RB", "LB") and pool_fb:
        return "FB"
    if role == "AM" and pool_am:
        return "MID"
    return role


def _percent_rank(values: list[float], value: float) -> float:
    """Postgres percent_rank(): share of the OTHER peers strictly below."""
    if len(values) <= 1:
        return 0.0
    below = sum(1 for v in values if v < value)
    return 100.0 * below / (len(values) - 1)


def peer_pooling(eligible: list[Row]) -> tuple[bool, bool]:
    """(pool_fb, pool_am): whether RB + LB rank as FB and AM inside MID,
    because a role has fewer than PEER_POOL_MIN eligible players."""
    role_n: dict[str, int] = {}
    for r in eligible:
        role_n[r["role_group"]] = role_n.get(r["role_group"], 0) + 1
    pool_fb = any(role_n.get(g, 0) < PEER_POOL_MIN for g in ("RB", "LB")
                  if g in role_n)
    pool_am = "AM" in role_n and role_n["AM"] < PEER_POOL_MIN
    return pool_fb, pool_am


def spine_columns() -> list[str]:
    """Every column the spine reads, plus the denominators of its floors are
    added by the caller from gold.percentile_metrics."""
    cols: set[str] = set()
    for cards in PLAYER_SPINE.values():
        for _, metrics in cards:
            for m in metrics:
                cols.add(m.key)
                if m.out_of:
                    cols.add(m.out_of)
    return sorted(cols)


def rank_season(
    rows: list[Row],
    metric_rows: list[Row],
    min_minutes: Callable[[Row], float],
) -> dict[tuple[int, int], dict[str, Any]]:
    """Rank one season. Returns {(player_id, team_id): result}.

    A result is {eligible, role, peer_group, pooled_fb, pooled_am,
    ranks: {metric: (percentile, peer_n)}}. A metric absent from `ranks` is
    not ranked for that player: wrong role, under its attempt floor, or NULL.
    """
    spec: dict[str, Row] = {}
    roles_of: dict[str, set[str]] = {}
    for m in metric_rows:
        spec[m["metric"]] = m
        roles_of.setdefault(m["position_group"], set()).add(m["metric"])

    eligible = [r for r in rows
                if r["role_group"] is not None
                and r["minutes_played"] >= min_minutes(r)]
    pool_fb, pool_am = peer_pooling(eligible)

    # Rule 5: a pooled set ranks on the union of its roles' metrics.
    peers: dict[str, list[Row]] = {}
    peer_metrics: dict[str, set[str]] = {}
    for r in eligible:
        pg = _peer_group(r["role_group"], pool_fb, pool_am)
        peers.setdefault(pg, []).append(r)
        peer_metrics.setdefault(pg, set()).update(roles_of.get(r["role_group"], set()))

    def value_of(r: Row, metric: str) -> float | None:
        s = spec[metric]
        if s["attempts_column"] is not None:
            attempts = r.get(s["attempts_column"])
            if attempts is None or attempts < s["min_attempts"]:
                return None
        v = r.get(metric)
        if v is None:
            return None
        v = float(v)
        return v if s["higher_is_better"] else -v

    ranks: dict[tuple[int, int], dict[str, tuple[float, int]]] = {}
    for pg, members in peers.items():
        for metric in sorted(peer_metrics[pg]):
            scored = [(r, value_of(r, metric)) for r in members]
            scored = [(r, v) for r, v in scored if v is not None]
            values = [v for _, v in scored]
            for r, v in scored:
                if metric in roles_of.get(r["role_group"], set()):
                    key = (r["player_id"], r["team_id"])
                    ranks.setdefault(key, {})[metric] = (
                        _percent_rank(values, v), len(values))

    out: dict[tuple[int, int], dict[str, Any]] = {}
    eligible_keys = {(r["player_id"], r["team_id"]) for r in eligible}
    for r in rows:
        key = (r["player_id"], r["team_id"])
        role = r["role_group"]
        out[key] = {
            "eligible": key in eligible_keys,
            "role": role,
            "peer_group": _peer_group(role, pool_fb, pool_am) if role else None,
            "pooled_fb": role in ("RB", "LB") and pool_fb,
            "pooled_am": role == "AM" and pool_am,
            "ranks": ranks.get(key, {}),
        }
    return out


def _display(row: Row, m: SpineMetric) -> float | None:
    v = row.get(m.key)
    if v is None:
        return None
    v = float(v) * 100 if m.scale == "pct" else float(v)
    return round(v, m.decimals)


def spine_block(row: Row, result: dict[str, Any], min_minutes: float,
                higher_is_better: dict[str, bool]) -> dict[str, Any]:
    """The `percentiles` object of players/{id}.json (WEB_DATA §9)."""
    cards = []
    for card_key, metrics in PLAYER_SPINE[result["role"]]:
        entries = []
        for m in metrics:
            rank = result["ranks"].get(m.key) if result["eligible"] else None
            entry: dict[str, Any] = {
                "key": m.key,
                "value": _display(row, m),
                "percentile": None if rank is None else round(rank[0]),
                "higher_is_better": higher_is_better.get(m.key, True),
                # How the site prints the value: "%" on a pct, and the
                # precision PLAYER_SPINE gives it.
                "scale": m.scale,
                "decimals": m.decimals,
            }
            if m.out_of:
                entry["out_of"] = row.get(m.out_of)
            entries.append(entry)
        cards.append({"key": card_key, "metrics": entries})
    peer_ns = [n for _, n in result["ranks"].values()]
    return {
        "eligible": result["eligible"],
        "role": result["role"],
        "peer_group": result["peer_group"],
        # The largest peer set behind any bar: a rate with an attempt floor
        # can rank fewer players than the role has.
        "peer_n": max(peer_ns) if peer_ns else None,
        "pooled_fb": result["pooled_fb"],
        "pooled_am": result["pooled_am"],
        "min_minutes": round(min_minutes),
        "cards": cards if result["eligible"] else [],
    }


def player_percentiles(conn, cs_id: int) -> dict[tuple[int, int], dict[str, Any]]:
    """Percentile blocks for every player-club row of a season, keyed
    (player_id, team_id). The player exporter picks its own club's rows."""
    metric_rows = db.fetch_percentile_metrics(conn)
    floors = {m["attempts_column"] for m in metric_rows if m["attempts_column"]}
    columns = sorted(set(spine_columns()) | floors
                     | {m["metric"] for m in metric_rows})
    rows = db.fetch_player_season_rows(conn, cs_id, columns)
    gate = lambda r: min_minutes_for(r["team_matches"])  # noqa: E731
    results = rank_season(rows, metric_rows, gate)
    direction = {m["metric"]: m["higher_is_better"] for m in metric_rows}
    return {
        (r["player_id"], r["team_id"]): spine_block(
            r, results[(r["player_id"], r["team_id"])], gate(r), direction)
        for r in rows if r["role_group"] is not None
    }

# ---------------------------------------------------------------------------
# KPI verdict bands (WEB_DATA §9, 2 Nov 2026)
# ---------------------------------------------------------------------------
#
# The profile card colours each headline KPI good / middling / bad, the
# percentile cards' tiers (good >= 70, bad < 45). Its value can be rebuilt for
# any selection of matches on the page, so the export ships the two CUT-OFFS
# in display units, not a percentile: the page compares any value with them.
# Peers: every La Liga player of the same peer group past the site's minutes
# gate, pooled exactly as rank_season pools. A season total is compared per
# 90 (a striker with 1,200 minutes against one with 3,000), a rate as is.
# Every headline KPI is higher-is-better.

GOOD_FROM_PCT = 70
BAD_BELOW_PCT = 45


def kpi_measure(kpi: PlayerKpi, row: Row) -> float | None:
    """What a KPI is compared on, in display units: per 90 for a total."""
    raw = row.get(kpi.key)
    if raw is None:
        return None
    if kpi.total:
        minutes = row["minutes_played"]
        return float(raw) * 90.0 / minutes if minutes else None
    return float(raw) * 100 if kpi.scale == "pct" else float(raw)


def _cut(values: list[float], pct: float) -> float | None:
    """The smallest value whose percent_rank reaches `pct` (values sorted)."""
    n = len(values)
    for v in values:
        # Sorted, so the first index of v is the count strictly below it.
        if 100.0 * values.index(v) / (n - 1) >= pct:
            return v
    return None


def kpi_bands(rows: list[Row], min_minutes: Callable[[Row], float]
              ) -> dict[str, Any]:
    """{"pool_fb", "pool_am", "bands": {(peer_group, key): band}} for a season.

    A band is {peer_group, peer_n, bad_below, good_from}; good_from is None
    when no value reaches the 70th percent_rank (ties at the top).
    """
    eligible = [r for r in rows
                if r["role_group"] is not None
                and r["minutes_played"] >= min_minutes(r)]
    pool_fb, pool_am = peer_pooling(eligible)
    groups: dict[str, list[Row]] = {}
    for r in eligible:
        groups.setdefault(_peer_group(r["role_group"], pool_fb, pool_am), []).append(r)

    kpis = {k.key: k for ks in PLAYER_KPIS.values() for k in ks}
    bands: dict[tuple[str, str], dict[str, Any]] = {}
    for pg in sorted(groups):
        for key in sorted(kpis):
            values = sorted(v for v in (kpi_measure(kpis[key], r) for r in groups[pg])
                            if v is not None)
            if len(values) < 2:
                continue
            bands[(pg, key)] = {
                "peer_group": pg,
                "peer_n": len(values),
                "bad_below": _round(_cut(values, BAD_BELOW_PCT)),
                "good_from": _round(_cut(values, GOOD_FROM_PCT)),
            }
    return {"pool_fb": pool_fb, "pool_am": pool_am, "bands": bands}


def _round(v: float | None) -> float | None:
    return None if v is None else round(v, 4)


def band_for(bands: dict[str, Any], role: str, key: str) -> dict[str, Any] | None:
    pg = _peer_group(role, bands["pool_fb"], bands["pool_am"])
    return bands["bands"].get((pg, key))


def player_kpi_bands(conn, cs_id: int) -> dict[str, Any]:
    """kpi_bands for one season, read from gold.player_season_stats."""
    keys = sorted({k.key for ks in PLAYER_KPIS.values() for k in ks})
    rows = db.fetch_player_season_rows(conn, cs_id, keys)
    return kpi_bands(rows, lambda r: min_minutes_for(r["team_matches"]))

