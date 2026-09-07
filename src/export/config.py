"""
config.py
---------
Everything the export needs to know that is not in the database: which teams
and seasons to publish, their slugs, the metric registries the design's blocks
render, and the percentile gate.

Spec: md/WEB_DATA.md §1, §3.6, §5.1, §6, §7.2.

Adding a club to the site is one entry in EXPORT_TEAMS. Nothing else here or
in the exporters is team-specific.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUT = PROJECT_ROOT / "web" / "public" / "data"

# Bumped when a field is removed or retyped in any published file (§2).
SCHEMA_VERSION = 1


# ---------------------------------------------------------------------------
# Scope (§1)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class TeamConfig:
    slug: str
    team_id: int


# The clubs whose pages are published. Order is the manifest's order.
EXPORT_TEAMS: list[TeamConfig] = [
    TeamConfig(slug="espanyol", team_id=8),
]

# competition_season_id -> published, newest first. 2024/25 (id 3) is
# deliberately absent: only 47 of its 380 league matches are loaded, so its
# table, ranks and percentiles are wrong (WEB_PLAN.md §9.4 A).
EXPORT_SEASONS: list[int] = [9, 1]


# ---------------------------------------------------------------------------
# Percentile gate (§3.6)
# ---------------------------------------------------------------------------

# A player enters the percentile peer set once they have played this share of
# the minutes their club has had available so far:
#
#     minutes_played >= MIN_MINUTES_SHARE * team_matches * 90
#
# gold.player_season_percentiles uses a flat 450-minute floor instead, which is
# empty for the first ~13 matchdays of every season. The site's numbers are the
# export's and will differ from gold's; gold stays the authority in Postgres.
MIN_MINUTES_SHARE = 0.25
MINUTES_PER_MATCH = 90


def min_minutes_for(team_matches: int) -> float:
    """The §3.6 eligibility floor for a club that has played `team_matches`."""
    return MIN_MINUTES_SHARE * team_matches * MINUTES_PER_MATCH


# ---------------------------------------------------------------------------
# Team profile: the 24 metrics of screen 01 block 5 (§5.1)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ProfileMetric:
    key: str                # also the column name on gold.team_season_stats
    card: str               # defensive | possession | progression | finishing
    kind: str               # "count" -> divide by matches_played; "rate" -> as is
    # gold stores every share as a 0-1 fraction (possession_pct 0.68 == 68%).
    # "pct" multiplies by 100 for display; "raw" is a real ratio like
    # xg_per_shot, which must not be scaled.
    scale: str = "raw"
    decimals: int = 1
    invert: bool = False    # percentile reversed (lower value is better)


PROFILE_METRICS: list[ProfileMetric] = [
    # -- Defensive ----------------------------------------------------------
    ProfileMetric("duels_won",                    "defensive",   "count"),
    ProfileMetric("aerials_won",                  "defensive",   "count"),
    ProfileMetric("aerial_win_rate",              "defensive",   "rate", scale="pct"),
    ProfileMetric("tackles_won",                  "defensive",   "count"),
    ProfileMetric("tackle_success_rate",          "defensive",   "rate", scale="pct"),
    ProfileMetric("fouls_committed",              "defensive",   "count", invert=True),
    # -- Possession ---------------------------------------------------------
    ProfileMetric("possession_pct",               "possession",  "rate", scale="pct"),
    ProfileMetric("pass_share_def_third",         "possession",  "rate", scale="pct"),
    ProfileMetric("pass_share_mid_third",         "possession",  "rate", scale="pct"),
    ProfileMetric("pass_share_att_third",         "possession",  "rate", scale="pct"),
    ProfileMetric("touches_in_box",               "possession",  "count"),
    ProfileMetric("field_tilt",                   "possession",  "rate", scale="pct"),
    # -- Progression --------------------------------------------------------
    ProfileMetric("progressive_passes",           "progression", "count"),
    ProfileMetric("progressive_passes_completed", "progression", "count"),
    ProfileMetric("crosses",                      "progression", "count"),
    ProfileMetric("progressive_carries",          "progression", "count"),
    ProfileMetric("take_ons",                     "progression", "count"),
    ProfileMetric("take_on_success_rate",         "progression", "rate", scale="pct"),
    # -- Finishing ----------------------------------------------------------
    ProfileMetric("shot_accuracy",                "finishing",   "rate", scale="pct"),
    # npxg_for is a count of expected goals, so it is per-match like any count.
    ProfileMetric("npxg_for",                     "finishing",   "count", decimals=2),
    # xg_per_shot is already a ratio on gold.team_season_stats -- dividing it
    # by matches_played would be meaningless.
    ProfileMetric("xg_per_shot",                  "finishing",   "rate",  decimals=3),
    ProfileMetric("xg_open_play",                 "finishing",   "count", decimals=2),
    ProfileMetric("xg_set_piece",                 "finishing",   "count", decimals=2),
    ProfileMetric("xg_fast_break",                "finishing",   "count", decimals=2),
]

PROFILE_CARDS = ["defensive", "possession", "progression", "finishing"]


# ---------------------------------------------------------------------------
# Season overview: KPI strip and player leaders (§6)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Kpi:
    key: str                   # column on gold.team_season_stats
    unit: str | None
    higher_is_better: bool     # drives the rank direction; PPDA is the odd one
    scale: str = "raw"         # "pct" for the 0-1 shares gold stores
    decimals: int = 1
    secondary: str | None = None   # key of the derived note value, if any
    # When set, the rank chip reads this column instead of ranking `key`.
    # Points needs it: ranking on points alone ties clubs that the standings
    # separate on goal difference, so the chip would disagree with the table
    # two rows below it.
    rank_from: str | None = None


KPIS: list[Kpi] = [
    Kpi("points",             "pts", True,  decimals=0,
        secondary="points_per_match", rank_from="league_position"),
    Kpi("xg_difference",      "xG",  True,  decimals=1,
        secondary="xg_difference_per_match"),
    Kpi("possession_pct",     "%",   True,  scale="pct", decimals=1),
    # Fewer opponent passes per defensive action is more pressing, so PPDA
    # ranks ascending. Getting this backwards silently inverts the rank chip.
    Kpi("ppda",               None,  False, decimals=1),
    Kpi("set_piece_goals_for", None, True,  decimals=0,
        secondary="set_piece_goal_share"),
]

# Four leader boxes, six rows each, in the design's order.
LEADER_METRICS: list[tuple[str, str | None]] = [
    ("goals", None),
    ("xt_per_90", None),
    ("progressive_passes", None),
    ("carries_into_final_third", None),
]
LEADER_ROWS = 6


# ---------------------------------------------------------------------------
# Shaping limits used by the match file (§7.1). Kept here so the exposure
# rules of WEB_PLAN.md §6 are visible in one place.
# ---------------------------------------------------------------------------

NETWORK_MIN_COMBINATIONS = 4
PROGRESSION_ARROWS = 30
DEFENSIVE_ACTIONS = 34
MATCH_SEQUENCES = 12
SEQUENCE_MAX_WAYPOINTS = 6
SEQUENCE_MIN_EVENTS = 3     # sequences.json filter (GOLD_LAYER.md §4.1.9)
