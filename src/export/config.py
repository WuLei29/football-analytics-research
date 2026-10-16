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


# A role with fewer eligible players than this is ranked inside a larger one:
# RB + LB -> FB, AM -> MID. Same number as build_player_percentiles.sql.
PEER_POOL_MIN = 20


# ---------------------------------------------------------------------------
# Player percentile spine: screen 03 block 4 (§9, README-PLAYER §4)
# ---------------------------------------------------------------------------
#
# Four cards of six metrics per ROLE (player_season_stats.role_group: the
# formation-slot group, with high-playing midfielders split out as AM). The
# keys are columns on gold.player_season_stats.
#
# WHICH rows get a bar is not decided here: a metric is ranked for a role iff
# gold.percentile_metrics lists it for that role, and its direction and
# attempt floor come from the same row. So Postgres and the site agree on
# what is ranked; they differ only in the minutes gate (§3.6). A row that is
# not ranked ships its value with `percentile: null` and renders as "—".

@dataclass(frozen=True)
class SpineMetric:
    key: str                    # column on gold.player_season_stats
    # "pct" for the 0-1 rates gold stores (shipped x100); "raw" otherwise.
    scale: str = "raw"
    decimals: int = 2
    # Shown as "value / out_of" without a bar (penalties saved of faced).
    out_of: str | None = None


def _m(key: str, scale: str = "raw", decimals: int | None = None,
       out_of: str | None = None) -> SpineMetric:
    if decimals is None:
        decimals = 1 if scale == "pct" else 2
    return SpineMetric(key, scale, decimals, out_of)


# (card key, metrics). Card keys are translated in web/lib/labels.ts.
PLAYER_SPINE: dict[str, list[tuple[str, list[SpineMetric]]]] = {
    "GK": [
        ("goalkeeping", [_m("save_pct", "pct"), _m("goals_prevented_per_90"),
                         _m("saves_per_90"), _m("goals_conceded_per_90"),
                         _m("clean_sheet_rate", "pct"), _m("xg_faced_per_90")]),
        ("area",        [_m("claims_per_90"), _m("claim_rate", "pct"),
                         _m("punches_per_90"), _m("keeper_sweeper_actions_per_90"),
                         _m("sweeper_avg_x", decimals=1),
                         _m("padj_clearances_per_90")]),
        ("distribution", [_m("passes_per_90", decimals=1),
                          _m("gk_pass_completion_rate", "pct"),
                          _m("long_pass_completion_rate", "pct"),
                          _m("gk_long_ball_pct", "pct"),
                          _m("progressive_passes_per_90"),
                          _m("passes_received_per_90", decimals=1)]),
        ("value",       [_m("vaep_per_90", decimals=3),
                         _m("vaep_defensive_per_90", decimals=3),
                         _m("vaep_offensive_per_90", decimals=3),
                         _m("errors_per_90"),
                         _m("penalties_saved", decimals=0, out_of="penalties_faced")]),
    ],
    "CB": [
        ("defending",   [_m("padj_interceptions_per_90"), _m("padj_clearances_per_90"),
                         _m("shots_blocked_per_90"), _m("padj_blocked_passes_per_90"),
                         _m("padj_ball_recoveries_per_90"), _m("errors_per_90")]),
        ("duels",       [_m("padj_tackles_per_90"), _m("ground_duel_win_rate", "pct"),
                         _m("dribbled_past_per_90"), _m("aerials_won_per_90"),
                         _m("aerial_win_rate", "pct"), _m("fouls_committed_per_90")]),
        ("passing",     [_m("passes_per_90", decimals=1),
                         _m("pass_completion_rate", "pct"),
                         _m("long_balls_per_90"), _m("long_ball_accuracy", "pct"),
                         _m("progressive_passes_per_90"),
                         _m("passes_into_final_third_per_90")]),
        ("impact",      [_m("vaep_per_90", decimals=3),
                         _m("vaep_defensive_per_90", decimals=3),
                         _m("vaep_offensive_per_90", decimals=3),
                         _m("progressive_carries_per_90"), _m("headed_shots_per_90"),
                         _m("aerials_won_att_third_per_90")]),
    ],
    "FB": [
        ("defending",   [_m("padj_interceptions_per_90"), _m("padj_ball_recoveries_per_90"),
                         _m("padj_clearances_per_90"), _m("shots_blocked_per_90"),
                         _m("def_actions_final_third_per_90"),
                         _m("vaep_defensive_per_90", decimals=3)]),
        ("duels",       [_m("padj_tackles_per_90"), _m("ground_duel_win_rate", "pct"),
                         _m("dribbled_past_per_90"), _m("aerials_won_per_90"),
                         _m("aerial_win_rate", "pct"), _m("fouls_committed_per_90")]),
        ("progression", [_m("pass_completion_rate", "pct"),
                         _m("progressive_passes_per_90"), _m("progressive_carries_per_90"),
                         _m("progressive_carries_into_final_third_per_90"),
                         _m("receptions_final_third_per_90"),
                         _m("take_on_success_rate", "pct")]),
        ("creation",    [_m("crosses_per_90"), _m("cross_completion_rate", "pct"),
                         _m("passes_into_penalty_area_per_90"), _m("xa_per_90"),
                         _m("shot_creating_actions_per_90"),
                         _m("vaep_per_90", decimals=3)]),
    ],
    "MID": [
        ("defending",   [_m("padj_interceptions_per_90"), _m("padj_ball_recoveries_per_90"),
                         _m("padj_blocked_passes_per_90"),
                         _m("def_actions_final_third_per_90"),
                         _m("vaep_defensive_per_90", decimals=3), _m("errors_per_90")]),
        ("duels",       [_m("padj_tackles_per_90"), _m("ground_duel_win_rate", "pct"),
                         _m("dribbled_past_per_90"), _m("aerials_won_per_90"),
                         _m("aerial_win_rate", "pct"), _m("fouls_committed_per_90")]),
        ("passing_progression",
                        [_m("passes_per_90", decimals=1),
                         _m("pass_completion_rate", "pct"),
                         _m("progressive_passes_per_90"),
                         _m("passes_into_final_third_per_90"),
                         _m("progressive_carries_per_90"), _m("xt_per_90", decimals=3)]),
        ("creation",    [_m("key_passes_per_90"), _m("xa_per_90"),
                         _m("through_balls_per_90"), _m("shot_creating_actions_per_90"),
                         _m("npxg_per_90"), _m("vaep_per_90", decimals=3)]),
    ],
    "AM": [
        ("pressing",    [_m("padj_ball_recoveries_per_90"),
                         _m("def_actions_final_third_per_90"),
                         _m("padj_tackles_per_90"), _m("ground_duel_win_rate", "pct"),
                         _m("padj_interceptions_per_90"),
                         _m("vaep_defensive_per_90", decimals=3)]),
        ("passing",     [_m("passes_per_90", decimals=1),
                         _m("pass_completion_rate", "pct"),
                         _m("progressive_passes_per_90"),
                         _m("passes_into_final_third_per_90"),
                         _m("passes_into_penalty_area_per_90"),
                         _m("through_balls_per_90")]),
        ("carrying",    [_m("progressive_carries_into_final_third_per_90"),
                         _m("take_ons_per_90"), _m("take_on_success_rate", "pct"),
                         _m("receptions_final_third_per_90"),
                         _m("xt_per_90", decimals=3), _m("dispossessed_per_90")]),
        ("creation",    [_m("key_passes_per_90"), _m("xa_per_90"),
                         _m("shot_creating_actions_per_90"), _m("npxg_per_90"),
                         _m("goals_non_penalty_per_90"), _m("vaep_per_90", decimals=3)]),
    ],
    "W": [
        ("pressing",    [_m("padj_ball_recoveries_per_90"),
                         _m("def_actions_final_third_per_90"),
                         _m("padj_tackles_per_90"), _m("ground_duel_win_rate", "pct"),
                         _m("padj_interceptions_per_90"),
                         _m("vaep_defensive_per_90", decimals=3)]),
        ("carrying",    [_m("take_ons_per_90"), _m("take_on_success_rate", "pct"),
                         _m("progressive_carries_into_final_third_per_90"),
                         _m("carries_into_penalty_area_per_90"),
                         _m("progressive_receptions_per_90"), _m("dispossessed_per_90")]),
        ("creation",    [_m("crosses_per_90"), _m("cross_completion_rate", "pct"),
                         _m("passes_into_penalty_area_per_90"), _m("key_passes_per_90"),
                         _m("xa_per_90"), _m("shot_creating_actions_per_90")]),
        ("finishing",   [_m("npxg_per_90"), _m("goals_non_penalty_per_90"),
                         _m("shots_per_90"), _m("touches_in_box_per_90"),
                         _m("npxg_per_shot", decimals=3), _m("vaep_per_90", decimals=3)]),
    ],
    "ST": [
        ("pressing",    [_m("padj_ball_recoveries_per_90"),
                         _m("def_actions_final_third_per_90"),
                         _m("aerials_won_per_90"), _m("aerial_win_rate", "pct"),
                         _m("aerials_won_att_third_per_90"), _m("fouls_won_per_90")]),
        ("link_play",   [_m("pass_completion_rate", "pct"),
                         _m("passes_received_per_90", decimals=1),
                         _m("key_passes_per_90"), _m("xa_per_90"),
                         _m("shot_creating_actions_per_90"),
                         _m("vaep_per_90", decimals=3)]),
        ("movement",    [_m("receptions_in_box_per_90"),
                         _m("progressive_receptions_per_90"),
                         _m("touches_in_box_per_90"), _m("take_on_success_rate", "pct"),
                         _m("progressive_carries_per_90"), _m("dispossessed_per_90")]),
        ("finishing",   [_m("npxg_per_90"), _m("goals_non_penalty_per_90"),
                         _m("shots_per_90"), _m("npxg_per_shot", decimals=3),
                         _m("xg_overperformance_per_90"), _m("headed_shots_per_90")]),
    ],
}
# RB and LB share the full-back sheet; the peer set is still per side.
PLAYER_SPINE["RB"] = PLAYER_SPINE["FB"]
PLAYER_SPINE["LB"] = PLAYER_SPINE["FB"]


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
    # PPDA is already a ratio (opponent passes per defensive action) and
    # fewer is more pressing, so it ships as a rate with the percentile
    # reversed. It replaced duels_won on 11 Sep 2026, which was the sum of
    # the two rows below it rather than independent evidence.
    ProfileMetric("ppda",                         "defensive",   "rate", invert=True),
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
    higher_is_better: bool     # drives the rank direction (1 = best)
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
    # Goals replaced PPDA in the strip on 11 Sep 2026; PPDA moved to the
    # Defensive profile card. Every strip metric now ranks descending.
    Kpi("goals_for",          None,  True,  decimals=0,
        secondary="goals_per_match"),
    Kpi("set_piece_goals_for", None, True,  decimals=0,
        secondary="set_piece_goal_share"),
]

@dataclass(frozen=True)
class LeaderMetric:
    key: str                   # column on gold.player_season_stats
    unit: str | None = None
    # Per-90 rates need the §3.6 minutes floor: without it a 98-minute
    # third keeper tops the box. Counts rank themselves by volume, so
    # they need no gate.
    gated: bool = False
    # VAEP credits keepers for every save and claim, so a GK per-90 reads
    # as the best outfield player's; GOLD_LAYER.md warns against reading
    # it that way. Leave keepers out of that box.
    exclude_gk: bool = False


# Four leader boxes, six rows each, in the design's order.
LEADER_METRICS: list[LeaderMetric] = [
    LeaderMetric("goals"),
    LeaderMetric("vaep_per_90", gated=True, exclude_gk=True),
    LeaderMetric("progressive_passes"),
    LeaderMetric("carries_into_final_third"),
]
LEADER_ROWS = 6


# ---------------------------------------------------------------------------
# Shaping limits used by the match file (§7.1). Kept here so the exposure
# rules of WEB_PLAN.md §6 are visible in one place.
# ---------------------------------------------------------------------------

NETWORK_MIN_COMBINATIONS = 4
PROGRESSION_ARROWS = 30
# No cap on defensive actions: the block map exports all ~128 of them, so the
# dashed average line and the marks describe the same set (db.py DEFENCE_SQL).
MATCH_SEQUENCES = 12
SEQUENCE_MAX_WAYPOINTS = 6
SEQUENCE_MIN_EVENTS = 3     # sequences.json filter (GOLD_LAYER.md §4.1.9)


# ---------------------------------------------------------------------------
# Match file: the 14 stat rows of screen 02 block 3 (§7.2)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class MatchTotal:
    key: str                # what the site translates and renders
    column: str             # column on gold.team_match_stats
    scale: str = "raw"      # "pct" for the 0-1 shares gold stores
    decimals: int = 1


# Order is the design's, top to bottom, plus four rows added on 14 Sep 2026
# (pass completion, crosses completed, duels won, recoveries), each slotted
# next to the row it qualifies. Keys differ from their column where the row is
# labelled from the reader's side of the match, not the table's: `xg` is
# xg_for, `xt_created` is xt_for, `corners` is corners_for, `recoveries` is
# ball_recoveries, and `pass_completion_pct` is the 0-1 pass_completion_rate.
MATCH_TOTALS: list[MatchTotal] = [
    MatchTotal("possession_pct",      "possession_pct", scale="pct"),
    MatchTotal("xg",                  "xg_for",                decimals=2),
    MatchTotal("shots",               "shots",                 decimals=0),
    MatchTotal("shots_on_target",     "shots_on_target",       decimals=0),
    MatchTotal("big_chances",         "big_chances",           decimals=0),
    MatchTotal("passes_completed",    "passes_completed",      decimals=0),
    MatchTotal("pass_completion_pct", "pass_completion_rate", scale="pct"),
    MatchTotal("final_third_entries", "final_third_entries",   decimals=0),
    MatchTotal("crosses_completed",   "crosses_completed",     decimals=0),
    MatchTotal("xt_created",          "xt_for",                decimals=3),
    MatchTotal("yellow_cards",        "yellow_cards",          decimals=0),
    MatchTotal("red_cards",           "red_cards",             decimals=0),
    MatchTotal("fouls_committed",     "fouls_committed",       decimals=0),
    MatchTotal("duels_won",           "duels_won",             decimals=0),
    MatchTotal("recoveries",          "ball_recoveries",       decimals=0),
    MatchTotal("corners",             "corners_for",           decimals=0),
    MatchTotal("xg_open_play",        "xg_open_play",          decimals=2),
    MatchTotal("xg_set_piece",        "xg_set_piece",          decimals=2),
]

# The four player boxes at the foot of screen 02. A box is a key plus the
# column(s) summed into its value; only the last one has two, and it ships its
# components as well so a reader can tell a finisher from a creator.
MATCH_PLAYER_BOXES: list[tuple[str, tuple[str, ...]]] = [
    ("recoveries", ("ball_recoveries",)),
    ("passes_completed", ("passes_completed",)),
    ("passes_into_final_third", ("passes_into_final_third",)),
    ("xg_xa", ("xg", "xa")),
]
MATCH_PLAYER_ROWS = 6


# ---------------------------------------------------------------------------
# Screen 03: squad.json and players/{player_id}.json (§8, §9, README-PLAYER)
# ---------------------------------------------------------------------------

# Tab-strip order: GK -> CB -> FB -> MID -> W -> ST, then minutes. `role_group`
# is the key, so an AM sits with the midfielders and RB precedes LB.
SQUAD_ROLE_ORDER = ["GK", "CB", "RB", "LB", "MID", "AM", "W", "ST"]


@dataclass(frozen=True)
class PlayerKpi:
    key: str                # column on gold.player_season_stats
    scale: str = "raw"      # "pct" for the 0-1 rates gold stores (shipped x100)
    decimals: int = 0
    # A season total ships its per-90 as the sub line; a rate or a per-90
    # ships its rank inside the squad instead (README-PLAYER §3a).
    total: bool = True


# The four headline KPIs of the profile card, per role (decided 16 Oct 2026).
# VAEP, not xT, is the model value: it covers every on-ball action and charges
# losing the ball. Season totals unless the key says /90 or the scale is pct.
_VAEP90 = PlayerKpi("vaep_per_90", decimals=3, total=False)
PLAYER_KPIS: dict[str, list[PlayerKpi]] = {
    "ST": [PlayerKpi("goals"), PlayerKpi("assists"),
           PlayerKpi("npxg", decimals=2), _VAEP90],
    "AM": [PlayerKpi("goal_contributions"), PlayerKpi("xa", decimals=2),
           PlayerKpi("key_passes"), _VAEP90],
    "MID": [_VAEP90, PlayerKpi("progressive_passes"), PlayerKpi("ball_recoveries"),
            PlayerKpi("pass_completion_rate", "pct", 1, total=False)],
    "CB": [_VAEP90, PlayerKpi("ground_duel_win_rate", "pct", 1, total=False),
           PlayerKpi("aerial_win_rate", "pct", 1, total=False),
           PlayerKpi("progressive_passes")],
    "GK": [PlayerKpi("goals_prevented", decimals=2),
           PlayerKpi("save_pct", "pct", 1, total=False),
           PlayerKpi("clean_sheets"),
           PlayerKpi("gk_pass_completion_rate", "pct", 1, total=False)],
}
PLAYER_KPIS["W"] = PLAYER_KPIS["ST"]
PLAYER_KPIS["RB"] = PLAYER_KPIS["CB"]
PLAYER_KPIS["LB"] = PLAYER_KPIS["CB"]

# Exposure caps of the event map (WEB_PLAN §6 level B, README-PLAYER open
# decision 5). Shipping every pass of a season with coordinates is the
# per-player version of the "every pass" file §6 rules out; the note on the
# map states the cap ("150 DE 1,284 PASES").
PLAYER_PASS_MAP_MAX = 150          # the most recent passes
PLAYER_PROGRESSIVE_MAX = 60        # progressive passes, and carries, top by xT
# Key passes, assists, shots, defensive actions and progressive receptions
# ship in full: dozens a season, not thousands.

# A reception whose receiver first acted more than this far from the pass end
# is dropped from the DOTS (its first touch is not where the ball arrived),
# never from the heat (GOLD_LAYER §4.7).
RECEPTION_MAX_GAP_M = 5.0

# The sequence block: the ten best chains by xT and by VAEP.
PLAYER_TOP_SEQUENCES = 10
