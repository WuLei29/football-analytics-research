"""
vaep.py
───────
VAEP (Valuing Actions by Estimating Probabilities) column calculation.

Uses the XGBoost artifact exported by the vaep-model training repo
(``models/vaep/{vaep_model.json, metrics.json}``).

Feature construction must match the training pipeline exactly — see
md/VAEP_RETRAINING_v2.md §10.  The artifact is self-describing: the ordered
feature list (`config.feature_names`), the coordinate frame
(`config.frame_convention`) and the per-head probability calibrator
(`config.calibration`) are all read from metrics.json, never assumed here.

Stateless: takes a DataFrame of ALL match events, returns the same DataFrame
with three new columns: vaep_offensive, vaep_defensive, vaep_value.

Only SPADL-valid events (spadl_type_id IS NOT NULL) participate in game state
construction, ordered by json_index before the sliding window is applied.
Non-SPADL events (cards, substitutions, etc.) receive NULL for all three columns.

The first SPADL action in each match gets NULL (no previous state to delta against).

Backfill CLI:
    python -m src.silver.events.vaep
    python -m src.silver.events.vaep --match-ids 1 2 3
    python -m src.silver.events.vaep --limit 10
"""

import base64
import json
import logging
from pathlib import Path
from typing import List, Optional, Tuple

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)

# ── Paths ─────────────────────────────────────────────────────────────────────

_REPO_ROOT    = Path(__file__).resolve().parents[3]
_MODEL_PATH   = _REPO_ROOT / "models" / "vaep" / "vaep_model.json"
_METRICS_PATH = _REPO_ROOT / "models" / "vaep" / "metrics.json"

# ── SPADL ID → feature name mappings ─────────────────────────────────────────

_TYPE_NAMES: dict = {
    0: "pass",            1: "cross",           2: "throw_in",
    3: "freekick_crossed", 4: "freekick_short",
    5: "corner_crossed",  6: "corner_short",
    7: "take_on",         8: "foul",            9: "tackle",
    10: "interception",
    11: "shot",           12: "shot_penalty",   13: "shot_freekick",
    14: "keeper_save",    15: "keeper_claim",
    16: "keeper_punch",   17: "keeper_pick_up",
    18: "clearance",      19: "bad_touch",      20: "non_action",
    21: "dribble",        22: "goalkick",
}

_RESULT_NAMES: dict = {
    0: "fail", 1: "success", 2: "offside",
    3: "owngoal", 4: "yellow_card", 5: "red_card",
}

# Period start offsets in minutes (for time_seconds within period)
_PERIOD_OFFSETS: dict = {1: 0, 2: 45, 3: 90, 4: 105, 5: 120}

# Attacking goal centre (SPADL standard: acting team attacks toward x=105)
_GOAL_X, _GOAL_Y = 105.0, 34.0

# Pitch dimensions, used to mirror opposing-team actions into a common frame.
# Opta records every event in its own acting team's attacking frame, so an
# action by the other team is rotated 180° about the pitch centre relative to
# a0.  Verified on 10,855 aerial-duel pairs (two events at one physical point,
# one per team): x_a + x_b = 105.00 and y_a + y_b = 68.00 in 99.9% of cases.
_PITCH_X, _PITCH_Y = 105.0, 68.0

# ── Formula guards (socceraction.vaep.formula) ────────────────────────────────
# A previous state older than this no longer bears on the current action.
_SAMEPHASE_SECONDS = 10.0
# SPADL type ids: shot / shot_penalty / shot_freekick, and the two corner types
_SHOT_TYPES        = (11, 12, 13)
_SHOT_PENALTY      = 12
_CORNER_TYPES      = (5, 6)
_RESULT_SUCCESS    = 1
_RESULT_OWNGOAL    = 3
# Fixed pre-action scoring odds, taken from socceraction's reference values
_PENALTY_PREV_SCORES = 0.792453
_CORNER_PREV_SCORES  = 0.046500

# ── Model cache ───────────────────────────────────────────────────────────────

_CACHE: dict = {}


def _load_models():
    """Load and cache the two XGBoost classifiers from vaep_model.json.

    Returns (scores_model, concedes_model, feature_names, mirror_to_a0_frame,
    calibration).

    ``mirror_to_a0_frame`` comes from ``config.frame_convention`` in
    metrics.json and must match what the training run did:
      "ltr"          — actions already in the acting team's attacking frame,
                       fed to the model as-is (silly-kicks >= 3.0.0 default).
      "a0_mirrored"  — a1/a2 rotated 180° into a0's frame
                       (socceraction's play_left_to_right).
    There is no default: the frame is a contract between the two ends and
    guessing it wrong degrades every game state spanning a turnover silently.

    ``calibration`` carries one block per head from ``config.calibration``.
    It is likewise required — the raw boosters fail the calibration gate on
    Opta, so an artifact without a calibrator must not be scored by accident.
    ``{"method": "none"}`` is the explicit way to say "no correction".
    """
    if "scores" not in _CACHE:
        from xgboost import XGBClassifier

        with open(_MODEL_PATH) as f:
            envelope = json.load(f)

        m_scores = XGBClassifier()
        m_scores.load_model(bytearray(base64.b64decode(envelope["scores_booster_b64"])))

        m_concedes = XGBClassifier()
        m_concedes.load_model(bytearray(base64.b64decode(envelope["concedes_booster_b64"])))

        with open(_METRICS_PATH) as f:
            metrics = json.load(f)

        config = metrics["config"]

        if "frame_convention" not in config:
            raise ValueError(
                f"{_METRICS_PATH.name} declares no frame_convention. It is the "
                "contract between training and inference and must not be guessed."
            )
        convention = config["frame_convention"]
        if convention not in ("ltr", "a0_mirrored"):
            raise ValueError(f"Unknown frame_convention {convention!r} in {_METRICS_PATH}")

        if "calibration" not in config:
            raise ValueError(
                f"{_METRICS_PATH.name} carries no config.calibration. The raw "
                'boosters are not calibrated for Opta; declare {"method": "none"} '
                "explicitly if that is really what is wanted."
            )
        calibration = config["calibration"]
        for head in ("scores", "concedes"):
            if head not in calibration:
                raise ValueError(
                    f"config.calibration in {_METRICS_PATH.name} has no {head!r} block"
                )

        _CACHE["scores"]        = m_scores
        _CACHE["concedes"]      = m_concedes
        _CACHE["feature_names"] = config["feature_names"]
        _CACHE["mirror"]        = convention == "a0_mirrored"
        _CACHE["calibration"]   = calibration
        log.info(
            "VAEP models loaded from %s (frame_convention=%s, %d features, "
            "calibration=%s)",
            _MODEL_PATH, convention, len(config["feature_names"]),
            calibration.get("method", "none"),
        )

    return (
        _CACHE["scores"], _CACHE["concedes"], _CACHE["feature_names"],
        _CACHE["mirror"], _CACHE["calibration"],
    )


# ── Probability calibration ───────────────────────────────────────────────────
# Verbatim from vaep_model.calibrate.CONSUMER_REFERENCE, which the training
# repo asserts bit-identical to the code that fitted the parameters
# (tests/test_calibrate.py, through a JSON round-trip).  Do not "simplify":
# the naive 1/(1+exp(-z)) differs in the last bits and overflows on extreme
# log-odds.  Applied per head AFTER predict_proba and BEFORE the VAEP formula.


def _stable_sigmoid(z):
    """1/(1+exp(-z)) without overflowing on large-magnitude z."""
    out = np.empty_like(z, dtype=np.float64)
    pos = z >= 0
    out[pos] = 1.0 / (1.0 + np.exp(-z[pos]))
    ez = np.exp(z[~pos])
    out[~pos] = ez / (1.0 + ez)
    return out


def _apply_calibration(p, cal):
    """metrics.json -> config.calibration, applied to one head's probabilities."""
    method = cal.get("method", "none")
    if method == "none":
        return p
    if method == "platt":
        q = np.clip(np.asarray(p, dtype=np.float64), 1e-12, 1 - 1e-12)
        return _stable_sigmoid(cal["a"] * np.log(q / (1.0 - q)) + cal["b"])
    if method == "isotonic":
        return np.interp(np.asarray(p, dtype=np.float64), cal["x"], cal["y"])
    raise ValueError(f"unknown calibration method {method!r}")


# ── Feature engineering ───────────────────────────────────────────────────────

def _state_index(period: np.ndarray, k: int) -> np.ndarray:
    """Row index of the action k steps back, within the same period.

    Mirrors silly_kicks.vaep.feature_framework.gamestates: the game-state
    window never crosses a period boundary, and the first k actions of a
    period fall back to the first action of that period rather than being
    padded with zeros.
    """
    n = len(period)
    if n == 0:
        return np.empty(0, dtype=int)

    # First row of each period block (events arrive ordered by json_index)
    block_start = np.zeros(n, dtype=int)
    starts = np.flatnonzero(np.concatenate([[True], period[1:] != period[:-1]]))
    for s, e in zip(starts, np.append(starts[1:], n)):
        block_start[s:e] = s

    return np.maximum(np.arange(n) - k, block_start)


def _polar(x: np.ndarray, y: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
    """Distance and angle (radians) from each position to the attacking goal."""
    dx    = _GOAL_X - x
    dy    = y - _GOAL_Y
    dist  = np.sqrt(dx ** 2 + dy ** 2)
    angle = np.arctan2(np.abs(dy), np.abs(dx))
    return dist, angle


def _goalscore(
    type_id: np.ndarray, result_id: np.ndarray, team: np.ndarray
) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Score BEFORE each action, from the acting team's perspective.

    ``cumsum() - self``, so the action's own goal is excluded.  Both reference
    libraries' docstrings say "after the action" and are wrong — the code
    computes before, which is the correct non-leaking definition.  Cumulative
    within a match, so this must be called per match on json_index-ordered
    actions.

    Reproduces silly_kicks.vaep.features.context.goalscore, including its
    shot-type gate on the own-goal branch: that branch is live on our Opta
    feed, whose own goals are type shot / result owngoal.  An own goal counts
    toward the OTHER team's score, hence the cross-attribution below.
    """
    n = len(type_id)
    if n == 0:
        empty = np.empty(0, dtype=float)
        return empty, empty.copy(), empty.copy()

    is_shot  = np.isin(type_id, _SHOT_TYPES)
    goals    = is_shot & (result_id == _RESULT_SUCCESS)
    owngoals = is_shot & (result_id == _RESULT_OWNGOAL)

    # "team a" is whoever acted first in the match; "team b" is everyone else.
    is_a = team == team[0]
    is_b = ~is_a

    goals_a = (goals & is_a) | (owngoals & is_b)
    goals_b = (goals & is_b) | (owngoals & is_a)
    score_a = np.cumsum(goals_a) - goals_a
    score_b = np.cumsum(goals_b) - goals_b

    gs_team     = score_a * is_a + score_b * is_b
    gs_opponent = score_b * is_a + score_a * is_b
    return (
        gs_team.astype(float),
        gs_opponent.astype(float),
        (gs_team - gs_opponent).astype(float),
    )


def _build_features(
    valid_df: pd.DataFrame, feature_names: List[str], mirror: bool
) -> pd.DataFrame:
    """
    Build the 148-feature matrix for one match's SPADL-valid events.

    valid_df must already be:
      - filtered to rows where spadl_type_id IS NOT NULL
      - sorted by json_index (ascending)
      - reset to a 0-based integer index

    mirror: rotate a1/a2 into a0's attacking frame. Must match the training
    run's coordinate convention — see _load_models.

    Returns a DataFrame with exactly 148 columns in model order.
    """
    n = len(valid_df)

    # Extract source arrays; fill NaN to prevent propagation into features
    type_id   = valid_df["spadl_type_id"].fillna(0).to_numpy(dtype=float)
    result_id = valid_df["spadl_result_id"].fillna(0).to_numpy(dtype=float)
    body_id   = valid_df["spadl_bodypart_id"].fillna(0).to_numpy(dtype=float)
    sx        = valid_df["x"].fillna(0).to_numpy(dtype=float)
    sy        = valid_df["y"].fillna(0).to_numpy(dtype=float)
    ex        = valid_df["end_x"].fillna(0).to_numpy(dtype=float)
    ey        = valid_df["end_y"].fillna(0).to_numpy(dtype=float)
    minute    = valid_df["minute"].fillna(0).to_numpy(dtype=float)
    second    = valid_df["second"].fillna(0).to_numpy(dtype=float)
    period    = valid_df["period"].fillna(1).to_numpy(dtype=float)
    team      = valid_df["team_id"].fillna(0).to_numpy(dtype=float)

    # Derived arrays computed once over the full (unshifted) sequence.
    # Coordinate-derived features (dx/dy, polar) are NOT precomputed here: they
    # must be built per game-state position, after the shifted action has been
    # mirrored into a0's attacking frame (see the loop below).
    time_overall  = minute * 60.0 + second
    period_offset = np.array([_PERIOD_OFFSETS.get(int(p), 0) for p in period], dtype=float)
    time_period   = np.clip((minute - period_offset) * 60.0 + second, 0.0, None)

    period_int = period.astype(int)

    data: dict = {}

    for k, suffix in enumerate(["a0", "a1", "a2"]):
        idx       = _state_index(period_int, k)
        s_type    = type_id[idx]
        s_result  = result_id[idx]
        s_body    = body_id[idx]
        s_sx      = sx[idx]
        s_sy      = sy[idx]
        s_ex      = ex[idx]
        s_ey      = ey[idx]
        s_to      = time_overall[idx]
        s_tp      = time_period[idx]
        s_per     = period[idx]
        s_team    = team[idx]

        if mirror:
            # Put the whole game state in a0's attacking frame, rotating actions
            # by the other team 180° about the pitch centre — socceraction's
            # play_left_to_right(gamestates, ...).  Only correct when the model
            # was trained that way; silly-kicks >= 3.0.0 removed this step and
            # feeds canonical LTR coordinates straight through.
            flip = s_team != team
            s_sx = np.where(flip, _PITCH_X - s_sx, s_sx)
            s_ex = np.where(flip, _PITCH_X - s_ex, s_ex)
            s_sy = np.where(flip, _PITCH_Y - s_sy, s_sy)
            s_ey = np.where(flip, _PITCH_Y - s_ey, s_ey)

        # Movement and polar features derive from the mirrored coordinates
        s_dx      = s_ex - s_sx
        s_dy      = s_ey - s_sy
        s_sd, s_sa = _polar(s_sx, s_sy)
        s_ed, s_ea = _polar(s_ex, s_ey)

        # Action type one-hot (23 columns)
        for t_id, t_name in _TYPE_NAMES.items():
            data[f"actiontype_{t_name}_{suffix}"] = (s_type == t_id).astype(float)

        # Result one-hot (6 columns)
        for r_id, r_name in _RESULT_NAMES.items():
            data[f"result_{r_name}_{suffix}"] = (s_result == r_id).astype(float)

        # Body part one-hot (4 columns), per silly_kicks bodypart_onehot:
        # foot_left (4) and foot_right (5) collapse into foot, and head/other
        # is the union of head (1), other (2) and head/other (3) — it is NOT
        # mutually exclusive with the head and other columns.
        data[f"bodypart_foot_{suffix}"]       = np.isin(s_body, [0, 4, 5]).astype(float)
        data[f"bodypart_head_{suffix}"]       = (s_body == 1).astype(float)
        data[f"bodypart_other_{suffix}"]      = (s_body == 2).astype(float)
        data[f"bodypart_head/other_{suffix}"] = np.isin(s_body, [1, 2, 3]).astype(float)

        # Time features
        data[f"period_id_{suffix}"]           = s_per
        data[f"time_seconds_{suffix}"]         = s_tp
        data[f"time_seconds_overall_{suffix}"] = s_to

        # Coordinates
        data[f"start_x_{suffix}"] = s_sx
        data[f"start_y_{suffix}"] = s_sy
        data[f"end_x_{suffix}"]   = s_ex
        data[f"end_y_{suffix}"]   = s_ey

        # Polar (distance and angle to attacking goal)
        data[f"start_dist_to_goal_{suffix}"]  = s_sd
        data[f"start_angle_to_goal_{suffix}"] = s_sa
        data[f"end_dist_to_goal_{suffix}"]    = s_ed
        data[f"end_angle_to_goal_{suffix}"]   = s_ea

        # Movement
        data[f"dx_{suffix}"]       = s_dx
        data[f"dy_{suffix}"]       = s_dy
        data[f"movement_{suffix}"] = np.sqrt(s_dx ** 2 + s_dy ** 2)

    # Cross-action features: every slot is compared against a0, and team_i is
    # TRUE when a_i was performed by a0's team (silly_kicks team / time_delta).
    idx_1 = _state_index(period_int, 1)
    idx_2 = _state_index(period_int, 2)

    data["team_1"] = (team[idx_1] == team).astype(float)
    data["team_2"] = (team[idx_2] == team).astype(float)

    # Seconds between a_i and a0, on the within-period clock
    data["time_delta_1"] = time_period - time_period[idx_1]
    data["time_delta_2"] = time_period - time_period[idx_2]

    # Goalscore (3): computed on a0 only, hence unsuffixed.  Not decorated with
    # @simple upstream, which is why it adds 3 columns and not 3x3.
    gs_team, gs_opponent, gs_diff = _goalscore(type_id, result_id, team)
    data["goalscore_team"]     = gs_team
    data["goalscore_opponent"] = gs_opponent
    data["goalscore_diff"]     = gs_diff

    # A name that doesn't match the model's list would silently become an
    # all-NaN column rather than an error, so check before reindexing.
    if set(data) != set(feature_names):
        missing = sorted(set(feature_names) - set(data))
        extra   = sorted(set(data) - set(feature_names))
        raise ValueError(
            f"Feature mismatch against {_METRICS_PATH.name}: "
            f"missing={missing} unexpected={extra}"
        )

    # float32 throughout, as the training shards were: the DMatrix is float32
    # regardless, so this only makes the contract explicit — and keeps a parity
    # diff from looking like a coordinate bug when it is really a dtype one.
    return pd.DataFrame(
        {name: np.asarray(data[name], dtype="float32") for name in feature_names},
        columns=feature_names,
    )


# ── Public transform ───────────────────────────────────────────────────────────

def calculate_vaep(df: pd.DataFrame) -> pd.DataFrame:
    """Add vaep_offensive, vaep_defensive, vaep_value columns.

    Only SPADL-valid events (non-NULL spadl_type_id) receive values; all others
    stay NULL.  Events are sorted by json_index before game-state construction so
    the sliding window reflects true chronological order.

    The first SPADL action of each match receives NULL for all three columns
    because there is no previous game state to compute a delta against.

    Args:
        df: Full match events DataFrame. Required columns: spadl_type_id,
            spadl_result_id, spadl_bodypart_id, x, y, end_x, end_y,
            minute, second, period, team_id, json_index.

    Returns:
        DataFrame with vaep_offensive, vaep_defensive, vaep_value added.
    """
    df = df.copy()
    df["vaep_offensive"] = np.nan
    df["vaep_defensive"] = np.nan
    df["vaep_value"]     = np.nan

    valid_mask = df["spadl_type_id"].notna()
    if not valid_mask.any():
        return df

    # Deliberately outside the try below: a broken or mis-declared artifact is
    # not a per-match data problem, and swallowing it would turn a whole
    # backfill into "0 events updated" with a warning per match.
    m_scores, m_concedes, feature_names, mirror, calibration = _load_models()

    # Isolate SPADL-valid events in chronological order; preserve original indices
    valid_df = (
        df.loc[valid_mask]
        .sort_values("json_index", na_position="last")
        .reset_index()  # original df index goes into column "index"
    )
    orig_idx = valid_df["index"].to_numpy()

    try:
        X = _build_features(valid_df, feature_names, mirror)

        # Per-head Platt correction, fitted on held-out Opta.  It must land
        # between predict_proba and the formula: VAEP is a DIFFERENCE of
        # probabilities, so calibrating afterwards is not the same operation.
        p_scores   = _apply_calibration(m_scores.predict_proba(X)[:, 1],
                                        calibration["scores"])
        p_concedes = _apply_calibration(m_concedes.predict_proba(X)[:, 1],
                                        calibration["concedes"])

        # Delta against the previous game state; first action has no predecessor → NaN
        prev_scores_raw   = np.concatenate([[np.nan], p_scores[:-1]])
        prev_concedes_raw = np.concatenate([[np.nan], p_concedes[:-1]])

        # Both probabilities are from the ACTING team's perspective, so when
        # possession changes hands the previous action's P(scores) is the new
        # team's P(concedes) and vice versa.  Mirrors socceraction's
        # offensive_value/defensive_value:
        #   prev_scores   = _prev(scores)   * sameteam + _prev(concedes) * (~sameteam)
        #   prev_concedes = _prev(concedes) * sameteam + _prev(scores)   * (~sameteam)
        team_arr  = valid_df["team_id"].fillna(-1).to_numpy(dtype=float)
        same_team = np.concatenate([[False], team_arr[1:] == team_arr[:-1]])

        prev_scores   = np.where(same_team, prev_scores_raw, prev_concedes_raw)
        prev_concedes = np.where(same_team, prev_concedes_raw, prev_scores_raw)

        # ── Guards, per socceraction.vaep.formula ─────────────────────────────
        # The reference compares actions on the within-period clock, so a new
        # period always trips the staleness guard.
        minute_arr = valid_df["minute"].fillna(0).to_numpy(dtype=float)
        period_arr = valid_df["period"].fillna(1).to_numpy(dtype=float)
        offset_arr = np.array(
            [_PERIOD_OFFSETS.get(int(p), 0) for p in period_arr], dtype=float
        )
        t_secs    = np.clip(
            (minute_arr - offset_arr) * 60.0
            + valid_df["second"].fillna(0).to_numpy(dtype=float),
            0.0, None,
        )
        prev_t    = np.concatenate([[np.nan], t_secs[:-1]])
        cur_type  = valid_df["spadl_type_id"].fillna(-1).to_numpy(dtype=float)
        prev_type = np.concatenate([[np.nan], cur_type[:-1]])
        prev_res  = np.concatenate(
            [[np.nan], valid_df["spadl_result_id"].fillna(-1).to_numpy(dtype=float)[:-1]]
        )

        # A stale predecessor carries no information about the current state
        toolong = np.abs(t_secs - prev_t) > _SAMEPHASE_SECONDS
        # After a goal, play restarts from a neutral kick-off state
        prevgoal = np.isin(prev_type, _SHOT_TYPES) & (prev_res == _RESULT_SUCCESS)
        for guard in (toolong, prevgoal):
            prev_scores[guard]   = 0.0
            prev_concedes[guard] = 0.0

        # Penalties and corners have known pre-action scoring odds.  Applied to
        # the offensive term only, matching the reference implementation.
        prev_scores[cur_type == _SHOT_PENALTY]         = _PENALTY_PREV_SCORES
        prev_scores[np.isin(cur_type, _CORNER_TYPES)]  = _CORNER_PREV_SCORES

        offensive = p_scores     - prev_scores
        defensive = prev_concedes - p_concedes
        vaep      = offensive    + defensive

        # The guards above can overwrite row 0 with a fixed value, so re-assert
        # the contract that the first action of a match has no delta.
        offensive[0] = defensive[0] = vaep[0] = np.nan

        df.loc[orig_idx, "vaep_offensive"] = offensive
        df.loc[orig_idx, "vaep_defensive"] = defensive
        df.loc[orig_idx, "vaep_value"]     = vaep

        n_scored = int(np.sum(~np.isnan(vaep)))
        log.debug("VAEP: %d/%d SPADL actions scored", n_scored, len(valid_df))

    except Exception as exc:
        log.warning("VAEP prediction failed — columns left NULL: %s", exc)

    return df


# ── Backfill ──────────────────────────────────────────────────────────────────

_BACKFILL_DISCOVER = """
    SELECT DISTINCT match_id
    FROM silver.events
    WHERE spadl_type_id IS NOT NULL
      AND vaep_value IS NULL
    ORDER BY match_id
"""

_BACKFILL_SELECT_MATCH = """
    SELECT event_id, match_id,
           x, y, end_x, end_y,
           minute, second, period,
           team_id, json_index,
           spadl_type_id, spadl_result_id, spadl_bodypart_id
    FROM silver.events
    WHERE match_id = %s
    ORDER BY json_index NULLS LAST, event_id
"""

_BACKFILL_UPDATE = """
    UPDATE silver.events
    SET vaep_offensive = data.vo,
        vaep_defensive = data.vd,
        vaep_value     = data.vv
    FROM (VALUES %s) AS data(vo, vd, vv, eid)
    WHERE silver.events.event_id = data.eid
"""


def backfill_vaep(
    conn,
    match_ids: Optional[List[int]] = None,
    limit: Optional[int] = None,
    batch_size: int = 5,
) -> dict:
    """Backfill VAEP columns for existing events.

    Requires SPADL columns to already be populated (spadl_type_id IS NOT NULL
    on at least one event per match).

    Args:
        conn:       psycopg2 connection.
        match_ids:  Specific match IDs, or None to auto-discover.
        limit:      Max matches when auto-discovering.
        batch_size: Matches committed per DB transaction.

    Returns:
        Stats dict: {matches_processed, events_updated}.
    """
    import psycopg2.extras

    if match_ids is None:
        sql = _BACKFILL_DISCOVER
        if limit:
            sql += f" LIMIT {int(limit)}"
        with conn.cursor() as cur:
            cur.execute(sql)
            match_ids = [r[0] for r in cur.fetchall()]
        if not match_ids:
            log.info("No unprocessed matches found — nothing to backfill.")
            return {"matches_processed": 0, "events_updated": 0}
        log.info("Auto-discovered %d matches with missing VAEP", len(match_ids))
    else:
        log.info("Processing %d specified matches", len(match_ids))

    stats = {"matches_processed": 0, "events_updated": 0}

    for i, mid in enumerate(match_ids):
        with conn.cursor() as cur:
            cur.execute(_BACKFILL_SELECT_MATCH, (mid,))
            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()

        if not rows:
            log.warning("No events for match_id=%s", mid)
            stats["matches_processed"] += 1
            continue

        df = pd.DataFrame(rows, columns=cols)
        df = calculate_vaep(df)

        updates = [
            (float(row["vaep_offensive"]), float(row["vaep_defensive"]),
             float(row["vaep_value"]), int(row["event_id"]))
            for _, row in df.iterrows()
            if pd.notna(row.get("vaep_value"))
        ]

        if updates:
            with conn.cursor() as cur:
                for page_start in range(0, len(updates), 500):
                    page = updates[page_start : page_start + 500]
                    psycopg2.extras.execute_values(
                        cur, _BACKFILL_UPDATE, page,
                        template="(%s::float, %s::float, %s::float, %s::bigint)",
                        page_size=500,
                    )

        stats["matches_processed"] += 1
        stats["events_updated"] += len(updates)

        if (i + 1) % batch_size == 0 or (i + 1) == len(match_ids):
            conn.commit()
            log.info(
                "[%d/%d] match_id=%-6s  vaep_scored=%d  total=%d",
                i + 1, len(match_ids), mid, len(updates), stats["events_updated"],
            )

    log.info(
        "Backfill complete: %(matches_processed)d matches, "
        "%(events_updated)d events updated",
        stats,
    )
    return stats


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    import os
    import sys

    import psycopg2
    from dotenv import load_dotenv

    load_dotenv(_REPO_ROOT / ".env")

    parser = argparse.ArgumentParser(description="Backfill VAEP columns in silver.events")
    parser.add_argument(
        "--match-ids", nargs="+", type=int, default=None,
        help="Specific match IDs (default: auto-discover unprocessed)",
    )
    parser.add_argument(
        "--limit", type=int, default=None,
        help="Max matches to process when auto-discovering",
    )
    parser.add_argument("--batch-size", type=int, default=5)
    parser.add_argument(
        "--log-level", default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%H:%M:%S",
    )

    dsn = os.getenv("FOOTBALL_DB_DSN")
    if not dsn:
        log.error("FOOTBALL_DB_DSN not set.")
        sys.exit(1)

    conn = psycopg2.connect(dsn)
    try:
        result = backfill_vaep(
            conn,
            match_ids=args.match_ids,
            limit=args.limit,
            batch_size=args.batch_size,
        )
        print(f"\nDone: {result}")
    except Exception:
        log.exception("Backfill failed")
        conn.rollback()
        sys.exit(1)
    finally:
        conn.close()
