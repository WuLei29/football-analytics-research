"""
Compute xG for shots in silver.events.

Model: XGBoost trained on StatsBomb open data (~82k shots).
Two models: open-play (22 features, Platt-calibrated) and free-kick
(11 features, raw — its shipped calibrator is degenerate, see _load_models).
Penalties are never modelled (different generative process) — they are assigned
the fixed Opta value PENALTY_XG. They are also kept in the query so
score_diff_at_shot is accurate for the shots around them.

Artifacts required in models/xg/ (relative to this file):
  06_xgboost_full.json, 07_platt_xgboost_full.joblib, fk_xgboost.json
  (fk_platt_calibrator.joblib is present but intentionally unused)

Usage (adjust module path to your project layout):
    python -m models.compute_xg                    # matches with NULL xG
    python -m models.compute_xg --match-ids 1 2 3  # specific matches
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd
import psycopg2
import psycopg2.extras
from xgboost import XGBClassifier

# ---------------------------------------------------------------------------
# Pitch & feature constants (from xg-model/configs/features.yaml)
# ---------------------------------------------------------------------------

GOAL_X = 105.0
GOAL_Y_CENTER = 34.0
POST_L = 30.34   # left post y (34 - 3.66)
POST_R = 37.66   # right post y (34 + 3.66)
CENTRAL_HW = 8.5  # |y - 34| <= 8.5 → central corridor

OPEN_PLAY_FEATURES: list[str] = [
    "distance_to_goal", "visible_angle", "distance_to_near_post",
    "distance_to_far_post", "x_from_goal_line", "is_central",
    "is_header", "is_right_foot", "is_left_foot", "is_other_bodypart",
    "is_normal_technique", "is_volley", "is_diving_header",
    "is_exotic_technique", "is_regular_play", "is_from_counter",
    "is_from_set_piece_restart", "is_first_time", "is_weak_foot",
    "minute", "score_diff_at_shot", "is_late_game",
]

FREEKICK_FEATURES: list[str] = [
    "distance_to_goal", "visible_angle", "distance_to_near_post",
    "distance_to_far_post", "x_from_goal_line", "is_central",
    "is_right_foot", "is_left_foot",
    "minute", "score_diff_at_shot", "is_late_game",
]

# SPADL type IDs
_SHOT = 11
_SHOT_PENALTY = 12
_SHOT_FREEKICK = 13
_XG_ELIGIBLE = {_SHOT, _SHOT_FREEKICK}

# Opta's fixed xG for a penalty kick. Penalties bypass the model entirely.
PENALTY_XG = 0.79

# SPADL bodypart → xG model label
_BODYPART_MAP: dict[int, str | None] = {
    1: "Head",        # HEAD
    4: "Left Foot",   # FOOT_LEFT
    5: "Right Foot",  # FOOT_RIGHT
    2: "Other",       # OTHER
    0: None,          # generic FOOT → resolved via preferred_foot
}

# Opta qualifier IDs → shot technique
_TECHNIQUE_QUALIFIERS: dict[int, str] = {
    108: "Volley",
    109: "Overhead Kick",
    117: "Lob",
    262: "Backheel",
}

# silver.events.shot_play_pattern → xG model play_pattern
_PLAY_PATTERN_MAP: dict[str, str] = {
    "regular_play": "Regular Play",
    "fast_break": "From Counter",
    "set_piece": "From Free Kick",
    "from_corner": "From Corner",
    "throw_in_set_piece": "From Throw In",
}

# ---------------------------------------------------------------------------
# Paths & model cache
# ---------------------------------------------------------------------------

ROOT = Path(__file__).resolve().parents[3]
MODELS_DIR = ROOT / "models" / "xg"

EPS = 1e-7
_models: dict[str, Any] = {}

# ---------------------------------------------------------------------------
# SQL
# ---------------------------------------------------------------------------

_SHOTS_QUERY = """
SELECT
    e.event_id,
    e.source_event_id,
    e.match_id,
    e.team_id,
    e.player_id,
    e.event_type,
    e.spadl_type_id,
    e.spadl_bodypart_id,
    e.shot_play_pattern,
    e.first_time,
    e.is_weak_foot,
    e.x,
    e.y,
    e.minute,
    e.second,
    e.period,
    e.raw_data,
    p.preferred_foot
FROM silver.events e
LEFT JOIN silver.players p ON p.player_id = e.player_id
WHERE e.spadl_type_id IN (11, 12, 13)
  AND e.spadl_result_id IS DISTINCT FROM 3
  {match_filter}
ORDER BY e.match_id, e.period, e.minute, e.second
"""

_NULL_XG_MATCHES = """
AND e.match_id IN (
    SELECT DISTINCT match_id FROM silver.events
    WHERE spadl_type_id IN (11, 12, 13)
      AND spadl_result_id IS DISTINCT FROM 3
      AND xg IS NULL
)
"""

# Keyed on the primary key: source_event_id is not indexed, so keying on it
# makes every UPDATE a sequential scan of the whole events table.
_UPDATE_XG = "UPDATE silver.events SET xg = %s WHERE event_id = %s"

# ---------------------------------------------------------------------------
# Model loading
# ---------------------------------------------------------------------------

def _load_models() -> dict[str, Any]:
    if _models:
        return _models

    print("Loading open-play model ...", flush=True)
    op = XGBClassifier()
    op.load_model(str(MODELS_DIR / "06_xgboost_full.json"))
    _models["op_model"] = op
    _models["op_cal"] = joblib.load(MODELS_DIR / "07_platt_xgboost_full.joblib")
    print("  open-play model loaded", flush=True)

    print("Loading free-kick model ...", flush=True)
    fk = XGBClassifier()
    fk.load_model(str(MODELS_DIR / "fk_xgboost.json"))
    _models["fk_model"] = fk
    print("  free-kick model loaded (uncalibrated — see note below)", flush=True)

    # models/xg/fk_platt_calibrator.joblib is deliberately NOT loaded.
    # It is degenerate: slope 15.75, intercept -3.75 (vs 1.03 / 0.05 for the
    # open-play calibrator), fit with C=1e9 on a small free-kick sample. It maps
    # sigmoid(15.75 * logit(p) - 3.75), which collapses every realistic free-kick
    # probability to ~1e-17 — it zeroed out all 293 free kicks in the dataset.
    # The raw model is already well calibrated on Opta: 16.65 predicted vs 14
    # actual goals over those 293 shots (0.67 sd). Refit the calibrator in the
    # xg-model repo before reinstating it here.

    return _models


def _logit(p: np.ndarray) -> np.ndarray:
    p = np.clip(p, EPS, 1 - EPS)
    return np.log(p / (1 - p))


def _apply_platt(cal: Any, p_raw: np.ndarray) -> np.ndarray:
    return cal.predict_proba(_logit(p_raw).reshape(-1, 1))[:, 1]


# ---------------------------------------------------------------------------
# Column mapping: silver.events → xG model schema
# ---------------------------------------------------------------------------

def _extract_technique(raw_data: Any) -> str:
    if raw_data is None:
        return "Normal"
    if isinstance(raw_data, str):
        try:
            raw_data = json.loads(raw_data)
        except (json.JSONDecodeError, ValueError):
            return "Normal"
    if not isinstance(raw_data, dict):
        return "Normal"
    for q in raw_data.get("qualifier", []):
        qid = q.get("qualifierId") or q.get("id")
        if qid is not None and int(qid) in _TECHNIQUE_QUALIFIERS:
            return _TECHNIQUE_QUALIFIERS[int(qid)]
    return "Normal"


def _map_body_part(spadl_id: int | None, preferred_foot: str | None) -> str:
    if spadl_id is not None:
        label = _BODYPART_MAP.get(int(spadl_id))
        if label is not None:
            return label
    if preferred_foot == "left":
        return "Left Foot"
    return "Right Foot"


def _map_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame(index=df.index)

    out["event_id"] = df["event_id"]
    out["source_event_id"] = df["source_event_id"]
    out["match_id"] = df["match_id"]
    out["team_id"] = df["team_id"]
    out["period"] = df["period"]
    out["second"] = df["second"]
    out["minute"] = df["minute"]
    out["spadl_type_id"] = df["spadl_type_id"]

    out["location_x"] = df["x"].astype(float)
    out["location_y"] = df["y"].astype(float)

    out["is_goal"] = (df["event_type"] == "Goal").astype(int)

    out["shot_type"] = np.where(
        df["spadl_type_id"] == _SHOT_FREEKICK, "Free Kick", "Open Play"
    )

    out["shot_body_part"] = [
        _map_body_part(bp, pf)
        for bp, pf in zip(df["spadl_bodypart_id"], df["preferred_foot"])
    ]

    out["shot_technique"] = df["raw_data"].apply(_extract_technique)

    out["play_pattern"] = (
        df["shot_play_pattern"]
        .map(_PLAY_PATTERN_MAP)
        .fillna("Other")
    )

    out["is_first_time"] = df["first_time"].astype("boolean").fillna(False).astype(int)
    out["is_weak_foot"] = df["is_weak_foot"].astype("boolean").fillna(False).astype(int)

    return out


# ---------------------------------------------------------------------------
# Feature engineering (from xg-model/src/features/build.py)
# ---------------------------------------------------------------------------

def _add_spatial(df: pd.DataFrame) -> pd.DataFrame:
    x = df["location_x"].to_numpy(dtype=float)
    y = df["location_y"].to_numpy(dtype=float)
    dx = GOAL_X - x

    df["distance_to_goal"] = np.sqrt(dx**2 + (GOAL_Y_CENTER - y) ** 2)

    angle_l = np.arctan2(POST_L - y, dx)
    angle_r = np.arctan2(POST_R - y, dx)
    df["visible_angle"] = np.degrees(np.abs(angle_r - angle_l))

    near_y = np.where(y <= GOAL_Y_CENTER, POST_L, POST_R)
    far_y = np.where(y <= GOAL_Y_CENTER, POST_R, POST_L)
    df["distance_to_near_post"] = np.sqrt(dx**2 + (near_y - y) ** 2)
    df["distance_to_far_post"] = np.sqrt(dx**2 + (far_y - y) ** 2)

    df["x_from_goal_line"] = dx
    df["is_central"] = (np.abs(y - GOAL_Y_CENTER) <= CENTRAL_HW).astype(int)
    return df


def _add_categorical(df: pd.DataFrame) -> pd.DataFrame:
    bp = df["shot_body_part"]
    df["is_header"] = (bp == "Head").astype(int)
    df["is_right_foot"] = (bp == "Right Foot").astype(int)
    df["is_left_foot"] = (bp == "Left Foot").astype(int)
    df["is_other_bodypart"] = bp.isin({"Other", "Unknown"}).astype(int)

    tech = df["shot_technique"]
    df["is_normal_technique"] = tech.isin({"Normal", "None"}).astype(int)
    df["is_volley"] = tech.isin({"Volley", "Half Volley"}).astype(int)
    df["is_diving_header"] = (tech == "Diving Header").astype(int)
    df["is_exotic_technique"] = tech.isin(
        {"Backheel", "Overhead Kick", "Lob"}
    ).astype(int)

    pp = df["play_pattern"]
    df["is_regular_play"] = (pp == "Regular Play").astype(int)
    df["is_from_counter"] = (pp == "From Counter").astype(int)
    df["is_from_set_piece_restart"] = pp.isin(
        {"From Corner", "From Free Kick", "From Throw In"}
    ).astype(int)
    return df


def _add_contextual(df: pd.DataFrame) -> pd.DataFrame:
    for col in ("is_first_time", "is_weak_foot"):
        df[col] = df[col].fillna(0).astype(int)
    return df


def _add_gamestate(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values(
        ["match_id", "period", "minute", "second"], kind="mergesort"
    ).reset_index(drop=True)

    team_goals = (
        df.groupby(["match_id", "team_id"])["is_goal"]
        .cumsum()
        .sub(df["is_goal"])
    )
    total_goals = (
        df.groupby("match_id")["is_goal"].cumsum().sub(df["is_goal"])
    )
    df["score_diff_at_shot"] = (team_goals - (total_goals - team_goals)).astype(int)
    df["is_late_game"] = (df["minute"] > 75).astype(int)
    return df


def _build_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df = _add_spatial(df)
    df = _add_categorical(df)
    df = _add_contextual(df)
    df = _add_gamestate(df)
    return df


# ---------------------------------------------------------------------------
# Inference
# ---------------------------------------------------------------------------

def compute_xg(df: pd.DataFrame) -> pd.DataFrame:
    """
    Full pipeline: map Opta columns → build features → predict.

    Parameters
    ----------
    df : DataFrame from the shots query (all shot types including penalties).

    Returns
    -------
    DataFrame with (event_id, source_event_id, xg) for every scorable shot:
    open-play and free-kick from the models, penalties at the fixed
    PENALTY_XG value. Own goals are excluded upstream by the query.
    """
    print("Building features ...", flush=True)
    mapped = _map_columns(df)
    featured = _build_features(mapped)

    frames: list[pd.DataFrame] = []

    # Penalties bypass the model: fixed Opta value.
    pens = featured[featured["spadl_type_id"] == _SHOT_PENALTY]
    if len(pens):
        print(f"Assigning fixed xG {PENALTY_XG} to {len(pens):,} penalties ...", flush=True)
        frames.append(pd.DataFrame({
            "event_id": pens["event_id"].values,
            "source_event_id": pens["source_event_id"].values,
            "xg": np.full(len(pens), PENALTY_XG, dtype=float),
        }))

    eligible = featured[featured["spadl_type_id"].isin(_XG_ELIGIBLE)].copy()
    if len(eligible):
        models = _load_models()
        is_fk = eligible["shot_type"] == "Free Kick"
        xg = pd.Series(np.nan, index=eligible.index, dtype=float)

        op = eligible[~is_fk]
        if len(op):
            print(f"Predicting open-play xG for {len(op):,} shots ...", flush=True)
            p_raw = models["op_model"].predict_proba(op[OPEN_PLAY_FEATURES].values)[:, 1]
            xg.loc[op.index] = _apply_platt(models["op_cal"], p_raw)

        fk = eligible[is_fk]
        if len(fk):
            print(f"Predicting free-kick xG for {len(fk):,} shots ...", flush=True)
            # Raw output, no Platt step — the shipped FK calibrator is degenerate.
            xg.loc[fk.index] = models["fk_model"].predict_proba(
                fk[FREEKICK_FEATURES].values
            )[:, 1]

        frames.append(pd.DataFrame({
            "event_id": eligible["event_id"].values,
            "source_event_id": eligible["source_event_id"].values,
            "xg": xg.values,
        }))

    if not frames:
        return pd.DataFrame(columns=["event_id", "source_event_id", "xg"])
    return pd.concat(frames, ignore_index=True)


# ---------------------------------------------------------------------------
# DB operations
# ---------------------------------------------------------------------------

def get_connection():
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
    dsn = os.environ.get("FOOTBALL_DB_DSN")
    if not dsn:
        raise EnvironmentError("FOOTBALL_DB_DSN not set in environment / .env file")
    return psycopg2.connect(dsn)


def fetch_shots(
    conn, match_ids: list[int] | None = None
) -> pd.DataFrame:
    if match_ids:
        placeholders = ",".join(["%s"] * len(match_ids))
        match_filter = f"AND e.match_id IN ({placeholders})"
        params: tuple = tuple(match_ids)
    else:
        match_filter = _NULL_XG_MATCHES
        params = ()

    query = _SHOTS_QUERY.format(match_filter=match_filter)
    with conn.cursor() as cur:
        cur.execute(query, params)
        cols = [desc[0] for desc in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)


def write_xg(conn, results: pd.DataFrame) -> int:
    rows = list(zip(results["xg"], results["event_id"]))
    total = len(rows)
    page_size = 500
    print(f"Writing {total:,} xG values to DB ...", flush=True)
    with conn.cursor() as cur:
        for i in range(0, total, page_size):
            batch = rows[i : i + page_size]
            psycopg2.extras.execute_batch(cur, _UPDATE_XG, batch, page_size=page_size)
            print(f"  {min(i + page_size, total):,}/{total:,} written", flush=True)
    conn.commit()
    return total


# ---------------------------------------------------------------------------
# Pipeline API
# ---------------------------------------------------------------------------

def backfill_xg(conn) -> dict:
    """
    Compute xG for all shots with NULL xg in silver.events.

    Returns {"matches_processed": int, "events_updated": int}.
    """
    df = fetch_shots(conn)
    if df.empty:
        return {"matches_processed": 0, "events_updated": 0}

    n_matches = df["match_id"].nunique()
    results = compute_xg(df)
    if results.empty:
        return {"matches_processed": n_matches, "events_updated": 0}

    updated = write_xg(conn, results)
    return {"matches_processed": n_matches, "events_updated": updated}


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(
        description="Compute xG for shots in silver.events"
    )
    parser.add_argument(
        "--match-ids", nargs="*", type=int,
        help="Specific match_ids (default: all matches with NULL xG)",
    )
    args = parser.parse_args()

    conn = get_connection()
    try:
        df = fetch_shots(conn, args.match_ids)
        if df.empty:
            print("No shots found.")
            return

        n_matches = df["match_id"].nunique()
        n_penalties = (df["spadl_type_id"] == _SHOT_PENALTY).sum()
        print(
            f"Loaded {len(df):,} shots from {n_matches} matches "
            f"({n_penalties} penalties at fixed xG {PENALTY_XG})"
        )

        results = compute_xg(df)
        if results.empty:
            print("No xG-eligible shots to update.")
            return

        scored_types = df.loc[
            df["source_event_id"].isin(results["source_event_id"]), "spadl_type_id"
        ]
        n_fk = (scored_types == _SHOT_FREEKICK).sum()
        n_pen = (scored_types == _SHOT_PENALTY).sum()
        n_op = len(results) - n_fk - n_pen

        updated = write_xg(conn, results)
        print(
            f"Updated {updated:,} shots "
            f"(open-play: {n_op}, free-kick: {n_fk}, penalty: {n_pen})"
        )
        print(f"  Mean xG : {results['xg'].mean():.4f}")
        print(f"  Range   : [{results['xg'].min():.4f}, {results['xg'].max():.4f}]")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
