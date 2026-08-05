"""
vaep.py
───────
VAEP (Valuing Actions by Estimating Probabilities) column calculation.

Uses a pre-trained XGBoost model:
  https://huggingface.co/luxury-lakehouse/vaep-model-statsbomb-wyscout

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
_PERIOD_OFFSETS: dict = {1: 0, 2: 45, 3: 90, 4: 105}

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
# Fixed pre-action scoring odds, taken from socceraction's reference values
_PENALTY_PREV_SCORES = 0.792453
_CORNER_PREV_SCORES  = 0.046500

# ── Model cache ───────────────────────────────────────────────────────────────

_CACHE: dict = {}


def _load_models():
    """Load and cache the two XGBoost classifiers from vaep_model.json."""
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

        _CACHE["scores"]        = m_scores
        _CACHE["concedes"]      = m_concedes
        _CACHE["feature_names"] = metrics["config"]["feature_names"]
        log.info("VAEP models loaded from %s", _MODEL_PATH)

    return _CACHE["scores"], _CACHE["concedes"], _CACHE["feature_names"]


# ── Feature engineering ───────────────────────────────────────────────────────

def _shift(arr: np.ndarray, k: int) -> np.ndarray:
    """Shift array right by k, zero-padding the leading k positions."""
    if k == 0:
        return arr.copy()
    out = np.zeros(len(arr), dtype=arr.dtype)
    out[k:] = arr[:-k]
    return out


def _polar(x: np.ndarray, y: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
    """Distance and angle (radians) from each position to the attacking goal."""
    dx    = _GOAL_X - x
    dy    = y - _GOAL_Y
    dist  = np.sqrt(dx ** 2 + dy ** 2)
    angle = np.arctan2(np.abs(dy), np.abs(dx))
    return dist, angle


def _build_features(valid_df: pd.DataFrame, feature_names: List[str]) -> pd.DataFrame:
    """
    Build the 145-feature matrix for one match's SPADL-valid events.

    valid_df must already be:
      - filtered to rows where spadl_type_id IS NOT NULL
      - sorted by json_index (ascending)
      - reset to a 0-based integer index

    Returns a DataFrame with exactly 145 columns in model order.
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

    # Position index, used to spot the leading rows that _shift zero-pads
    positions = np.arange(n)

    data: dict = {}

    for k, suffix in enumerate(["a0", "a1", "a2"]):
        s_type    = _shift(type_id,      k)
        s_result  = _shift(result_id,    k)
        s_body    = _shift(body_id,      k)
        s_sx      = _shift(sx,           k)
        s_sy      = _shift(sy,           k)
        s_ex      = _shift(ex,           k)
        s_ey      = _shift(ey,           k)
        s_to      = _shift(time_overall, k)
        s_tp      = _shift(time_period,  k)
        s_per     = _shift(period,       k)
        s_team    = _shift(team,         k)

        # Put the whole game state in a0's attacking frame, mirroring actions
        # by the other team 180° about the pitch centre.  This is socceraction's
        # play_left_to_right(gamestates, ...), which flips a0/a1/a2 together on
        # a0's team, so x=105 is a0's attacking goal in every frame.
        # Leading rows (position < k) are _shift zero-padding, not real actions,
        # so they are left untouched rather than mirrored to the far corner.
        flip = (s_team != team) & (positions >= k)
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

        # Body part one-hot (4 columns)
        # foot_left (4) and foot_right (5) collapse into foot
        data[f"bodypart_foot_{suffix}"]       = np.isin(s_body, [0, 4, 5]).astype(float)
        data[f"bodypart_head_{suffix}"]       = (s_body == 1).astype(float)
        data[f"bodypart_other_{suffix}"]      = (s_body == 2).astype(float)
        data[f"bodypart_head/other_{suffix}"] = np.zeros(n, dtype=float)

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

    # Team change flags between consecutive game-state positions
    t_a0 = team
    t_a1 = _shift(team, 1)
    t_a2 = _shift(team, 2)
    data["team_1"] = (t_a0 != t_a1).astype(float)
    data["team_2"] = (t_a1 != t_a2).astype(float)

    # Time elapsed between consecutive game-state positions (seconds)
    data["time_delta_1"] = time_overall - _shift(time_overall, 1)
    data["time_delta_2"] = _shift(time_overall, 1) - _shift(time_overall, 2)

    return pd.DataFrame(data, columns=feature_names)


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

    # Isolate SPADL-valid events in chronological order; preserve original indices
    valid_df = (
        df.loc[valid_mask]
        .sort_values("json_index", na_position="last")
        .reset_index()  # original df index goes into column "index"
    )
    orig_idx = valid_df["index"].to_numpy()

    try:
        m_scores, m_concedes, feature_names = _load_models()
        X = _build_features(valid_df, feature_names)

        p_scores   = m_scores.predict_proba(X)[:, 1]
        p_concedes = m_concedes.predict_proba(X)[:, 1]

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
        t_secs    = (valid_df["minute"].fillna(0).to_numpy(dtype=float) * 60.0
                     + valid_df["second"].fillna(0).to_numpy(dtype=float))
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
