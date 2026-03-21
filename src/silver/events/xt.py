"""
xt.py
─────
Expected Threat (xT) calculation.

Stateless: takes a DataFrame, returns a DataFrame.
No DB access. No parsing. No carries.
"""

import numpy as np
import pandas as pd
from typing import Optional


# 8 rows (Y: 0-68) × 12 columns (X: 0-105)
# Row 0 = bottom of pitch, col 0 = own goal line
XT_GRID = np.array([
    [0.00638303, 0.00779616, 0.00844854, 0.00977659, 0.01126267, 0.01248344, 0.01473596, 0.0174506,  0.02122129, 0.02756312, 0.03485072, 0.0379259 ],
    [0.00750072, 0.00878589, 0.00942382, 0.0105949,  0.01214719, 0.0138454,  0.01611813, 0.01870347, 0.02401521, 0.02953272, 0.04066992, 0.04647721],
    [0.0088799,  0.00977745, 0.01001304, 0.01110462, 0.01269174, 0.01429128, 0.01685596, 0.01935132, 0.0241224,  0.02855202, 0.05491138, 0.06442595],
    [0.00941056, 0.01082722, 0.01016549, 0.01132376, 0.01262646, 0.01484598, 0.01689528, 0.0199707,  0.02385149, 0.03511326, 0.10805102, 0.25745362],
    [0.00941056, 0.01082722, 0.01016549, 0.01132376, 0.01262646, 0.01484598, 0.01689528, 0.0199707,  0.02385149, 0.03511326, 0.10805102, 0.25745362],
    [0.0088799,  0.00977745, 0.01001304, 0.01110462, 0.01269174, 0.01429128, 0.01685596, 0.01935132, 0.0241224,  0.02855202, 0.05491138, 0.06442595],
    [0.00750072, 0.00878589, 0.00942382, 0.0105949,  0.01214719, 0.0138454,  0.01611813, 0.01870347, 0.02401521, 0.02953272, 0.04066992, 0.04647721],
    [0.00638303, 0.00779616, 0.00844854, 0.00977659, 0.01126267, 0.01248344, 0.01473596, 0.0174506,  0.02122129, 0.02756312, 0.03485072, 0.0379259 ],
])


def _get_xt_value(x: Optional[float], y: Optional[float]) -> Optional[float]:
    if x is None or y is None or np.isnan(x) or np.isnan(y):
        return None
    col = int(np.clip(x / 105 * 12, 0, 11))
    row = int(np.clip(y / 68  *  8, 0,  7))
    return float(XT_GRID[row, col])


def calculate_xt(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add start_zone_value_xt, end_zone_value_xt, and xt columns for
    Pass and Carry events. All other rows get NULL.

    Args:
        df: Events DataFrame. Must have columns: event_type, x, y, end_x, end_y.

    Returns:
        DataFrame with the three xT columns filled in.
    """
    df = df.copy()
    df["start_zone_value_xt"] = np.nan
    df["end_zone_value_xt"]   = np.nan
    df["xt"]                  = np.nan

    mask = df["event_type"].isin(["Pass", "Carry"])
    if not mask.any():
        return df

    df.loc[mask, "start_zone_value_xt"] = df.loc[mask].apply(
        lambda r: _get_xt_value(r["x"], r["y"]), axis=1
    )
    df.loc[mask, "end_zone_value_xt"] = df.loc[mask].apply(
        lambda r: _get_xt_value(r["end_x"], r["end_y"]), axis=1
    )
    df.loc[mask, "xt"] = (
        df.loc[mask, "end_zone_value_xt"] - df.loc[mask, "start_zone_value_xt"]
    )

    return df