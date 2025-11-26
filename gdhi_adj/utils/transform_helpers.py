"""Define helper functions that wrap regularly-used functions."""

from typing import Any, List

import numpy as np
import pandas as pd
from pandas.api.types import is_list_like


def ensure_list(x: any) -> list:
    """Ensure the input is returned as a list.

    Args:
        x (any): Input value to be converted to a list.
    Returns:
        list: The input value wrapped in a list if it was not already a list.
    """
    # handle list-like first — avoids calling pd.isna on arrays/Series
    if is_list_like(x) and not isinstance(x, (str, bytes)):
        return list(x)
    elif pd.isna(x):
        return []
    elif isinstance(x, (list, tuple, set)):
        return list(x)
    else:
        return [x]


def to_int_list(cell: Any) -> List[int]:
    """
    Convert a cell to a list of ints.
    Accepts:
      - a comma-separated string like "2010,2011, 2012"
      - a list/tuple of strings or numbers
      - NaN/None -> returns []
    Raises ValueError if an item cannot be converted to int.
    """
    parts: List[str] = []
    # If already a list/tuple/ndarray (e.g. after split), iterate items
    if isinstance(cell, (list, tuple, np.ndarray, pd.Series)):
        for it in cell:
            if pd.isna(it):
                continue
            s = str(it).strip()
            if s == "" or s.lower() == "nan":
                continue
            parts.append(s)
    else:
        # treat as string otherwise
        s = str(cell).strip()
        if s == "" or s.lower() == "nan":
            return []
        # remove surrounding brackets optionally: "[2001,2002]" -> "2001,2002"
        parts = [p.strip() for p in s.split(",") if p.strip() != ""]

    out: List[int] = []
    for token in parts:
        try:
            out.append(int(token))
        except ValueError:
            try:
                out.append(int(float(token)))
            except Exception:
                raise ValueError(
                    f"Cannot convert value {token!r} to int in cell {cell!r}"
                )
    return out


def increment_until_not_in(
    year: int, adjust_years: list, limit_year: int, is_increasing: bool = True
):
    """Increase or decrease year until it is not in a list of adjust_years.
    Args:
        year (int): The starting year.
        adjust_years (list): List of years to avoid.
        limit_year (int): The limit year to stop at.
        is_increasing (bool): If True, increase year; if False, decrease year.
    Returns:
        int: The first year not in adjust_years list.
    """
    if is_increasing:
        adjust_years_set = (
            set(adjust_years) if adjust_years is not None else set()
        )
        while year in adjust_years_set and year < (limit_year + 1):
            year += 1
        return year
    else:
        adjust_years_set = (
            set(adjust_years) if adjust_years is not None else set()
        )
        while year in adjust_years_set and year > (limit_year - 1):
            year -= 1
        return year


def sum_match_check(
    df: pd.DataFrame,
    grouping_cols: list,
    unadjusted_col: str,
    adjusted_col: str,
    sum_tolerance: float = 0.000001,
):
    """
    Check that the sums of adjusted column, matches that of the unadjusted
    column for the same groupings.

    If the difference exceeds a specified tolerance, raise an error.

    Args:
        df (pd.DataFrame): DataFrame containing data for sums.
        grouping_cols: (list): List of columns to group for sums.
        unadjusted_col (str): Unadjusted column.
        adjusted_col (str): Adjsuted column
        sum_tolerance (float): Tolerance for the sums to match, default is
        based on the floating point error: 0.000001.

    Returns:
        ValueError: if adjusted and unadjusted sums do not match.
    """
    df["unadjusted_sum"] = df.groupby(grouping_cols)[unadjusted_col].transform(
        "sum"
    )

    df["adjusted_sum"] = df.groupby(grouping_cols)[adjusted_col].transform(
        "sum"
    )

    df["adjustment_check"] = abs(df["unadjusted_sum"] - df["adjusted_sum"])

    df = df[df["adjustment_check"] > sum_tolerance]

    if not df.empty:
        raise ValueError(
            "Adjustment check failed: LAD sums do not match after adjustment."
        )
