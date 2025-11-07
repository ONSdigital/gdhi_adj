"""Module for joining adjustment data in the gdhi_adj project."""

from typing import Any, List

import numpy as np
import pandas as pd


def reformat_adjust_col(df: pd.DataFrame) -> pd.DataFrame:
    """
    Reformat data within the adjust column.

    Args:
        df (pd.DataFrame): Input DataFrame to be reformatted.

    Returns:
        pd.DataFrame: DataFrame with reformatted columns.
    """
    conditions = [df["adjust"] == "TRUE"]
    descriptors = [True]
    df["adjust"] = np.select(conditions, descriptors, default=False)

    df["adjust"] = df["adjust"].astype("bool")

    return df


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


def reformat_year_col(
    df: pd.DataFrame, start_year: int, end_year: int
) -> pd.DataFrame:
    """
    Reformat data within the year column.

    Args:
        df (pd.DataFrame): Input DataFrame to be reformatted.

    Returns:
        pd.DataFrame: DataFrame with reformatted columns.
    """

    # Normalize year cells safely (handle NaN and non-string values)
    def _normalize_year_cell(x: Any) -> str:
        if x is None or pd.isna(x):
            return ""
        return str(x).replace(" ", "")

    df["year"] = df["year"].apply(_normalize_year_cell)
    df["year"] = df["year"].apply(lambda x: x.split(",") if x != "" else [])
    df["year"] = df["year"].apply(to_int_list)

    df["year"] = df["year"].apply(
        lambda x: tuple(x) if isinstance(x, (list, tuple, np.ndarray)) else x
    )

    def _ensure_no_duplicates(seq):
        if len(seq) != len(set(seq)):
            raise ValueError(
                "Duplicate years found in year column within LSOA."
            )

    df["year"].apply(_ensure_no_duplicates)

    # Check that all years specified for adjustment are within valid range
    def _ensure_years_in_range(years, start_year, end_year):
        for year in years:
            if year < start_year or year > end_year:
                raise ValueError(
                    f"Year {year} in year column is out of valid range "
                    f"{start_year}-{end_year}."
                )

    df["year"].apply(
        lambda years: _ensure_years_in_range(years, start_year, end_year)
    )

    return df
