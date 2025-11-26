"""Module for joining adjustment data in the gdhi_adj project."""

from typing import Any

import numpy as np
import pandas as pd

from gdhi_adj.utils.transform_helpers import to_int_list


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
