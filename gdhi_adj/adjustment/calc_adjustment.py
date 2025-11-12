"""Module for calculations to adjust data in the gdhi_adj project."""

import numpy as np
import pandas as pd
from pandas.api.types import is_list_like


def calc_midpoint_val(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate the midpoint value for a given LSOA code.

    Args:
        df (pd.DataFrame): DataFrame containing data to calculate midpoint.

    Returns:
        pd.DataFrame: DataFrame containing outlier midpoints.
    """

    # ensure year_to_adjust is list-like and normalize missing
    def ensure_list(x):
        # handle list-like first — avoids calling pd.isna on arrays/Series
        if is_list_like(x) and not isinstance(x, (str, bytes)):
            return list(x)
        elif pd.isna(x):
            return []
        elif isinstance(x, (list, tuple, set)):
            return list(x)
        else:
            return [x]

    df["year_to_adjust"] = df["year_to_adjust"].apply(ensure_list)

    mask = df.apply(lambda r: (r["year"] in r["year_to_adjust"]), axis=1)

    midpoint_df = df.loc[mask].copy()

    # prepare lookup table of values by (lsoa_code, year)
    lookup = df[["lsoa_code", "year", "con_gdhi"]].copy()

    # join previous year value
    midpoint_df["prev_year"] = midpoint_df["year"] - 1
    midpoint_df = midpoint_df.merge(
        lookup.rename(
            columns={"year": "prev_year", "con_gdhi": "prev_con_gdhi"}
        ),
        on=["lsoa_code", "prev_year"],
        how="left",
    )

    # join next year value
    midpoint_df["next_year"] = midpoint_df["year"] + 1
    midpoint_df = midpoint_df.merge(
        lookup.rename(
            columns={"year": "next_year", "con_gdhi": "next_con_gdhi"}
        ),
        on=["lsoa_code", "next_year"],
        how="left",
    )

    # midpoint: average of prev and next (NaN if either missing)
    midpoint_df["midpoint"] = midpoint_df[
        ["prev_con_gdhi", "next_con_gdhi"]
    ].mean(axis=1)

    return midpoint_df


def calc_midpoint_adjustment(
    df: pd.DataFrame,
    midpoint_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Calculate the adjustment required for imputing constrained values with
    their respective midpoints.

    Args:
        df (pd.DataFrame): DataFrame containing outlier midpoints.
        midpoint_df (pd.DataFrame): DataFrame containing outlier midpoints.

    Returns:
        adjustment_df (pd.DataFrame): DataFrame containing outlier adjustment
        values.
    """
    adjustment_df = midpoint_df[
        ["lsoa_code", "year", "con_gdhi", "midpoint"]
    ].copy()

    adjustment_df["midpoint_diff"] = (
        adjustment_df["con_gdhi"] - adjustment_df["midpoint"]
    )

    adjustment_df = df.merge(
        adjustment_df[["lsoa_code", "year", "midpoint", "midpoint_diff"]],
        on=["lsoa_code", "year"],
        how="left",
    )
    adjustment_df["adjustment_val"] = adjustment_df.groupby(
        ["lad_code", "year"]
    )["midpoint_diff"].transform("sum")

    return adjustment_df


def apportion_adjustment(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apportion the adjustment values to all years for each LSOA.

    Args:
        df (pd.DataFrame): DataFrame containing data to adjust.

    Returns:
        pd.DataFrame: DataFrame with outlier values imputed and adjustment
        values apportioned accross all years within LSOA.
    """
    adjusted_df = df.copy()

    adjusted_df["lsoa_count"] = adjusted_df.groupby(["lad_code", "year"])[
        "lsoa_code"
    ].transform("count")

    adjusted_df["adjusted_con_gdhi"] = np.where(
        adjusted_df["midpoint"].notna(),
        adjusted_df["midpoint"],
        adjusted_df["con_gdhi"],
    )

    adjusted_df["adjusted_con_gdhi"] += np.where(
        adjusted_df["adjustment_val"].notna(),
        adjusted_df["adjustment_val"] / adjusted_df["lsoa_count"],
        0,
    )

    return adjusted_df.sort_values(by=["lad_code", "year"]).reset_index(
        drop=True
    )


def apportion_negative_adjustment(df: pd.DataFrame) -> pd.DataFrame:
    """
    Change negative values to 0 and apportion negative adjustment values to all
    LSOAs within an LAD/year group.

    Args:
        df (pd.DataFrame): DataFrame containing data to adjust.

    Returns:
        pd.DataFrame: DataFrame with negative adjustment values apportioned
        accross all years within LSOA.
    """
    adjusted_df = df.copy()

    adjusted_df["negative_diff"] = np.where(
        adjusted_df["adjusted_con_gdhi"] < 0,
        0 - adjusted_df["adjusted_con_gdhi"],
        0,
    )

    adjusted_df["adjustment_val"] = adjusted_df.groupby(["lad_code", "year"])[
        "negative_diff"
    ].transform("sum")

    adjusted_df["lsoa_count"] = (
        adjusted_df[adjusted_df["adjusted_con_gdhi"] > 0]
        .groupby(["lad_code", "year"])["lsoa_code"]
        .transform("count")
    )

    zero_lsoa_check = adjusted_df[
        (adjusted_df["adjusted_con_gdhi"] > 0)
        & (~adjusted_df["lsoa_count"].map(lambda x: x >= 0))
    ].empty

    if zero_lsoa_check is False:
        raise ValueError(
            "Zero LSOA count check failed: no LSOAs have been found with a "
            "positive adjusted_con_gdhi value, this will lead to div0 error."
        )

    adjusted_df["adjusted_con_gdhi"] = np.where(
        adjusted_df["adjusted_con_gdhi"] > 0,
        adjusted_df["adjusted_con_gdhi"]
        - (adjusted_df["adjustment_val"] / adjusted_df["lsoa_count"]),
        0,
    )

    # Checks after adjustment
    # Check that there are no negative values in adjusted_con_gdhi
    adjusted_df_check = adjusted_df.copy()
    negative_value_check = adjusted_df_check[
        adjusted_df_check["adjusted_con_gdhi"] < 0
    ].empty

    if negative_value_check is False:
        raise ValueError(
            "Negative value check failed: negative values found in "
            "adjusted_con_gdhi after adjustment."
        )

    # Adjustment check: sums by (lad_code, year) should match pre- and post-
    # adjustment
    adjusted_df_check["unadjusted_sum"] = adjusted_df.groupby(
        ["lad_code", "year"]
    )["con_gdhi"].transform("sum")

    adjusted_df_check["adjusted_sum"] = adjusted_df_check.groupby(
        ["lad_code", "year"]
    )["adjusted_con_gdhi"].transform("sum")

    adjusted_df_check["adjustment_check"] = abs(
        adjusted_df_check["unadjusted_sum"] - adjusted_df_check["adjusted_sum"]
    )

    adjusted_df_check = adjusted_df_check[
        adjusted_df_check["adjustment_check"] > 0.000001
    ]

    if not adjusted_df_check.empty:
        raise ValueError(
            "Adjustment check failed: LAD sums do not match after adjustment."
        )

    return adjusted_df.sort_values(by=["lad_code", "year"]).reset_index(
        drop=True
    )
