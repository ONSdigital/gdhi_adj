"""Module for apportioning values from adjustment in the gdhi_adj project."""

import numpy as np
import pandas as pd

from gdhi_adj.utils.transform_helpers import sum_match_check


def calc_non_outlier_proportions(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate the proportion of a non-outlier LSOA to the LAD for each year.

    Args:
        df (pd.DataFrame): DataFrame containing all GDHI data.

    Returns:
        pd.DataFrame: DataFrame with proportions for non-outlier LSOAs
        calculated per year/LAD group.
    """
    # Filter into outlier and non-outlier LSOAs that need adjusting
    mask = df.apply(lambda r: (r["year"] in r["year_to_adjust"]), axis=1)

    # Calculate the total GDHI for each LAD per year
    df["lad_total"] = df.groupby(["lad_code", "year"])["con_gdhi"].transform(
        "sum"
    )

    # Calculate the total GDHI for each LAD per year for non outlier years
    df["non_outlier_total"] = (
        df[~mask].groupby(["lad_code", "year"])["con_gdhi"].transform("sum")
    )

    # Guard: if any non_outlier_total is zero this will cause div-by-zero
    # when calculating proportions — raise a clear error with offending groups.
    zero_mask = df["non_outlier_total"] == 0
    if zero_mask.any():
        bad_groups = (
            df.loc[zero_mask, ["lad_code", "year"]]
            .drop_duplicates()
            .sort_values(["lad_code", "year"])  # deterministic order
        )
        # convert to a compact list of tuples for a friendly error message
        bad_list = [tuple(x) for x in bad_groups.values.tolist()]
        raise ValueError(
            "Non-outlier total check failed: found zero non_outlier_total for "
            f"groups: {bad_list}"
        )

    df["gdhi_proportion"] = df["con_gdhi"] / df["non_outlier_total"]

    return df


def apportion_adjustment(
    df: pd.DataFrame, imputed_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Apportion the adjustment values to all years for each LSOA.

    Args:
        df (pd.DataFrame): DataFrame containing data to adjust.
        imputed_df (pd.DataFrame): DataFrame containing outlier imputed values.

    Returns:
        pd.DataFrame: DataFrame with outlier values imputed and adjustment.
        values apportioned accross all years within LSOA.
    """
    adjusted_df = df.merge(
        imputed_df[["lsoa_code", "year", "imputed_gdhi"]],
        on=["lsoa_code", "year"],
        how="left",
    )

    adjusted_df["adjusted_total"] = adjusted_df[
        "lad_total"
    ] - adjusted_df.groupby(["lad_code", "year"])["imputed_gdhi"].transform(
        "sum"
    )

    adjusted_df["adjusted_con_gdhi"] = np.where(
        adjusted_df["imputed_gdhi"].notna(),
        adjusted_df["imputed_gdhi"],
        adjusted_df["gdhi_proportion"] * adjusted_df["adjusted_total"],
    )

    # Adjustment check: sums by (lad_code, year) should match pre- and post-
    # adjustment
    adjusted_sum_check_df = adjusted_df.copy()
    sum_match_check(
        adjusted_sum_check_df,
        grouping_cols=["lad_code", "year"],
        unadjusted_col="con_gdhi",
        adjusted_col="adjusted_con_gdhi",
        sum_tolerance=0.000001,
    )

    return adjusted_df


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
    adjusted_sum_check_df = adjusted_df.copy()
    sum_match_check(
        adjusted_sum_check_df,
        grouping_cols=["lad_code", "year"],
        unadjusted_col="con_gdhi",
        adjusted_col="adjusted_con_gdhi",
        sum_tolerance=0.000001,
    )

    return adjusted_df.sort_values(by=["lad_code", "year"]).reset_index(
        drop=True
    )
