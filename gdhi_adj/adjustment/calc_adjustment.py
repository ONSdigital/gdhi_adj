"""Module for calculations to adjust data in the gdhi_adj project."""

import numpy as np
import pandas as pd

from gdhi_adj.utils.transform_helpers import (
    ensure_list,
    increment_until_not_in,
)


def calc_imputed_val(
    df: pd.DataFrame, start_year: int = 1900, end_year: int = 2100
) -> pd.DataFrame:
    """
    Calculate the imputed value for a given LSOA code where the year has been
    flagged as an outlier to adjust.

    For sequential years flagged for adjustment, the previous and next safe
    years are located (i.e., years not flagged for adjustment). The imputed
    value is then extrapolated from these two safe years.

    For years flagged for adjustment at the start or end of the range, the
    imputed value is extrapolated from the nearest two safe years.

    Args:
        df (pd.DataFrame): DataFrame with data to calculate imputed value.
        start_year (int): Starting year for the data.
        end_year (int): End year for the data.

    Returns:
        pd.DataFrame: DataFrame containing outlier imputed values.
    """
    # ensure year_to_adjust is list-like and normalize missing
    df["year_to_adjust"] = df["year_to_adjust"].apply(ensure_list)

    mask = df.apply(lambda r: (r["year"] in r["year_to_adjust"]), axis=1)

    imputed_df = df.loc[mask].copy()

    # prepare lookup table of values by (lsoa_code, year)
    lookup = df[["lsoa_code", "year", "con_gdhi"]].copy()

    # Find previous year value not flagged to adjust
    imputed_df["prev_safe_year"] = imputed_df.apply(
        lambda r: increment_until_not_in(
            r["year"], r["year_to_adjust"], start_year, is_increasing=False
        ),
        axis=1,
    )
    imputed_df = imputed_df.merge(
        lookup.rename(
            columns={"year": "prev_safe_year", "con_gdhi": "prev_con_gdhi"}
        ),
        on=["lsoa_code", "prev_safe_year"],
        how="left",
    )

    # Find next year value not flagged to adjust
    imputed_df["next_safe_year"] = imputed_df.apply(
        lambda r: increment_until_not_in(
            r["year"], r["year_to_adjust"], end_year, is_increasing=True
        ),
        axis=1,
    )
    imputed_df = imputed_df.merge(
        lookup.rename(
            columns={"year": "next_safe_year", "con_gdhi": "next_con_gdhi"}
        ),
        on=["lsoa_code", "next_safe_year"],
        how="left",
    )

    # Interpolate imputed_gdhi where both previous and next safe years exist
    imputed_df["imputed_gdhi"] = np.where(
        (
            imputed_df["prev_con_gdhi"].notna()
            & imputed_df["next_con_gdhi"].notna()
        ),
        (imputed_df["next_con_gdhi"] - imputed_df["prev_con_gdhi"])
        / (imputed_df["next_safe_year"] - imputed_df["prev_safe_year"])
        * (imputed_df["year"] - imputed_df["prev_safe_year"])
        + imputed_df["prev_con_gdhi"],
        np.NaN,
    )

    # Handle cases where only one side is available for extrapolation
    # Get additional safe year and its value
    imputed_df["additional_safe_year"] = np.where(
        imputed_df["prev_con_gdhi"].isna(),
        (imputed_df["next_safe_year"] + 1),
        np.where(
            imputed_df["next_con_gdhi"].isna(),
            (imputed_df["prev_safe_year"] - 1),
            np.NaN,
        ),
    )
    imputed_df = imputed_df.merge(
        lookup.rename(
            columns={
                "year": "additional_safe_year",
                "con_gdhi": "additional_con_gdhi",
            }
        ),
        on=["lsoa_code", "additional_safe_year"],
        how="left",
    )

    # Extrapolate imputed_gdhi where only one side is available
    imputed_df["imputed_gdhi"] = np.where(
        imputed_df["imputed_gdhi"].isna()
        & imputed_df["additional_con_gdhi"].notna(),
        np.where(
            imputed_df["prev_con_gdhi"].isna(),
            imputed_df["next_con_gdhi"]
            - (imputed_df["additional_con_gdhi"] - imputed_df["next_con_gdhi"])
            * (imputed_df["next_safe_year"] - imputed_df["year"]),
            imputed_df["prev_con_gdhi"]
            + (imputed_df["prev_con_gdhi"] - imputed_df["additional_con_gdhi"])
            * (imputed_df["year"] - imputed_df["prev_safe_year"]),
        ),
        imputed_df["imputed_gdhi"],
    )

    return imputed_df.drop(
        columns=["additional_safe_year", "additional_con_gdhi"]
    )


def calc_imputed_adjustment(
    df: pd.DataFrame,
    imputed_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Calculate the adjustment required for imputing constrained values with
    their respective imputed values.

    Args:
        df (pd.DataFrame): DataFrame containing all data.
        imputed_df (pd.DataFrame): DataFrame containing outlier imputed values.

    Returns:
        adjustment_df (pd.DataFrame): DataFrame containing outlier adjustment
        values.
    """
    adjustment_df = imputed_df[
        ["lsoa_code", "year", "con_gdhi", "imputed_gdhi"]
    ].copy()

    adjustment_df["imputed_diff"] = (
        adjustment_df["con_gdhi"] - adjustment_df["imputed_gdhi"]
    )

    adjustment_df = df.merge(
        adjustment_df[["lsoa_code", "year", "imputed_gdhi", "imputed_diff"]],
        on=["lsoa_code", "year"],
        how="left",
    )
    adjustment_df["adjustment_val"] = adjustment_df.groupby(
        ["lad_code", "year"]
    )["imputed_diff"].transform("sum")

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
        adjusted_df["imputed_gdhi"].notna(),
        adjusted_df["imputed_gdhi"],
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
