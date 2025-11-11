"""Module for calculations to adjust data in the gdhi_adj project."""

import numpy as np
import pandas as pd
from pandas.api.types import is_list_like

# from scipy.interpolate import interp1d


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

    imputed_df = df.loc[mask].copy()

    # prepare lookup table of values by (lsoa_code, year)
    lookup = df[["lsoa_code", "year", "con_gdhi"]].copy()

    # Find previous year value not flagged to adjust
    def decrease_until_not_in(year, adjust_years, start_year):
        # convert to set for speed (if it's not already)
        adjust_years_set = (
            set(adjust_years) if adjust_years is not None else set()
        )
        while year in adjust_years_set and year > (start_year - 1):
            year -= 1
        return year

    imputed_df["prev_safe_year"] = imputed_df.apply(
        lambda r: decrease_until_not_in(
            r["year"], r["year_to_adjust"], start_year
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
    def increase_until_not_in(year, adjust_years, end_year):
        # convert to set for speed (if it's not already)
        adjust_years_set = (
            set(adjust_years) if adjust_years is not None else set()
        )
        while year in adjust_years_set and year < (end_year + 1):
            year += 1
        return year

    imputed_df["next_safe_year"] = imputed_df.apply(
        lambda r: increase_until_not_in(
            r["year"], r["year_to_adjust"], end_year
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

    return imputed_df


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

    # Adjustment check: sums by (lad_code, year) should match pre- and post-
    # adjustment
    adjusted_df_check = adjusted_df.copy()
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
