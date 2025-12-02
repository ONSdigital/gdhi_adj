"""Module for calculations to adjust data in the gdhi_adj project."""

import numpy as np
import pandas as pd


def interpolate_imputed_val(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate the imputed value for a given LSOA code where the year that has
    been flagged as an outlier to adjust has a valid safe year either side.

    Args:
        df (pd.DataFrame): DataFrame with data to calculate imputed value.
    Returns:
        pd.DataFrame: DataFrame containing outlier imputed values.
    """
    # Interpolate imputed_gdhi where both previous and next safe years exist
    df["imputed_gdhi"] = np.where(
        (
            df["prev_con_gdhi"].notna()
            & df["next_con_gdhi"].notna()
            & ~df["rollback_flag"]
        ),
        (df["next_con_gdhi"] - df["prev_con_gdhi"])
        / (df["next_safe_year"] - df["prev_safe_year"])
        * (df["year"] - df["prev_safe_year"])
        + df["prev_con_gdhi"],
        np.nan,
    )

    return df


def extrapolate_imputed_val(
    df: pd.DataFrame, imputed_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Calculate the imputed value for a given LSOA code where the year that has
    been flagged as an outlier to adjust only has one valid safe year either
    side.

    The imputed value is extrapolated from the nearest safe year and the
    year 4 years after. This is to avoid short term fluctuations.

    Args:
        df (pd.DataFrame): DataFrame with full data for lookup.
        imputed_df (pd.DataFrame): DataFrame to calculate imputed value.

    Returns:
        pd.DataFrame: DataFrame containing outlier imputed values.
    """
    # prepare lookup table of values by (lsoa_code, year)
    lookup = df[["lsoa_code", "year", "con_gdhi"]].copy()

    # Handle cases where only one side is available for extrapolation
    # Get additional safe year and its value, 4 year difference is used to
    # avoid short term fluctuations
    imputed_df["additional_safe_year"] = np.where(
        # For rollback years only next_con_gdhi should populated
        (imputed_df["prev_con_gdhi"].isna() | imputed_df["rollback_flag"]),
        (imputed_df["next_safe_year"] + 4),
        np.where(
            imputed_df["next_con_gdhi"].isna(),
            (imputed_df["prev_safe_year"] - 4),
            np.nan,
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
            (imputed_df["prev_con_gdhi"].isna() | imputed_df["rollback_flag"]),
            imputed_df["next_con_gdhi"]
            - (
                (
                    imputed_df["additional_con_gdhi"]
                    - imputed_df["next_con_gdhi"]
                )
                / (
                    imputed_df["additional_safe_year"]
                    - imputed_df["next_safe_year"]
                )
            )
            * (imputed_df["next_safe_year"] - imputed_df["year"]),
            imputed_df["prev_con_gdhi"]
            + (
                (
                    imputed_df["prev_con_gdhi"]
                    - imputed_df["additional_con_gdhi"]
                )
                / (
                    imputed_df["prev_safe_year"]
                    - imputed_df["additional_safe_year"]
                )
            )
            * (imputed_df["year"] - imputed_df["prev_safe_year"]),
        ),
        imputed_df["imputed_gdhi"],
    )

    return imputed_df.drop(
        columns=["additional_safe_year", "additional_con_gdhi"]
    )
