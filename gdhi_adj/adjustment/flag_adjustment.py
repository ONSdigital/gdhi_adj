"""Module for flagging data to adjust data in the gdhi_adj project."""

import pandas as pd

from gdhi_adj.utils.transform_helpers import (
    ensure_list,
    increment_until_not_in,
)


def identify_safe_years(
    df: pd.DataFrame, start_year: int = 1900, end_year: int = 2100
) -> pd.DataFrame:
    """
    Identify safe years for each LSOA where no adjustment is needed.

    For sequential years flagged for adjustment, the previous and next safe
    years are located at the end of the sequence of years.

    For end of range years flagged for adjustment, it will return one safe year
    in the range, and one outside, which will return NaN for con_gdhi

    Args:
        df (pd.DataFrame): The input DataFrame.
        start_year (int): The starting year for the data range.
        end_year (int): The ending year for the data range.
    Returns:
        pd.DataFrame: DataFrame with additional columns for safe years.
    """
    # ensure year_to_adjust is list-like and normalize missing
    df["year_to_adjust"] = df["year_to_adjust"].apply(ensure_list)

    mask = df.apply(lambda r: (r["year"] in r["year_to_adjust"]), axis=1)

    safe_years_df = df.loc[mask].copy()

    # prepare lookup table of values by (lsoa_code, year)
    lookup = df[["lsoa_code", "year", "con_gdhi"]].copy()

    # Find previous year value not flagged to adjust
    safe_years_df["prev_safe_year"] = safe_years_df.apply(
        lambda r: increment_until_not_in(
            r["year"], r["year_to_adjust"], start_year, is_increasing=False
        ),
        axis=1,
    )
    safe_years_df = safe_years_df.merge(
        lookup.rename(
            columns={"year": "prev_safe_year", "con_gdhi": "prev_con_gdhi"}
        ),
        on=["lsoa_code", "prev_safe_year"],
        how="left",
    )

    # Find next year value not flagged to adjust
    safe_years_df["next_safe_year"] = safe_years_df.apply(
        lambda r: increment_until_not_in(
            r["year"], r["year_to_adjust"], end_year, is_increasing=True
        ),
        axis=1,
    )
    safe_years_df = safe_years_df.merge(
        lookup.rename(
            columns={"year": "next_safe_year", "con_gdhi": "next_con_gdhi"}
        ),
        on=["lsoa_code", "next_safe_year"],
        how="left",
    )

    return df, safe_years_df
