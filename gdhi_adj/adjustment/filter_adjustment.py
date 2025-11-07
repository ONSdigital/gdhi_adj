"""Module for filtering adjustment data in the gdhi_adj project."""

import pandas as pd


def filter_adjust(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filter data to keep only LSOAs for adjustment and subset.

    Args:
        df (pd.DataFrame): Input DataFrame containing LSOA data.

    Returns:
        pd.DataFrame: Filtered DataFrame with only relevant columns and rows.
    """
    df = df[df["adjust"].astype("boolean").fillna(False)]

    cols_to_keep = [
        "lsoa_code",
        "lad_code",
        "adjust",
        "year",
    ]

    df = df[cols_to_keep]

    return df


def filter_year(
    df: pd.DataFrame, start_year: int, end_year: int
) -> pd.DataFrame:
    """
    Filter DataFrame by a range of years inclusively.
    Args:
        df (pd.DataFrame): Input DataFrame containing year data.
        start_year (int): Start year for filtering (inclusive).
        end_year (int): End year for filtering (inclusive).
    Returns:
        pd.DataFrame: Filtered DataFrame containing only rows within the year
        range.
    """
    df = df[(df["year"] >= start_year) & (df["year"] <= end_year)]

    df = df.reset_index(drop=True)

    return df


def filter_component(
    df: pd.DataFrame,
    sas_code_filter: str,
    cord_code_filter: str,
    credit_debit_filter: str,
) -> pd.DataFrame:
    """
    Filter DataFrame by component codes.

    Args:
        df (pd.DataFrame): Constrained DataFrame with component code data.
        sas_code_filter (str): SAS code to filter by.
        cord_code_filter (str): CORD code to filter by.
        credit_debit_filter (str): Credit/Debit code to filter by.

    Returns:
        pd.DataFrame: Filtered DataFrame containing only rows matching the
        specified component codes.
    """
    if sas_code_filter not in df["sas_code"].unique():
        raise ValueError(f"SAS code '{sas_code_filter}' not found in data.")
    if cord_code_filter not in df["cord_code"].unique():
        raise ValueError(f"CORD code '{cord_code_filter}' not found in data.")
    if credit_debit_filter not in df["credit_debit"].unique():
        raise ValueError(
            f"Credit/Debit code '{credit_debit_filter}' not found in data."
        )

    df = df[
        (df["sas_code"] == sas_code_filter)
        & (df["cord_code"] == cord_code_filter)
        & (df["credit_debit"] == credit_debit_filter)
    ]

    df = df.reset_index(drop=True)

    return df
