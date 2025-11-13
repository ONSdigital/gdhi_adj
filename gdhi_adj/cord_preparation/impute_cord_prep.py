"""Module for imputing values ready for CORD in the gdhi_adj project."""

from typing import List

import pandas as pd


def impute_suppression_x(
    df: pd.DataFrame,
    target_cols: List[str],
    transaction_col: str = "transaction",
    lad_col: str = "lad_code",
    transaction_value: str = "D623",
    lad_val: List[str] = ["95", "S"],
) -> pd.DataFrame:
    """
    Set cells in target_cols to "X" where both conditions are met:
      - The value in transaction_col equals transaction_value.
      - The value in lad_col starts with any values in lad_val list.

    Args:
      df (pd.DataFrame): input DataFrame
      target_cols (List[str]): list of column names to modify.
      transaction_col (str): name of the transaction column.
      lad_col (str): name of the LAD column.
      transaction_value (str): transaction value to match.
      lad_val (List[str]): list of starting strings for LAD codes to match (
        case sensitive).

    Returns:
      pd.DataFrame: DataFrame with suppressed values.
    """
    target_cols = list(target_cols)
    missing = [c for c in target_cols if c not in df.columns]
    if missing:
        raise KeyError(f"Target columns not found in DataFrame: {missing}")

    # Change column dtypes so that we can insert 'X' strings
    for c in target_cols:
        df[c] = df[c].astype("string")

    # Create mask for rows matching both conditions
    mask = (df[transaction_col] == transaction_value) & (
        df[lad_col].str.startswith(tuple(lad_val))
    )

    # Apply 'X' to all target columns for rows matching mask
    df.loc[mask, target_cols] = "X"

    return df
