"""Module for calculations to preprocess data in the gdhi_adj project."""

import numpy as np
import pandas as pd
from scipy.stats import zscore


def calc_rate_of_change(
    ascending: bool,
    df: pd.DataFrame,
    sort_cols: list,
    group_col: str,
    val_col: str,
) -> pd.DataFrame:
    """
    Calculate the rate of change going forward and backwards in time in the
    DataFrame.

    Args:
        ascending (bool): If True, calculates forward rate of change;
        otherwise, backward.
        df (pd.DataFrame): The input DataFrame.
        sort_cols (list): Columns to sort by before calculating rate of change.
        group_col (str): The column to group by for rate of change calculation.
        val_col (str): The column for which the rate of change is calculated.

    Returns:
        pd.DataFrame: A DataFrame containing the rate of change values.
    """
    if ascending:
        # If ascending, sort in ascending order
        df = df.sort_values(by=sort_cols).reset_index(drop=True)

        df["forward_pct_change"] = (
            df.groupby(group_col)[val_col].pct_change() + 1.0
        )

    else:
        # If not ascending, sort in descending order
        df = df.sort_values(by=sort_cols, ascending=ascending).reset_index(
            drop=True
        )
        df["backward_pct_change"] = (
            df.groupby(group_col)[val_col].pct_change() + 1.0
        )

    return df


def calc_zscores(
    df: pd.DataFrame,
    score_prefix: str,
    group_col: str,
    val_col: str,
    zscore_upper_threshold: float,
    zscore_lower_threshold: float,
) -> pd.DataFrame:
    """
    Calculates the z-scores for percent changes and raw data in DataFrame.

    Args:
        df (pd.DataFrame): The input DataFrame.
        score_prefix (str): Prefix for the zscore column names.
        group_col (str): The column to group by for z-score calculation.
        val_col (str): The column values to calculate zscores.
        zscore_upper_threshold (float): The upper threshold for z-score flag.
        zscore_lower_threshold (float): The lower threshold for z-score flag.

    Returns:
        pd.DataFrame: The DataFrame with an additional 'zscore' column.
    """
    # Mask for when rollback_flag is false
    mask = ~df["rollback_flag"]

    # If the value column is 1, the data has been rolled back so should not be
    # flagged, else flag based on zscore
    # Calculate z-scores when rollback_flag is false
    df.loc[mask, f"{score_prefix}_zscore"] = (
        df.loc[mask]
        .groupby(group_col)[val_col]
        .transform(lambda x: zscore(x, nan_policy="omit", ddof=1))
    )

    # Descriptor whether the zscore exceeds the upper or lower threshold
    conditions = [
        df[f"{score_prefix}_zscore"] > zscore_upper_threshold,
        df[f"{score_prefix}_zscore"] < zscore_lower_threshold,
    ]
    descriptors = ["upper", "lower"]

    df[f"z_{score_prefix}_direction"] = np.select(
        conditions, descriptors, default=None
    )
    df[f"z_{score_prefix}_flag"] = np.select(
        conditions, [True, True], default=False
    )

    return df


def calc_iqr(
    df: pd.DataFrame,
    iqr_prefix: str,
    group_col: str,
    val_col: str,
    iqr_multiplier: float,
) -> pd.DataFrame:
    """
    Calculates the interquartile range (IQR) for each LSOA in the DataFrame.

    Args:
        df (pd.DataFrame): The input DataFrame.
        iqr_prefix (str): Prefix for the IQR column names.
        group_col (str): The column to group by for IQR calculation.
        val_col (str): The column containing values to calculate IQR.

    Returns:
        pd.DataFrame: The DataFrame with additional columns for IQR and outlier
        bounds.
    """
    # Mask for when rollback_flag is false
    mask = ~df["rollback_flag"]

    # Calculate quartiles only on unflagged data
    quartiles = (
        df[mask]
        .groupby(group_col)[val_col]
        .agg(
            [
                (f"{iqr_prefix}_q1", lambda x: x.quantile(0.25)),
                (f"{iqr_prefix}_q3", lambda x: x.quantile(0.75)),
            ]
        )
    ).reset_index()

    # Assign quartiles back to the full DataFrame
    df = df.merge(quartiles, on=group_col, how="left")

    # Calculate IQR for each LSOA
    df[f"{iqr_prefix}_iqr"] = df[f"{iqr_prefix}_q3"] - df[f"{iqr_prefix}_q1"]

    # Calculate lower and upper bounds for outliers for each LSOA
    df[f"{iqr_prefix}_lower_bound"] = df[f"{iqr_prefix}_q1"] - (
        iqr_multiplier * df[f"{iqr_prefix}_iqr"]
    )
    df[f"{iqr_prefix}_upper_bound"] = df[f"{iqr_prefix}_q3"] + (
        iqr_multiplier * df[f"{iqr_prefix}_iqr"]
    )

    # If the value column is 1, the data has been rolled back so should not be
    # flagged, else flag based on zscore
    df[f"iqr_{iqr_prefix}_flag"] = (
        df[val_col] < df[f"{iqr_prefix}_lower_bound"]
    ) | (df[val_col] > df[f"{iqr_prefix}_upper_bound"])

    return df


def calc_lad_mean(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Calculates the mean GDHI for each non outlier LSOA in the DataFrame.

    Args:
        df (pd.DataFrame): The input DataFrame.

    Returns:
        pd.DataFrame: The DataFrame with an added 'mean_non_out_gdhi' column.
    """
    # Separate out LSOAs that are not flagged
    non_outlier_df = df[~df["master_flag"]]

    # Aggregate GDHI values for non-outlier LSOAs by LADs
    non_outlier_df = non_outlier_df.groupby(["lad_code", "year"]).agg(
        mean_non_out_gdhi=("gdhi_annual", "mean")
    )

    df = df.join(non_outlier_df, on=["lad_code", "year"], how="left")
    df = df[df["master_flag"]].reset_index(drop=True)

    return df
