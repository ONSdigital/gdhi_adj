"""Module for pre-processing data in the gdhi_adj project."""

import pandas as pd
from scipy.stats import zscore

# from gdhi_adj.utils.logger import logger_creator


def pivot_long_dataframe(
    df: pd.DataFrame, new_var_col: str, new_val_col: str
) -> pd.DataFrame:
    """
    Pivots the DataFrame based on specified index, columns, and values.

    Args:
        df (pd.DataFrame): The input DataFrame.
        new_var_col (str): The name for the column containing old column names.
        new_val_col (str): The name for the column containing values.

    Returns:
    pd.DataFrame: The pivoted DataFrame.
    """
    id_cols = [col for col in df.columns if col[0].isalpha()]
    df = df.melt(id_vars=id_cols, var_name=new_var_col, value_name=new_val_col)

    # convert year column dtype from str to int
    df["year"] = df["year"].astype(int)

    return df


def rate_of_change(
    ascending: bool, df: pd.DataFrame, sort_cols: list, group_col: str, val_col
) -> pd.DataFrame:
    """
    Calculates the rate of change going forward and backwards in time in the DataFrame.

    Args:
        ascending (bool): If True, calculates forward rate of change; otherwise, backward.
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

        df["forward_pct_change"] = df.groupby(group_col)[val_col].pct_change() + 1.0

    else:
        # If not ascending, sort in descending order
        df = df.sort_values(by=sort_cols, ascending=ascending).reset_index(drop=True)
        df["backward_pct_change"] = df.groupby(group_col)[val_col].pct_change() + 1.0

    return df


def calc_zscores(
    df: pd.DataFrame, score_prefix: str, group_col: str, pct_change_col: str
) -> pd.DataFrame:
    """
    Calculates the z-scores for percent changes and raw data in DataFrame.

    Args:
        df (pd.DataFrame): The input DataFrame.
        score_prefix (str): Prefix for the zscore column names.
        group_col (str): The column to group by for z-score calculation.
        pct_change_col (str): The column containing percent change values to calculate zscores.

    Returns:
        pd.DataFrame: The DataFrame with an additional 'zscore' column.
    """
    df[score_prefix + "_zscore"] = df.groupby(group_col)[pct_change_col].transform(
        lambda x: zscore(x, nan_policy="omit")
    )

    df["z_" + score_prefix + "_flag"] = df[score_prefix + "_zscore"] > 3.0
    return df


def calc_iqr(df: pd.DataFrame, iqr_prefix: str) -> pd.DataFrame:
    """
    Calculates the interquartile range (IQR) for each LSOA in the DataFrame.
    """
    # Calculate quartiles for each LSOA
    df[iqr_prefix + "_q1"] = df.groupby("lsoa_code")["gdhi_annual"].transform(
        lambda x: x.quantile(0.25)
    )
    df[iqr_prefix + "_q3"] = df.groupby("lsoa_code")["gdhi_annual"].transform(
        lambda x: x.quantile(0.75)
    )
    # Calculate IQR for each LSOA
    df[iqr_prefix + "_iqr"] = df[iqr_prefix + "_q3"] - df[iqr_prefix + "_q1"]

    # Calculate lower and upper bounds for outliers for each LSOA
    df[iqr_prefix + "_lower_bound"] = df[iqr_prefix + "_q1"] - (
        3 * df[iqr_prefix + "_iqr"]
    )
    df[iqr_prefix + "_upper_bound"] = df[iqr_prefix + "_q3"] + (
        3 * df[iqr_prefix + "_iqr"]
    )
