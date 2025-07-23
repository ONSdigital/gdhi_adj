"""Module for pre-processing data in the gdhi_adj project."""

import pandas as pd
from scipy.stats import zscore

from gdhi_adj.utils.logger import GDHI_adj_logger

GDHI_adj_LOGGER = GDHI_adj_logger(__name__)
logger = GDHI_adj_LOGGER.logger


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
    df: pd.DataFrame, score_prefix: str, group_col: str, pct_change_col: str
) -> pd.DataFrame:
    """
    Calculates the z-scores for percent changes and raw data in DataFrame.

    Args:
        df (pd.DataFrame): The input DataFrame.
        score_prefix (str): Prefix for the zscore column names.
        group_col (str): The column to group by for z-score calculation.
        pct_change_col (str): The column containing percent change values to
        calculate zscores.

    Returns:
        pd.DataFrame: The DataFrame with an additional 'zscore' column.
    """
    df[score_prefix + "_zscore"] = df.groupby(group_col)[
        pct_change_col
    ].transform(lambda x: zscore(x, nan_policy="omit"))

    df["z_" + score_prefix + "_flag"] = df[score_prefix + "_zscore"] > 3.0
    return df.drop(columns=[score_prefix + "_zscore"])


def calc_iqr(
    df: pd.DataFrame, iqr_prefix: str, group_col: str, val_col: str
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
    # Calculate quartiles for each LSOA
    df[iqr_prefix + "_q1"] = df.groupby(group_col)[val_col].transform(
        lambda x: x.quantile(0.25)
    )
    df[iqr_prefix + "_q3"] = df.groupby(group_col)[val_col].transform(
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

    # Flag outliers based on lower and upper bounds
    df["iqr_" + iqr_prefix + "_flag"] = (
        df[val_col] < df[iqr_prefix + "_lower_bound"]
    ) | (df[val_col] > df[iqr_prefix + "_upper_bound"])

    return df.drop(
        columns=[
            iqr_prefix + "_q1",
            iqr_prefix + "_q3",
            iqr_prefix + "_iqr",
            iqr_prefix + "_lower_bound",
            iqr_prefix + "_upper_bound",
        ]
    )


def create_master_flag(df: pd.DataFrame) -> pd.DataFrame:
    """
    Creates a master flag based on z score and IQR flag columns.

    Args:
        df (pd.DataFrame): The input DataFrame.

    Returns:
        pd.DataFrame: The DataFrame with an additional 'master_flag' columns.
    """
    # Create a master flag that is True if any of the IQR columns are True
    z_score_cols = [col for col in df.columns if col[0:2] == "z_"]
    z_count = df.groupby("lsoa_code").agg({col: "sum" for col in z_score_cols})
    z_count["z_master_flag"] = (z_count[z_score_cols] >= 1).sum(axis=1) >= 2

    # Create a master flag that is True if any of the IQR columns are True
    iqr_score_cols = [col for col in df.columns if col[0:4] == "iqr_"]
    iqr_count = df.groupby("lsoa_code").agg(
        {col: "sum" for col in iqr_score_cols}
    )
    iqr_count["iqr_master_flag"] = (iqr_count[iqr_score_cols] >= 1).sum(
        axis=1
    ) >= 2

    # Join the master flags back to the original DataFrame
    df = df.join(z_count[["z_master_flag"]], on="lsoa_code", how="left")
    df = df.join(iqr_count[["iqr_master_flag"]], on="lsoa_code", how="left")

    # Create a master flag that is True if either z_master_flag or iqr_master
    # flag is True
    df["master_flag"] = df["z_master_flag"] | df["iqr_master_flag"]

    return df.drop(
        columns=(
            z_score_cols
            + iqr_score_cols
            + ["backward_pct_change", "forward_pct_change"]
        )
    )
