"""Module for flagging preprocessing data in the gdhi_adj project."""

import numpy as np
import pandas as pd


def flag_rollback_years(df: pd.DataFrame) -> pd.DataFrame:
    """
    Flags years where the GDHI has rolled back from future years.
    Typically 2010-2014 has 2015 data copied to them as it is missing.

    Args:
        df (pd.DataFrame): The input DataFrame.

    Returns:
        pd.DataFrame: DataFrame with an additional 'rollback_flag' column.
    """
    # Create a mask for years where the GDHI has rolled back
    # 2015 is included due to forward percentage change column
    rollback_mask = (
        (df["backward_pct_change"] == 1.0) | (df["forward_pct_change"] == 1.0)
    ) & (df["year"].between(2010, 2014))

    # Create a new column 'rollback_flag' based on the mask
    df["rollback_flag"] = np.where(rollback_mask, True, False)

    return df


def create_master_flag(df: pd.DataFrame) -> pd.DataFrame:
    """
    Creates a master flag based on z score and IQR flag columns.

    Args:
        df (pd.DataFrame): The input DataFrame.

    Returns:
        pd.DataFrame: The DataFrame with an additional 'master_flag' columns.
    """
    # Create list of zscore flag columns (these should be the only columns
    # prefixed with 'z_')
    z_score_cols = [col for col in df.columns if col.startswith("z_")]
    # Create a master flag that is True if any of the IQR columns are True
    z_count = df.groupby("lsoa_code").agg({col: "sum" for col in z_score_cols})
    z_count["master_z_flag"] = (z_count[z_score_cols] >= 1).sum(axis=1) >= 1

    # Create list of IQR flag columns (these should be the only columns
    # prefixed with 'iqr_')
    iqr_score_cols = [col for col in df.columns if col.startswith("iqr_")]
    # Create a master flag that is True if any of the IQR columns are True
    iqr_count = df.groupby("lsoa_code").agg(
        {col: "sum" for col in iqr_score_cols}
    )
    iqr_count["master_iqr_flag"] = (iqr_count[iqr_score_cols] >= 1).sum(
        axis=1
    ) >= 1

    # # Create a master flag that is True if any IQR flag is true and if any
    # # zscore flag is true
    # df["master_flag"] = (df[iqr_score_cols].any(axis=1)
    #                      & df[z_score_cols].any(axis=1))

    # Join the master flags back to the original DataFrame
    df = df.join(z_count[["master_z_flag"]], on="lsoa_code", how="left")
    df = df.join(iqr_count[["master_iqr_flag"]], on="lsoa_code", how="left")

    # Create a master flag that is True if either master_z_flag or iqr_master
    # flag is True
    df["master_flag"] = df["master_z_flag"] | df["master_iqr_flag"]

    return df
