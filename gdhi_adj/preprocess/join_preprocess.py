"""Module for flagging preprocessing data in the gdhi_adj project."""

import numpy as np
import pandas as pd


def constrain_to_reg_acc(
    df: pd.DataFrame,
    reg_acc: pd.DataFrame,
    transaction_name: str,
) -> pd.DataFrame:
    """
    Calculate contrained and unconstrained values for each outlier case.

    Args:
        df (pd.DataFrame): The input DataFrame with outliers to be constrained.
        reg_acc (pd.DataFrame): The regional accounts DataFrame.
        transaction_code (str): Transaction code to filter regional accounts.

    Returns:
        pd.DataFrame: The constrained DataFrame.
    """
    reg_acc = reg_acc[reg_acc["transaction_name"] == transaction_name].drop(
        columns=[
            "Region",
            "Region name",
            "Transaction code",
            "transaction_name",
        ]
    )

    # Ensure that both DataFrames have the same columns for merging
    if not reg_acc.columns.isin(df.columns).all():
        raise ValueError("DataFrames have different columns for joining.")

    # Remove commas separating thousands and convert to numeric
    if reg_acc["uncon_gdhi"].dtype == "object":
        reg_acc["uncon_gdhi"] = (
            reg_acc["uncon_gdhi"].str.replace(",", "").astype("float64")
        )

    reg_acc.rename(columns={"uncon_gdhi": "conlad_gdhi"}, inplace=True)

    df = df.merge(
        reg_acc[["lad_code", "year", "conlad_gdhi"]],
        on=["lad_code", "year"],
        how="left",
    )

    df["unconlad"] = df["uncon_gdhi"] + df["mean_non_out_gdhi"]

    df["rate"] = np.where(
        df["unconlad"] == 0, 0, df["conlad_gdhi"] / df["unconlad"]
    )

    df["conlsoa_gdhi"] = df["uncon_gdhi"] * df["rate"]
    df["conlsoa_mean"] = df["mean_non_out_gdhi"] * df["rate"]

    df["master_flag"] = df["master_flag"].replace(
        {True: "TRUE", False: "MEAN"}
    )

    return df.drop(
        columns=[
            "conlad_gdhi",
            "unconlad",
            "rate",
        ]
    )


def concat_wide_dataframes(
    df_wide_outlier: pd.DataFrame, df_wide_mean: pd.DataFrame
) -> pd.DataFrame:
    """
    Concatenates two wide dataframes to create a final wide DataFrame.

    Args:
        df_wide_outlier (pd.DataFrame): The DataFrame containing outlier data.
        df_wide_mean (pd.DataFrame): The DataFrame containing mean data.

    Returns:
        pd.DataFrame: The concatenated DataFrame in wide format.
    """
    # Join DataFrames and sort to match desired output for PowerBI
    df_wide = pd.concat([df_wide_outlier, df_wide_mean], ignore_index=True)
    df_wide.sort_values(
        by=["lsoa_code", "master_flag"], ascending=[True, False], inplace=True
    )
    df_wide.reset_index(drop=True, inplace=True)

    return df_wide
