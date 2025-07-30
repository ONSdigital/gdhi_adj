"""Module for pre-processing data in the gdhi_adj project."""

import os

import pandas as pd
from scipy.stats import zscore

from gdhi_adj.utils.helpers import read_with_schema, write_with_schema
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
    ].transform(lambda x: zscore(x, nan_policy="omit", ddof=1))

    df["z_" + score_prefix + "_flag"] = df[score_prefix + "_zscore"] > 3.0
    return df


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

    return df


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
    z_count["master_z_flag"] = (z_count[z_score_cols] >= 1).sum(axis=1) >= 2

    # Create a master flag that is True if any of the IQR columns are True
    iqr_score_cols = [col for col in df.columns if col[0:4] == "iqr_"]
    iqr_count = df.groupby("lsoa_code").agg(
        {col: "sum" for col in iqr_score_cols}
    )
    iqr_count["master_iqr_flag"] = (iqr_count[iqr_score_cols] >= 1).sum(
        axis=1
    ) >= 2

    # Join the master flags back to the original DataFrame
    df = df.join(z_count[["master_z_flag"]], on="lsoa_code", how="left")
    df = df.join(iqr_count[["master_iqr_flag"]], on="lsoa_code", how="left")

    # Create a master flag that is True if either master_z_flag or iqr_master
    # flag is True
    df["master_flag"] = df["master_z_flag"] | df["master_iqr_flag"]

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
    # currently using this to match Jim's averages
    # outlier_lsoas = ["E01015597", "E01018153"]
    # non_outlier_df = df[~df["lsoa_code"].isin(outlier_lsoas)]

    # Aggregate GDHI values for non-outlier LSOAs by LADs
    non_outlier_df = non_outlier_df.groupby(["lad_code", "year"]).agg(
        mean_non_out_gdhi=("gdhi_annual", "mean")
    )

    df = df.join(non_outlier_df, on=["lad_code", "year"], how="left")

    return df


def constrain_to_reg_acc(
    df: pd.DataFrame, reg_acc: pd.DataFrame
) -> pd.DataFrame:
    """
    Calculate contrained and unconstrained values for each outlier case.

    Args:
        df (pd.DataFrame): The input DataFrame with outliers to be constrained.
        reg_acc (pd.DataFrame): The regional accounts DataFrame.

    Returns:
        pd.DataFrame: The constrained DataFrame.
    """
    # Ensure that both DataFrames have the same columns for merging
    if not reg_acc.columns.isin(df.columns).all():
        raise ValueError("DataFrames have different columns for joining.")

    reg_acc.rename(columns={"gdhi_annual": "conlad_gdhi"}, inplace=True)

    df = df.merge(
        reg_acc[["lad_code", "year", "conlad_gdhi"]],
        on=["lad_code", "year"],
        how="left",
    )

    df["unconlad"] = df["gdhi_annual"] + df["mean_non_out_gdhi"]

    df["rate"] = df["conlad_gdhi"] / df["unconlad"]

    df["conlsoa_gdhi"] = df["gdhi_annual"] * df["rate"]
    df["conlsoa_mean"] = df["mean_non_out_gdhi"] * df["rate"]

    return df.drop(
        columns=[
            "conlad_gdhi",
            "unconlad",
            "rate",
        ]
    )


def run_preprocessing(config: dict) -> None:
    """
    Run the preprocessing steps for the GDHI adjustment project.

    This function performs the following steps:
    1. Load the configuration settings.
    2. Load the input data.
    3. Pivot the DataFrame to long format.
    4. Calculate backward and forward rate of change.
    5. Calculate z-scores and IQRs.
    6. Create master flags.
    7. Calculate LAD mean GDHI.
    8. Constrain outliers to regional accounts.
    9. Save the processed data.

    Args:
        config (dict): Configuration dictionary containing user settings and
        pipeline settings.
    Returns:
        None: The function does not return any value. It saves the processed
        DataFrame to a CSV file.
    """
    # Initialize logger
    local_or_shared = config["user_settings"]["local_or_shared"]
    filepath_dict = config[f"{local_or_shared}_settings"]
    input_gdhi_file_path = (
        "C:/Users/" + os.getlogin() + filepath_dict["input_gdhi_file_path"]
    )
    input_ra_lad_file_path = (
        "C:/Users/" + os.getlogin() + filepath_dict["input_ra_lad_file_path"]
    )
    output_dir = "C:/Users/" + os.getlogin() + filepath_dict["output_dir"]
    input_gdhi_schema_path = config["pipeline_settings"][
        "input_gdhi_schema_path"
    ]
    input_ra_lad_schema_path = config["pipeline_settings"][
        "input_ra_lad_schema_path"
    ]
    output_schema_path = config["pipeline_settings"]["output_schema_path"]
    new_filename = config["pipeline_settings"].get("output_filename", None)
    # Import data
    df = read_with_schema(input_gdhi_file_path, input_gdhi_schema_path)
    ra_lad = read_with_schema(input_ra_lad_file_path, input_ra_lad_schema_path)

    # Pivot data to long format
    df = pivot_long_dataframe(df, "year", "gdhi_annual")
    ra_lad = pivot_long_dataframe(ra_lad, "year", "gdhi_annual")

    # Calculate annual rate of change
    df = rate_of_change(
        False, df, ["lsoa_code", "year"], "lsoa_code", "gdhi_annual"
    )
    df = rate_of_change(
        True, df, ["lsoa_code", "year"], "lsoa_code", "gdhi_annual"
    )

    # Assign prefixes
    backward_prefix = "bkwd"
    forward_prefix = "frwd"
    raw_prefix = "raw"

    # Calculate outlier identification scores and create flags
    df = calc_zscores(df, backward_prefix, "lsoa_code", "backward_pct_change")
    df = calc_zscores(df, forward_prefix, "lsoa_code", "forward_pct_change")
    df = calc_zscores(df, raw_prefix, "lsoa_code", "gdhi_annual")

    df = calc_iqr(df, backward_prefix, "lsoa_code", "backward_pct_change")
    df = calc_iqr(df, forward_prefix, "lsoa_code", "forward_pct_change")
    df = calc_iqr(df, raw_prefix, "lsoa_code", "gdhi_annual")

    df = create_master_flag(df)

    # Export interim scores for QA
    logger.info("Saving interim data")
    logger.info(f"{output_dir}manual_adj_preprocessing_interim_scores.csv")
    df.to_csv(
        output_dir + "manual_adj_preprocessing_interim_scores.csv",
        index=False,
    )
    logger.info("Data saved successfully")

    # Keep base data and flags, dropping scores columns
    cols_to_keep = [
        "lsoa_code",
        "lsoa_name",
        "lad_code",
        "lad_name",
        "transaction_code",
        "year",
        "gdhi_annual",
        "master_z_flag",
        "master_iqr_flag",
        "master_flag",
    ]

    df = df[cols_to_keep]

    df = calc_lad_mean(df)

    df = constrain_to_reg_acc(df, ra_lad)

    # Save output file with new filename if specified
    if config["pipeline_settings"]["output_data"]:
        # Write DataFrame to CSV
        write_with_schema(df, output_schema_path, output_dir, new_filename)
        print(df)
        # print(df[df["master_flag"] == True]["lsoa_code"].unique())
