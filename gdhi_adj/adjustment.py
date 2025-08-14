"""Module for adjusting data in the gdhi_adj project."""

import os

import pandas as pd

from gdhi_adj.utils.helpers import read_with_schema, write_with_schema
from gdhi_adj.utils.logger import GDHI_adj_logger

GDHI_adj_LOGGER = GDHI_adj_logger(__name__)
logger = GDHI_adj_LOGGER.logger


def filter_lsoa_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filter LSOA data to keep only relevant columns and rows.

    Args:
        df (pd.DataFrame): Input DataFrame containing LSOA data.

    Returns:
        pd.DataFrame: Filtered DataFrame with only relevant columns and rows.
    """
    # Check for rows where one column is null and the other is not
    mismatch = df["master_flag"].isnull() != df["adjust"].isnull()

    if mismatch.any():
        raise ValueError(
            "Mismatch: master_flag and Adjust column booleans do not match."
        )

    df = df[df["adjust"].fillna(False).astype(bool)]

    cols_to_keep = [
        "lsoa_code",
        "lad_code",
        "transaction_code",
        "adjust",
        "year",
    ]

    df = df[cols_to_keep]

    return df


def join_analyst_constrained_data(
    df_constrained: pd.DataFrame, df_analyst: pd.DataFrame
) -> pd.DataFrame:
    """
    Join analyst data to constrained data based on LSOA code, LAD code and
    transaction code.

    Args:
        df_constrained (pd.DataFrame): DataFrame containing constrained data.
        df_analyst (pd.DataFrame): DataFrame containing analyst data.

    Returns:
        pd.DataFrame: Joined DataFrame with relevant columns.
    """
    df = df_constrained.merge(
        df_analyst,
        on=["lsoa_code", "lad_code", "transaction_code"],
        how="left",
    )

    # Obtain list of columns to rename
    exclude_cols = [
        "lsoa_code",
        "lad_code",
        "transaction_code",
        "adjust",
        "year",
    ]
    included_cols = [col for col in df.columns if col not in exclude_cols]

    # Create a renaming dictionary with prefix
    rename_dict = {col: f"CON_{col}" for col in included_cols}

    df = df.rename(columns=rename_dict)

    if df["adjust"].sum() != df_analyst["adjust"].sum():
        raise ValueError(
            "Number of rows to adjust between analyst and constrained data"
            " do not match."
        )

    if df.shape[0] != df_constrained.shape[0]:
        raise ValueError(
            "Number of rows of constrained data after join has increased."
        )

    return df


def join_analyst_unconstrained_data(
    df_unconstrained: pd.DataFrame, df_analyst: pd.DataFrame
) -> pd.DataFrame:
    """
    Join analyst data to unconstrained data based on LSOA code, LAD code and
    transaction code.

    Args:
        df_unconstrained (pd.DataFrame): DataFrame with unconstrained data.
        df_analyst (pd.DataFrame): DataFrame containing analyst data.

    Returns:
        pd.DataFrame: Joined DataFrame with relevant columns.
    """
    df = df_unconstrained.merge(
        df_analyst,
        on=["lsoa_code", "lad_code", "transaction_code"],
        how="left",
    )

    if df["adjust"].sum() != df_analyst["adjust"].sum():
        raise ValueError(
            "Number of rows to adjust between analyst and unconstrained data"
            " do not match."
        )

    if df.shape[0] != df_unconstrained.shape[0]:
        raise ValueError(
            "Number of rows of unconstrained data after join has increased."
        )

    return df


def pivot_adjustment_long(df: pd.DataFrame) -> pd.DataFrame:
    """
    Pivot the adjustment DataFrame from wide to long format.

    Args:
        df (pd.DataFrame): DataFrame containing data to be adjusted.

    Returns:
        pd.DataFrame: Pivoted DataFrame in long format.
    """
    # Create lists of GDHI columns
    uncon_cols = [col for col in df.columns if col[0].isdigit()]
    con_cols = [col for col in df.columns if col.startswith("CON_")]

    df.rename(columns={"year": "year_to_adjust"}, inplace=True)

    df_uncon = df.melt(
        id_vars=[
            "lsoa_code",
            "lad_code",
            "transaction_code",
            "adjust",
            "year_to_adjust",
        ],
        value_vars=uncon_cols,
        var_name="year",
        value_name="uncon_gdhi",
    )

    df_con = df.melt(
        id_vars=[
            "lsoa_code",
            "lad_code",
            "transaction_code",
            "adjust",
            "year_to_adjust",
        ],
        value_vars=con_cols,
        var_name="year",
        value_name="con_gdhi",
    ).reset_index(drop=True)
    df_con["year"] = df_con["year"].str.replace("^CON_", "", regex=True)

    df_combined = df_uncon.merge(
        df_con,
        on=[
            "lsoa_code",
            "lad_code",
            "transaction_code",
            "adjust",
            "year_to_adjust",
            "year",
        ],
        how="left",
    )
    df_combined["year"] = df_combined["year"].astype(int)

    return df_combined


def calc_scaling_factors(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate scaling factors for the adjustment.

    Args:
        df (pd.DataFrame): DataFrame with unconstrained and constrained data.

    Returns:
        pd.DataFrame: DataFrame with scaling factors added.
    """
    df_uncon = (
        df.groupby(["transaction_code", "year"])["uncon_gdhi"]
        .sum()
        .reset_index()
    )
    df_con = (
        df.groupby(["transaction_code", "year"])["con_gdhi"]
        .sum()
        .reset_index()
    )

    df_scaling = df_uncon.merge(
        df_con, on=["transaction_code", "year"], how="left"
    )

    df_scaling["scaling"] = df_scaling["con_gdhi"] / df_scaling["uncon_gdhi"]

    return df_scaling


def create_anaomaly_list(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create a list of anomalies in the DataFrame.

    Args:
        df (pd.DataFrame): DataFrame containing data to check for anomalies.

    Returns:
        pd.DataFrame: DataFrame with unique anomalies listed.
    """
    anomaly_lsoa = df[df["adjust"].fillna(False).astype(bool)]
    anomaly_lsoa = (
        anomaly_lsoa[["lsoa_code", "transaction_code", "year_to_adjust"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )

    return anomaly_lsoa


def calc_adjustment_headroom_val(
    df: pd.DataFrame,
    df_scaling: pd.DataFrame,
    lsoa_code: str,
    transaction_code: str,
    year_to_adjust: int,
) -> pd.DataFrame:
    """
    Calculate the adjustment headroom available to smooth timeseries.

    Args:
        df (pd.DataFrame): DataFrame containing constrained and unconstrained
        data.
        df_scaling (pd.DataFrame): DataFrame containing scaling factors.
        lsoa_code (str): LSOA code for the adjustment.
        transaction_code (str): Transaction code for the adjustment.
        year_to_adjust (int): Year for the adjustment.

    Returns:
        uncon_non_out_sum (float): The sum of non outlier unconstrained GDHI.
        headroom_val (float): The calculated headroom for adjustment.
    """
    mean_scaling = df_scaling[
        (df_scaling["transaction_code"] == transaction_code)
        & (df_scaling["year"] != year_to_adjust)
    ]["scaling"].mean()

    uncon_non_out_sum = df[
        (df["lsoa_code"] != lsoa_code)
        & (df["transaction_code"] == transaction_code)
        & (df["year"] == year_to_adjust)
    ]["uncon_gdhi"].sum()

    con_out_val = df_scaling[
        (df_scaling["transaction_code"] == transaction_code)
        & (df_scaling["year"] == year_to_adjust)
    ]["con_gdhi"].iloc[0]

    headroom_val = con_out_val - (uncon_non_out_sum * mean_scaling)

    return uncon_non_out_sum, headroom_val


def calc_midpoint_val(
    df: pd.DataFrame,
    lsoa_code: str,
    transaction_code: str,
    year_to_adjust: int,
) -> float:
    """
    Calculate the midpoint value for a given LSOA code.

    Args:
        df (pd.DataFrame): DataFrame containing data to calculate midpoint.
        lsoa_code (str): LSOA code for the adjustment.
        transaction_code (str): Transaction code for the adjustment.
        year_to_adjust (int): Year for the adjustment.

    Returns:
        float: Midpoint value for the specified LSOA code.
    """
    # if year_to_adjust is the first or last year in the series,
    # method required for determining what to do with midpoint
    outlier_val = df[
        (df["lsoa_code"] == lsoa_code)
        & (df["transaction_code"] == transaction_code)
        & (df["year"] == year_to_adjust)
    ]["con_gdhi"].iloc[0]

    prev_year_val = df[
        (df["lsoa_code"] == lsoa_code)
        & (df["transaction_code"] == transaction_code)
        & (df["year"] == (year_to_adjust - 1))
    ]["con_gdhi"].iloc[0]

    next_year_val = df[
        (df["lsoa_code"] == lsoa_code)
        & (df["transaction_code"] == transaction_code)
        & (df["year"] == (year_to_adjust + 1))
    ]["con_gdhi"].iloc[0]

    midpoint_val = (prev_year_val + next_year_val) / 2

    return outlier_val, midpoint_val


def calc_adjustment_val(
    headroom_val: float, outlier_val: float, midpoint_val: float
) -> float:
    """
    Calculate the adjustment value based on the midpoint and scaled difference.

    Args:
        headroom_val (float): Scaled difference value calculated from the data.
        outlier_val (float): Outlier value to be adjusted.
        midpoint_val (float): Midpoint value to peak/trough.

    Returns:
        adjustment_val (float): The adjustment value to be applied.
    """
    if abs((headroom_val - outlier_val) <= abs(midpoint_val)):
        adjustment_val = headroom_val - outlier_val
    else:
        adjustment_val = midpoint_val - outlier_val

    return adjustment_val


def apply_adjustment(
    df: pd.DataFrame,
    transaction_code: str,
    year_to_adjust: int,
    adjustment_val: float,
    uncon_non_out_sum: float,
) -> pd.DataFrame:
    """
    Apply the adjustment values to the LSOAs for the anomaly year.

    Args:
        df (pd.DataFrame): DataFrame containing data to adjust.
        transaction_code (str): Transaction code for the adjustment.
        year_to_adjust (int): Year for the adjustment.
        adjustment_val (float): Adjustment value to be applied.
        uncon_non_out_sum (float): Sum of non-outlier unconstrained GDHI.

    Returns:
        pd.DataFrame: DataFrame with adjustment values calculated.
    """
    condition_outlier = (
        (df["adjust"].fillna(False))
        & (df["transaction_code"] == transaction_code)
        & (df["year"] == year_to_adjust)
    )
    df.loc[condition_outlier, "con_gdhi"] = df["con_gdhi"] + adjustment_val

    condition_non_outlier = (
        (~df["adjust"].fillna(False))
        & (df["transaction_code"] == transaction_code)
        & (df["year"] == year_to_adjust)
    )
    df.loc[condition_non_outlier, "con_gdhi"] = df["con_gdhi"] + (
        abs(adjustment_val) * (df["uncon_gdhi"] / uncon_non_out_sum)
    )

    return df


def pivot_wide_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Pivots the DataFrame from long to wide format.

    Args:
        df (pd.DataFrame): The input DataFrame in long format.
        index_cols (list): List of columns to use as index in the pivot.

    Returns:
        pd.DataFrame: The pivoted DataFrame in wide format.
    """
    df = df.drop(columns=["uncon_gdhi"])

    # Pivot wide to get dates as columns
    df_wide = df.pivot(
        index=[
            "lsoa_code",
            "transaction_code",
        ],
        columns="year",
        values="con_gdhi",
    )
    df_wide.columns.name = None  # This removes the label from columns
    df_wide = df_wide.reset_index()

    id_cols = ["lsoa_code", "transaction_code"]

    # Rename only numeric column names
    df_wide = df_wide.rename(
        columns=lambda col: f"Adjust_Con_{col}" if col not in id_cols else col
    )

    return df_wide


def run_adjustment(config: dict) -> None:
    """
    Run the adjustment steps for the GDHI adjustment project.

    This function performs the following steps:
    1. Load the configuration settings.
    2. Load the input data.
    3. Filter PowerBI adjustment selection data.
    4. Join analyst output with Regional Accounts data.
    5. Pivot the DataFrame to long format for manipulation.
    6. Calculate scaling factors.
    7. Calculate headroom and midpoint of time series.
    8. Calculate adjustment and distribute amongst time series.
    9. Pivot data back to wide format.
    10. Save the adjusted data.

    Args:
        config (dict): Configuration dictionary containing user settings and
        pipeline settings.
    Returns:
        None: The function does not return any value. It saves the processed
        DataFrame to a CSV file.
    """
    logger.info("Adjustment started")

    logger.info("Loading configuration settings")
    local_or_shared = config["user_settings"]["local_or_shared"]
    filepath_dict = config[f"adjustment_{local_or_shared}_settings"]

    input_adj_file_path = (
        "C:/Users/" + os.getlogin() + filepath_dict["input_adj_file_path"]
    )
    input_constrained_file_path = (
        "C:/Users/"
        + os.getlogin()
        + filepath_dict["input_constrained_file_path"]
    )
    input_unconstrained_file_path = (
        "C:/Users/"
        + os.getlogin()
        + filepath_dict["input_unconstrained_file_path"]
    )

    input_adj_schema_path = config["pipeline_settings"][
        "input_adj_schema_path"
    ]
    input_constrained_schema_path = config["pipeline_settings"][
        "input_constrained_schema_path"
    ]
    input_unconstrained_schema_path = config["pipeline_settings"][
        "input_unconstrained_schema_path"
    ]

    output_dir = "C:/Users/" + os.getlogin() + filepath_dict["output_dir"]
    output_schema_path = filepath_dict["output_schema_path"]
    new_filename = filepath_dict.get("output_filename", None)

    logger.info("Reading in data with schemas")
    df_powerbi_output = read_with_schema(
        input_adj_file_path, input_adj_schema_path
    )
    df_constrained = read_with_schema(
        input_constrained_file_path, input_constrained_schema_path
    )
    df_unconstrained = read_with_schema(
        input_unconstrained_file_path, input_unconstrained_schema_path
    )

    logger.info("Filtering for data that requires adjustment.")
    df_powerbi_output = filter_lsoa_data(df_powerbi_output)

    logger.info("Joining analyst output and constrained DAP output")
    df = join_analyst_constrained_data(df_constrained, df_powerbi_output)

    logger.info("Joining analyst output and unconstrained DAP output")
    df = join_analyst_unconstrained_data(df_unconstrained, df)

    logger.info("Pivoting DataFrame long")
    df = pivot_adjustment_long(df)

    logger.info("Calculate scaling factors")
    df_scaling = calc_scaling_factors(df)

    logger.info("Calculating adjustment")
    df_anomaly_lsoas = create_anaomaly_list(df)

    for i in range(len(df_anomaly_lsoas)):
        lsoa_code = df_anomaly_lsoas.iloc[i]["lsoa_code"]
        print(lsoa_code)
        transaction_code = df_anomaly_lsoas.iloc[i]["transaction_code"]
        year_to_adjust = df_anomaly_lsoas.iloc[i]["year_to_adjust"].astype(int)

        logger.info("Calculating adjustment headroom")
        uncon_non_out_sum, headroom_val = calc_adjustment_headroom_val(
            df, df_scaling, lsoa_code, transaction_code, year_to_adjust
        )
        print(uncon_non_out_sum, headroom_val)
        logger.info("Calculating adjustment midpoint")
        outlier_val, midpoint_val = calc_midpoint_val(
            df, lsoa_code, transaction_code, year_to_adjust
        )
        print(outlier_val, midpoint_val)
        logger.info("Calculating adjustment value")
        adjustment_val = calc_adjustment_val(
            headroom_val, outlier_val, midpoint_val
        )
        print(adjustment_val)
        logger.info("Updating anomaly year")
        df = apply_adjustment(
            df,
            transaction_code,
            year_to_adjust,
            adjustment_val,
            uncon_non_out_sum,
        )
    df = pivot_wide_dataframe(df)

    # Save output file with new filename if specified
    if config["pipeline_settings"]["output_data"]:
        # Write DataFrame to CSV
        write_with_schema(df, output_schema_path, output_dir, new_filename)
