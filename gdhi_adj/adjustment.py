"""Module for adjusting data in the gdhi_adj project."""

import os

import pandas as pd

from gdhi_adj.utils.helpers import read_with_schema
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

    df = df[df["adjust"].fillna(False)]

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


def run_adjustment(config: dict) -> None:
    """
    Run the adjustment steps for the GDHI adjustment project.

    This function performs the following steps:
    1. Load the configuration settings.
    2. Load the input data.
    3. Filter PowerBI adjustment selection data.

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

    # output_dir = "C:/Users/" + os.getlogin() + filepath_dict["output_dir"]
    # output_schema_path = config["pipeline_settings"]["output_schema_path"]

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

    logger.info("Joining analyst output and constrained DAP output")
    df = join_analyst_unconstrained_data(df_unconstrained, df)
    print(df)
