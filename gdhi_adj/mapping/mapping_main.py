"""Module for local authority units mapped to LADs."""

import os
import re

import pandas as pd

from gdhi_adj.utils.helpers import read_with_schema, write_with_schema
from gdhi_adj.utils.logger import GDHI_adj_logger

# Initialize logger
GDHI_adj_LOGGER = GDHI_adj_logger(__name__)
logger = GDHI_adj_LOGGER.logger


def rename_s30_to_lau(config, df):
    """
    Rename column containing S30 area codes to lau_

    Args:
        config (dict): Configuration dictionary containing user settings and
            pipeline settings.
        df (pd.DataFrame): DataFrame containing adjusted data.

    Returns:
        pd.DataFrame: DataFrame with area code column renamed if S30 codes
            have been found, otherwise returns original DataFrame.
        need_mapping (Boolean): Returned boolean to show mapping is needed.

    """
    # If S30 column exists, rename LAD columns to to LAU, because for England
    # it's the same, but for Scotland they are actually LAU codes
    lad_code_col = config["mapping_settings"]["data_lad_code"]
    lad_name_col = config["mapping_settings"]["data_lad_name"]

    # By default, assume no mapping is needed
    need_mapping = False

    has_lad_column = lad_code_col in df.columns
    if has_lad_column:
        logger.info(f"Dataframe has column {lad_code_col}")
        has_S30_codes = (
            df[lad_code_col].astype(str).str.startswith("S30").any()
        )
        if has_S30_codes:
            logger.info("Detected S30 codes in LAD code column")
            logger.info(
                f"Renaming {lad_code_col} to LAU code and "
                f"{lad_name_col} to LAU name"
            )
            df = df.rename(
                columns={
                    lad_code_col: "data_lau_code",
                    lad_name_col: "data_lau_name",
                }
            )
            need_mapping = True
        else:
            logger.info("No S30 codes detected in LAD code column.")
    return df, need_mapping


def clean_validate_mapper(mapper_df):
    """
    Subset the mapper and get a unique DataFrame of values.

    Args:
        mapper_df (pd.DataFrame): DataFrame containing data used to join
        LADs to LAUs.

    Returns:
        pd.DataFrame: DataFrame with unique values for LADs and LAUs.
    """
    mapper_df = mapper_df[
        [
            "mapper_lad_code",
            "mapper_lad_name",
            "mapper_lau_code",
            "mapper_lau_name",
        ]
    ]

    mapper_df = mapper_df.drop_duplicates()

    return mapper_df, True


def join_mapper(df, mapper_df):
    """
    Join mapper containing a lookup of LAU and LAD values, to adjusted data.

    Args:
        df (pd.DataFrame): DataFrame containing adjusted data.
        mapper_df (pd.DataFrame): DataFrame containing LAU to LAD lookup.

    Returns:
        pd.DataFrame: DataFrame with LADs joined on LAU codes.
    """
    result_df = df.merge(
        mapper_df,
        left_on="data_lau_code",
        right_on="mapper_lau_code",
        how="left",
    )
    null_series = result_df["mapper_lad_code"].isna()
    cond = null_series.any()
    if cond:
        logger.info(f"There are null LADs: {result_df[null_series]}")
    result_df = result_df.drop(columns=["data_lau_code", "data_lau_name"])
    return result_df


def aggregate_lad(df):
    """
    Aggregate values on LADs and other identifiers.

    Args:
        df (pd.DataFrame): DataFrame containing adjusted data with LAD codes
            joined.

    Returns:
        pd.DataFrame: DataFrame containing value columns now aggregated by
            sum on identifier columns.
    """
    geo_columns = ["mapper_lad_code", "mapper_lad_name"]

    # Get all column names
    all_columns = df.columns.tolist()

    # Define the pattern: starts with 1 or 2, followed by exactly 3 digits
    pattern = r"^[12]\d{3}$"

    # Filter columns matching the pattern
    value_columns = [col for col in all_columns if re.match(pattern, col)]
    other_columns = [
        col for col in all_columns if col not in geo_columns + value_columns
    ]

    agg_columns = geo_columns + other_columns
    agg_df = df.groupby(agg_columns, as_index=False)[value_columns].sum()
    logger.info("Aggregated data to LAD level")
    return agg_df


def reformat(df, original_columns):
    """
    Rename LAD columns for end format.

    Args:
        df (pd.DataFrame): DataFrame containing adjusted data and LAD codes.
        original_columns (list): List of columns from original DataFrame.

    Returns:
        pd.DataFrame: Renamed DataFrame with desired columns.
    """
    df.rename(
        columns={"mapper_lad_code": "lad_code", "mapper_lad_name": "lad_name"},
        inplace=True,
    )
    return df[original_columns]


def run_mapping(config: dict, df=pd.DataFrame()):
    """
    Run the mapping steps for the GDHI adjustment pipeline.

    This function performs the follwing steps:
    1. Load the input data.
    2. Rename column containing S30 values to LAU and verify mapping is
        required.
    If mapping is required:
    3. Load in and clean mapper containing LAU to LAD lookup.
    4. Join LAU-LAD mapper to adjusted data.
    5. Aggregate to LAD if specified in config.
    6. Reformat and save output.

    """
    logger.info("Started mapping LAUs to LADs")

    df = read_with_schema(
        input_file_path=os.path.join(
            "C:/Users",
            os.getlogin(),
            config["mapping_settings"]["input_adj_file_dir"],
            config["mapping_settings"]["input_adj_file_name"],
        ),
        input_schema_path=os.path.join(
            config["pipeline_settings"]["schema_path"],
            config["pipeline_settings"]["output_adjustment_schema_path"],
        ),
    )

    original_columns = df.columns.tolist()
    df, need_mapping = rename_s30_to_lau(config, df)

    print(f"Mapping needed: {need_mapping}")
    if need_mapping:
        mapper_df = read_with_schema(
            input_file_path=os.path.join(
                "C:/Users/",
                os.getlogin(),
                config["mapping_settings"]["input_lau_lad_mapper_dir"],
                config["mapping_settings"]["input_lau_lad_mapper_file"],
            ),
            input_schema_path=os.path.join(
                config["pipeline_settings"]["schema_path"],
                config["pipeline_settings"][
                    "input_mapping_lau_lad_schema_name"
                ],
            ),
        )

        mapper_df, valid_mapper = clean_validate_mapper(mapper_df)

        result_df = join_mapper(df, mapper_df)

        output_name = (
            config["user_settings"]["output_data_prefix"]
            + "_"
            + config["mapping_settings"]["output_filename"]
        )
        if config["mapping_settings"]["aggregate_to_lad"]:
            logger.info("Starting aggregating data to LAD level as requested.")
            result_df = aggregate_lad(result_df)
            output_name = output_name.replace(".csv", "_aggregated.csv")
        else:
            logger.info("Aggregation to LAD not requested.")
            output_name = output_name.replace(".csv", "_not_aggregated.csv")
        result_df = reformat(result_df, original_columns)

        write_with_schema(
            result_df,
            output_schema_path=os.path.join(
                config["pipeline_settings"]["schema_path"],
                config["pipeline_settings"]["output_mapping_schema_path"],
            ),
            output_dir=os.path.join(
                "C:/Users/",
                os.getlogin(),
                config["mapping_settings"]["output_dir"],
            ),
            new_filename=output_name,
        )
        logger.info("Completed mapping LAUs to LADs")
    else:
        logger.info("Mapping LAUs to LADs not needed. No S30 codes detected.")
