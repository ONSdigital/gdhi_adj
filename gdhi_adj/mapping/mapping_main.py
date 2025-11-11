"""Mapping functions for local authority units mapped to LADs."""

import re
from os.path import join

import pandas as pd

from gdhi_adj.utils.helpers import load_toml_config, read_with_schema
from gdhi_adj.utils.logger import GDHI_adj_logger

# Initialize logger
GDHI_adj_LOGGER = GDHI_adj_logger("Mapping")
logger = GDHI_adj_LOGGER.logger


def load_data(config, df=pd.DataFrame()):
    if df.empty:
        logger.info("Input dataframe is empty.")
        data_file_path = join(
            config["mapping"]["data_dir"], config["mapping"]["data_file"]
        )
        logger.info(f"Reading data from {data_file_path}")
        df = pd.read_csv(data_file_path)

    return df


def rename_s30_to_lad(config, df):
    # If S30 column exists, rename LAD columns to to LAU, because for England
    # it's the same, but for Scotland they are actually LAU codes
    lad_code_col = config["mapping"]["data_lad_code"]
    lad_name_col = config["mapping"]["data_lad_name"]

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


def load_mapper(config):
    mapper_file_path = join(
        config["mapping"]["mapper_dir"], config["mapping"]["lau_lad_file"]
    )
    mapper_schema_path = join(
        config["pipeline_settings"]["schema_path"],
        config["mapping"]["lau_lad_schema_name"],
    )
    logger.info(f"Loading LAU to LAD mapping from {mapper_file_path}")
    mapper_df = read_with_schema(mapper_file_path, mapper_schema_path)
    return mapper_df


def clean_validate_mapper(mapper_df):

    mapper_df = mapper_df[
        [
            "mapper_lad_code",
            "mapper_lad_name",
            "mapper_lau_code",
            "mapper_lau_name",
        ]
    ]

    mapper_df = mapper_df.drop_duplicates()

    print(mapper_df.columns)

    return mapper_df, True


def join_mapper(df, mapper_df):
    result_df = df.merge(
        mapper_df,
        left_on="data_lau_code",
        right_on="mapper_lau_code",
        how="left",
    )
    cond = result_df["mapper_lad_code"].isna().any()

    logger.info(f"There are null LADs: {cond}")
    result_df = result_df.drop(columns=["data_lau_code", "data_lau_name"])
    return result_df


def aggregate_lad(df):
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

    df.rename(
        columns={"mapper_lad_code": "lad_code", "mapper_lad_name": "lad_name"},
        inplace=True,
    )
    return df[original_columns]


def save_output(config, df):
    output_dir = config["mapping"]["output_dir"]
    output_file = config["mapping"]["output_file"]
    output_path = join(output_dir, output_file)
    logger.info(f"Saving output to {output_path}")
    df.to_csv(output_path, index=None)


def lau_lad_main(config_path="config/config.toml", df=pd.DataFrame()):
    logger.info("Started mapping LAUs to LADs")
    config = load_toml_config(config_path)

    # Load data file, if it is not provided as a DataFrame
    df = load_data(config)
    original_columns = df.columns.tolist()
    df, need_mapping = rename_s30_to_lad(config, df)

    print(f"Mapping needed: {need_mapping}")
    if need_mapping:
        mapper_df = load_mapper(config)
        mapper_df, valid_mapper = clean_validate_mapper(mapper_df)

        result_df = join_mapper(df, mapper_df)
        if config["mapping"]["aggregate_to_lad"]:
            logger.info("Starting aggregating data to LAD level as requested.")
            result_df = aggregate_lad(result_df)
        else:
            logger.info("Aggregation to LAD not requested.")
        result_df = reformat(result_df, original_columns)
        save_output(config, result_df)
        logger.info("Completed mapping LAUs to LADs")
        return result_df
    else:
        logger.info(
            "Mapping LAUs to LADs not needed. Returning original dataframe."
        )
        return df


if __name__ == "__main__":
    lau_lad_main()
