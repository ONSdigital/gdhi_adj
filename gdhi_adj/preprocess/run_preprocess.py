"""Module for pre-processing data in the gdhi_adj project."""

import os

from gdhi_adj.preprocess.calc_preprocess import (
    calc_iqr,
    calc_lad_mean,
    calc_rate_of_change,
    calc_zscores,
)
from gdhi_adj.preprocess.flag_preprocess import (
    create_master_flag,
    flag_rollback_years,
)
from gdhi_adj.preprocess.join_preprocess import (
    concat_wide_dataframes,
    constrain_to_reg_acc,
)
from gdhi_adj.preprocess.pivot_preprocess import (
    pivot_output_long,
    pivot_wide_dataframe,
    pivot_years_long_dataframe,
)
from gdhi_adj.utils.helpers import read_with_schema, write_with_schema
from gdhi_adj.utils.logger import GDHI_adj_logger

GDHI_adj_LOGGER = GDHI_adj_logger(__name__)
logger = GDHI_adj_LOGGER.logger


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
    9. Pivot the DataFrame back to wide format.
    10. Save the processed data.

    Args:
        config (dict): Configuration dictionary containing user settings and
        pipeline settings.
    Returns:
        None: The function does not return any value. It saves the processed
        DataFrame to a CSV file.
    """
    logger.info("Preprocessing started")

    logger.info("Loading configuration settings")
    local_or_shared = config["user_settings"]["local_or_shared"]
    filepath_dict = config[f"preprocessing_{local_or_shared}_settings"]

    input_gdhi_file_path = (
        "C:/Users/" + os.getlogin() + filepath_dict["input_gdhi_file_path"]
    )
    input_ra_lad_file_path = (
        "C:/Users/" + os.getlogin() + filepath_dict["input_ra_lad_file_path"]
    )

    input_gdhi_schema_path = config["pipeline_settings"][
        "input_gdhi_schema_path"
    ]
    input_ra_lad_schema_path = config["pipeline_settings"][
        "input_ra_lad_schema_path"
    ]
    transaction_name = config["preprocessing_shared_settings"][
        "transaction_name"
    ]

    output_dir = "C:/Users/" + os.getlogin() + filepath_dict["output_dir"]
    output_schema_path = filepath_dict["output_schema_path"]
    new_filename = filepath_dict.get("output_filename", None)
    logger.info("Configuration settings loaded successfully")

    logger.info("Reading in data with schemas")
    df = read_with_schema(input_gdhi_file_path, input_gdhi_schema_path)
    ra_lad = read_with_schema(input_ra_lad_file_path, input_ra_lad_schema_path)

    logger.info("Pivoting data to long format")
    df = pivot_years_long_dataframe(df, "year", "gdhi_annual")
    ra_lad = pivot_years_long_dataframe(ra_lad, "year", "gdhi_annual")

    logger.info("Calculating rate of change")
    df = calc_rate_of_change(
        False, df, ["lsoa_code", "year"], "lsoa_code", "gdhi_annual"
    )
    df = calc_rate_of_change(
        True, df, ["lsoa_code", "year"], "lsoa_code", "gdhi_annual"
    )
    df = flag_rollback_years(df)

    # Assign prefixes
    backward_prefix = "bkwd"
    forward_prefix = "frwd"
    raw_prefix = "raw"

    logger.info("Calculating z-scores")
    df = calc_zscores(df, backward_prefix, "lsoa_code", "backward_pct_change")
    df = calc_zscores(df, forward_prefix, "lsoa_code", "forward_pct_change")
    df = calc_zscores(df, raw_prefix, "lsoa_code", "gdhi_annual")
    logger.info("Calculating IQRs")
    df = calc_iqr(df, backward_prefix, "lsoa_code", "backward_pct_change")
    df = calc_iqr(df, forward_prefix, "lsoa_code", "forward_pct_change")
    df = calc_iqr(df, raw_prefix, "lsoa_code", "gdhi_annual")

    df = create_master_flag(df)

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
        "year",
        "gdhi_annual",
        "master_z_flag",
        "master_iqr_flag",
        "master_flag",
    ]

    df = df[cols_to_keep]

    logger.info("Calculating LAD mean and constraining to regional accounts")
    df = calc_lad_mean(df)

    df = constrain_to_reg_acc(df, ra_lad, transaction_name)

    logger.info("Pivoting data back to wide format")
    # Pivot outlier df
    df_outlier = df.drop(columns=["mean_non_out_gdhi", "conlsoa_mean"])
    df_outlier = pivot_output_long(df_outlier, "gdhi_annual", "conlsoa_gdhi")
    df_outlier = pivot_wide_dataframe(df_outlier)

    # Pivot mean df
    df_mean = df.drop(columns=["gdhi_annual", "conlsoa_gdhi"])
    df_mean = pivot_output_long(df_mean, "mean_non_out_gdhi", "conlsoa_mean")
    df_mean = pivot_wide_dataframe(df_mean)
    df_mean["master_flag"] = "MEAN"

    df = concat_wide_dataframes(df_outlier, df_mean)

    # Save output file with new filename if specified
    if config["pipeline_settings"]["output_data"]:
        # Write DataFrame to CSV
        write_with_schema(df, output_schema_path, output_dir, new_filename)
