"""Module for pre-processing data in the gdhi_adj project."""

import os

import pandas as pd

from gdhi_adj.adjustment.filter_adjustment import filter_year
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
    4. Calculate percentage rate of change and flag rollback years.
    5. Calculate z-scores and IQRs if desired as per config.
    6. Create master flags.
    7. Save interim data with all calculated values.
    8. Calculate LAD mean GDHI.
    9. Constrain outliers to regional accounts.
    10. Pivot the DataFrame back to wide format.
    11. Save the preprocessed data ready for PowerBI analysis.

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
    schema_path = config["pipeline_settings"]["schema_path"]

    input_unconstrained_file_path = (
        "C:/Users/"
        + os.getlogin()
        + filepath_dict["input_dir"]
        + filepath_dict["input_unconstrained_file_path"]
    )
    input_ra_lad_file_path = (
        "C:/Users/"
        + os.getlogin()
        + filepath_dict["input_dir"]
        + filepath_dict["input_ra_lad_file_path"]
    )

    # match = re.search(
    #     r".GDHI_Disclosure_(.*?)_[^_]+\.csv", input_unconstrained_file_path
    # )

    # if match:
    #     gdhi_suffix = match.group(1) + "_"
    gdhi_suffix = config["user_settings"]["output_data_prefix"] + "_"

    input_gdhi_schema_path = (
        schema_path + config["pipeline_settings"]["input_gdhi_schema_name"]
    )
    input_ra_lad_schema_path = (
        schema_path + config["pipeline_settings"]["input_ra_lad_schema_name"]
    )

    start_year = config["user_settings"]["start_year"]
    end_year = config["user_settings"]["end_year"]

    zscore_calculation = config["user_settings"]["zscore_calculation"]
    iqr_calculation = config["user_settings"]["iqr_calculation"]

    zscore_upper_threshold = config["user_settings"]["zscore_upper_threshold"]
    zscore_lower_threshold = config["user_settings"]["zscore_lower_threshold"]

    iqr_lower_quantile = config["user_settings"]["iqr_lower_quantile"]
    iqr_upper_quantile = config["user_settings"]["iqr_upper_quantile"]
    iqr_multiplier = config["user_settings"]["iqr_multiplier"]

    transaction_name = config["user_settings"]["transaction_name"]

    output_dir = "C:/Users/" + os.getlogin() + filepath_dict["output_dir"]
    output_schema_path = (
        schema_path
        + config["pipeline_settings"]["output_preprocess_schema_path"]
    )
    interim_filename = gdhi_suffix + filepath_dict.get(
        "interim_filename", None
    )
    new_filename = gdhi_suffix + filepath_dict.get("output_filename", None)
    logger.info("Configuration settings loaded successfully")

    logger.info("Reading in data with schemas")
    df = read_with_schema(
        input_unconstrained_file_path, input_gdhi_schema_path
    )
    ra_lad = read_with_schema(input_ra_lad_file_path, input_ra_lad_schema_path)

    logger.info("Pivoting data to long format")
    df = pivot_years_long_dataframe(
        df, new_var_col="year", new_val_col="uncon_gdhi"
    )
    ra_lad = pivot_years_long_dataframe(
        ra_lad, new_var_col="year", new_val_col="uncon_gdhi"
    )

    logger.info("Filtering data for specified years")
    df = filter_year(df, start_year, end_year)

    logger.info("Calculating rate of change")
    df = calc_rate_of_change(
        df,
        ascending=False,
        sort_cols=["lsoa_code", "year"],
        group_col="lsoa_code",
        val_col="uncon_gdhi",
    )
    df = calc_rate_of_change(
        df,
        ascending=True,
        sort_cols=["lsoa_code", "year"],
        group_col="lsoa_code",
        val_col="uncon_gdhi",
    )
    df = flag_rollback_years(df)

    # Assign prefixes
    backward_prefix = "bkwd"
    forward_prefix = "frwd"
    raw_prefix = "raw"

    logger.info("Flagging of outliers")
    if zscore_calculation:

        logger.info("Calculating z-scores")
        df = calc_zscores(
            df,
            score_prefix=backward_prefix,
            group_col="lad_code",
            val_col="backward_pct_change",
            zscore_upper_threshold=zscore_upper_threshold,
            zscore_lower_threshold=zscore_lower_threshold,
        )
        df = calc_zscores(
            df,
            score_prefix=forward_prefix,
            group_col="lad_code",
            val_col="forward_pct_change",
            zscore_upper_threshold=zscore_upper_threshold,
            zscore_lower_threshold=zscore_lower_threshold,
        )

    if iqr_calculation:

        logger.info("Calculating IQRs")
        df = calc_iqr(
            df,
            iqr_prefix=raw_prefix,
            group_col=["lad_code", "year"],
            val_col="uncon_gdhi",
            iqr_lower_quantile=iqr_lower_quantile,
            iqr_upper_quantile=iqr_upper_quantile,
            iqr_multiplier=iqr_multiplier,
        )

    df = create_master_flag(df, zscore_calculation, iqr_calculation)

    logger.info("Saving interim data")
    qa_df = pd.DataFrame(
        {
            "config": [
                f"zscore_lower_threshold = {zscore_lower_threshold}",
                f"zscore_upper_threshold = {zscore_upper_threshold}",
                f"iqr_lower_quantile = {iqr_lower_quantile}",
                f"iqr_upper_quantile = {iqr_upper_quantile}",
                f"iqr_multiplier = {iqr_multiplier}",
                f"transaction_name = {transaction_name}",
            ],
        }
    )
    qa_df.to_csv(
        output_dir + gdhi_suffix + "manual_adj_preprocessing_config.txt",
        index=False,
        header=False,
    )

    logger.info(f"{output_dir + interim_filename}")
    df.to_csv(
        output_dir + interim_filename,
        index=False,
    )
    logger.info("Data saved successfully")

    # Keep base data and flags, dropping scores columns
    flag_cols = [col for col in df.columns if col.startswith("master_")]
    cols_to_keep = [
        "lsoa_code",
        "lsoa_name",
        "lad_code",
        "lad_name",
        "year",
        "uncon_gdhi",
    ] + flag_cols

    df = df[cols_to_keep]

    logger.info("Calculating LAD mean and constraining to regional accounts")
    df = calc_lad_mean(df)

    df = constrain_to_reg_acc(df, ra_lad, transaction_name)

    logger.info("Pivoting data back to wide format")
    # Pivot outlier df
    df_outlier = df.drop(columns=["mean_non_out_gdhi", "conlsoa_mean"])
    df_outlier = pivot_output_long(df_outlier, "uncon_gdhi", "conlsoa_gdhi")
    df_outlier = pivot_wide_dataframe(df_outlier)

    # Pivot mean df
    df_mean = df.drop(columns=["uncon_gdhi", "conlsoa_gdhi"])
    df_mean = pivot_output_long(df_mean, "mean_non_out_gdhi", "conlsoa_mean")
    df_mean = pivot_wide_dataframe(df_mean)
    df_mean["master_flag"] = "MEAN"

    df = concat_wide_dataframes(df_outlier, df_mean)

    # Save output file with new filename if specified
    if config["user_settings"]["output_data"]:
        # Write DataFrame to CSV
        write_with_schema(df, output_schema_path, output_dir, new_filename)
