"""Title for pipeline.py module"""

import os

from gdhi_adj.preprocess import (
    calc_iqr,
    calc_lad_mean,
    calc_zscores,
    constrain_to_reg_acc,
    create_master_flag,
    pivot_long_dataframe,
    rate_of_change,
)
from gdhi_adj.utils.helpers import (
    load_toml_config,
    read_with_schema,
    write_with_schema,
)
from gdhi_adj.utils.logger import GDHI_adj_logger

GDHI_adj_LOGGER = GDHI_adj_logger(__name__)
logger = GDHI_adj_LOGGER.logger


def run_pipeline(config_path):
    logger.info("Pipeline started")

    # Load config
    config = load_toml_config(config_path)
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

    try:
        # Import data
        df = read_with_schema(input_gdhi_file_path, input_gdhi_schema_path)
        ra_lad = read_with_schema(
            input_ra_lad_file_path, input_ra_lad_schema_path
        )

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
        df = calc_zscores(
            df, backward_prefix, "lsoa_code", "backward_pct_change"
        )
        df = calc_zscores(
            df, forward_prefix, "lsoa_code", "forward_pct_change"
        )
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
    except Exception as e:
        logger.error(
            f"An error occurred during the pipeline execution: {e}",
            exc_info=True,
        )
