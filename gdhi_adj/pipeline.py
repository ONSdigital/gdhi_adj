"""Title for pipeline.py module"""

import os

from gdhi_adj.preprocess import (
    calc_iqr,
    calc_zscores,
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
    input_file_path = (
        "C:/Users/" + os.getlogin() + filepath_dict["input_file_path"]
    )
    output_dir = "C:/Users/" + os.getlogin() + filepath_dict["output_dir"]
    input_schema_path = config["pipeline_settings"]["input_schema_path"]
    output_schema_path = config["pipeline_settings"]["output_schema_path"]
    new_filename = config["pipeline_settings"].get("output_filename", None)

    try:

        df = read_with_schema(input_file_path, input_schema_path)

        # Preprocess
        df = pivot_long_dataframe(df, "year", "gdhi_annual")
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

        # Save output file with new filename if specified
        if config["pipeline_settings"]["output_data"]:
            # Write DataFrame to CSV
            write_with_schema(df, output_schema_path, output_dir, new_filename)
            # print(df)
            # print(df[df["master_flag"] == True]["lsoa_code"].unique())
    except Exception as e:
        logger.error(
            f"An error occurred during the pipeline execution: {e}",
            exc_info=True,
        )
