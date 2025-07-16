"""Title for pipeline.py module"""

import os
from gdhi_adj.utils.logger import logger_creator
from gdhi_adj.utils.helpers import (
    load_toml_config,
    read_with_schema,
    write_with_schema,
)
from gdhi_adj.preprocess import (
    pivot_long_dataframe,
    rate_of_change,
    calc_zscores,
)


def run_pipeline(config_path):
    logger = logger_creator()
    logger.info("Pipeline started")

    # Load config
    config = load_toml_config(config_path)
    local_or_shared = config["user_settings"]["local_or_shared"]
    filepath_dict = config[f"{local_or_shared}_settings"]
    input_file_path = "C:/Users/" + os.getlogin() + filepath_dict["input_file_path"]
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
        df = rate_of_change(True, df, ["lsoa_code", "year"], "lsoa_code", "gdhi_annual")

        df = calc_zscores(df, "bkwd", "lsoa_code", "backward_pct_change")
        df = calc_zscores(df, "frwd", "lsoa_code", "forward_pct_change")
        df = calc_zscores(df, "raw", "lsoa_code", "gdhi_annual")

        # print(df.head(20))
        # print(df.tail(20))

        # Save output file with new filename if specified
        if config["pipeline_settings"]["output_data"]:
            # Write DataFrame to CSV
            write_with_schema(df, output_schema_path, output_dir, new_filename)
            # print(df.head(1))
    except Exception as e:
        logger.error(
            f"An error occurred during the pipeline execution: {e}", exc_info=True
        )
