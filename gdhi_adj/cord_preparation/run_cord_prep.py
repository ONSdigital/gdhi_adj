"""Module for pre-processing data in the gdhi_adj project."""

import os

from gdhi_adj.cord_preparation.impute_cord_prep import impute_suppression_x
from gdhi_adj.utils.helpers import read_with_schema, write_with_schema
from gdhi_adj.utils.logger import GDHI_adj_logger

GDHI_adj_LOGGER = GDHI_adj_logger(__name__)
logger = GDHI_adj_LOGGER.logger


def run_cord_preparation(config: dict) -> None:
    """
    Run the CORD preparation steps for the GDHI adjustment project.

    This function performs the following steps:
    1. Load the configuration settings.
    2. Load the input data.
    3. Apply CORD-specific transformations.
    4. Save the prepared CORD data for further processing.

    Args:
        config (dict): Configuration dictionary containing user settings and
        pipeline settings.
    Returns:
        None: The function does not return any value. It saves the processed
        DataFrame to a CSV file.
    """
    logger.info("CORD Preparation started")

    logger.info("Loading configuration settings")
    local_or_shared = config["user_settings"]["local_or_shared"]
    adjustment_filepath_dict = config[f"adjustment_{local_or_shared}_settings"]
    filepath_dict = config[f"cord_prep_{local_or_shared}_settings"]
    schema_path = config["pipeline_settings"]["schema_path"]

    input_cord_file_path = os.path.join(
        "C:/Users/",
        os.getlogin(),
        adjustment_filepath_dict["output_dir"],
        adjustment_filepath_dict["output_filename"],
    )
    input_cord_prep_schema_path = os.path.join(
        schema_path,
        config["pipeline_settings"]["output_adjustment_schema_path"],
    )

    output_data_prefix = config["user_settings"]["output_data_prefix"] + "_"
    output_dir = "C:/Users/" + os.getlogin() + filepath_dict["output_dir"]
    output_schema_path = (
        schema_path
        + config["pipeline_settings"]["output_cord_prep_schema_path"]
    )
    output_filename = output_data_prefix + filepath_dict.get(
        "output_filename", None
    )

    logger.info("Reading in data with schemas")
    df = read_with_schema(input_cord_file_path, input_cord_prep_schema_path)

    logger.info("Applying CORD-specific transformations and filters")
    df = impute_suppression_x(
        df,
        target_cols=[
            "2010",
            "2011",
            "2012",
        ],
        transaction_col="transaction",
        lad_col="lad_code",
        transaction_value="D623",
        lad_val=["95", "S"],
    )

    # Save prepared CORD data file with new filename if specified
    if config["user_settings"]["output_data"]:
        write_with_schema(df, output_schema_path, output_dir, output_filename)

    logger.info("CORD Preparation completed and data saved")
