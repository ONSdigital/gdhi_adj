import pandas as pd
from rap_project_example.utils.logger import logger_creator
from rap_project_example.cleanliness_checks import check_column_names, check_missing_data
from rap_project_example.preprocess import remove_nan_rows
from rap_project_example.test_function_agg import aggregate_data
from rap_project_example.config import load_toml_config


def run_pipeline(config_path):
    logger = logger_creator()
    logger.info("Pipeline started")
    # Load config
    config = load_toml_config(config_path)
    local_or_shared = config["user_settings"]["local_or_shared"]
    filepath_dict = config[f"{local_or_shared}_settings"]
    input_dir = filepath_dict["input_dir"]
    output_dir = filepath_dict["output_dir"]
    try:
        logger.info(f"Loading data from {input_dir}")
        df = pd.read_csv(input_dir)
        # Cleanliness check
        logger.info("Performing cleanliness checks")
        check_column_names(df)
        check_missing_data(df)
        # Preprocess
        logger.info("Preprocessing data")
        df = remove_nan_rows(df)
        # Perform aggregation
        logger.info("Aggregating data")
        result_df = aggregate_data(df)
        logger.info("Pipeline completed successfully")
        # Only save if output_data is True in config
        if config["pipeline_settings"]["output_data"]:
            logger.info(f"Saving data to {output_dir}")
            result_df.to_csv(output_dir, index=False)
    except Exception as e:
        logger.error(f"An error occurred during the pipeline execution: {e}", exc_info=True)
