import os
import pandas as pd
from rap_project_example.utils.logger import logger_creator
from rap_project_example.cleanliness_checks import check_column_names, check_missing_data
from rap_project_example.preprocess import remove_nan_rows
from rap_project_example.test_function_agg import aggregate_data
from rap_project_example.config import load_toml_config
from rap_project_example.utils.configuration import load_schema_from_toml, validate_schema  


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

        # Load data
        logger.info(f"Loading data from {input_dir}")
        df = pd.read_excel(input_dir)
        logger.info("Data loaded successfully")

        # Load and validate schema
        schema_path = config["pipeline_settings"]["schema_path"]
        logger.info(f"Schema path specified in config: {schema_path}")
        logger.info("Loading schema configuration from TOML file")
        expected_schema = load_schema_from_toml(schema_path)
        logger.debug(f"Parsed expected schema: {expected_schema}")
        logger.info("Validating schema")
        validate_schema(df, expected_schema)
        logger.info("Schema validation passed successfully")

        # # Preprocess
        # logger.info("Preprocessing data")
        # df = remove_nan_rows(df)

        # # Perform aggregation
        # logger.info("Aggregating data")
        # result_df = aggregate_data(df)
        # logger.info("Pipeline completed successfully")

        # Save output file with new filename if specified
        if config["pipeline_settings"]["output_data"]:
            new_filename = config["pipeline_settings"].get("output_filename", None)
            if new_filename:
                new_output_path = os.path.join(os.path.dirname(output_dir), new_filename)
            else:
                new_output_path = output_dir # fallback to original
            logger.info(f"Saving data to {new_output_path}")
            df.to_csv(new_output_path, index=False)
    except Exception as e:
        logger.error(f"An error occurred during the pipeline execution: {e}", exc_info=True)
