import os
import pandas as pd
from rap_project_example.utils.logger import logger_creator
from rap_project_example.preprocess import remove_nan_rows
from rap_project_example.utils.helpers import load_schema_from_toml, validate_schema, convert_column_types, rename_columns, load_toml_config


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
        df = pd.read_csv(input_dir)
        logger.info("Data loaded successfully")

        # Load and validate schema
        schema_path = config["pipeline_settings"]["schema_path"]
        logger.info(f"Schema path specified in config: {schema_path}")
        logger.info("Loading schema configuration from TOML file")
        expected_schema = load_schema_from_toml(schema_path)
        rename_columns(df, expected_schema, logger)
        logger.debug(f"Renamed columns based on schema: {expected_schema}")
        convert_column_types(df, expected_schema, logger)
        logger.debug(f"Parsed expected schema: {expected_schema}")
        logger.info("Validating schema")
        validate_schema(df, expected_schema)
        logger.info("Schema validation passed successfully")

        # # Preprocess
        # logger.info("Preprocessing data")
        #df = remove_nan_rows(df)

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
