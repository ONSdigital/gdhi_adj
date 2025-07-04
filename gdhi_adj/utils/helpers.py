"""Define helper functions that wrap regularly-used functions."""

import os
import tomli  # tomli can be upgraded to tomllib in Python 3.11+
import logging
from typing import Union
import pathlib
import toml
import pandas as pd
from gdhi_adj.utils.logger import logger_creator

logger = logger_creator()


def load_toml_config(path: Union[str, pathlib.Path]) -> dict | None:
    """Load a .toml file from a path, with logging and safe error handling.

    Args:
        path (Union[str, pathlib.Path]): The path to load the .toml file from.

    Returns:
        dict | None: The loaded toml file as a dictionary, or None on error.
    """
    logger = logging.getLogger("ConfigLoader")
    if not os.path.exists(path):
        logger.error(f"Config file does not exist: {path}")
        return None
    ext = os.path.splitext(path)[1]
    if ext != ".toml":
        logger.error(f"Expected a .toml file. Got {ext}")
        return None
    try:
        with open(path, "rb") as f:
            toml_dict = tomli.load(f)
        return toml_dict
    except tomli.TOMLDecodeError as e:
        logger.error(f"Failed to decode TOML file: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error loading TOML file: {e}")
        return None


def load_schema_from_toml(schema_path):
    raw_schema = toml.load(schema_path)
    return {
        new_name: {
            "old_name": props["old_name"],
            "Deduced_Data_Type": props["Deduced_Data_Type"],
        }
        for new_name, props in raw_schema.items()
    }


def validate_schema(df, schema):
    type_map = {"int": int, "float": float, "str": str, "bool": bool}

    for column, props in schema.items():
        expected_type_str = props.get("Deduced_Data_Type")
        expected_type = type_map.get(expected_type_str)

        if column not in df.columns:
            raise ValueError(f"Missing expected column: {column}")
        if expected_type and not df[column].map(type).eq(expected_type).all():
            raise TypeError(
                f"Column '{column}' does not match expected type {expected_type.__name__}"
            )


def rename_columns(df, schema, logger):
    """
    Renames columns in the DataFrame based on the schema.
    Schema should be a dict where keys are new column names and values are dicts with 'old_name'.
    """
    for new_name, props in schema.items():
        old_name = props.get("old_name")
        if old_name and old_name in df.columns and old_name != new_name:
            df.rename(columns={old_name: new_name}, inplace=True)
            logger.info(f"Renamed column '{old_name}' to '{new_name}'")
    return df


def convert_column_types(df, schema, logger):
    type_map = {"int": int, "float": float, "str": str, "bool": bool}

    for column, props in schema.items():
        expected_type_str = props.get("Deduced_Data_Type")
        expected_type = type_map.get(expected_type_str)

        if column in df.columns and expected_type:
            original_dtype = df[column].dtype
            try:
                if expected_type == int:
                    df[column] = pd.to_numeric(df[column], errors="coerce").astype(
                        "Int64"
                    )
                elif expected_type == float:
                    df[column] = pd.to_numeric(df[column], errors="coerce").astype(
                        "float"
                    )
                elif expected_type == str:
                    df[column] = df[column].astype(str)
                elif expected_type == bool:
                    df[column] = df[column].astype(bool)
                logger.info(
                    f"Converted column '{column}' from {original_dtype} to {expected_type_str}"
                )
            except Exception as e:
                logger.warning(
                    (
                        f"Failed to convert column '{column}' from {original_dtype} "
                        f"to {expected_type_str}: {e}"
                    )
                )
    return df


def read_with_schema(input_dir, input_schema):
    """
    Reads in a csv file and compares it to a data dictionary schema.

    Parameters:
    input_dir (string): Filepath to the csv file to be read in.
    input_schema (string): Filepath to the schema file in TOML format.

    Returns:
    df (pd.DataFrame): Formatted dataFrame containing data from the csv file.
    """
    # Load data
    logger.info(f"Loading data from {input_dir}")
    df = pd.read_csv(input_dir)
    logger.info("Data loaded successfully")

    # Load and validate schema
    logger.info(f"Schema path specified in config: {input_schema}")
    logger.info("Loading schema configuration from TOML file")
    expected_schema = load_schema_from_toml(input_schema)
    rename_columns(df, expected_schema, logger)
    logger.debug(f"Renamed columns based on schema: {expected_schema}")
    convert_column_types(df, expected_schema, logger)
    logger.debug(f"Parsed expected schema: {expected_schema}")
    logger.info("Validating schema")
    validate_schema(df, expected_schema)
    logger.info("Schema validation passed successfully")

    return df


def write_with_schema(df, output_schema, output_dir, new_filename=None):
    """
    Reads in a csv file and compares it to a data dictionary schema.

    Parameters:
    output_schema (string): Filepath to the schema file in TOML format.
    output_dir (string): Filepath to the csv file to be written out.

    Returns:
    None: Writes the DataFrame to a CSV file after validating against the schema.
    """
    # Load and validate schema
    logger.info(f"Schema path specified in config: {output_schema}")
    logger.info("Loading schema configuration from TOML file")
    expected_schema = load_schema_from_toml(output_schema)
    rename_columns(df, expected_schema, logger)
    logger.debug(f"Renamed columns based on schema: {expected_schema}")
    logger.info("Validating schema")
    validate_schema(df, expected_schema)
    logger.info("Schema validation passed successfully")

    # Write DataFrame to CSV
    logger.info(f"Writing data to {output_dir}")
    # Ensure output directory exists
    if new_filename:
        new_output_path = os.path.join(os.path.dirname(output_dir), new_filename)
    else:
        new_output_path = output_dir  # fallback to original
    logger.debug(f"Ensured output directory exists: {os.path.dirname(output_dir)}")
    # Convert DataFrame to CSV
    logger.info(f"Saving data to {new_output_path}")
    df.to_csv(new_output_path, index=False)
    logger.info("Data saved successfully")
