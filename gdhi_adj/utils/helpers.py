import os
import tomli  # tomli can be upgraded to tomllib in Python 3.11+
import logging
from typing import Union
import pathlib
import toml
import pandas as pd

"""Define helper functions that wrap regularly-used functions."""


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
