import os
import tomli  # tomli can be upgraded to tomllib in Python 3.11+
import logging
from typing import Union
import pathlib


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
