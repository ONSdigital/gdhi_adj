"""Module for validation checks prior to CORD in the gdhi_adj project."""

import pandas as pd

from gdhi_adj.utils.logger import GDHI_adj_logger

GDHI_adj_LOGGER = GDHI_adj_logger(__name__)
logger = GDHI_adj_LOGGER.logger


def check_lsoa_consistency(df: pd.DataFrame) -> pd.DataFrame:
    """
    Performs an internal consistency check on the DataFrame to ensure
    'lsoa_code' uniqueness matches the total row count.

    This function verifies that the number of unique values in the 'lsoa_code'
    column is exactly equal to the number of rows in the DataFrame.

    Args:
        df (pd.DataFrame): The input pandas DataFrame containing an
            'lsoa_code' column.

    Returns:
        pd.DataFrame: The original DataFrame, unchanged. This allows the
            function to be used in method chaining (e.g., .pipe()).

    Raises:
        ValueError: If the number of unique 'lsoa_code' values does not
            match the total number of rows in the DataFrame.
        KeyError: If the 'lsoa_code' column is missing from the DataFrame.
    """
    logger.info("Starting internal consistency check on DataFrame.")

    if "lsoa_code" not in df.columns:
        raise KeyError(
            "The column 'lsoa_code' was not found in the DataFrame."
        )

    n_unique = df["lsoa_code"].nunique()
    n_rows = len(df)

    if n_unique != n_rows:
        error_msg = (
            "Internal Consistency Check Failed: 'lsoa_code' "
            "is not unique per row. "
            f"Found {n_unique} unique codes across {n_rows} rows."
        )
        logger.error(error_msg)
        raise ValueError(error_msg)

    logger.info(
        f"Consistency check passed: {n_rows} rows match "
        f"{n_unique} unique lsoa_codes."
    )

    return df


def check_no_nulls(df: pd.DataFrame) -> pd.DataFrame:
    """
    Checks the entire DataFrame to ensure it contains no
    Null, NaN, or None values.

    This function scans all cells in the DataFrame. It detects
    standard numpy NaNs, Python None objects, and pandas pd.NA values.

    If any such value is found, it raises a ValueError.

    Args:
        df (pd.DataFrame): The input DataFrame to be validated.

    Returns:
        pd.DataFrame: The original DataFrame, unchanged, for method chaining.

    Raises:
        ValueError: If any null/NaN/None values are found in the DataFrame.
    """
    logger.info("Starting null value check on DataFrame.")

    if df.isnull().any().any():
        # Get a count of nulls per column for the error message
        null_counts = df.isnull().sum()
        columns_with_nulls = null_counts[null_counts > 0].to_dict()

        error_msg = (
            f"Null Value Check Failed: "
            "The DataFrame contains null/NaN values. "
            f"Columns with nulls: {columns_with_nulls}"
        )
        logger.error(error_msg)
        raise ValueError(error_msg)

    logger.info("Null value check passed: No nulls found in DataFrame.")

    return df


def check_year_column_completeness(df: pd.DataFrame) -> pd.DataFrame:
    """
    Verifies that the DataFrame contains a complete set of
    consecutive year columns.

    This function automatically identifies numeric column names
    (integers or strings representing integers), determines the
    minimum and maximum years, and checks if every year between
    that minimum and maximum exists as a column.

    Args:
        df (pd.DataFrame): The input DataFrame.

    Returns:
        pd.DataFrame: The original DataFrame, unchanged, for method chaining.

    Raises:
        ValueError:
            - If no numeric/year columns are found.
            - If there are gaps in the sequence of years detected.
    """
    logger.info("Starting year column completeness check.")

    # Identify columns that look like years (numeric)
    year_cols = []
    for col in df.columns:
        # Handle cases where column names are strings or integers
        if str(col).isdigit():
            year_cols.append(int(col))

    if not year_cols:
        error_msg = (
            "Year Column Check Failed: No numeric/year "
            "columns found in DataFrame."
        )
        logger.error(error_msg)
        raise ValueError(error_msg)

    min_year = min(year_cols)
    max_year = max(year_cols)

    logger.info(f"Detected year range: {min_year} to {max_year}")

    expected_years = set(range(min_year, max_year + 1))
    found_years = set(year_cols)

    missing_years = sorted(list(expected_years - found_years))

    if missing_years:
        error_msg = (
            f"Year Column Check Failed: Detected range {min_year}-{max_year} "
            f"but missing columns for years: {missing_years}."
        )
        logger.error(error_msg)
        raise ValueError(error_msg)

    logger.info(
        "Year column completeness check passed: "
        f"Continuous range {min_year}-{max_year} present."
    )

    return df
