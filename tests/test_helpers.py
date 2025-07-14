"""Unit tests for helper functions."""
import pytest
import toml
import pandas as pd
from gdhi_adj.utils.helpers import (
    rename_columns,
    read_with_schema,
    write_with_schema,
)
from gdhi_adj.utils.logger import logger_creator

logger = logger_creator()


@pytest.fixture
def test_schema() -> str:
    """Create a sample schema for testing pass scenario."""
    schema_content = """
    [new_col_name]
    old_name = "Old col name"
    Deduced_Data_Type = "Int"

    [lsoa_code]
    old_name = "LSOA code"
    Deduced_Data_Type = "str"
    """
    return schema_content


@pytest.fixture
def test_schema_file(tmp_path, test_schema: str) -> str:
    """Create a sample schema file path for testing."""
    test_schema_filepath = tmp_path / "schema.toml"
    test_schema_filepath.write_text(test_schema)

    return test_schema_filepath


@pytest.fixture
def test_schema_wrong_col() -> str:
    """Create a sample schema for testing when data columns do not match schema."""
    schema_content = """
    [new_col_name]
    old_name = "Old col nom"
    Deduced_Data_Type = "Int"

    [lsoa_code]
    old_name = "LSOA code"
    Deduced_Data_Type = "str"
    """
    return schema_content


@pytest.fixture
def test_schema_file_wrong_col(tmp_path, test_schema_wrong_col: str) -> str:
    """Create a sample schema file path for testing when data columns do not match schema."""
    test_schema_filepath = tmp_path / "schema.toml"
    test_schema_filepath.write_text(test_schema_wrong_col)

    return test_schema_filepath


@pytest.fixture
def input_data() -> pd.DataFrame:
    """Create a sample input DataFrame for testing."""
    data = {
        "Old col name": [1, 2],
        "LSOA code": ["A1", "B2"],
        "Additional col": ["test1", "test2"],
    }
    return pd.DataFrame(data)


@pytest.fixture
def expout_data() -> pd.DataFrame:
    """Create a sample expected output DataFrame for testing."""
    data = {
        "new_col_name": [1, 2],
        "lsoa_code": ["A1", "B2"],
        "Additional col": ["test1", "test2"],
    }
    return pd.DataFrame(data)


@pytest.fixture
def test_csv_file(tmp_path, input_data: pd.DataFrame) -> str:
    """Create a sample CSV file for testing."""
    # Write the input data to a csv for testing
    filepath = tmp_path / "test.csv"
    input_data.to_csv(filepath, index=False)
    return filepath


def test_rename_columns(input_data, test_schema, expout_data):
    """Test renaming columns based on schema."""
    # Convert test_schema to dict
    test_schema = toml.loads(test_schema)
    # Rename columns using the function
    renamed_df = rename_columns(input_data, test_schema, logger)

    # Check that the renamed df matches the expected output
    pd.testing.assert_frame_equal(renamed_df, expout_data)

    # Ensure old column names are not present
    with pytest.raises(KeyError, match="Old col name"):
        renamed_df["Old col name"]


# def test_rename_columns_missing_col(input_data, test_schema_wrong_col):
#     """Test renaming columns when the columns in data do not match schema."""
#     with pytest.raises(expected_exception=ValueError, match="schema does not exist in DataFrame"):
#         rename_columns(input_data, test_schema_wrong_col, logger)


def test_read_with_schema(test_csv_file, test_schema_file, expout_data):
    # Creating df using the function and test csv
    df = read_with_schema(test_csv_file, test_schema_file)
    # Make sure the reader function has returned a df
    assert isinstance(df, pd.DataFrame)
    # Check that the df is the same as the expected data
    pd.testing.assert_frame_equal(df, expout_data)
    pytest.raises(KeyError, match="Old col name")  # Ensure old column is not present


def test_read_with_schema_missing_col(test_csv_file, test_schema_file_wrong_col):
    """Test reading a CSV with schema when the columns in data do not match schema."""
    with pytest.raises(expected_exception=ValueError, match="schema does not exist in DataFrame"):
        read_with_schema(test_csv_file, test_schema_file_wrong_col)


def test_write_with_schema(tmp_path, input_data, test_schema_file, expout_data):
    # Write the output data to a csv using the write function
    output_filepath = tmp_path / "test_output.csv"
    write_with_schema(input_data, test_schema_file, output_filepath, "test_output.csv")

    # Read the output file back in
    output_df = pd.read_csv(output_filepath)

    # Check that the output df is the same as the expected data
    pd.testing.assert_frame_equal(output_df, expout_data)


def test_write_with_schema_missing_col(tmp_path, input_data, test_schema_file_wrong_col):
    """Test reading a CSV with schema when the columns in data do not match schema."""
    output_filepath = tmp_path / "test_output.csv"
    with pytest.raises(expected_exception=ValueError, match="schema does not exist in DataFrame"):
        write_with_schema(
            input_data, test_schema_file_wrong_col, output_filepath, "test_output.csv"
        )
