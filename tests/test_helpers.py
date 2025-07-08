"""Unit tests for helper functions."""
import pytest
import pandas as pd
from gdhi_adj.utils.helpers import (
    read_with_schema,
    write_with_schema,
)


@pytest.fixture
def test_schema():
    """Create a sample schema file path for testing."""
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
def test_schema_file(tmp_path, test_schema):
    """Create a sample schema file path for testing."""
    test_schema_filepath = tmp_path / "schema.toml"
    test_schema_filepath.write_text(test_schema)

    return test_schema_filepath


@pytest.fixture
def input_data():
    data = {
        "Old col name": [1, 2],
        "LSOA code": ["A1", "B2"],
        "Additional col": ["test1", "test2"],
    }
    return pd.DataFrame(data)


@pytest.fixture
def expout_data():
    data = {
        "new_col_name": [1, 2],
        "lsoa_code": ["A1", "B2"],
        "Additional col": ["test1", "test2"],
    }
    return pd.DataFrame(data)


@pytest.fixture
def test_csv_file(tmp_path, input_data):
    # Write the input data to a csv for testing
    filepath = tmp_path / "test.csv"
    input_data.to_csv(filepath, index=False)
    return filepath


def test_read_with_schema(test_csv_file, test_schema_file, expout_data):
    # Creating df using the function and test csv
    df = read_with_schema(test_csv_file, test_schema_file)
    # Make sure the reader function has returned a df
    assert isinstance(df, pd.DataFrame)
    # Check that the df is the same as the expected data
    pd.testing.assert_frame_equal(df, expout_data)


def test_write_with_schema(tmp_path, input_data, test_schema_file, expout_data):
    # Write the output data to a csv using the write function
    output_filepath = tmp_path / "test_output.csv"
    write_with_schema(input_data, test_schema_file, output_filepath, "test_output.csv")

    # Read the output file back in
    output_df = pd.read_csv(output_filepath)

    # Check that the output df is the same as the expected data
    pd.testing.assert_frame_equal(output_df, expout_data)
