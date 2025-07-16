import pandas as pd
from gdhi_adj.preprocess import (
    pivot_long_dataframe,
    rate_of_change,
)


def test_pivot_long_dataframe():
    """Test the pivot_long_dataframe function."""
    # Create a sample DataFrame with years as column names
    df = pd.DataFrame({
        "lsoa_code": ["E1", "E2", "E3"],
        "lad_code": ["E01", "E02", "E03"],
        "2003": [10, 20, 30],
        "2004": [11, 22, 33]
    })
    # Call the function to pivot the DataFrame
    result_df = pivot_long_dataframe(df, "year", "value_col")

    # Expected DataFrame after pivoting, pivoting all columns that don't start with letters
    expected_df = pd.DataFrame({
        "lsoa_code": ["E1", "E2", "E3", "E1", "E2", "E3"],
        "lad_code": ["E01", "E02", "E03", "E01", "E02", "E03"],
        "year": [2003, 2003, 2003, 2004, 2004, 2004],
        "value_col": [10, 20, 30, 11, 22, 33]
    })

    pd.testing.assert_frame_equal(result_df, expected_df, check_dtype=False)


def test_rate_of_change_forward():
    """Test the rate_of_change function for forward rate of change."""
    # Create a sample DataFrame
    df = pd.DataFrame({
        "lsoa_code": ["E1", "E2", "E1", "E2"],
        "year": [2001, 2001, 2002, 2002],
        "gdhi_annual": [100, 200, 110, 240]
    })

    # Calculate forward rate of change
    result_df = rate_of_change(True, df, ["lsoa_code", "year"], "lsoa_code", "gdhi_annual")

    # Expected DataFrame after forward rate of change
    expected_df = pd.DataFrame({
        "lsoa_code": ["E1", "E1", "E2", "E2"],
        "year": [2001, 2002, 2001, 2002],
        "gdhi_annual": [100, 110, 200, 240],
        "forward_pct_change": [None, 1.1, None, 1.2]
    })

    pd.testing.assert_frame_equal(result_df, expected_df)


def test_rate_of_change_backward():
    """Test the rate_of_change function for backward rate of change."""
    # Create a sample DataFrame
    df = pd.DataFrame({
        "lsoa_code": ["E1", "E2", "E1", "E2"],
        "year": [2001, 2001, 2002, 2002],
        "gdhi_annual": [100, 200, 110, 240]
    })

    # Calculate backward rate of change
    result_df = rate_of_change(False, df, ["lsoa_code", "year"], "lsoa_code", "gdhi_annual")

    # Expected DataFrame after backward rate of change
    expected_df = pd.DataFrame({
        "lsoa_code": ["E2", "E2", "E1", "E1"],
        "year": [2002, 2001, 2002, 2001],
        "gdhi_annual": [240, 200, 110, 100],
        "backward_pct_change": [None, 0.83333, None, 0.90909]
    })

    pd.testing.assert_frame_equal(result_df, expected_df)
