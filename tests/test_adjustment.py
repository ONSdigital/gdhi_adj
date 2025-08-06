import pandas as pd
import pytest

from gdhi_adj.adjustment import (  # join_analyst_constrained_data,
    filter_lsoa_data,
)


def test_filter_lsoa_data():
    """Test the filter_lsoa_data function."""
    # Create a sample DataFrame
    df = pd.DataFrame({
        "lsoa_code": ["E1", "E2", "E3"],
        "lad_code": ["E01", "E02", "E03"],
        "transaction_code": ["T1", "T2", "T3"],
        "master_flag": [True, None, True],
        "adjust": [True, None, False],
        "year": [2003, 2004, 2005],
        "2002": [10, 20, 30],
    })
    # Call the function to filter the DataFrame
    result_df = filter_lsoa_data(df)

    # Expected DataFrame after filtering
    expected_df = pd.DataFrame({
        "lsoa_code": ["E1"],
        "lad_code": ["E01"],
        "transaction_code": ["T1"],
        "adjust": [True],
        "year": [2003],
    })

    pd.testing.assert_frame_equal(result_df, expected_df, check_dtype=False)

    # Create a sample DataFrame with a mismatch
    df_mismatch = pd.DataFrame({
        "lsoa_code": ["E1", "E2", "E3"],
        "lad_code": ["E01", "E02", "E03"],
        "transaction_code": ["T1", "T2", "T3"],
        "master_flag": [True, None, True],
        "adjust": [True, False, False],
        "year": [2003, 2004, 2005]
    })

    # Check if ValueError is raised for mismatch master_flag and adjust columns
    with pytest.raises(
        expected_exception=ValueError,
        match="Mismatch: master_flag and Adjust column booleans do not match."
    ):
        filter_lsoa_data(df_mismatch)
