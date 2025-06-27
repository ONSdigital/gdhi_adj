import pandas as pd
import pytest
from rap_project_example.preprocess import remove_nan_rows

def test_remove_nan_rows():
    # Create a DataFrame with NaN values in 'Area' and 'ID'
    df = pd.DataFrame({
        'Area': ['A', None, 'B', 'C', None],
        'ID': [1, 2, None, 4, None],
        'Value': [10, 20, 30, 40, 50]
    })
    # Expected DataFrame after removing rows with NaN in 'Area' or 'ID'
    expected_df = pd.DataFrame({
        'Area': ['A', 'C'],
        'ID': [1, 4],
        'Value': [10, 40]
    }).reset_index(drop=True)
    result_df = remove_nan_rows(df).reset_index(drop=True)
    pd.testing.assert_frame_equal(result_df, expected_df, check_dtype=False)
