import pandas as pd

from gdhi_adj.preprocess import (
    calc_iqr,
    calc_zscores,
    create_master_flag,
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

    # Expected DataFrame after pivoting, pivoting all columns that don't start
    # with letters
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
    result_df = rate_of_change(
        True, df, ["lsoa_code", "year"], "lsoa_code", "gdhi_annual"
    )

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
    result_df = rate_of_change(
        False, df, ["lsoa_code", "year"], "lsoa_code", "gdhi_annual"
    )

    # Expected DataFrame after backward rate of change
    expected_df = pd.DataFrame({
        "lsoa_code": ["E2", "E2", "E1", "E1"],
        "year": [2002, 2001, 2002, 2001],
        "gdhi_annual": [240, 200, 110, 100],
        "backward_pct_change": [None, 0.83333, None, 0.90909]
    })

    pd.testing.assert_frame_equal(result_df, expected_df)


def test_calc_zscores():
    """Test the calc_zscores function."""
    # Create a sample DataFrame
    df = pd.DataFrame({
        "lsoa_code": ["E1", "E2", "E1", "E2", "E1", "E2", "E1", "E2", "E1",
                      "E2", "E1", "E2", "E1", "E2", "E1", "E2", "E1", "E2",
                      "E1", "E2", "E1", "E2", "E1", "E2"],
        "year": [2002, 2002, 2003, 2003, 2004, 2004, 2005, 2005, 2006, 2006,
                 2007, 2007, 2008, 2008, 2009, 2009, 2010, 2010, 2011, 2011,
                 2012, 2012, 2013, 2013],
        "backward_pct_change": [1.0, 1.5, -1.2, 1.6, 50.0, 2.0, 1.1, -0.2, 1.2,
                                -1.0, 0.9, -2.0, -0.6, 0.5, 0.8, 1.3, -1.0,
                                0.9, 1.3, -1.1, -0.7, 0.7, 1.1, -0.3]
    })

    # Calculate z-scores
    result_df = calc_zscores(df, "bkwd", "lsoa_code", "backward_pct_change")

    # Expected DataFrame after calculating z-scores
    expected_df = pd.DataFrame({
        "lsoa_code": ["E1", "E2", "E1", "E2", "E1", "E2", "E1", "E2", "E1",
                      "E2", "E1", "E2", "E1", "E2", "E1", "E2", "E1", "E2",
                      "E1", "E2", "E1", "E2", "E1", "E2"],
        "year": [2002, 2002, 2003, 2003, 2004, 2004, 2005, 2005, 2006, 2006,
                 2007, 2007, 2008, 2008, 2009, 2009, 2010, 2010, 2011, 2011,
                 2012, 2012, 2013, 2013],
        "backward_pct_change": [1.0, 1.5, -1.2, 1.6, 50.0, 2.0, 1.1, -0.2, 1.2,
                                -1.0, 0.9, -2.0, -0.6, 0.5, 0.8, 1.3, -1.0,
                                0.9, 1.3, -1.1, -0.7, 0.7, 1.1, -0.3],
        "z_bkwd_flag": [False, False, False, False, True, False, False, False,
                        False, False, False, False, False, False, False, False,
                        False, False, False, False, False, False, False, False]
    })

    pd.testing.assert_frame_equal(result_df, expected_df)


def test_calc_iqr():
    """Test the calc_iqr function."""
    # Create a sample DataFrame
    df = pd.DataFrame({
        "lsoa_code": ["E1", "E2", "E1", "E2", "E1", "E2", "E1", "E2", "E1",
                      "E2"],
        "year": [2001, 2002, 2001, 2002, 2001, 2002, 2001, 2002, 2001, 2002],
        "backward_pct_change": [1.0, 1.1, -1.2, 1.6, 10.0, 2.0, -0.9, -1.5,
                                -0.6, -2.5]
    })

    # Calculate IQR
    result_df = calc_iqr(df, "bkwd", "lsoa_code", "backward_pct_change")

    # Expected DataFrame after calculating IQR
    expected_df = pd.DataFrame({
        "lsoa_code": ["E1", "E2", "E1", "E2", "E1", "E2", "E1", "E2", "E1",
                      "E2"],
        "year": [2001, 2002, 2001, 2002, 2001, 2002, 2001, 2002, 2001, 2002],
        "backward_pct_change": [1.0, 1.1, -1.2, 1.6, 10.0, 2.0, -0.9, -1.5,
                                -0.6, -2.5],
        "iqr_bkwd_flag": [False, False, False, False, True, False, False,
                          False, False, False]
    })

    pd.testing.assert_frame_equal(result_df, expected_df)


def test_create_master_flag():
    """Test the create_master_flag function."""
    # Create a sample DataFrame
    df = pd.DataFrame({
        "lsoa_code": ["E1", "E1", "E2", "E2", "E3", "E3"],
        "year": [2001, 2002, 2001, 2002, 2001, 2002],
        "backward_pct_change": [1.0, 1.1, -1.2, 1.6, 10.0, 2.0],
        "forward_pct_change": [1.0, 1.1, -1.2, 1.6, 10.0, 6.0],
        "z_bkwd_flag": [True, False, False, False, True, True],
        "z_frwd_flag": [True, False, False, False, False, False],
        "z_raw_flag": [True, False, False, False, False, False],
        "iqr_bkwd_flag": [False, False, False, False, False, False],
        "iqr_frwd_flag": [False, False, True, False, False, False],
        "iqr_raw_flag": [False, False, True, True, True, False],
    })

    # Create master flags
    result_df = create_master_flag(df)

    # Expected DataFrame after creating master flags
    expected_df = pd.DataFrame({
        "lsoa_code": ["E1", "E1", "E2", "E2", "E3", "E3"],
        "year": [2001, 2002, 2001, 2002, 2001, 2002],
        "z_master_flag": [True, True, False, False, False, False],
        "iqr_master_flag": [False, False, True, True, False, False],
        "master_flag": [True, True, True, True, False, False]
    })

    pd.testing.assert_frame_equal(result_df, expected_df)
