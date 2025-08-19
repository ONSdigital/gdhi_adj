import pandas as pd
import pytest

from gdhi_adj.preprocess import (
    calc_iqr,
    calc_lad_mean,
    calc_zscores,
    concat_wide_dataframes,
    constrain_to_reg_acc,
    create_master_flag,
    pivot_output_long,
    pivot_wide_dataframe,
    pivot_years_long_dataframe,
    rate_of_change,
)


def test_pivot_years_long_dataframe():
    """Test the pivot_years_long_dataframe function."""
    # Create a sample DataFrame with years as column names
    df = pd.DataFrame({
        "lsoa_code": ["E1", "E2", "E3"],
        "lad_code": ["E01", "E02", "E03"],
        "2003": [10, 20, 30],
        "2004": [11, 22, 33]
    })
    # Call the function to pivot the DataFrame
    result_df = pivot_years_long_dataframe(df, "year", "value_col")

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
        "bkwd_zscore": [-0.243105, 0.941783, -0.396278, 1.021934, 3.168487,
                        1.342541, -0.236142, -0.420796, -0.229180, -1.062010,
                        -0.250067, -1.863527, -0.354504, 0.140265, -0.257030,
                        0.781479, -0.382354, 0.460872, -0.222218, -1.142162,
                        -0.361466, 0.300569, -0.236142, -0.500948],
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
        "year": [2001, 2001, 2002, 2002, 2003, 2003, 2004, 2004, 2005, 2005],
        "backward_pct_change": [1.0, 1.1, -1.2, 1.6, 10.0, 2.0, -0.9, -1.5,
                                -0.6, -2.5]
    })

    # Calculate IQR
    result_df = calc_iqr(df, "bkwd", "lsoa_code", "backward_pct_change")

    # Expected DataFrame after calculating IQR
    expected_df = pd.DataFrame({
        "lsoa_code": ["E1", "E2", "E1", "E2", "E1", "E2", "E1", "E2", "E1",
                      "E2"],
        "year": [2001, 2001, 2002, 2002, 2003, 2003, 2004, 2004, 2005, 2005],
        "backward_pct_change": [1.0, 1.1, -1.2, 1.6, 10.0, 2.0, -0.9, -1.5,
                                -0.6, -2.5],
        "bkwd_q1": [-0.9, -1.5, -0.9, -1.5, -0.9, -1.5, -0.9, -1.5, -0.9, -1.5
                    ],
        "bkwd_q3": [1.0, 1.6, 1.0, 1.6, 1.0, 1.6, 1.0, 1.6, 1.0, 1.6],
        "bkwd_iqr": [1.9, 3.1, 1.9, 3.1, 1.9, 3.1, 1.9, 3.1, 1.9, 3.1],
        "bkwd_lower_bound": [-6.6, -10.8, -6.6, -10.8, -6.6, -10.8, -6.6,
                             -10.8, -6.6, -10.8],
        "bkwd_upper_bound": [6.7, 10.9, 6.7, 10.9, 6.7, 10.9, 6.7, 10.9, 6.7,
                             10.9],
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
        "backward_pct_change": [1.0, 1.1, -1.2, 1.6, 10.0, 2.0],
        "forward_pct_change": [1.0, 1.1, -1.2, 1.6, 10.0, 6.0],
        "z_bkwd_flag": [True, False, False, False, True, True],
        "z_frwd_flag": [True, False, False, False, False, False],
        "z_raw_flag": [True, False, False, False, False, False],
        "iqr_bkwd_flag": [False, False, False, False, False, False],
        "iqr_frwd_flag": [False, False, True, False, False, False],
        "iqr_raw_flag": [False, False, True, True, True, False],
        "master_z_flag": [True, True, False, False, False, False],
        "master_iqr_flag": [False, False, True, True, False, False],
        "master_flag": [True, True, True, True, False, False]
    })

    pd.testing.assert_frame_equal(result_df, expected_df)


def test_calc_lad_mean():
    """Test the calc_lad_mean function."""
    # Create a sample DataFrame
    df = pd.DataFrame({
        "lsoa_code": [
            "E1", "E1", "E2", "E2", "E3", "E3", "E4", "E4", "E5", "E5"],
        "lad_code": [
            "E01", "E01", "E01", "E01", "E01", "E01", "E02", "E02", "E02",
            "E02"],
        "year": [2001, 2002, 2001, 2002, 2001, 2002, 2001, 2002, 2001, 2002],
        "gdhi_annual": [
            100.0, 110.0, 200.0, 220.0, 300.0, 330.0, 400.0, 440.0, 500.0,
            550.0],
        "master_flag": [
            True, True, False, False, False, False, False, False, True, True]
    })

    # Calculate mean percentage difference
    result_df = calc_lad_mean(df)

    # Expected DataFrame after calculating mean percentage difference
    expected_df = pd.DataFrame({
        "lsoa_code": ["E1", "E1", "E5", "E5"],
        "lad_code": ["E01", "E01", "E02", "E02"],
        "year": [2001, 2002, 2001, 2002],
        "gdhi_annual": [100.0, 110.0, 500.0, 550.0],
        "master_flag": [True, True, True, True],
        "mean_non_out_gdhi": [250.0, 275.0, 400.0, 440.0]
    })

    pd.testing.assert_frame_equal(result_df, expected_df)


def test_constrain_to_reg_acc():
    """Test the constrain_to_reg_acc function."""
    # Create a sample DataFrame
    df = pd.DataFrame({
        "lsoa_code": ["E1", "E2", "E3", "E1", "E2", "E3"],
        "lad_code": ["E01", "E01", "E02", "E01", "E01", "E02"],
        "year": [2001, 2001, 2001, 2002, 2002, 2002],
        "gdhi_annual": [10, 20, 30, 45, 50, 70],
        "mean_non_out_gdhi": [15, 15, 25, 45, 45, 50],
        "master_flag": [True, True, False, False, False, True],
    })

    # Define regional and local authority codes
    reg_acc = pd.DataFrame({
        "lad_code": ["E01", "E02", "E01", "E02"],
        "year": [2001, 2001, 2002, 2002],
        "gdhi_annual": [100, 200, 300, 400]
    })

    # Constrain to regional and local authority codes
    result_df = constrain_to_reg_acc(df, reg_acc)

    # Expected DataFrame after constraining
    expected_df = pd.DataFrame({
        "lsoa_code": ["E1", "E2", "E3", "E1", "E2", "E3"],
        "lad_code": ["E01", "E01", "E02", "E01", "E01", "E02"],
        "year": [2001, 2001, 2001, 2002, 2002, 2002],
        "gdhi_annual": [10, 20, 30, 45, 50, 70],
        "mean_non_out_gdhi": [15, 15, 25, 45, 45, 50],
        "master_flag": ["TRUE", "TRUE", "MEAN", "MEAN", "MEAN", "TRUE"],
        "conlsoa_gdhi": [40.0, 57.143, 109.091, 150.0, 157.895, 233.333],
        "conlsoa_mean": [60.0, 42.857, 90.909, 150.0, 142.105, 166.667]
    })

    pd.testing.assert_frame_equal(result_df, expected_df, rtol=1e-3)


def test_constrain_to_reg_acc_col_mismatch():
    """Test the constrain_to_reg_acc function with column mismatch."""
    # Create a sample DataFrame with different column names
    df = pd.DataFrame({
        "lsoa_code": [],
        "lad_code": [],
        "year": [],
        "gdhi_annual": [],
    })

    # Define regional and local authority codes
    reg_acc = pd.DataFrame({
        "lad_code": [],
        "year": [],
        "wrong_col": []
    })

    # Ensure error is raised if regional accounts columns aren't present in df
    with pytest.raises(
        expected_exception=ValueError,
        match="DataFrames have different columns"
    ):
        constrain_to_reg_acc(df, reg_acc)


def test_pivot_output_long():
    """Test the pivot_output_long function."""
    # Create a sample DataFrame
    df = pd.DataFrame({
        "lsoa_code": ["E1", "E2", "E1", "E2"],
        "lsoa_name": ["A", "B", "A", "B"],
        "lad_code": ["E01", "E01", "E01", "E01"],
        "lad_name": ["AA", "AA", "AA", "AA"],
        "year": [2002, 2002, 2003, 2003],
        "master_z_flag": [True, True, True, True],
        "master_iqr_flag": [True, True, True, True],
        "master_flag": ["TRUE", "TRUE", "TRUE", "TRUE"],
        "gdhi_annual": [10.0, 11.0, 12.0, 13.0],
        "mean_non_out_gdhi": [100.0, 110.0, 120.0, 130.0],
    })

    # Call the function to pivot the DataFrame
    result_df = pivot_output_long(df, "gdhi_annual", "mean_non_out_gdhi")

    # Expected DataFrame after pivoting
    expected_df = pd.DataFrame({
        "lsoa_code": ["E1", "E2", "E1", "E2", "E1", "E2", "E1", "E2"],
        "lsoa_name": ["A", "B", "A", "B", "A", "B", "A", "B"],
        "lad_code": ["E01", "E01", "E01", "E01", "E01", "E01", "E01", "E01"],
        "lad_name": ["AA", "AA", "AA", "AA", "AA", "AA", "AA", "AA"],
        "year": [2002, 2002, 2003, 2003, 2002, 2002, 2003, 2003],
        "master_z_flag": [True, True, True, True, True, True, True, True],
        "master_iqr_flag": [True, True, True, True, True, True, True, True],
        "master_flag": [
            "TRUE", "TRUE", "TRUE", "TRUE", "TRUE", "TRUE", "TRUE", "TRUE"],
        "metric": ["annual", "annual", "annual", "annual",
                   "CONLSOA", "CONLSOA", "CONLSOA", "CONLSOA"],
        "value": [10.0, 11.0, 12.0, 13.0, 100.0, 110.0, 120.0, 130.0],
        "metric_date": [
            "annual_2002", "annual_2002", "annual_2003", "annual_2003",
            "CONLSOA_2002", "CONLSOA_2002", "CONLSOA_2003", "CONLSOA_2003"],
    })

    pd.testing.assert_frame_equal(result_df, expected_df)


def test_pivot_wide_dataframe():
    """Test the pivot_wide_dataframe function."""
    # Create a sample DataFrame
    df = pd.DataFrame({
        "lsoa_code": ["E1", "E2", "E1", "E2", "E1", "E2", "E1", "E2"],
        "lsoa_name": ["A", "B", "A", "B", "A", "B", "A", "B"],
        "lad_code": ["E01", "E01", "E01", "E01", "E01", "E01", "E01", "E01"],
        "lad_name": ["AA", "AA", "AA", "AA", "AA", "AA", "AA", "AA"],
        "year": [2002, 2002, 2003, 2003, 2002, 2002, 2003, 2003],
        "master_z_flag": [True, True, True, True, True, True, True, True],
        "master_iqr_flag": [True, True, True, True, True, True, True, True],
        "master_flag": [
            "TRUE", "TRUE", "TRUE", "TRUE", "TRUE", "TRUE", "TRUE", "TRUE"],
        "metric": ["annual", "annual", "annual", "annual",
                   "CONLSOA", "CONLSOA", "CONLSOA", "CONLSOA"],
        "value": [10.0, 11.0, 12.0, 13.0, 100.0, 110.0, 120.0, 130.0],
        "metric_date": [
            "annual_2002", "annual_2002", "annual_2003", "annual_2003",
            "CONLSOA_2002", "CONLSOA_2002", "CONLSOA_2003", "CONLSOA_2003"],
    })

    # Call the function to pivot the DataFrame
    result_df = pivot_wide_dataframe(df)

    # Expected DataFrame after pivoting
    expected_df = pd.DataFrame({
        "lsoa_code": ["E1", "E2"],
        "lsoa_name": ["A", "B"],
        "lad_code": ["E01", "E01"],
        "lad_name": ["AA", "AA"],
        "2002": [10.0, 11.0],
        "2003": [12.0, 13.0],
        "master_z_flag": [True, True],
        "master_iqr_flag": [True, True],
        "master_flag": ["TRUE", "TRUE"],
        "CONLSOA_2002": [100.0, 110.0],
        "CONLSOA_2003": [120.0, 130.0],
    })

    pd.testing.assert_frame_equal(result_df, expected_df)


def test_concat_wide_dataframes():
    """Test the concat_wide_dataframes function."""
    # Create two sample DataFrames
    df_outlier = pd.DataFrame({
        "lsoa_code": ["E1", "E2"],
        "2002": [10.0, 11.0],
        "master_flag": ["TRUE", "TRUE"],
        "CONLSOA_2002": [100.0, 110.0]
    })

    df_mean = pd.DataFrame({
        "lsoa_code": ["E1", "E2"],
        "2002": [20.0, 21.0],
        "master_flag": ["MEAN", "MEAN"],
        "CONLSOA_2002": [200.0, 210.0]
    })

    # Concatenate the DataFrames
    result_df = concat_wide_dataframes(df_outlier, df_mean)

    # Expected DataFrame after concatenation
    expected_df = pd.DataFrame({
        "lsoa_code": ["E1", "E1", "E2", "E2"],
        "2002": [10.0, 20.0, 11.0, 21.0],
        "master_flag": ["TRUE", "MEAN", "TRUE", "MEAN"],
        "CONLSOA_2002": [100.0, 200.0, 110.0, 210.0]
    })

    pd.testing.assert_frame_equal(result_df, expected_df)
