import pandas as pd

from gdhi_adj.preprocess.flag_preprocess import (
    create_master_flag,
    flag_rollback_years,
)


def test_flag_rollback_years():
    """Test the flag_rollback_years function."""
    df = pd.DataFrame({
        "lsoa_code": ["E1", "E1", "E1", "E2", "E2", "E2"],
        "year": [2010, 2014, 2015, 2001, 2012, 2013],
        "backward_pct_change": [1.0, 0.9, 0.9, 1.0, 0.95, 1.0],
        "forward_pct_change": [1.0, 1.0, 1.0, 1.0, 1.05, 0.95]
    })

    result_df = flag_rollback_years(df)

    expected_df = pd.DataFrame({
        "lsoa_code": ["E1", "E1", "E1", "E2", "E2", "E2"],
        "year": [2010, 2014, 2015, 2001, 2012, 2013],
        "backward_pct_change": [1.0, 0.9, 0.9, 1.0, 0.95, 1.0],
        "forward_pct_change": [1.0, 1.0, 1.0, 1.0, 1.05, 0.95],
        "rollback_flag": [True, True, False, False, False, True]
    })

    pd.testing.assert_frame_equal(result_df, expected_df)


def test_create_master_flag():
    """Test the create_master_flag function."""
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

    result_df = create_master_flag(df)

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
