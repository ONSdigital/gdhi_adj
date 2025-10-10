import pandas as pd

from gdhi_adj.preprocess.calc_preprocess import (
    calc_iqr,
    calc_lad_mean,
    calc_rate_of_change,
    calc_zscores,
)


class TestCalcRateOfChange:
    """Tests for calc_rate_of_change function."""

    def test_calc_rate_of_change_forward(self):
        """Test the calc_rate_of_change function for forward rate of change."""
        df = pd.DataFrame({
            "lsoa_code": ["E1", "E2", "E1", "E2"],
            "year": [2001, 2001, 2002, 2002],
            "gdhi_annual": [100, 200, 110, 240]
        })

        # Calculate forward rate of change
        result_df = calc_rate_of_change(
            df,
            ascending=True,
            sort_cols=["lsoa_code", "year"],
            group_col="lsoa_code",
            val_col="gdhi_annual"
        )

        expected_df = pd.DataFrame({
            "lsoa_code": ["E1", "E1", "E2", "E2"],
            "year": [2001, 2002, 2001, 2002],
            "gdhi_annual": [100, 110, 200, 240],
            "forward_pct_change": [None, 1.1, None, 1.2]
        })

        pd.testing.assert_frame_equal(result_df, expected_df)

    def test_calc_rate_of_change_backward(self):
        """Test calc_rate_of_change function for backward rate of change."""
        df = pd.DataFrame({
            "lsoa_code": ["E1", "E2", "E1", "E2"],
            "year": [2001, 2001, 2002, 2002],
            "gdhi_annual": [100, 200, 110, 240]
        })

        # Calculate backward rate of change
        result_df = calc_rate_of_change(
            df,
            ascending=False,
            sort_cols=["lsoa_code", "year"],
            group_col="lsoa_code",
            val_col="gdhi_annual"
        )

        expected_df = pd.DataFrame({
            "lsoa_code": ["E2", "E2", "E1", "E1"],
            "year": [2002, 2001, 2002, 2001],
            "gdhi_annual": [240, 200, 110, 100],
            "backward_pct_change": [None, 0.83333, None, 0.90909]
        })

        pd.testing.assert_frame_equal(result_df, expected_df)


class TestCalcZscores:
    """Tests for calc_zscores function."""

    def test_calc_zscores_upper(self):
        """Test the calc_zscores function for upper zscore direction."""
        df = pd.DataFrame({
            "lad_code": ["E1", "E2", "E1", "E2", "E1", "E2", "E1", "E2", "E1",
                         "E2", "E1", "E2", "E1", "E2", "E1", "E2", "E1", "E2",
                         "E1", "E2", "E1", "E2", "E1", "E2", "E3"],
            "year": [2002, 2002, 2003, 2003, 2004, 2004, 2005, 2005, 2006,
                     2006, 2007, 2007, 2008, 2008, 2009, 2009, 2010, 2010,
                     2011, 2011, 2012, 2012, 2013, 2013, 2013],
            "backward_pct_change": [1.0, 1.5, -1.2, 1.6, 50.0, 2.0, 1.1, -0.2,
                                    1.2, -1.0, 0.9, -2.0, -0.6, 0.5, 0.8, 1.3,
                                    -1.0, 0.9, 1.3, -1.1, -0.7, 0.7, 1.1, -0.3,
                                    1.0],
            "rollback_flag": [False, False, False, False, False, False, False,
                              False, False, False, False, False, False, False,
                              False, False, False, False, False, False, False,
                              False, False, False, True]
        })

        zscore_upper_threshold = 3.0
        zscore_lower_threshold = -3.0

        result_df = calc_zscores(
            df,
            score_prefix="bkwd",
            group_col="lad_code",
            val_col="backward_pct_change",
            zscore_upper_threshold=zscore_upper_threshold,
            zscore_lower_threshold=zscore_lower_threshold,
        )

        expected_df = pd.DataFrame({
            "lad_code": ["E1", "E2", "E1", "E2", "E1", "E2", "E1", "E2", "E1",
                         "E2", "E1", "E2", "E1", "E2", "E1", "E2", "E1", "E2",
                         "E1", "E2", "E1", "E2", "E1", "E2", "E3"],
            "year": [2002, 2002, 2003, 2003, 2004, 2004, 2005, 2005, 2006,
                     2006, 2007, 2007, 2008, 2008, 2009, 2009, 2010, 2010,
                     2011, 2011, 2012, 2012, 2013, 2013, 2013],
            "backward_pct_change": [1.0, 1.5, -1.2, 1.6, 50.0, 2.0, 1.1, -0.2,
                                    1.2, -1.0, 0.9, -2.0, -0.6, 0.5, 0.8, 1.3,
                                    -1.0, 0.9, 1.3, -1.1, -0.7, 0.7, 1.1, -0.3,
                                    1.0],
            "rollback_flag": [False, False, False, False, False, False, False,
                              False, False, False, False, False, False, False,
                              False, False, False, False, False, False, False,
                              False, False, False, True],
            "bkwd_zscore": [-0.243105, 0.941783, -0.396278, 1.021934, 3.168487,
                            1.342541, -0.236142, -0.420796, -0.229180,
                            -1.062010, -0.250067, -1.863527, -0.354504,
                            0.140265, -0.257030, 0.781479, -0.382354, 0.460872,
                            -0.222218, -1.142162, -0.361466, 0.300569,
                            -0.236142, -0.500948, None],
            "bkwd_zscore_threshold": [None, None, None, None, "upper", None,
                                      None, None, None, None, None, None, None,
                                      None, None, None, None, None, None, None,
                                      None, None, None, None, None],
            "z_bkwd_flag": [False, False, False, False, True, False, False,
                            False, False, False, False, False, False, False,
                            False, False, False, False, False, False, False,
                            False, False, False, False]
        })

        pd.testing.assert_frame_equal(result_df, expected_df)

    def test_calc_zscores_lower(self):
        """Test the calc_zscores function for lower zscore direction."""
        df = pd.DataFrame({
            "lad_code": ["E1", "E2", "E1", "E2", "E1", "E2", "E1", "E2", "E1",
                         "E2", "E1", "E2", "E1", "E2", "E1", "E2", "E1", "E2",
                         "E1", "E2", "E1", "E2", "E1", "E2", "E3"],
            "year": [2002, 2002, 2003, 2003, 2004, 2004, 2005, 2005, 2006,
                     2006, 2007, 2007, 2008, 2008, 2009, 2009, 2010, 2010,
                     2011, 2011, 2012, 2012, 2013, 2013, 2013],
            "backward_pct_change": [1.0, 1.5, -1.2, 1.6, -50.0, 2.0, 1.1, -0.2,
                                    1.2, -1.0, 0.9, -2.0, -0.6, 0.5, 0.8, 1.3,
                                    -1.0, 0.9, 1.3, -1.1, -0.7, 0.7, 1.1, -0.3,
                                    1.0],
            "rollback_flag": [False, False, False, False, False, False, False,
                              False, False, False, False, False, False, False,
                              False, False, False, False, False, False, False,
                              False, False, False, True]
        })

        zscore_upper_threshold = 3.0
        zscore_lower_threshold = -3.0

        result_df = calc_zscores(
            df,
            score_prefix="bkwd",
            group_col="lad_code",
            val_col="backward_pct_change",
            zscore_upper_threshold=zscore_upper_threshold,
            zscore_lower_threshold=zscore_lower_threshold,
        )

        expected_df = pd.DataFrame({
            "lad_code": ["E1", "E2", "E1", "E2", "E1", "E2", "E1", "E2", "E1",
                         "E2", "E1", "E2", "E1", "E2", "E1", "E2", "E1", "E2",
                         "E1", "E2", "E1", "E2", "E1", "E2", "E3"],
            "year": [2002, 2002, 2003, 2003, 2004, 2004, 2005, 2005, 2006,
                     2006, 2007, 2007, 2008, 2008, 2009, 2009, 2010, 2010,
                     2011, 2011, 2012, 2012, 2013, 2013, 2013],
            "backward_pct_change": [1.0, 1.5, -1.2, 1.6, -50.0, 2.0, 1.1, -0.2,
                                    1.2, -1.0, 0.9, -2.0, -0.6, 0.5, 0.8, 1.3,
                                    -1.0, 0.9, 1.3, -1.1, -0.7, 0.7, 1.1, -0.3,
                                    1.0],
            "rollback_flag": [False, False, False, False, False, False, False,
                              False, False, False, False, False, False, False,
                              False, False, False, False, False, False, False,
                              False, False, False, True],
            "bkwd_zscore": [0.332371, 0.941783, 0.181345, 1.021934, -3.168680,
                            1.342541, 0.339236, -0.420796, 0.346101, -1.062010,
                            0.325506, -1.863527, 0.222534, 0.140265, 0.318641,
                            0.781479, 0.195075, 0.460872, 0.352965, -1.142162,
                            0.215669, 0.300569, 0.339236, -0.500948, None],
            "bkwd_zscore_threshold": [None, None, None, None, "lower", None,
                                      None, None, None, None, None, None, None,
                                      None, None, None, None, None, None, None,
                                      None, None, None, None, None],
            "z_bkwd_flag": [False, False, False, False, True, False, False,
                            False, False, False, False, False, False, False,
                            False, False, False, False, False, False, False,
                            False, False, False, False]
        })

        pd.testing.assert_frame_equal(result_df, expected_df)


def test_calc_iqr():
    """Test the calc_iqr function."""
    df = pd.DataFrame({
        "lsoa_code": ["E1", "E2", "E1", "E2", "E1", "E2", "E1", "E2", "E1",
                      "E2", "E3"],
        "year": [2001, 2001, 2002, 2002, 2003, 2003, 2004, 2004, 2005, 2005,
                 2005],
        "backward_pct_change": [1.0, 1.1, -1.2, 1.6, 10.0, 2.0, -0.9, -1.5,
                                -0.6, -2.5, 1.0],
        "rollback_flag": [False, False, False, False, False, False, False,
                          False, False, False, True]
    })

    iqr_lower_quantile = 0.25
    iqr_upper_quantile = 0.75
    iqr_multiplier = 3.0

    result_df = calc_iqr(
        df,
        iqr_prefix="bkwd",
        group_col="lsoa_code",
        val_col="backward_pct_change",
        iqr_lower_quantile=iqr_lower_quantile,
        iqr_upper_quantile=iqr_upper_quantile,
        iqr_multiplier=iqr_multiplier
    )

    expected_df = pd.DataFrame({
        "lsoa_code": ["E1", "E2", "E1", "E2", "E1", "E2", "E1", "E2", "E1",
                      "E2", "E3"],
        "year": [2001, 2001, 2002, 2002, 2003, 2003, 2004, 2004, 2005, 2005,
                 2005],
        "backward_pct_change": [1.0, 1.1, -1.2, 1.6, 10.0, 2.0, -0.9, -1.5,
                                -0.6, -2.5, 1.0],
        "rollback_flag": [False, False, False, False, False, False, False,
                          False, False, False, True],
        "bkwd_q1": [-0.9, -1.5, -0.9, -1.5, -0.9, -1.5, -0.9, -1.5, -0.9, -1.5,
                    None],
        "bkwd_q3": [1.0, 1.6, 1.0, 1.6, 1.0, 1.6, 1.0, 1.6, 1.0, 1.6, None],
        "bkwd_iqr": [1.9, 3.1, 1.9, 3.1, 1.9, 3.1, 1.9, 3.1, 1.9, 3.1, None],
        "bkwd_lower_bound": [-6.6, -10.8, -6.6, -10.8, -6.6, -10.8, -6.6,
                             -10.8, -6.6, -10.8, None],
        "bkwd_upper_bound": [6.7, 10.9, 6.7, 10.9, 6.7, 10.9, 6.7, 10.9, 6.7,
                             10.9, None],
        "iqr_bkwd_flag": [False, False, False, False, True, False, False,
                          False, False, False, False]
    })

    pd.testing.assert_frame_equal(result_df, expected_df)


def test_calc_lad_mean():
    """Test the calc_lad_mean function."""
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

    result_df = calc_lad_mean(df)

    expected_df = pd.DataFrame({
        "lsoa_code": ["E1", "E1", "E5", "E5"],
        "lad_code": ["E01", "E01", "E02", "E02"],
        "year": [2001, 2002, 2001, 2002],
        "gdhi_annual": [100.0, 110.0, 500.0, 550.0],
        "master_flag": [True, True, True, True],
        "mean_non_out_gdhi": [250.0, 275.0, 400.0, 440.0]
    })

    pd.testing.assert_frame_equal(result_df, expected_df)
