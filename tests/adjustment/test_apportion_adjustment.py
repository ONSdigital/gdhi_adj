import pandas as pd
import pytest

from gdhi_adj.adjustment.apportion_adjustment import (
    apportion_adjustment,
    apportion_negative_adjustment,
    calc_non_outlier_proportions,
)


class TestCalcNoneOutlierProportions():
    """Test suite for calc_non_outlier_proportions function."""
    def test_calc_non_outlier_proportions_success(self):
        """Tests for the calc_non_outlier_proportions function."""
        df = pd.DataFrame({
            "lsoa_code": ["E1", "E2", "E1", "E2", "E1", "E2",],
            "lad_code": ["E01", "E01", "E01", "E01", "E01", "E01"],
            "year": [2000, 2000, 2001, 2001, 2002, 2002],
            "con_gdhi": [3.0, 9.0, 8.0, 12.0, 10.0, 15.0],
            "year_to_adjust": [[2001], [], [2001], [], [2001], []],
        })

        result_df = calc_non_outlier_proportions(df)

        expected_df = pd.DataFrame({
            "lsoa_code": ["E1", "E2", "E1", "E2", "E1", "E2",],
            "lad_code": ["E01", "E01", "E01", "E01", "E01", "E01"],
            "year": [2000, 2000, 2001, 2001, 2002, 2002],
            "con_gdhi": [3.0, 9.0, 8.0, 12.0, 10.0, 15.0],
            "year_to_adjust": [[2001], [], [2001], [], [2001], []],
            "lad_total": [12.0, 12.0, 20.0, 20.0, 25.0, 25.0],
            "non_outlier_total": [12.0, 12.0, None, 12.0, 25.0, 25.0],
            "gdhi_proportion": [0.25, 0.75, None, 1.0, 0.4, 0.6],
        })

        pd.testing.assert_frame_equal(result_df, expected_df)

    def test_calc_non_outlier_proportions_zero_error(self):
        """Tests for the calc_non_outlier_proportions function."""
        df = pd.DataFrame({
            "lsoa_code": ["E1", "E2"],
            "lad_code": ["E01", "E01"],
            "year": [2001, 2001],
            "con_gdhi": [8.0, 0.0],
            "year_to_adjust": [[2001], []],
        })

        with pytest.raises(
            ValueError,
            match="Non-outlier total check failed:"
        ):
            calc_non_outlier_proportions(df)


class TestApportionAdjustment:
    """Test suite for apportion_adjustment function."""
    def test_apportion_adjustment_success(self):
        """Test apportion_adjustment computes year_count and adjusted_con_gdhi
        correctly and returns the full dataframe sorted.
        """
        df = pd.DataFrame({
            "lsoa_code": ["E1", "E2", "E1", "E2"],
            "lad_code": ["E01", "E01", "E01", "E01"],
            "year": [2000, 2000, 2001, 2001],
            "con_gdhi": [3.0, 9.0, 8.0, 12.0],
            "lad_total": [12.0, 12.0, 20.0, 20.0],
            "gdhi_proportion": [0.25, 0.75, None, 1.0],
        })

        imputed_df = pd.DataFrame({
            "lsoa_code": ["E1"],
            "year": [2001],
            "con_gdhi": [12.0],
            "year_to_adjust": [[2001]],
            "rollback_flag": [True],
            "prev_safe_year": [2000],
            "prev_con_gdhi": [10.0],
            "next_safe_year": [2003],
            "next_con_gdhi": [40.0],
            "imputed_gdhi": [5.0],
        })

        result_df = apportion_adjustment(df, imputed_df)

        expected_df = pd.DataFrame({
            "lsoa_code": ["E1", "E2", "E1", "E2"],
            "lad_code": ["E01", "E01", "E01", "E01"],
            "year": [2000, 2000, 2001, 2001],
            "con_gdhi": [3.0, 9.0, 8.0, 12.0],
            "lad_total": [12.0, 12.0, 20.0, 20.0],
            "gdhi_proportion": [0.25, 0.75, None, 1.0],
            "imputed_gdhi": [None, None, 5.0, None],
            "adjusted_total": [12.0, 12.0, 15.0, 15.0],
            "adjusted_con_gdhi": [3.0, 9.0, 5.0, 15.0],
        })

        pd.testing.assert_frame_equal(
            result_df, expected_df, check_dtype=False
        )


class TestApportionNegativeAdjustment:
    """Test suite for apportion_negative_adjustment function."""
    def test_apportion_negative_adjustment_negatives(self):
        """Test apportion_negative_adjustment computes adjusted_con_gdhi
        correctly when adjusted_con_gdhi is negative for a given (lad_code,
        year) group.
        """

        df = pd.DataFrame({
            "lsoa_code": ["E1", "E2", "E3", "E4", "E5", "E6"],
            "lad_code": ["E01", "E01", "E01", "E02", "E02", "E02"],
            "year": [2002, 2002, 2002, 2002, 2002, 2002],
            "con_gdhi": [1.0, 0.2, 4.0, 2.0, 0.0, 0.0],
            # old column that needs to be overwritten
            "lsoa_count": [2, 2, 2, 2, 2, 2],
            "adjustment_val": [-0.6, -0.6, 1.2, 1.0, -0.5, -0.5],
            "adjusted_con_gdhi": [0.4, -0.4, 5.2, 3.0, -0.5, -0.5],
        })

        result_df = apportion_negative_adjustment(df)

        expected_df = pd.DataFrame({
            "lsoa_code": ["E1", "E2", "E3", "E4", "E5", "E6"],
            "lad_code": ["E01", "E01", "E01", "E02", "E02", "E02"],
            "year": [2002, 2002, 2002, 2002, 2002, 2002],
            "con_gdhi": [1.0, 0.2, 4.0, 2.0, 0.0, 0.0],
            "lsoa_count": [2, None, 2, 1, None, None],
            "adjustment_val": [0.4, 0.4, 0.4, 1.0, 1.0, 1.0],
            "adjusted_con_gdhi": [0.2, 0.0, 5.0, 2.0, 0.0, 0.0],
            "negative_diff": [0.0, 0.4, 0.0, 0.0, 0.5, 0.5],
        })

        pd.testing.assert_frame_equal(
            result_df, expected_df, check_dtype=False
        )

    def test_apportion_negative_adjustment_no_negatives(self):
        """Test apportion_negative_adjustment returns the same
        adjusted_con_gdhi values.
        """

        df = pd.DataFrame({
            "lsoa_code": ["E1", "E2", "E3"],
            "lad_code": ["E01", "E01", "E01"],
            "year": [2002, 2002, 2002],
            "con_gdhi": [5.0, 8.0, 10.0],
            "lsoa_count": [3, 3, 3],
            "adjustment_val": [0.6, 0.6, 0.6],
            "adjusted_con_gdhi": [5.2, 7.6, 10.2],
        })

        result_df = apportion_negative_adjustment(df)

        expected_df = pd.DataFrame({
            "lsoa_code": ["E1", "E2", "E3"],
            "lad_code": ["E01", "E01", "E01"],
            "year": [2002, 2002, 2002],
            "con_gdhi": [5.0, 8.0, 10.0],
            "lsoa_count": [3, 3, 3],
            "adjustment_val": [0.0, 0.0, 0.0],
            "adjusted_con_gdhi": [5.2, 7.6, 10.2],
            "negative_diff": [0.0, 0.0, 0.0],
        })

        pd.testing.assert_frame_equal(
            result_df, expected_df, check_dtype=False
        )

    # def test_apportion_negative_adjustment_zero_lsoa_count(self):
    #     """Test apportion_negative_adjustment returns ValueError when the
    #     lsoa_count is zero for a (lad_code, year) group.
    #     """

    #     df = pd.DataFrame({
    #         "lsoa_code": ["E1", "E2"],
    #         "lad_code": ["E01", "E01"],
    #         "year": [2002, 2002],
    #         "con_gdhi": [1.0, 0.0],
    #         "lsoa_count": [0, 0],
    #         "adjustment_val": [-1.5, -1.5],
    #         "adjusted_con_gdhi": [-5.0, -1.0],
    #     })

    #     with pytest.raises(
    #         ValueError,
    #         match="Zero LSOA count check failed:"
    #     ):
    #         apportion_negative_adjustment(df)

    def test_apportion_negative_adjustment_remains_negative(self):
        """Test apportion_negative_adjustment returns ValueError when the
        adjusted_con_gdhi still contains a negative value after adjustment.
        """

        df = pd.DataFrame({
            "lsoa_code": ["E1", "E2"],
            "lad_code": ["E01", "E01"],
            "year": [2002, 2002],
            "con_gdhi": [10.0, 1.0],
            "lsoa_count": [2, 2],
            "adjustment_val": [-1.5, -1.5],
            "adjusted_con_gdhi": [-5.0, 1.0],
        })

        with pytest.raises(
            ValueError,
            match="Negative value check failed:"
        ):
            apportion_negative_adjustment(df)
