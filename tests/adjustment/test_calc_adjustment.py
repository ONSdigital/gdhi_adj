import pandas as pd
import pytest

from gdhi_adj.adjustment.calc_adjustment import (
    apportion_adjustment,
    apportion_negative_adjustment,
    calc_midpoint_adjustment,
    calc_midpoint_val,
)


def test_calc_midpoint_val():
    """Test the calc_midpoint_val function returns the expected midpoint row.

    The function should:
    - select rows where the row's `year` is contained in that row's
      `year_to_adjust` value,
    - compute `prev_con_gdhi` and `next_con_gdhi` by looking up the
      same `lsoa_code` at year-1 and year+1,
    - compute `midpoint` as the mean of the two neighbouring values.
    """
    df = pd.DataFrame({
        "lsoa_code": ["E1", "E1", "E1", "E2"],
        "year": [2002, 2003, 2004, 2003],
        "uncon_gdhi": [10.0, 20.0, 26.0, 45.0],
        "con_gdhi": [5.0, 8.0, 10.0, 15.0],
        # only the 2003 row for E1 should be flagged for adjustment
        "year_to_adjust": [[], [2003], [], None],
    })

    result_df = calc_midpoint_val(df)

    expected_df = pd.DataFrame({
        "lsoa_code": ["E1"],
        "year": [2003],
        "uncon_gdhi": [20.0],
        "con_gdhi": [8.0],
        # only the 2003 row for E1 should be flagged for adjustment
        "year_to_adjust": [[2003]],
        "prev_year": [2002],
        "prev_con_gdhi": [5.0],
        "next_year": [2004],
        "next_con_gdhi": [10.0],
        "midpoint": [7.5],
    })

    pd.testing.assert_frame_equal(result_df, expected_df, check_dtype=False)


def test_calc_midpoint_adjustment():
    """Test calc_midpoint_adjustment computes midpoint_diff and apportions
    the summed adjustment_val across all rows for the same LSOA.
    """
    df = pd.DataFrame({
        "lsoa_code": ["E1", "E2", "E3", "E1"],
        "lad_code": ["E01", "E01", "E01", "E01"],
        "year": [2002, 2002, 2002, 2003],
        "con_gdhi": [5.0, 8.0, 10.0, 15.0],
    })

    # midpoint_df contains only the outlier row(s) with their computed midpoint
    midpoint_df = pd.DataFrame({
        "lsoa_code": ["E2", "E3"],
        "year": [2002, 2002],
        "con_gdhi": [8.0, 10.0],
        "midpoint": [7.5, 11.0],
    })

    result_df = calc_midpoint_adjustment(df, midpoint_df)

    expected_df = pd.DataFrame({
        "lsoa_code": ["E1", "E2", "E3", "E1"],
        "lad_code": ["E01", "E01", "E01", "E01"],
        "year": [2002, 2002, 2002, 2003],
        "con_gdhi": [5.0, 8.0, 10.0, 15.0],
        "midpoint": [None, 7.5, 11.0, None],
        "midpoint_diff": [None, 0.5, -1.0, None],
        # group sum of midpoint_diff for E01 2002 is -0.5; for E01 2003 (all
        # NaN) pandas produces 0.0 when summing; transform('sum') therefore
        # gives 0.5 for 2002 rows and 0.0 for 2003 rows.
        "adjustment_val": [-0.5, -0.5, -0.5, 0.0],
    })

    pd.testing.assert_frame_equal(result_df, expected_df, check_dtype=False)


class TestApportionAdjustment:
    """Test suite for apportion_adjustment function."""
    def test_apportion_adjustment_base(self):
        """Test apportion_adjustment computes year_count and adjusted_con_gdhi
        correctly and returns the full dataframe sorted.
        """

        df = pd.DataFrame({
            "lsoa_code": ["E1", "E2", "E3", "E1"],
            "lad_code": ["E01", "E01", "E01", "E01"],
            "year": [2002, 2002, 2002, 2003],
            "con_gdhi": [5.0, 8.0, 10.0, 15.0],
            # midpoint only present for E1 2003
            "midpoint": [None, 7.4, None, None],
            # adjustment_val is set for E1 (will be apportioned), None for E2
            "adjustment_val": [0.6, 0.6, 0.6, None],
        })

        result_df = apportion_adjustment(df)

        expected_df = pd.DataFrame({
            "lsoa_code": ["E1", "E2", "E3", "E1"],
            "lad_code": ["E01", "E01", "E01", "E01"],
            "year": [2002, 2002, 2002, 2003],
            "con_gdhi": [5.0, 8.0, 10.0, 15.0],
            "midpoint": [None, 7.4, None, None],
            "adjustment_val": [0.6, 0.6, 0.6, None],
            "lsoa_count": [3, 3, 3, 1],
            "adjusted_con_gdhi": [5.2, 7.6, 10.2, 15.0],
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
