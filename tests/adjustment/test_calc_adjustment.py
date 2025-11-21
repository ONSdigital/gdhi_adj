import numpy as np
import pandas as pd

from gdhi_adj.adjustment.calc_adjustment import (
    calc_imputed_adjustment,
    calc_lad_totals,
    extrapolate_imputed_val,
    interpolate_imputed_val,
)


def test_calc_lad_totals_aggregates_sum_per_lad_and_year():
    """Test calc_lad_totals correctly aggregates con_gdhi sums per
    (lad_code, year) group."""
    df = pd.DataFrame({
        "lsoa_code": ["E1", "E2", "E1", "E3"],
        "lad_code": ["E01", "E01", "E01", "E02"],
        "year": [2000, 2000, 2001, 2000],
        "con_gdhi": [100.0, 50.0, 30.0, 20.0],
    })

    result_df = calc_lad_totals(df)

    expected_df = pd.DataFrame({
        "lad_code": ["E01", "E01", "E02"],
        "year": [2000, 2001, 2000],
        "con_gdhi": [150.0, 30.0, 20.0],
    })

    pd.testing.assert_frame_equal(result_df, expected_df)


class TestInterpolateImputedVal:
    """Tests for the interpolate_imputed_val function."""
    def test_interpolate_imputed_val(self):
        """Test the interpolate_imputed_val function returns the expected
        imputed values.
        """
        df = pd.DataFrame({
            "lsoa_code": ["E1", "E1"],
            "year": [2001, 2002],
            "con_gdhi": [12.0, 13.0],
            "year_to_adjust": [[2001, 2002], [2001, 2002]],
            "rollback_flag": [True, False],
            "prev_safe_year": [2000, 2000],
            "prev_con_gdhi": [10.0, 10.0],
            "next_safe_year": [2003, 2003],
            "next_con_gdhi": [40.0, 40.0],
        })

        result_df = interpolate_imputed_val(df)

        expected_df = pd.DataFrame({
            "lsoa_code": ["E1", "E1"],
            "year": [2001, 2002],
            "con_gdhi": [12.0, 13.0],
            "year_to_adjust": [[2001, 2002], [2001, 2002]],
            "rollback_flag": [True, False],
            "prev_safe_year": [2000, 2000],
            "prev_con_gdhi": [10.0, 10.0],
            "next_safe_year": [2003, 2003],
            "next_con_gdhi": [40.0, 40.0],
            "imputed_gdhi": [np.NaN, 30.0],
        })

        pd.testing.assert_frame_equal(
            result_df, expected_df, check_names=False
        )


class TestExtrapolateImputedVal:
    """Tests for the extrapolate_imputed_val function."""
    def test_extrapolate_imputed_val(self):
        """Test the extrapolate_imputed_val function returns the expected
        imputed values.
        """
        df = pd.DataFrame({
            "lsoa_code": ["E1", "E1", "E1", "E1", "E1", "E1", "E1"],
            "year": [2001, 2002, 2003, 2004, 2005, 2006, 2007],
            "con_gdhi": [12.0, 13.0, 40.0, 49.0, 55.0, 63.0, 72.0],
            "year_to_adjust": [[2001, 2002], [2001, 2002], [2001, 2002],
                               [2001, 2002], [2001, 2002], [2001, 2002],
                               [2001, 2002]],
            "rollback_flag": [True, False, False, False, False, False, False],
        })

        imputed_df = pd.DataFrame({
            "lsoa_code": ["E1", "E1"],
            "year": [2001, 2002],
            "con_gdhi": [12.0, 13.0],
            "year_to_adjust": [[2001, 2002], [2001, 2002]],
            "rollback_flag": [True, False],
            "prev_safe_year": [2000, 2000],
            "prev_con_gdhi": [10.0, 10.0],
            "next_safe_year": [2003, 2003],
            "next_con_gdhi": [40.0, 40.0],
            "imputed_gdhi": [np.NaN, 30.0],
        })

        result_df = extrapolate_imputed_val(df, imputed_df)

        expected_df = pd.DataFrame({
            "lsoa_code": ["E1", "E1"],
            "year": [2001, 2002],
            "con_gdhi": [12.0, 13.0],
            "year_to_adjust": [[2001, 2002], [2001, 2002]],
            "rollback_flag": [True, False],
            "prev_safe_year": [2000, 2000],
            "prev_con_gdhi": [10.0, 10.0],
            "next_safe_year": [2003, 2003],
            "next_con_gdhi": [40.0, 40.0],
            "imputed_gdhi": [24.0, 30.0],
        })

        pd.testing.assert_frame_equal(
            result_df, expected_df, check_names=False
        )


def test_calc_imputed_adjustment():
    """Test calc_imputed_adjustment computes imputed_diff and apportions
    the summed adjustment_val across all rows for the same LSOA.
    """
    df = pd.DataFrame({
        "lsoa_code": ["E1", "E2", "E3", "E1"],
        "lad_code": ["E01", "E01", "E01", "E01"],
        "year": [2002, 2002, 2002, 2003],
        "con_gdhi": [5.0, 8.0, 10.0, 15.0],
    })

    # imputed_df contains only the outlier row(s) with their computed
    # imputed_gdhi
    imputed_df = pd.DataFrame({
        "lsoa_code": ["E2", "E3"],
        "year": [2002, 2002],
        "con_gdhi": [8.0, 10.0],
        "imputed_gdhi": [7.5, 11.0],
    })

    result_df = calc_imputed_adjustment(df, imputed_df)

    expected_df = pd.DataFrame({
        "lsoa_code": ["E1", "E2", "E3", "E1"],
        "lad_code": ["E01", "E01", "E01", "E01"],
        "year": [2002, 2002, 2002, 2003],
        "con_gdhi": [5.0, 8.0, 10.0, 15.0],
        "imputed_gdhi": [None, 7.5, 11.0, None],
        "imputed_diff": [None, 0.5, -1.0, None],
        # group sum of imputed_diff for E01 2002 is -0.5; for E01 2003 (all
        # NaN) pandas produces 0.0 when summing; transform('sum') therefore
        # gives 0.5 for 2002 rows and 0.0 for 2003 rows.
        "adjustment_val": [-0.5, -0.5, -0.5, 0.0],
    })

    pd.testing.assert_frame_equal(result_df, expected_df, check_dtype=False)
