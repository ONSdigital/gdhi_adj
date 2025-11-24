import numpy as np
import pandas as pd

from gdhi_adj.adjustment.calc_adjustment import (
    extrapolate_imputed_val,
    interpolate_imputed_val,
)


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
