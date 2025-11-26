import numpy as np
import pandas as pd

from gdhi_adj.adjustment.flag_adjustment import identify_safe_years


class TestIdentifySafeYears:
    def test_identify_safe_years_middle(self):
        df = pd.DataFrame({
            "lsoa_code": ["E1", "E1", "E1", "E1", "E2"],
            "year": [2000, 2001, 2002, 2003, 2000],
            "con_gdhi": [10.0, 20.0, 30.0, 40.0, 50.0],
            "year_to_adjust": [
                [2001, 2002], [2001, 2002], [2001, 2002], [2001, 2002], []
            ],
        })

        base_df, result_df = identify_safe_years(
            df, start_year=2000, end_year=2003
        )

        expected_df = pd.DataFrame({
            "lsoa_code": ["E1", "E1"],
            "year": [2001, 2002],
            "con_gdhi": [20.0, 30.0],
            "year_to_adjust": [[2001, 2002], [2001, 2002]],
            "prev_safe_year": [2000, 2000],
            "prev_con_gdhi": [10.0, 10.0],
            "next_safe_year": [2003, 2003],
            "next_con_gdhi": [40.0, 40.0],
        })

        pd.testing.assert_frame_equal(base_df, df)
        pd.testing.assert_frame_equal(result_df, expected_df)

    def test_identify_safe_years_end(self):
        df = pd.DataFrame({
            "lsoa_code": ["E1", "E1", "E1", "E1", "E2"],
            "year": [2000, 2001, 2002, 2003, 2000],
            "con_gdhi": [10.0, 20.0, 30.0, 40.0, 50.0],
            "year_to_adjust": [
                [2000, 2001], [2000, 2001], [2000, 2001], [2000, 2001], []
            ],
        })

        base_df, result_df = identify_safe_years(
            df, start_year=2000, end_year=2003
        )

        expected_df = pd.DataFrame({
            "lsoa_code": ["E1", "E1"],
            "year": [2000, 2001],
            "con_gdhi": [10.0, 20.0],
            "year_to_adjust": [[2000, 2001], [2000, 2001]],
            "prev_safe_year": [1999, 1999],
            "prev_con_gdhi": [np.nan, np.nan],
            "next_safe_year": [2002, 2002],
            "next_con_gdhi": [30.0, 30.0],
        })

        pd.testing.assert_frame_equal(base_df, df)
        pd.testing.assert_frame_equal(result_df, expected_df)
