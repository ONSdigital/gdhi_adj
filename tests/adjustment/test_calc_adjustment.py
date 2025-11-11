import pandas as pd

from gdhi_adj.adjustment.calc_adjustment import (
    apportion_adjustment,
    calc_imputed_adjustment,
    calc_imputed_val,
)


def test_calc_imputed_val():
    """Test the calc_imputed_val function returns the expected imputed values.

    The function should:
    - select rows where the row's `year` is contained in that row's
      `year_to_adjust` value,
    - compute `prev_con_gdhi` and `next_con_gdhi` by looking up the
      same `lsoa_code` at year-1 and year+1,
    - compute `imputed_gdhi` by either interpolating or extrapolating, two safe
      designated values.
    """
    df = pd.DataFrame({
        "lsoa_code": ["E1", "E1", "E1", "E2"],
        "year": [2002, 2003, 2004, 2003],
        "uncon_gdhi": [10.0, 20.0, 26.0, 45.0],
        "con_gdhi": [5.0, 8.0, 10.0, 15.0],
        # only the 2003 row for E1 should be flagged for adjustment
        "year_to_adjust": [[], [2003], [], None],
    })

    start_year = 2002
    end_year = 2004
    result_df = calc_imputed_val(df, start_year, end_year)

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
        "imputed_gdhi": [7.5],
    })

    pd.testing.assert_frame_equal(result_df, expected_df, check_dtype=False)


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


def test_apportion_adjustment():
    """Test apportion_adjustment computes year_count and adjusted_con_gdhi
    correctly and returns the full dataframe sorted.
    """

    df = pd.DataFrame({
        "lsoa_code": ["E1", "E2", "E3", "E1"],
        "lad_code": ["E01", "E01", "E01", "E01"],
        "year": [2002, 2002, 2002, 2003],
        "con_gdhi": [5.0, 8.0, 10.0, 15.0],
        # imputed_gdhi only present for E1 2003
        "imputed_gdhi": [None, 7.4, None, None],
        # adjustment_val is set for E1 (will be apportioned), None for E2
        "adjustment_val": [0.6, 0.6, 0.6, None],
    })

    result_df = apportion_adjustment(df)

    expected_df = pd.DataFrame({
        "lsoa_code": ["E1", "E2", "E3", "E1"],
        "lad_code": ["E01", "E01", "E01", "E01"],
        "year": [2002, 2002, 2002, 2003],
        "con_gdhi": [5.0, 8.0, 10.0, 15.0],
        "imputed_gdhi": [None, 7.4, None, None],
        "adjustment_val": [0.6, 0.6, 0.6, None],
        "lsoa_count": [3, 3, 3, 1],
        "adjusted_con_gdhi": [5.2, 7.6, 10.2, 15.0],
    })

    pd.testing.assert_frame_equal(result_df, expected_df, check_dtype=False)
