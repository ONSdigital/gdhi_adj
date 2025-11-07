import pandas as pd

from gdhi_adj.adjustment.pivot_adjustment import (
    pivot_adjustment_long,
    pivot_wide_final_dataframe,
)


def test_pivot_adjustment_long():
    """Test the pivot_adjustment_long function."""
    df = pd.DataFrame({
        "lsoa_code": ["E1", "E2"],
        "lsoa_name": ["AA", "BB"],
        "lad_code": ["E01", "E01"],
        "lad_name": ["AAA", "AAA"],
        "2002": [10.0, 11.0],
        "2003": [20.0, 21.0],
        "CON_2002": [30.0, 31.0],
        "CON_2003": [40.0, 41.0],
        "adjust": [True, float("NaN")],
        "year": [2002, float("NaN")]
    })

    result_df = pivot_adjustment_long(df)

    expected_df = pd.DataFrame({
        "lsoa_code": ["E1", "E2", "E1", "E2"],
        "lsoa_name": ["AA", "BB", "AA", "BB"],
        "lad_code": ["E01", "E01", "E01", "E01"],
        "lad_name": ["AAA", "AAA", "AAA", "AAA"],
        "adjust": [True, float("NaN"), True, float("NaN")],
        "year_to_adjust": [2002, float("NaN"), 2002, float("NaN")],
        "year": [2002, 2002, 2003, 2003],
        "uncon_gdhi": [10.0, 11.0, 20.0, 21.0],
        "con_gdhi": [30.0, 31.0, 40.0, 41.0]
    })

    pd.testing.assert_frame_equal(result_df, expected_df, check_dtype=False)


def test_pivot_wide_final_dataframe():
    """Test the pivot_wide_final_dataframe function."""
    df = pd.DataFrame({
        "lsoa_code": ["E1", "E1", "E1", "E2", "E2", "E2"],
        "lsoa_name": ["AA", "AA", "AA", "BB", "BB", "BB"],
        # Testing lad_code column is dropped during pivot
        "lad_code": ["E01", "E01", "E01", "E01", "E01", "E01"],
        "lad_name": ["AAA", "AAA", "AAA", "AAA", "AAA", "AAA"],
        "year": [2002, 2003, 2004, 2002, 2003, 2004],
        "uncon_gdhi": [10.0, 20.0, 30.0, 40.0, 50.0, 60.0],
        "con_gdhi": [5.0, 15.0, 25.0, 35.0, 45.0, 55.0]
    })

    result_df = pivot_wide_final_dataframe(df)

    expected_df = pd.DataFrame({
        "lsoa_code": ["E1", "E2"],
        "lsoa_name": ["AA", "BB"],
        "lad_code": ["E01", "E01"],
        "lad_name": ["AAA", "AAA"],
        2002: [5.0, 35.0],
        2003: [15.0, 45.0],
        2004: [25.0, 55.0],
    })

    pd.testing.assert_frame_equal(
        result_df, expected_df
    )
