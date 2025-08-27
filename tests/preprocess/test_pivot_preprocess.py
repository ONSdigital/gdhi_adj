import pandas as pd

from gdhi_adj.preprocess.pivot_preprocess import (
    pivot_output_long,
    pivot_wide_dataframe,
    pivot_years_long_dataframe,
)


def test_pivot_years_long_dataframe():
    """Test the pivot_years_long_dataframe function."""
    df = pd.DataFrame({
        "lsoa_code": ["E1", "E2", "E3"],
        "lad_code": ["E01", "E02", "E03"],
        "2003": [10, 20, 30],
        "2004": [11, 22, 33]
    })

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


def test_pivot_output_long():
    """Test the pivot_output_long function."""
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

    result_df = pivot_output_long(df, "gdhi_annual", "mean_non_out_gdhi")

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

    result_df = pivot_wide_dataframe(df)

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
