import pandas as pd
import pytest

from gdhi_adj.adjustment.join_adjustment import (
    join_analyst_constrained_data,
    join_analyst_unconstrained_data,
)


def test_join_analyst_constrained_data():
    """Test the join_analyst_constrained_data function."""
    df_constrained = pd.DataFrame({
        "lsoa_code": ["E1", "E2"],
        "lad_code": ["E01", "E01"],
        "2002": [10.0, 11.0],
        "2003": [20.0, 21.0]
    })

    df_analyst = pd.DataFrame({
        "lsoa_code": ["E1"],
        "lad_code": ["E01"],
        "adjust": [True],
        "year": [2002]
    })

    result_df = join_analyst_constrained_data(df_constrained, df_analyst)

    expected_df = pd.DataFrame({
        "lsoa_code": ["E1", "E2"],
        "lad_code": ["E01", "E01"],
        "CON_2002": [10.0, 11.0],
        "CON_2003": [20.0, 21.0],
        "adjust": [True, float("NaN")],
        "year": [2002, float("NaN")]
    })

    pd.testing.assert_frame_equal(result_df, expected_df)


def test_join_analyst_constrained_data_adjust_failed_merge():
    """Test the join_analyst_constrained_data function where the analyst data
    fails to join."""
    df_constrained = pd.DataFrame({
        "lsoa_code": ["E1", "E2"],
        "lad_code": ["E01", "E01"],
        "2002": [10.0, 11.0],
        "2003": [20.0, 21.0]
    })

    df_analyst = pd.DataFrame({
        "lsoa_code": ["E1"],
        "lad_code": ["E02"],  # Different lad_code
        "adjust": [True],
        "year": [2002]
    })

    with pytest.raises(
        expected_exception=ValueError,
        match="Number of rows to adjust between analyst and constrained data"
    ):
        join_analyst_constrained_data(df_constrained, df_analyst)


def test_join_analyst_constrained_data_row_increase():
    """Test the join_analyst_constrained_data function where merging the
    analyst data increases the number of rows."""
    df_constrained = pd.DataFrame({
        "lsoa_code": ["E1", "E2"],
        "lad_code": ["E01", "E01"],
        "2002": [10.0, 11.0],
        "2003": [20.0, 21.0]
    })

    df_analyst = pd.DataFrame({
        "lsoa_code": ["E1", "E1"],  # Duplicate entries for E1
        "lad_code": ["E01", "E01"],
        "adjust": [True, True],
        "year": [2002, 2003]
    })

    with pytest.raises(
        expected_exception=ValueError,
        match="Number of rows of constrained data after join has increased."
    ):
        join_analyst_constrained_data(df_constrained, df_analyst)


def test_join_analyst_unconstrained_data():
    """Test the join_analyst_unconstrained_data function."""
    df_unconstrained = pd.DataFrame({
        "lsoa_code": ["E1", "E2"],
        "lsoa_name": ["AA", "BB"],
        "lad_code": ["E01", "E01"],
        "lad_name": ["AAA", "AAA"],
        "2002": [10.0, 11.0],
        "2003": [20.0, 21.0]
    })

    df_analyst = pd.DataFrame({
        "lsoa_code": ["E1", "E2"],
        "lsoa_name": ["AA", "BB"],
        "lad_code": ["E01", "E01"],
        "lad_name": ["AAA", "AAA"],
        "adjust": [True, float("NaN")],
        "year": [2002, float("NaN")],
        "CON_2002": [10.0, 11.0],
        "CON_2003": [20.0, 21.0]
    })

    result_df = join_analyst_unconstrained_data(df_unconstrained, df_analyst)

    expected_df = pd.DataFrame({
        "lsoa_code": ["E1", "E2"],
        "lsoa_name": ["AA", "BB"],
        "lad_code": ["E01", "E01"],
        "lad_name": ["AAA", "AAA"],
        "2002": [10.0, 11.0],
        "2003": [20.0, 21.0],
        "adjust": [True, False],
        "year": [2002, float("NaN")],
        "CON_2002": [10.0, 11.0],
        "CON_2003": [20.0, 21.0]
    })

    pd.testing.assert_frame_equal(result_df, expected_df, check_dtype=False)


def test_join_analyst_unconstrained_data_adjust_failed_merge():
    """Test the join_analyst_unconstrained_data function where the analyst data
    fails to join."""
    df_constrained = pd.DataFrame({
        "lsoa_code": ["E1", "E2"],
        "lsoa_name": ["AA", "BB"],
        "lad_code": ["E01", "E01"],
        "lad_name": ["AAA", "AAA"],
        "2002": [10.0, 11.0],
        "2003": [20.0, 21.0]
    })

    df_analyst = pd.DataFrame({
        "lsoa_code": ["E1", "E2"],
        "lsoa_name": ["AA", "BB"],
        "lad_code": ["E02", "E01"],  # Different lad_code
        "lad_name": ["BBB", "AAA"],
        "adjust": [True, float("NaN")],
        "year": [2002, float("NaN")],
        "CON_2002": [10.0, 11.0],
        "CON_2003": [20.0, 21.0]
    })

    with pytest.raises(
        expected_exception=ValueError,
        match="Number of rows to adjust between analyst and unconstrained data"
    ):
        join_analyst_unconstrained_data(df_constrained, df_analyst)


def test_join_analyst_unconstrained_data_row_increase():
    """Test the join_analyst_unconstrained_data function where merging the
    analyst data increases the number of rows."""
    df_constrained = pd.DataFrame({
        "lsoa_code": ["E1", "E2"],
        "lsoa_name": ["AA", "BB"],
        "lad_code": ["E01", "E01"],
        "lad_name": ["AAA", "AAA"],
        "2002": [10.0, 11.0],
        "2003": [20.0, 21.0]
    })

    df_analyst = pd.DataFrame({
        "lsoa_code": ["E1", "E1"],  # Duplicate entries for E1
        "lsoa_name": ["AA", "AA"],
        "lad_code": ["E01", "E01"],
        "lad_name": ["AAA", "AAA"],
        "adjust": [True, float("NaN")],
        "year": [2002, float("NaN")],
        "CON_2002": [10.0, 11.0],
        "CON_2003": [20.0, 21.0]
    })

    with pytest.raises(
        expected_exception=ValueError,
        match="Number of rows of unconstrained data after join has increased."
    ):
        join_analyst_unconstrained_data(df_constrained, df_analyst)
