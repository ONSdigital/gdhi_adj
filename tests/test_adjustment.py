import pandas as pd
import pytest

from gdhi_adj.adjustment import (
    apply_adjustment,
    calc_adjustment_headroom_val,
    calc_adjustment_val,
    calc_midpoint_val,
    calc_scaling_factors,
    create_anaomaly_list,
    filter_lsoa_data,
    join_analyst_constrained_data,
    join_analyst_unconstrained_data,
    pivot_adjustment_long,
    pivot_wide_dataframe,
)


def test_filter_lsoa_data():
    """Test the filter_lsoa_data function."""
    df = pd.DataFrame({
        "lsoa_code": ["E1", "E2", "E3"],
        "lad_code": ["E01", "E02", "E03"],
        "transaction_code": ["T1", "T2", "T3"],
        "master_flag": [True, None, True],
        "adjust": [True, None, False],
        "year": [2003, 2004, 2005],
        "2002": [10, 20, 30],
    })
    result_df = filter_lsoa_data(df)

    expected_df = pd.DataFrame({
        "lsoa_code": ["E1"],
        "lad_code": ["E01"],
        "transaction_code": ["T1"],
        "adjust": [True],
        "year": [2003],
    })

    pd.testing.assert_frame_equal(result_df, expected_df, check_dtype=False)

    df_mismatch = pd.DataFrame({
        "lsoa_code": ["E1", "E2", "E3"],
        "lad_code": ["E01", "E02", "E03"],
        "transaction_code": ["T1", "T2", "T3"],
        "master_flag": [True, None, True],
        "adjust": [True, False, False],
        "year": [2003, 2004, 2005]
    })

    with pytest.raises(
        expected_exception=ValueError,
        match="Mismatch: master_flag and Adjust column booleans do not match."
    ):
        filter_lsoa_data(df_mismatch)


def test_join_analyst_constrained_data():
    """Test the join_analyst_constrained_data function."""
    df_constrained = pd.DataFrame({
        "lsoa_code": ["E1", "E2"],
        "lad_code": ["E01", "E01"],
        "transaction_code": ["T1", "T1"],
        "2002": [10.0, 11.0],
        "2003": [20.0, 21.0]
    })

    df_analyst = pd.DataFrame({
        "lsoa_code": ["E1"],
        "lad_code": ["E01"],
        "transaction_code": ["T1"],
        "adjust": [True],
        "year": [2002]
    })

    result_df = join_analyst_constrained_data(df_constrained, df_analyst)

    expected_df = pd.DataFrame({
        "lsoa_code": ["E1", "E2"],
        "lad_code": ["E01", "E01"],
        "transaction_code": ["T1", "T1"],
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
        "transaction_code": ["T1", "T1"],
        "2002": [10.0, 11.0],
        "2003": [20.0, 21.0]
    })

    df_analyst = pd.DataFrame({
        "lsoa_code": ["E1"],
        "lad_code": ["E01"],
        "transaction_code": ["T2"],  # Different transaction_code
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
        "transaction_code": ["T1", "T1"],
        "2002": [10.0, 11.0],
        "2003": [20.0, 21.0]
    })

    df_analyst = pd.DataFrame({
        "lsoa_code": ["E1", "E1"],  # Duplicate entries for E1
        "lad_code": ["E01", "E01"],
        "transaction_code": ["T1", "T1"],
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
        "lad_code": ["E01", "E01"],
        "transaction_code": ["T1", "T1"],
        "2002": [10.0, 11.0],
        "2003": [20.0, 21.0]
    })

    df_analyst = pd.DataFrame({
        "lsoa_code": ["E1", "E2"],
        "lad_code": ["E01", "E01"],
        "transaction_code": ["T1", "T1"],
        "adjust": [True, float("NaN")],
        "year": [2002, float("NaN")],
        "CON_2002": [10.0, 11.0],
        "CON_2003": [20.0, 21.0]
    })

    result_df = join_analyst_unconstrained_data(df_unconstrained, df_analyst)

    expected_df = pd.DataFrame({
        "lsoa_code": ["E1", "E2"],
        "lad_code": ["E01", "E01"],
        "transaction_code": ["T1", "T1"],
        "2002": [10.0, 11.0],
        "2003": [20.0, 21.0],
        "adjust": [True, float("NaN")],
        "year": [2002, float("NaN")],
        "CON_2002": [10.0, 11.0],
        "CON_2003": [20.0, 21.0]
    })

    pd.testing.assert_frame_equal(result_df, expected_df)


def test_join_analyst_unconstrained_data_adjust_failed_merge():
    """Test the join_analyst_unconstrained_data function where the analyst data
    fails to join."""
    df_constrained = pd.DataFrame({
        "lsoa_code": ["E1", "E2"],
        "lad_code": ["E01", "E01"],
        "transaction_code": ["T1", "T1"],
        "2002": [10.0, 11.0],
        "2003": [20.0, 21.0]
    })

    df_analyst = pd.DataFrame({
        "lsoa_code": ["E1", "E2"],
        "lad_code": ["E01", "E01"],
        "transaction_code": ["T2", "T1"],  # Different transaction_code
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
        "lad_code": ["E01", "E01"],
        "transaction_code": ["T1", "T1"],
        "2002": [10.0, 11.0],
        "2003": [20.0, 21.0]
    })

    df_analyst = pd.DataFrame({
        "lsoa_code": ["E1", "E1"],  # Duplicate entries for E1
        "lad_code": ["E01", "E01"],
        "transaction_code": ["T1", "T1"],
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


def test_pivot_adjustment_long():
    """Test the pivot_adjustment_long function."""
    df = pd.DataFrame({
        "lsoa_code": ["E1", "E2"],
        "lad_code": ["E01", "E01"],
        "transaction_code": ["T1", "T1"],
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
        "lad_code": ["E01", "E01", "E01", "E01"],
        "transaction_code": ["T1", "T1", "T1", "T1"],
        "adjust": [True, float("NaN"), True, float("NaN")],
        "year_to_adjust": [2002, float("NaN"), 2002, float("NaN")],
        "year": [2002, 2002, 2003, 2003],
        "uncon_gdhi": [10.0, 11.0, 20.0, 21.0],
        "con_gdhi": [30.0, 31.0, 40.0, 41.0]
    })

    pd.testing.assert_frame_equal(result_df, expected_df, check_dtype=False)


def test_calc_scaling_factors():
    """Test the calc_scaling_factors function."""
    df = pd.DataFrame({
        "lsoa_code": ["E1", "E2", "E3", "E1", "E2", "E3"],
        "lad_code": ["E01", "E01", "E01", "E01", "E01", "E01"],
        "transaction_code": ["T1", "T1", "T2", "T1", "T1", "T2"],
        "year": [2002, 2002, 2002, 2003, 2003, 2003],
        "uncon_gdhi": [10.0, 15.0, 25.0, 26.0, 50.0, 40.0],
        "con_gdhi": [30.0, 40.0, 10.0, 11.0, 20.0, 30.0]
    })

    result_df = calc_scaling_factors(df)

    expected_df = pd.DataFrame({
        "transaction_code": ["T1", "T1", "T2", "T2"],
        "year": [2002, 2003, 2002, 2003],
        "uncon_gdhi": [25.0, 76.0, 25.0, 40.0],
        "con_gdhi": [70.0, 31.0, 10.0, 30.0],
        "scaling": [2.8, 0.407895, 0.4, 0.75]
    })

    pd.testing.assert_frame_equal(result_df, expected_df)


def test_create_anaomaly_list():
    """Test create_anaomaly_list function."""
    df = pd.DataFrame({
        "lsoa_code": ["E1", "E1", "E2", "E3", "E3", "E3", "E4"],
        "lad_code": ["E01", "E01", "E01", "E01", "E01", "E01", "E01"],
        "transaction_code": ["T1", "T1", "T1", "T1", "T1", "T2", "T1"],
        "adjust": [True, True, float("NaN"), True, True, True, True],
        "year_to_adjust": [2002, 2002, float("NaN"), 2002, 2003, 2002, 2002],
        "uncon_gdhi": [10, 11, 12, 13, 14, 15, 16]
    })

    result_df = create_anaomaly_list(df)

    expected_df = pd.DataFrame({
        "lsoa_code": ["E1", "E3", "E3", "E3", "E4"],
        "transaction_code": ["T1", "T1", "T1", "T2", "T1"],
        "year_to_adjust": [2002, 2002, 2003, 2002, 2002],
    })

    pd.testing.assert_frame_equal(result_df, expected_df, check_dtype=False)


def test_calc_adjustment_headroom_val():
    """Test the calc_adjustment_headroom_val function."""
    df = pd.DataFrame({
        "lsoa_code": ["E1", "E2", "E3", "E4", "E5"],
        "transaction_code": ["T1", "T2", "T1", "T1", "T1"],
        "year": [2002, 2002, 2004, 2003, 2003],
        "uncon_gdhi": [10.0, 11.0, 12.0, 13.0, 17.0],
        "con_gdhi": [5.0, 15.0, 25.0, 20.0, 10.0]
    })

    df_scaling = pd.DataFrame({
        "transaction_code": ["T1", "T1", "T1", "T2"],
        "year": [2002, 2003, 2004, 2002],
        "uncon_gdhi": [10.0, 20.0, 30.0, 40.0],
        "con_gdhi": [50.0, 60.0, 70.0, 80.0],
        "scaling": [2.0, 10.0, 1.0, 0.2]
    })

    lsoa_code = "E1"
    transaction_code = "T1"
    year_to_adjust = 2003

    result_uncon_sum, result_headroom_val = calc_adjustment_headroom_val(
        df, df_scaling, lsoa_code, transaction_code, year_to_adjust
    )

    expected_uncon_sum = 30.0
    expected_headroom_val = 15.0

    assert result_uncon_sum == expected_uncon_sum
    assert result_headroom_val == expected_headroom_val


def test_calc_midpoint_val():
    """Test the calc_midpoint_val function."""
    df = pd.DataFrame({
        "lsoa_code": ["E1", "E1", "E1", "E2"],
        "transaction_code": ["T1", "T1", "T1", "T1"],
        "year": [2002, 2003, 2004, 2003],
        "uncon_gdhi": [10.0, 20.0, 26.0, 45.0],
        "con_gdhi": [5.0, 8.0, 10.0, 15.0]
    })

    lsoa_code = "E1"
    transaction_code = "T1"
    year_to_adjust = 2003

    result_outlier_val, result_midpoint_val = calc_midpoint_val(
        df, lsoa_code, transaction_code, year_to_adjust
    )

    expected_outlier_val = 8.0
    expected_midpoint_val = 7.5

    assert result_outlier_val == expected_outlier_val
    assert result_midpoint_val == expected_midpoint_val


def test_calc_adjustment_val():
    """Test the calc_adjustment_val function."""
    headroom_val = 15.0
    outlier_val = 7.5
    midpoint_val = -8.0

    result_adjustment_val = calc_adjustment_val(
        headroom_val, outlier_val, midpoint_val
    )

    expected_adjustment_val = 7.5

    assert result_adjustment_val == expected_adjustment_val

    headroom_val_high = 30.0

    result_adjustment_val_high_head = calc_adjustment_val(
        headroom_val_high, outlier_val, midpoint_val
    )

    expected_adjustment_val_high_head = -15.5

    assert result_adjustment_val_high_head == expected_adjustment_val_high_head


def test_apply_adjustment():
    """Test the apply_adjustment function."""
    df = pd.DataFrame({
        "lsoa_code": ["E1", "E2", "E3", "E4"],
        "transaction_code": ["T1", "T1", "T1", "T1"],
        "adjust": [True, float("NaN"), float("NaN"), float("NaN")],
        "year": [2002, 2002, 2002, 2003],
        "uncon_gdhi": [10.0, 20.0, 30.0, 40.0],
        "con_gdhi": [5.0, 15.0, 12.0, 20.0]
    })

    transaction_code = "T1"
    year_to_adjust = 2002
    adjustment_val = 7.5
    uncon_non_out_sum = 30.0

    result_df = apply_adjustment(
        df, transaction_code, year_to_adjust, adjustment_val, uncon_non_out_sum
    )

    expected_df = pd.DataFrame({
        "lsoa_code": ["E1", "E2", "E3", "E4"],
        "transaction_code": ["T1", "T1", "T1", "T1"],
        "adjust": [True, float("NaN"), float("NaN"), float("NaN")],
        "year": [2002, 2002, 2002, 2003],
        "uncon_gdhi": [10.0, 20.0, 30.0, 40.0],
        "con_gdhi": [12.5, 20.0, 19.5, 20.0]
    })

    pd.testing.assert_frame_equal(result_df, expected_df)


def test_pivot_wide_dataframe():
    """Test the pivot_wide_dataframe function."""
    df = pd.DataFrame({
        "lsoa_code": ["E1", "E1", "E1", "E2", "E2", "E2"],
        # Testing lad_code column is dropped during pivot
        "lad_code": ["E01", "E01", "E01", "E01", "E01", "E01"],
        "transaction_code": ["T1", "T1", "T1", "T1", "T1", "T1"],
        "year": [2002, 2003, 2004, 2002, 2003, 2004],
        "uncon_gdhi": [10.0, 20.0, 30.0, 40.0, 50.0, 60.0],
        "con_gdhi": [5.0, 15.0, 25.0, 35.0, 45.0, 55.0]
    })

    result_df = pivot_wide_dataframe(df)

    expected_df = pd.DataFrame({
        "lsoa_code": ["E1", "E2"],
        "transaction_code": ["T1", "T1"],
        "Adjust_Con_2002": [5.0, 35.0],
        "Adjust_Con_2003": [15.0, 45.0],
        "Adjust_Con_2004": [25.0, 55.0],
    })

    pd.testing.assert_frame_equal(result_df, expected_df)
