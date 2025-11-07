import pandas as pd
import pytest

from gdhi_adj.adjustment.filter_adjustment import (
    filter_adjust,
    filter_component,
    filter_year,
)


def test_filter_adjust():
    """Test the filter_adjust function."""
    df = pd.DataFrame({
        "lsoa_code": ["E1", "E2", "E3"],
        "lad_code": ["E01", "E02", "E03"],
        "master_flag": [True, None, True],
        "adjust": [True, None, False],
        "year": [2003, 2004, 2005],
        "2002": [10, 20, 30],
    })
    result_df = filter_adjust(df)

    expected_df = pd.DataFrame({
        "lsoa_code": ["E1"],
        "lad_code": ["E01"],
        "adjust": [True],
        "year": [2003],
    })

    pd.testing.assert_frame_equal(result_df, expected_df, check_dtype=False)


def test_filter_year():
    """Test the filter_year function."""
    df = pd.DataFrame({
        "lad_code": ["E01", "E01", "E01", "E01"],
        "adjust": [True, True, True, True],
        "year": [2010, 2011, 2012, 2013],
        "con_gdhi": [10, 20, 30, 40],
    })

    result_df = filter_year(df, start_year=2011, end_year=2012)

    expected_df = pd.DataFrame({
        "lad_code": ["E01", "E01"],
        "adjust": [True, True],
        "year": [2011, 2012],
        "con_gdhi": [20, 30],
    })

    pd.testing.assert_frame_equal(result_df, expected_df, check_dtype=False)


class TestFilterComponent:
    """Test the filter_component function."""

    def test_filter_component_correct(self):
        """Test filtering by component codes."""
        df = pd.DataFrame({
            "lsoa_code": ["E1", "E2", "E3", "E4"],
            "sas_code": ["A", "B", "C", "A"],
            "cord_code": ["C1", "C2", "C3", "C2"],
            "credit_debit": ["D", "C", "D", "C"],
            "value": [100, 200, 300, 400],
        })

        result_df = filter_component(
            df,
            sas_code_filter="A",
            cord_code_filter="C2",
            credit_debit_filter="C",
        )

        expected_df = pd.DataFrame({
            "lsoa_code": ["E4"],
            "sas_code": ["A"],
            "cord_code": ["C2"],
            "credit_debit": ["C"],
            "value": [400],
        })

        pd.testing.assert_frame_equal(
            result_df, expected_df, check_dtype=False
        )

    def test_filter_component_filter_not_found(self):
        """Test filtering when component code is not present in DataFrame."""
        df = pd.DataFrame({
            "lsoa_code": ["E1", "E2", "E3"],
            "sas_code": ["A", "B", "C"],
            "cord_code": ["C1", "C2", "C3"],
            "credit_debit": ["D", "C", "D"],
            "value": [100, 200, 300],
        })

        with pytest.raises(
            ValueError,
            match=("SAS code 'X' not found in data.")
        ):
            filter_component(
                df,
                sas_code_filter="X",
                cord_code_filter="Y",
                credit_debit_filter="Z",
            )
