import numpy as np
import pandas as pd
import pytest

from gdhi_adj.adjustment.reformat_adjustment import (
    reformat_adjust_col,
    reformat_year_col,
    to_int_list,
)


def test_reformat_adjust_col():
    """Test the reformat_adjust_col function."""
    df = pd.DataFrame({
        "adjust": ["TRUE", "FALSE", None, "TRUE", "FALSE"],
    })

    result_df = reformat_adjust_col(df)

    expected_df = pd.DataFrame({
        "adjust": [True, False, False, True, False],
    })

    pd.testing.assert_frame_equal(result_df, expected_df)


def test_to_int_list():
    """Test various inputs to to_int_list behave as implemented.

    Note: the function treats list-like inputs differently from scalar
    inputs. For scalar empty or 'nan' strings it returns an empty list; for
    list-like inputs it returns an empty list when all items are NA.
    """

    # comma-separated string
    assert to_int_list("2010,2011, 2012") == [2010, 2011, 2012]

    # list of strings
    assert to_int_list(["2010", "2011"]) == [2010, 2011]

    # list containing numeric types
    assert to_int_list([2010, 2011]) == [2010, 2011]

    # numpy array
    assert to_int_list(np.array(["2010", "2011"])) == [2010, 2011]

    # list with None/NaN items -> they are skipped
    assert to_int_list(["2010", None, float("nan"), "2012"]) == [2010, 2012]

    # float tokens should be converted via int(float(token))
    assert to_int_list("2010.0, 2011.0") == [2010, 2011]

    # empty string and explicit nan scalar return None (per current impl)
    assert to_int_list("") == []
    assert to_int_list(float("nan")) == []

    # invalid token raises ValueError
    with pytest.raises(ValueError):
        to_int_list("20a,2010")


class TestReformatYearCol:
    """Test reformat_year_col function."""
    def test_reformat_year_col_correct(self):
        """Test reformat_year_col normalises year cell formats to tuples

        It should turn:
        - single year strings into single-element tuples,
        - comma-separated year strings into tuples of ints,
        - missing/empty values into empty tuples.
        """

        df = pd.DataFrame({
            "year": ["2002", "2002, 2003", None, "", " 2004 ,2005 "],
        })

        start_year = 2002
        end_year = 2005

        result_df = reformat_year_col(df, start_year, end_year)

        expected_df = pd.DataFrame({
            "year": [(2002,), (2002, 2003), (), (), (2004, 2005)],
        })

        # sort/index are preserved; compare allowing dtype differences
        pd.testing.assert_frame_equal(
            result_df, expected_df, check_dtype=False
        )

    def test_reformat_year_col_duplicate_years(self):
        """Test reformat_year_col raises ValueError for duplicate years."""
        df = pd.DataFrame({
            "year": ["2002", "2002, 2003", None, "", " 2004 ,2004 "],
        })

        start_year = 2002
        end_year = 2005

        with pytest.raises(
            ValueError,
            match="Duplicate years found in year column within LSOA."
        ):
            reformat_year_col(df, start_year, end_year)

    def test_reformat_year_col_out_of_range_years(self):
        """Test reformat_year_col raises ValueError for out-of-range years."""
        df = pd.DataFrame({
            "year": ["2002", "2002, 2006", None, "", " 2004 ,2005 "],
        })

        start_year = 2002
        end_year = 2005

        with pytest.raises(
            ValueError,
            match=("Year 2006 in year column is out of valid range 2002-2005.")
        ):
            reformat_year_col(df, start_year, end_year)
