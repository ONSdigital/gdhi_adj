import numpy as np
import pandas as pd
import pytest

from gdhi_adj.utils.transform_helpers import (
    ensure_list,
    increment_until_not_in,
    sum_match_check,
    to_int_list,
)


def test_ensure_list():
    """Test various inputs to ensure_list behave as implemented."""

    # list input remains unchanged
    assert ensure_list([1, 2, 3]) == [1, 2, 3]

    # tuple input converted to list
    assert ensure_list((4, 5)) == [4, 5]

    # numpy array input converted to list
    assert ensure_list(np.array([6, 7])) == [6, 7]

    # scalar input wrapped in list
    assert ensure_list(8) == [8]

    # None input returns empty list
    assert ensure_list(None) == []


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


def test_increment_until_not_in():
    """Test increment_until_not_in function."""

    # Test increasing case
    assert increment_until_not_in(2010, [2010, 2011], 2015, True) == 2012
    assert increment_until_not_in(2012, [2010, 2011], 2015, True) == 2012
    assert increment_until_not_in(2014, [2010, 2011], 2015, True) == 2014
    assert increment_until_not_in(2015, [2015], 2015, True) == 2016
    assert increment_until_not_in(2016, [2015], 2015, True) == 2016

    # Test decreasing case
    assert increment_until_not_in(2011, [2010, 2011], 2005, False) == 2009
    assert increment_until_not_in(2009, [2010, 2011], 2005, False) == 2009
    assert increment_until_not_in(2006, [2007, 2008], 2005, False) == 2006
    assert increment_until_not_in(2005, [2005], 2005, False) == 2004
    assert increment_until_not_in(2004, [2005], 2005, False) == 2004


class TestSumMatchCheck():
    """Test suite for sum_match_check function."""
    def test_sum_match_check_success_and_tolerance(self):
        """Test the sum_match_check function computes values without raising
        error, when matches are exact and when the difference is within
        tolerance."""
        # exact match case
        df_exact = pd.DataFrame({
            "lsoa_code": ["E1", "E2", "E3", "E1"],
            "lad_code": ["E01", "E01", "E01", "E01"],
            "year": [2001, 2001, 2001, 2002],
            "unadjusted": [10.0, 20.0, 30.0, 40.0],
            "adjusted": [10.0, 20.0, 30.0, 40.0],
        })

        # should not raise
        sum_match_check(
            df_exact.copy(), ["lad_code", "year"], "unadjusted", "adjusted"
        )

        # within-tolerance case: per-group sum differs by less than default
        # tolerance
        df_tol = pd.DataFrame({
            "lsoa_code": ["E1", "E2", "E3", "E1"],
            "lad_code": ["E01", "E01", "E01", "E01"],
            "year": [2001, 2001, 2001, 2002],
            "unadjusted": [10.0, 20.0, 30.0, 40.0],
            # make total adjusted differ by 5e-7 (less than default 1e-6)
            "adjusted": [10.0, 20.0, 30.0, 40.0000005],
        })

        # should not raise
        sum_match_check(
            df_tol.copy(), ["lad_code", "year"], "unadjusted", "adjusted"
        )

    def test_sum_match_check_failure_raises(self):
        """sum_match_check should raise ValueError when group sums differ
        beyond tolerance."""

        df_fail = pd.DataFrame({
            "lsoa_code": ["E1", "E2", "E3", "E1"],
            "lad_code": ["E01", "E01", "E01", "E01"],
            "year": [2001, 2001, 2001, 2002],
            "unadjusted": [10.0, 20.0, 30.0, 40.0],
            # make total adjusted differ by 1e-4 (more than default 1e-6)
            "adjusted": [10.0, 20.0, 30.0, 40.0001],
        })

        with pytest.raises(ValueError):
            sum_match_check(
                df_fail.copy(), ["lad_code", "year"], "unadjusted", "adjusted"
            )
