import numpy as np
import pytest

from gdhi_adj.utils.transform_helpers import (
    ensure_list,
    increment_until_not_in,
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
