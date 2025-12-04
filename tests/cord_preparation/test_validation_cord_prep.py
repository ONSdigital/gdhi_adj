"""Tests for validation_cord_prep.py module."""

import numpy as np
import pandas as pd
import pytest

from gdhi_adj.cord_preparation.validation_cord_prep import (
    check_lsoa_consistency,
    check_no_negative_values,
    check_no_nulls,
    check_year_column_completeness,
)


class TestLSOAConsistency:
    """Tests for check_lsoa_consistency function."""

    def test_valid_consistency(self):
        """
        Test that a valid DataFrame with unique lsoa_codes passes the check.
        """
        df = pd.DataFrame({
            'lsoa_code': ['E01000001', 'E01000002', 'E01000003'],
            'value': [10, 20, 30]
        })

        result = check_lsoa_consistency(df)
        pd.testing.assert_frame_equal(df, result)

    def test_duplicate_lsoa_codes(self):
        """
        Test that duplicate lsoa_codes raise a ValueError.
        """
        df = pd.DataFrame({
            'lsoa_code': ['E01000001', 'E01000001', 'E01000002'],
            # Duplicate 'E01000001'
            'value': [10, 20, 30]
        })

        with pytest.raises(ValueError) as excinfo:
            check_lsoa_consistency(df)

        assert "Internal Consistency Check Failed" in str(excinfo.value)
        assert "Found 2 unique codes across 3 rows" in str(excinfo.value)

    def test_missing_column(self):
        """
        Test that a KeyError is raised if 'lsoa_code' column is missing.
        """
        df = pd.DataFrame({
            'wrong_column': ['A', 'B', 'C'],
            'value': [1, 2, 3]
        })

        with pytest.raises(KeyError) as excinfo:
            check_lsoa_consistency(df)

        assert "The column 'lsoa_code' was not found" in str(excinfo.value)

    def test_nan_values_in_lsoa(self):
        """
        Test behavior with NaN values in lsoa_code column specifically.
        """
        df = pd.DataFrame({
            'lsoa_code': ['E01000001', 'E01000002', np.nan],
            'value': [1, 2, 3]
        })

        with pytest.raises(ValueError) as excinfo:
            check_lsoa_consistency(df)

        assert "Internal Consistency Check Failed" in str(excinfo.value)

    def test_empty_dataframe(self):
        """
        Test that an empty DataFrame passes (0 rows == 0 unique values).
        """
        df = pd.DataFrame({'lsoa_code': []})
        result = check_lsoa_consistency(df)
        pd.testing.assert_frame_equal(df, result)

    def test_method_chaining_capability(self):
        """
        Test that the function can actually be used in a pandas pipe chain.
        """
        df = pd.DataFrame({
            'lsoa_code': ['A', 'B', 'C'],
            'val': [1, 1, 1]
        })

        result = (
            df
            .assign(val=lambda x: x['val'] + 1)
            .pipe(check_lsoa_consistency)
            .assign(val=lambda x: x['val'] * 2)
        )
        assert result['val'].tolist() == [4, 4, 4]


class TestNoNullsCheck:
    """Tests for check_no_nulls function."""

    def test_no_nulls_pass(self):
        """
        Test that a DataFrame with no null values passes the check unchanged.
        """
        df = pd.DataFrame({
            'col1': [1, 2, 3],
            'col2': ['a', 'b', 'c']
        })
        result = check_no_nulls(df)
        pd.testing.assert_frame_equal(df, result)

    def test_has_np_nan(self):
        """
        Test that the function detects and raises ValueError
        for numpy.nan values.
        """
        df = pd.DataFrame({
            'col1': [1, np.nan, 3]
        })
        with pytest.raises(ValueError) as excinfo:
            check_no_nulls(df)
        assert "Null Value Check Failed" in str(excinfo.value)
        assert "col1" in str(excinfo.value)

    def test_has_none(self):
        """
        Test that the function detects and raises ValueError for
        Python None values.
        """
        df = pd.DataFrame({
            'col1': [1, None, 3]
        })
        with pytest.raises(ValueError) as excinfo:
            check_no_nulls(df)
        assert "Null Value Check Failed" in str(excinfo.value)

    def test_has_pd_na(self):
        """
        Test that the function detects and raises ValueError for
        pandas pd.NA values.
        """
        df = pd.DataFrame({
            'col1': [1, pd.NA, 3]
        }, dtype="Int64")  # Int64 allows pd.NA
        with pytest.raises(ValueError) as excinfo:
            check_no_nulls(df)
        assert "Null Value Check Failed" in str(excinfo.value)


class TestNoNegativeValues:
    """Tests for check_no_negative_values function."""

    def test_no_negatives_pass(self):
        """
        Test that a DataFrame with positive integers and floats passes.
        """
        df = pd.DataFrame({
            'int_col': [1, 2, 0],
            'float_col': [0.1, 5.5, 100.0]
        })
        result = check_no_negative_values(df)
        pd.testing.assert_frame_equal(df, result)

    def test_has_negative_int(self):
        """
        Test that negative integers trigger the ValueError.
        """
        df = pd.DataFrame({
            'val': [10, -5, 20]
        })
        with pytest.raises(ValueError) as excinfo:
            check_no_negative_values(df)
        assert "Negative Value Check Failed" in str(excinfo.value)
        assert "val" in str(excinfo.value)

    def test_has_negative_float(self):
        """
        Test that negative floats trigger the ValueError.
        """
        df = pd.DataFrame({
            'val': [1.0, 0.5, -0.01]
        })
        with pytest.raises(ValueError) as excinfo:
            check_no_negative_values(df)
        assert "Negative Value Check Failed" in str(excinfo.value)

    def test_ignores_string_columns(self):
        """
        Test that non-numeric columns (strings) are ignored,
        even if they contain dashes.
        """
        df = pd.DataFrame({
            'numeric': [1, 2, 3],
            'string_col': ['-5', 'positive', 'text']
        })
        result = check_no_negative_values(df)
        pd.testing.assert_frame_equal(df, result)


class TestYearCompleteness:
    """Tests for check_year_column_completeness function."""

    def test_years_complete_strings(self):
        """
        Test that a DataFrame with complete sequential year
        columns (as strings) passes.
        """
        # Create columns 2010 through 2023 as strings
        years = [str(y) for y in range(2010, 2024)]
        df = pd.DataFrame(columns=years)
        result = check_year_column_completeness(df)
        pd.testing.assert_frame_equal(df, result)

    def test_years_complete_integers(self):
        """
        Test that a DataFrame with complete sequential year
        columns (as integers) passes.
        """
        # Create columns 2010 through 2023 as integers
        years = list(range(2010, 2024))
        df = pd.DataFrame(columns=years)
        result = check_year_column_completeness(df)
        pd.testing.assert_frame_equal(df, result)

    def test_missing_single_year(self):
        """
        Test that a single missing year in the sequence raises a ValueError.
        """
        # Missing 2015
        years = [str(y) for y in range(2010, 2024) if y != 2015]
        df = pd.DataFrame(columns=years)

        with pytest.raises(ValueError) as excinfo:
            check_year_column_completeness(df)

        assert "Year Column Check Failed" in str(excinfo.value)
        assert "Detected range 2010-2023" in str(excinfo.value)
        assert "2015" in str(excinfo.value)

    def test_missing_range_gap(self):
        """
        Test that missing multiple years in a gap raises a ValueError.
        """
        # Only has 2010 and 2015. Implied range 2010-2015.
        df = pd.DataFrame(columns=['2010', '2015'])

        with pytest.raises(ValueError) as excinfo:
            check_year_column_completeness(df)

        # Should detect range 2010-2015, and fail because 2011, 2012,
        # 2013, 2014 are missing
        assert "Detected range 2010-2015" in str(excinfo.value)
        assert "2011" in str(excinfo.value)
        assert "2014" in str(excinfo.value)

    def test_extra_non_year_columns_ignored(self):
        """
        Test that non-numeric columns are ignored and do not affect
        the completeness check.
        """
        # Has all years 2010-2012 + lsoa_code + some random string col.
        # Should detect range 2010-2012 and PASS.
        years = ['2010', '2011', '2012']
        df = pd.DataFrame(columns=['lsoa_code', 'Category'] + years)

        result = check_year_column_completeness(df)
        pd.testing.assert_frame_equal(df, result)

    def test_no_year_columns(self):
        """
        Test that a DataFrame with no numeric/year columns raises a ValueError.
        """
        # DataFrame with no numeric columns
        df = pd.DataFrame(columns=['lsoa_code', 'value'])

        with pytest.raises(ValueError) as excinfo:
            check_year_column_completeness(df)

        assert "No numeric/year columns found" in str(excinfo.value)

    def test_single_year_column(self):
        """
        Test that a single year column counts as a complete range of length 1.
        """
        # Technically a complete range of 1 year
        df = pd.DataFrame(columns=['2020'])
        result = check_year_column_completeness(df)
        pd.testing.assert_frame_equal(df, result)
