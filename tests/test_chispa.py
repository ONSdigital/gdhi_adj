"""Demonstrate how to use chispa for simple testing of pyspark dataframes."""

import pytest
import pyspark.sql.functions as F
from chispa.dataframe_comparer import assert_df_equality
from chispa.dataframe_comparer import assert_approx_df_equality


@pytest.fixture
def starting_df(spark_session):
    """Create a basic dataframe for unit tests."""
    columns = ["IDNumber", "Fibonacci_Numbers", "Triangular_Numbers", "Name"]
    values = [("1517", 0, 0.0, "Willow"),
              ("5512", 1, 1.0, "Grace"),
              ("8109", 1, 3.0, "Freya"),
              ("2365", 2, 6.0, "Oscar"),
              ("9467", 3, 10.0, "Theo"),
              ("3897", 5, 15.0, "Sophia"),
              ("2161", 8, 21.0, "Jack")
              ]
    df = spark_session.createDataFrame(values, columns)
    return df


@pytest.fixture
def reshaped_df(spark_session):
    """Create a version of starting_df with rows and columns out of order."""
    columns = ["IDNumber", "Name", "Fibonacci_Numbers", "Triangular_Numbers"]
    values = [("1517", "Willow", 0, 0.0),
              ("9467", "Theo", 3, 10.0),
              ("5512", "Grace", 1, 1.0),
              ("2365", "Oscar", 2, 6.0),
              ("3897", "Sophia", 5, 15.0),
              ("2161", "Jack", 8, 21.0),
              ("8109", "Freya", 1, 3.0)
              ]
    df = spark_session.createDataFrame(values, columns)
    return df


@pytest.fixture
def tripled_df(spark_session):
    """Create a version of starting_df but all numbers are multipled by three."""
    columns = ["IDNumber", "Fibonacci_Numbers", "Triangular_Numbers", "Name"]
    values = [("1517", 0, 0.0, "Willow"),
              ("5512", 3, 3.0, "Grace"),
              ("8109", 3, 9.0, "Freya"),
              ("2365", 6, 18.0, "Oscar"),
              ("9467", 9, 30.0, "Theo"),
              ("3897", 15, 45.0, "Sophia"),
              ("2161", 24, 63.0, "Jack")
              ]
    df = spark_session.createDataFrame(values, columns)
    return df


@pytest.fixture
def different_schema_df(spark_session):
    """Create a version of starting_df with a different schema."""
    columns = ["IDNumber", "Fibonacci_Numbers", "Triangular_Numbers", "Name"]
    values = [("1517", "0", 0.0, "Willow"),
              ("5512", "1", 1.0, "Grace"),
              ("8109", "1", 3.0, "Freya"),
              ("2365", "2", 6.0, "Oscar"),
              ("9467", "3", 10.0, "Theo"),
              ("3897", "5", 15.0, "Sophia"),
              ("2161", "8", 21.0, "Jack")
              ]
    df = spark_session.createDataFrame(values, columns)
    return df


class TestPyspark(object):
    """Define the class used to hold the unit tests for chispa.

    These tests demonstrate the most common use cases for chispa but the
    package has many more features. For more detail, see the documentation:
    https://github.com/MrPowers/chispa#readme
    """

    def test_df_equal(self, starting_df, reshaped_df):
        """Test that two dataframes are equal, ignoring column and row order.

        This test uses the ignore_column_order and ignore_row_order options to
        demonstrate the increased flexibility of chispa. Other useful flags
        when comparing dataframes are ignore_nullable, which ignores whether
        a column is nullable, and allow_nan_equality, which lets comparisons
        of NaN == NaN evaluate to True.
        """
        actual_df = reshaped_df
        expected_df = starting_df
        assert_df_equality(actual_df,
                           expected_df,
                           ignore_column_order=True,
                           ignore_row_order=True
                           )

    def test_df_approx_equal(self, starting_df, tripled_df):
        """Test that two dataframes are approximately equal.

        chispa's assert_approx_df_equality function works exactly the same as
        assert_df_equality but lets you specify a precision so you can ignore
        floating point errors or small differences. The precision is only
        applied to floating point numbers, so other types like strings and
        ints are compared normally.
        """
        actual_df = starting_df
        actual_df = actual_df.withColumn("Fibonacci_Numbers", F.col("Fibonacci_Numbers")*3)
        actual_df = actual_df.withColumn("Triangular_Numbers", F.col("Triangular_Numbers")*3.0001)
        expected_df = tripled_df
        assert_approx_df_equality(actual_df, expected_df, precision=0.01)

    @pytest.mark.xfail(reason="for demonstration purposes")
    def test_unequal_schemas(self, starting_df, different_schema_df):
        """Test that comparing dataframes with mismatched schemas fails.

        During an assert, chispa tests the schema before doing anything else,
        ensuring that it errors immediately if the schemas don't match. Note
        that this test fails but doesn't produce a traceback because it has
        been marked as expected to fail (xfail). To see how chispa presents
        schema mismatches, try commenting out the decorator above.
        """
        actual_df = starting_df
        expected_df = different_schema_df
        assert_df_equality(actual_df, expected_df)
