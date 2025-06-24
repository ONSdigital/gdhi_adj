"""Template for how to build and structure unit tests."""

import pytest

# Import the code under test here, e.g.
# from module_under_test import function_under_test


@pytest.fixture
def fixture_function():
    """Any functions that are needed specificially within this test file."""
    return 1


@pytest.fixture
def input_data(spark_session):
    """Create a dataframe used as input for a unit test."""
    cols = ["col1", "col2"]
    data = (("1.0", "2.0"),
            ("2.0", "4.0")
            )
    df = spark_session.createDataFrame(data, cols)

    return df


class TestSomeFunction(object):
    """Define the class used for holding unit tests.

    Each class defines a group of unit tests along with any functions needed
    to make them work.
    """

    def test_basic_function(self, spark_session, input_data):
        """Test code using a basic assert statement.

        This function contains the logic of the unit test itself. Its name
        must start with "test_" and it must end in an assert statement.
        """
        assert 1 == 1

    def test_another_function(Self, spark_session):
        """Test code using pytest's context manager.

        When testing for exceptions, the code that you expect to raise an
        exception must be within the pytest.raises contact manager.
        """
        with pytest.raises(ZeroDivisionError) as exc_info:
            a = 1/0
            print(a)
        assert exc_info.type is ZeroDivisionError
