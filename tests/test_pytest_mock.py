"""Demonstrate how to use pytest-mock to mock and patch in unit tests."""

import pytest
from tests.conftest import hdfs_skip

from rap_project_example.dummy_module import read_animal_rescue_data


@pytest.fixture
def mock_animal_rescue_df(spark_session):
    """Create a toy version of the animal rescue data."""
    columns = ["IncidentNumber", "IncidentType"]
    values = [("139091", "DOG WITH JAW TRAPPED IN MAGAZINE RACK"),
              ("275091", "ASSIST RSPCA WITH FOX TRAPPED")
              ]
    df = spark_session.createDataFrame(values, columns)
    return df


class TestPytestMock(object):
    """Define the class used to hold the unit tests for pytest-mock."""

    def test_read_data_mocked(self, spark_session, mocker, mock_animal_rescue_df):
        """Test the same function as above but mocking the long part.

        The read_animal_rescue_data() function reads data from the HDFS,
        which can be problematic in unit tests for two reasons. First, large
        datafiles can take a long time to read which makes a full run of unit
        tests long. Second, tests that read from the HDFS will fail on Jenkins
        since it can't access the HDFS, so they have to be skipped and this
        lets bugs slip through the CI/CD system designed to stop them. This
        test uses pytest-mock to patch the call to SparkSession.read.csv()
        used by read_animal_rescue_data() so that it return the fixture
        defined above, which is a toy version of the same data, letting it
        pass the test. Note that calling SparkSession.read actually returns
        pyspark.sql.DataFrameReader, so the object that needs to be patched
        is pyspark.sql.DataFramereader.csv, not SparkSession.read.csv
        """
        mocker.patch("pyspark.sql.DataFrameReader.csv", return_value=mock_animal_rescue_df)
        actual_df = read_animal_rescue_data(spark_session)
        actual_first_incidentno = actual_df.first()["IncidentNumber"]
        expected_first_incidentno = "139091"
        assert expected_first_incidentno == actual_first_incidentno
