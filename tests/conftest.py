"""Define global settings and configuration for tests."""

import logging
import os

import pytest
from pyspark.sql import SparkSession


def suppress_py4j_logging():
    """Suppresses spark logging."""
    logger = logging.getLogger('py4j')
    logger.setLevel(logging.WARN)


@pytest.fixture(scope="session")
def spark_session():
    """Set up a fixture for a spark session."""
    print('Setting up test spark session')

    os.environ['PYSPARK_PYTHON'] = '/usr/local/bin/python3'

    suppress_py4j_logging()

    return (
        SparkSession
        .builder
        .master("local[2]")
        .appName("foldername_placeholder_test_context")
        .config("spark.sql.shuffle.partitions", 1)
        .config('spark.ui.showConsoleProgress', 'false')
        .getOrCreate()
    )


@pytest.fixture
def root_dir():
    """Define the root directory for the project."""
    if os.environ.get('USER') == 'cdsw':
        root_dir = os.environ.get('HOME') + "/"
    else:
        root_dir = '.'

    return root_dir


# Custom marker for tests that must be skipped because they use the HDFS,
# which Jenkins cannot access.
hdfs_skip = pytest.mark.skipif(
  os.environ.get('USER') != 'cdsw',
  reason="HDFS cannot be accessed from Jenkins"
)
