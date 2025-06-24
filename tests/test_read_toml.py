"""Demonstrate how to read config files with tomli."""

import pytest
from datetime import date

import tomli


@pytest.fixture
def config_settings():
    """Read a TOML file to get a dictionary containing configuration settings.

    The main way to read a TOML config file is tomli.load(), which returns a
    normal Python dictionary that can be accessed or iterated over using
    whatever methods you prefer. The file has to be opened in a binary format so
    tomli can parse it in UTF-8 correctly
    """
    config_filepath = "/home/cdsw/config/config.toml"
    with open(config_filepath, "rb") as f:
        return tomli.load(f)


class TestReadTOML(object):
    """Define the class used to hold the unit test for config reading."""

    def test_read_parameter(self, config_settings):
        """Test reading parameters from a config file.

        Config files written in TOML are normally split into sections, so that
        related parameters can be grouped together and accessed together. When
        a TOML file is read in, the result is an ordinary Python dictionary,
        so you can access any attribute by just chaining the keys together.

        NB: Strings written in ISO format are automatically converted to Date
        or Datetime objects when TOML parses them, so we have to make the
        expected start date a Date object.
        """
        expected_start_date = date(2021, 1, 1)
        actual_start_date = config_settings["run_parameters"]["start_date"]
        assert expected_start_date == actual_start_date

    def test_read_dots_parameter(self, config_settings):
        """Test reading parameters with dots from a config file.

        You can also define parameters with dots, e.g. "object.name" and
        "object.location", behaving like an object with attributes. These
        are accessed the same way, chaining the keys together.
        """
        expected_input_name = "My Cool Input Table"
        actual_input_name = config_settings["run_parameters"]["input_table"]["name"]
        assert expected_input_name == actual_input_name

    def test_read_title(self, config_settings):
        """Test reading the config file's title.

        Most parameters will be inside sections but attributes placed outside
        them are also accessible. Since TOML is designed to be human-readable,
        this is a good place to put metadata users might want to see when
        editing, such as a title, description of what the config file does,
        authors and contact info, etc.
        """
        expected_title = "User configuration file"
        actual_title = config_settings["title"]
        assert expected_title == actual_title
