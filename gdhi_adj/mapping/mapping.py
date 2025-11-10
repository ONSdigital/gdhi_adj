"""Mapping functions for local authority units mapped to LADs."""

import os

import pandas as pd

root_dir = "d:/repos/gdhi_adj/"

os.chdir(root_dir)

from gdhi_adj.utils.helpers import load_toml_config
from gdhi_adj.utils.logger import GDHI_adj_logger

# Initialize logger
GDHI_adj_LOGGER = GDHI_adj_logger(__name__)
logger = GDHI_adj_LOGGER.logger


def lau_lad_main(config_path="config/config.toml", df=pd.DataFrame()):
    logger.info("Started mapping LAUs to LADs")
    config = load_toml_config(config_path)

    # Load data file, if it is not provided as a DataFrame
    if df.empty:
        data_file_path = config["adjustment_local_settings"]["output_filename"]
        print(data_file_path)
        # df = pd.read_csv(data_file_path)
    result_df = df.copy
    print(result_df)


if __name__ == "__main__":
    lau_lad_main()
