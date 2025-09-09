"""Title for pipeline.py module"""

import time

from gdhi_adj.adjustment.run_adjustment import run_adjustment
from gdhi_adj.preprocess.run_preprocess import run_preprocessing
from gdhi_adj.utils.helpers import load_toml_config
from gdhi_adj.utils.logger import GDHI_adj_logger

# Initialize logger
GDHI_adj_LOGGER = GDHI_adj_logger(__name__)
logger = GDHI_adj_LOGGER.logger


def run_pipeline(config_path):
    """Run the GDHI adjustment pipeline.
    Args:
        config_path (str): Path to the configuration file.
    """
    logger.info("Pipeline started")
    start_time = time.time()

    # Load config
    config = load_toml_config(config_path)

    try:
        if config["user_settings"]["preprocessing"]:
            run_preprocessing(config)

        if config["user_settings"]["adjustment"]:
            run_adjustment(config)

    except Exception as e:
        logger.error(
            f"An error occurred during the pipeline execution: {e}",
            exc_info=True,
        )

    logger.info(
        f"Running time: {((time.time() - start_time) / 60):.2f} minutes."
    )
