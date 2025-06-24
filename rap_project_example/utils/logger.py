import logging


class CustomFormatter(logging.Formatter):

    """Define logging formatter with colors for different log levels."""

    grey = "\x1b[38;20m"
    yellow = "\x1b[33;20m"
    red = "\x1b[31;20m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"
    format = "%(asctime)s - %(levelname)s - %(message)s"

    FORMATS = {
        logging.DEBUG: grey + format + reset,
        logging.INFO: grey + format + reset,
        logging.WARNING: yellow + format + reset,
        logging.ERROR: red + format + reset,
        logging.CRITICAL: bold_red + format + reset,
    }

    def format(self, record):
        """Set color formatting for logger."""
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt, datefmt="%Y-%m-%d - %H:%M:%S")
        return formatter.format(record)


def logger_creator():
    """Set up logging with a custom formatter for console output."""
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)  # Set logging level to INFO

    # Create a console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    # Apply the custom formatter to the console handler
    console_handler.setFormatter(CustomFormatter())

    # Add the handler to the logger
    logger.addHandler(console_handler)

    return logger
