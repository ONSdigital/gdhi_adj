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


class GDHI_adj_logger:
    """Custom logging class for use throughout the GDHI_adj pipeline.

    Parameters
    ----------
    name : str
        The name of the file the logger is being created from.
    """

    def __init__(self, name):
        """Initialise the logger class."""
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)

        self.logger = logging.getLogger(name)

        # self.LOG_FILE = "logfile.txt"
        self.FORMAT = logging.Formatter(
            "%(asctime)s (%(levelname)s) %(message)s (%(name)s)",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        # Set root logging level to ensure handlers receive appropriate logs.
        self.logger.root.setLevel(logging.INFO)
        # Set the handlers initialised below.
        # self._set_file_handler()
        self._set_stream_handler()

    def _set_file_handler(self):
        """Set the file handler for the logger to append to text file."""
        file_handler = logging.FileHandler(self.LOG_FILE, mode="a")
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(self.FORMAT)

        self.logger.addHandler(file_handler)

    def _set_stream_handler(self):
        """Set the stream handler for the logger to write to screen."""
        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(logging.INFO)
        stream_handler.setFormatter(self.FORMAT)

        self.logger.addHandler(stream_handler)
