import os
from datetime import datetime
import uuid


class RunLog:
    """Creates a runlog instance for the pipeline."""

    def __init__(
        self,
        config,
        version,
        file_exists_func,
        mkdir_func,
        read_csv_func,
        write_csv_func,
    ):
        # config based attrs
        self.config = config
        self.environment = config["global"]["platform"]
        self.logs_folder = config[f"{self.environment}_paths"]["logs_foldername"]
        self.log_filenames = config["log_filenames"]
        # user information
        self.user = self._generate_username()
        # attrs containing callables
        self.file_exists_func = file_exists_func
        self.mkdir_func = mkdir_func
        self.read_csv_func = read_csv_func
        self.write_func = write_csv_func
        self.write_csv_func = write_csv_func
        # pipeline information

        # Ensure the run_logs folder exists
        self.run_logs_folder = os.path.join(self.logs_folder, 'run_logs')
        if not self.file_exists_func(self.run_logs_folder):
            self.mkdir_func(self.run_logs_folder)

    def generate_and_save_run_id(self):
        """Generates a run ID and saves it to a text file in the run_logs folder."""
        run_id = f"{datetime.now().strftime('%Y%m%d%H%M%S')}_{uuid.uuid4()}"
        run_id_file = os.path.join(self.run_logs_folder, 'run_ids.txt')
        with open(run_id_file, 'a') as file:
            file.write(run_id + '\n')
        return run_id
