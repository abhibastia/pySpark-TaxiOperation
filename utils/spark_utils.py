import json
import logging

from pyspark.sql import SparkSession


class SparkUtils:
    def __init__(self):
        self.spark = None
        self.logger = logging.getLogger(__name__)

    """
    Create a SparkSession with the given app name.
    """

    def create_spark_session(self, app_name):
        self.spark = SparkSession.builder.master("local[*]").appName(app_name).getOrCreate()

    """
    Read properties from a JSON file and return them as a dictionary.
    """

    def read_properties(self, file_path):
        try:
            with open(file_path, "r") as f:
                properties = json.load(f)
            return properties
        except IOError as e:
            logging.error("Exception while reading config: " + str(e))

    """
    Configure logging level for the application.
    """

    def configure_logging(self, log_level):
        log_level = log_level.upper()  # Convert to uppercase for consistency
        logging.getLogger().setLevel(log_level)
        logging.basicConfig(level=log_level)

    """
    Initialize the Spark session and logging based on the properties in the config file.
    """

    def initialize(self, config_file_path):
        properties = self.read_properties(config_file_path)
        app_name = properties['Application']['app_name']
        log_level = properties['Application']['log_level']
        self.create_spark_session(app_name)
        self.configure_logging(log_level)

    """
    Get the SparkSession object.
    """

    def get_spark_session(self):
        return self.spark

    """
    Get the properties from the config file as a dictionary.
    """

    def get_properties(self, file_path):
        return self.read_properties(file_path)
