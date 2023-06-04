import traceback

from py4j.clientserver import logger


class DataLoader:
    def __init__(self, spark):
        self.spark = spark
        self.corrupted_files = []

    """
    Read data from the specified file or folder path with the given file format and return a DataFrame.
    Supported file formats: 'csv', 'json', 'parquet', 'avro', 'orc', 'text'.

    Parameters:
    - file_format (str): The format of the file(s) to be read.
    - file_path (str): The path of the file or folder to be read.
    Returns:
    - DataFrame: The DataFrame containing the data from the file(s).
    """

    def read_data(self, file_format, file_path, schema):
        try:
            options = self.get_format_options(file_format)

            dataframe = self.spark.read.format(file_format) \
                .schema(schema) \
                .options(**options) \
                .load(file_path)

            return dataframe

        except Exception as e:
            logger.error(f"Exception occurred while reading data: {str(e)}")
            traceback.print_exc()
            self.corrupted_files.append(file_path)
            return None

    """
    Get the format-specific options for reading a file based on the file format.

    Parameters:
    - file_format (str): The format of the file(s) to be read.
    Returns:
    - dict: A dictionary of format-specific options.
    """

    def get_format_options(self, file_format):
        try:
            options = {}

            if file_format == 'csv':
                options["header"] = "true"
            elif file_format == 'json':
                options["inferSchema"] = "true"
            elif file_format == 'text':
                options["encoding"] = "UTF-8"
            elif file_format == 'parquet':
                options["mergeSchema"] = "true"
            elif file_format == 'orc':
                options["compression"] = "snappy"
            # Add more format-specific options as needed.
            else:
                return options

            return options
        except Exception as e:
            logger.error(f"Exception occurred while getting format-specific options: {str(e)}")
            traceback.print_exc()
            return {}

    def get_corrupted_files(self):
        return self.corrupted_files
